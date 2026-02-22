"""
main.py - DataLens v2 — Intelligent Data Dictionary Agent
New in v2: SQL execution, lineage API, schema diff, semantic search
"""
import json, os, re, hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import shutil, tempfile
from pydantic import BaseModel
import uvicorn

from db_connectors import ConnectionConfig, DBType, get_connector
from quality_analyzer import analyze_table_quality, compute_database_quality_overview
from doc_generator import generate_json_export, generate_markdown_export, list_artifacts
from ai_generator import generate_table_description, generate_column_description, chat_with_schema

app = FastAPI(title="DataLens v2 — Intelligent Data Dictionary Agent", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

_sessions: Dict[str, Any] = {}
_chat_history: Dict[str, List[Dict]] = {}
_schema_snapshots: Dict[str, Any] = {}


class ConnectRequest(BaseModel):
    db_type: str
    file_path: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    schema: Optional[str] = None
    session_name: Optional[str] = "default"

class ChatRequest(BaseModel):
    session_id: str
    message: str

class EnrichRequest(BaseModel):
    session_id: str
    table_name: Optional[str] = None

class ExportRequest(BaseModel):
    session_id: str
    format: str = "json"

class SQLRequest(BaseModel):
    session_id: str
    sql: str
    limit: int = 200

class SearchRequest(BaseModel):
    session_id: str
    query: str


def _get_session(sid: str) -> Dict:
    if sid not in _sessions:
        raise HTTPException(status_code=404, detail=f"Session '{sid}' not found. Connect first.")
    return _sessions[sid]

def _open_connector(sid: str):
    sess = _get_session(sid)
    c = get_connector(sess["config"])
    c.connect()
    return c, sess

def _schema_fingerprint(tables, all_columns) -> str:
    payload = json.dumps({t["table_name"]: sorted([c["column_name"] for c in all_columns.get(t["table_name"], [])])
                          for t in tables}, sort_keys=True)
    return hashlib.md5(payload.encode()).hexdigest()


@app.get("/")
def root(): return {"message": "DataLens API v2", "status": "running"}

@app.get("/health")
def health(): return {"status": "healthy", "sessions": len(_sessions)}


@app.post("/upload-db")
async def upload_db(file: UploadFile = File(...)):
    """Accept a SQLite .db file upload and save it to a temp location."""
    if not file.filename.endswith('.db'):
        raise HTTPException(status_code=400, detail="Only .db files are supported.")
    upload_dir = "/tmp/datalens_uploads"
    os.makedirs(upload_dir, exist_ok=True)
    dest = os.path.join(upload_dir, file.filename)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"file_path": dest, "filename": file.filename, "size_bytes": os.path.getsize(dest)}


@app.post("/connect")
def connect(req: ConnectRequest):
    try:
        config = ConnectionConfig(
            db_type=DBType(req.db_type),
            file_path=req.file_path, host=req.host, port=req.port,
            database=req.database, username=req.username, password=req.password,
            account=req.account, warehouse=req.warehouse, schema=req.schema
        )
        connector = get_connector(config)
        connector.connect()
        schemas = connector.get_schemas()
        tables = connector.get_tables(req.schema)

        all_columns = {}
        for t in tables:
            try:
                cols = connector.get_columns(t["table_name"])
                t["column_count"] = len(cols)
                all_columns[t["table_name"]] = cols
            except: t["column_count"] = 0

        relationships = []
        for t in tables:
            try:
                for fk in connector.get_foreign_keys(t["table_name"]):
                    relationships.append({"source": t["table_name"], "target": fk["referenced_table"],
                                          "source_col": fk["column"], "target_col": fk["referenced_column"]})
            except: pass

        connector.disconnect()
        sid = req.session_name or "default"
        db_name = req.file_path or req.database or "unknown"
        fingerprint = _schema_fingerprint(tables, all_columns)

        _sessions[sid] = {
            "config": config, "db_name": os.path.basename(db_name), "db_type": req.db_type,
            "schemas": schemas, "tables": tables, "all_columns": all_columns,
            "relationships": relationships, "enriched_tables": {}, "quality_data": {},
            "quality_overview": {}, "connected_at": datetime.utcnow().isoformat(),
            "last_fingerprint": fingerprint
        }
        _schema_snapshots[f"{sid}_initial"] = {
            "fingerprint": fingerprint, "tables": tables,
            "columns": all_columns, "snapshot_at": datetime.utcnow().isoformat()
        }
        _chat_history[sid] = []

        return {
            "session_id": sid, "status": "connected",
            "db_name": os.path.basename(db_name), "db_type": req.db_type,
            "schemas": schemas, "table_count": len(tables), "tables": tables,
            "relationships": relationships,
            "total_columns": sum(len(c) for c in all_columns.values()),
            "total_rows": sum(t.get("row_count", 0) for t in tables)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/tables/{session_id}")
def get_tables(session_id: str):
    sess = _get_session(session_id)
    enriched = sess.get("enriched_tables", {})
    quality = sess.get("quality_data", {})
    result = []
    for t in sess.get("tables", []):
        n = t["table_name"]
        result.append({**t,
            "ai_summary": enriched.get(n, {}).get("ai_summary", {}),
            "enriched": n in enriched,
            "quality_score": quality.get(n, {}).get("overall_score"),
            "quality_grade": quality.get(n, {}).get("grade"),
            "issue_count": len(quality.get(n, {}).get("issues", []))
        })
    return {"session_id": session_id, "tables": result,
            "relationships": sess.get("relationships", []),
            "quality_overview": sess.get("quality_overview", {})}


@app.get("/table/{session_id}/{table_name}")
def get_table_detail(session_id: str, table_name: str, schema: Optional[str] = None):
    connector, sess = _open_connector(session_id)
    try:
        columns = connector.get_columns(table_name, schema)
        fks = connector.get_foreign_keys(table_name, schema)
        indexes = connector.get_indexes(table_name, schema)
        sample = connector.sample_data(table_name, schema, n=10)
        connector.disconnect()
    except Exception as e:
        connector.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

    enriched = sess.get("enriched_tables", {}).get(table_name, {})
    quality = sess.get("quality_data", {}).get(table_name, {})
    col_ai = enriched.get("column_descriptions", {})
    for col in columns:
        col["ai_description"] = col_ai.get(col["column_name"], "")
        col["stats"] = quality.get("column_metrics", {}).get(col["column_name"], {})

    return {"table_name": table_name, "columns": columns, "foreign_keys": fks,
            "indexes": indexes, "sample_data": sample,
            "ai_summary": enriched.get("ai_summary", {}), "quality": quality,
            "enriched": bool(enriched)}


@app.post("/analyze-quality/{session_id}")
def analyze_quality(session_id: str, table_name: Optional[str] = None):
    connector, sess = _open_connector(session_id)
    try:
        targets = [t for t in sess["tables"] if not table_name or t["table_name"] == table_name]
        results = {}
        for tbl in targets:
            n = tbl["table_name"]
            cols = connector.get_columns(n)
            results[n] = analyze_table_quality(connector, n, cols)
        connector.disconnect()
        sess["quality_data"].update(results)
        overview = compute_database_quality_overview(list(results.values()))
        sess["quality_overview"] = overview
        return {"analyzed_tables": len(results), "overview": overview,
                "table_scores": {n: {"score": m.get("overall_score"), "grade": m.get("grade"),
                                     "issues": len(m.get("issues", []))} for n, m in results.items()}}
    except Exception as e:
        connector.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/enrich")
def enrich_with_ai(req: EnrichRequest):
    connector, sess = _open_connector(req.session_id)
    try:
        targets = [t for t in sess["tables"] if not req.table_name or t["table_name"] == req.table_name]
        count = 0
        for tbl in targets:
            tbl_name = tbl["table_name"]
            try:
                cols = connector.get_columns(tbl_name)
                fks = connector.get_foreign_keys(tbl_name)
                sample = connector.sample_data(tbl_name, None, 5)
                ai_sum = generate_table_description(tbl_name, cols, sample, tbl.get("row_count", 0), fks)
                cq = sess.get("quality_data", {}).get(tbl_name, {}).get("column_metrics", {})
                col_descs = {}
                for col in cols[:15]:
                    sv = [str(r.get(col["column_name"], "")) for r in sample if r.get(col["column_name"]) is not None]
                    col_descs[col["column_name"]] = generate_column_description(tbl_name, col["column_name"], col["data_type"], cq.get(col["column_name"], {}), sv)
                sess["enriched_tables"][tbl_name] = {"ai_summary": ai_sum, "column_descriptions": col_descs, "enriched_at": datetime.utcnow().isoformat()}
                count += 1
            except Exception as tbl_err:
                sess["enriched_tables"][tbl_name] = {"error": str(tbl_err), "enriched_at": datetime.utcnow().isoformat()}
        connector.disconnect()
        return {"enriched_tables": count, "session_id": req.session_id}
    except Exception as e:
        connector.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat")
def chat(req: ChatRequest):
    sess = _get_session(req.session_id)
    connector, _ = _open_connector(req.session_id)
    enriched = sess.get("enriched_tables", {})
    try:
        schema_ctx = {"database_name": sess["db_name"], "tables": []}
        for tbl in sess.get("tables", []):
            n = tbl["table_name"]
            cols = connector.get_columns(n)
            schema_ctx["tables"].append({"table_name": n, "row_count": tbl.get("row_count", 0),
                                          "columns": cols, "ai_summary": enriched.get(n, {}).get("ai_summary", {})})
        connector.disconnect()
    except:
        connector.disconnect()
        schema_ctx = {"database_name": sess["db_name"], "tables": sess.get("tables", [])}

    history = _chat_history.get(req.session_id, [])
    response = chat_with_schema(req.message, schema_ctx, history)
    history.append({"role": "user", "content": req.message})
    history.append({"role": "assistant", "content": response.get("answer", "")})
    _chat_history[req.session_id] = history[-20:]
    return {"session_id": req.session_id, "response": response, "timestamp": datetime.utcnow().isoformat()}


@app.post("/execute-sql")
def execute_sql(req: SQLRequest):
    """Live SQL execution — powers the Query Runner."""
    connector, _ = _open_connector(req.session_id)
    try:
        clean = req.sql.strip().lstrip(";").strip()
        if not re.match(r"^\s*(SELECT|WITH)\b", clean, re.IGNORECASE):
            raise HTTPException(status_code=400, detail="Only SELECT statements are permitted.")
        if "LIMIT" not in clean.upper():
            clean = clean.rstrip(";") + f" LIMIT {req.limit}"
        start = datetime.utcnow()
        rows = connector.execute(clean)
        ms = int((datetime.utcnow() - start).microseconds / 1000)
        connector.disconnect()
        columns = list(rows[0].keys()) if rows else []
        return {"success": True, "columns": columns, "rows": rows, "row_count": len(rows), "duration_ms": ms, "sql": clean}
    except HTTPException: connector.disconnect(); raise
    except Exception as e:
        connector.disconnect()
        return {"success": False, "error": str(e), "rows": [], "columns": [], "row_count": 0}


@app.get("/lineage/{session_id}")
def get_lineage(session_id: str):
    """Rich graph data for D3 lineage visualization."""
    sess = _get_session(session_id)
    enriched = sess.get("enriched_tables", {})
    quality = sess.get("quality_data", {})
    nodes = []
    for t in sess.get("tables", []):
        n = t["table_name"]
        ai = enriched.get(n, {}).get("ai_summary", {})
        q = quality.get(n, {})
        nodes.append({
            "id": n, "label": n,
            "row_count": t.get("row_count", 0),
            "column_count": t.get("column_count", 0),
            "domain": ai.get("domain", "unknown"),
            "quality_score": q.get("overall_score"),
            "quality_grade": q.get("grade"),
            "issue_count": len(q.get("issues", [])),
            "enriched": bool(enriched.get(n)),
            "sensitivity": ai.get("data_sensitivity", "internal"),
            "tags": ai.get("tags", []),
            "summary": ai.get("business_summary", "")
        })
    edges = [{"source": r["source"], "target": r["target"],
              "source_col": r["source_col"], "target_col": r["target_col"]}
             for r in sess.get("relationships", [])]
    return {"nodes": nodes, "edges": edges}


@app.post("/search")
def semantic_search(req: SearchRequest):
    """Full-text search across table names, columns, AI descriptions, tags."""
    sess = _get_session(req.session_id)
    q = req.query.lower().strip()
    if not q: return {"results": []}
    enriched = sess.get("enriched_tables", {})
    all_cols = sess.get("all_columns", {})
    results = []
    for t in sess.get("tables", []):
        n = t["table_name"]
        ai = enriched.get(n, {}).get("ai_summary", {})
        col_descs = enriched.get(n, {}).get("column_descriptions", {})
        score, matches = 0, []
        if q in n.lower(): score += 10; matches.append({"type": "table", "text": n})
        summary = (ai.get("business_summary", "") + " " + " ".join(ai.get("tags", []))).lower()
        if q in summary: score += 8; matches.append({"type": "summary", "text": ai.get("business_summary", "")[:100]})
        if q in ai.get("domain", "").lower(): score += 6
        for col in all_cols.get(n, []):
            if q in col["column_name"].lower(): score += 5; matches.append({"type": "column", "text": f"{n}.{col['column_name']} ({col['data_type']})"})
            cdesc = col_descs.get(col["column_name"], "").lower()
            if q in cdesc: score += 3; matches.append({"type": "col_desc", "text": col_descs.get(col["column_name"], "")[:80]})
        if score > 0:
            results.append({"table_name": n, "score": score, "matches": matches[:4],
                            "row_count": t.get("row_count", 0), "enriched": bool(ai)})
    results.sort(key=lambda x: x["score"], reverse=True)
    return {"query": req.query, "results": results[:20]}


@app.post("/schema-diff/{session_id}")
def schema_diff(session_id: str):
    """Detect schema changes since the initial connection snapshot."""
    connector, sess = _open_connector(session_id)
    try:
        tables = connector.get_tables()
        all_cols = {}
        for t in tables:
            try: all_cols[t["table_name"]] = connector.get_columns(t["table_name"])
            except: pass
        connector.disconnect()
    except Exception as e:
        connector.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

    new_fp = _schema_fingerprint(tables, all_cols)
    old_snap = _schema_snapshots.get(f"{session_id}_initial", {})
    old_tables = {t["table_name"] for t in old_snap.get("tables", [])}
    new_tables = {t["table_name"] for t in tables}
    old_cols = old_snap.get("columns", {})
    changes = []
    for t in new_tables - old_tables: changes.append({"type": "table_added", "table": t, "severity": "info"})
    for t in old_tables - new_tables: changes.append({"type": "table_dropped", "table": t, "severity": "critical"})
    for t in old_tables & new_tables:
        oc = {c["column_name"] for c in old_cols.get(t, [])}
        nc = {c["column_name"] for c in all_cols.get(t, [])}
        for col in nc - oc: changes.append({"type": "column_added", "table": t, "column": col, "severity": "info"})
        for col in oc - nc: changes.append({"type": "column_dropped", "table": t, "column": col, "severity": "high"})

    _schema_snapshots[f"{session_id}_initial"] = {"fingerprint": new_fp, "tables": tables, "columns": all_cols, "snapshot_at": datetime.utcnow().isoformat()}
    return {"has_changes": len(changes) > 0, "change_count": len(changes), "changes": changes,
            "new_fingerprint": new_fp, "old_fingerprint": old_snap.get("fingerprint"),
            "checked_at": datetime.utcnow().isoformat()}


@app.post("/export")
def export_docs(req: ExportRequest):
    sess = _get_session(req.session_id)
    schema_data = {"database_name": sess["db_name"], "db_type": sess["db_type"], "tables": []}
    enriched = sess.get("enriched_tables", {})
    quality = sess.get("quality_data", {})
    conn_obj, _ = _open_connector(req.session_id)
    try:
        for tbl in sess["tables"]:
            tbl_name = tbl["table_name"]
            cols = conn_obj.get_columns(tbl_name)
            fks = conn_obj.get_foreign_keys(tbl_name)
            indexes = conn_obj.get_indexes(tbl_name)
            sample = conn_obj.sample_data(tbl_name, None, 3)
            col_ai = enriched.get(tbl_name, {}).get("column_descriptions", {})
            for col in cols: col["ai_description"] = col_ai.get(col["column_name"], "")
            schema_data["tables"].append({**tbl, "columns": cols, "foreign_keys": fks,
                                           "indexes": indexes, "sample_data": sample,
                                           "ai_summary": enriched.get(tbl_name, {}).get("ai_summary", {})})
        conn_obj.disconnect()
    except Exception as e:
        conn_obj.disconnect()
        raise HTTPException(status_code=500, detail=str(e))
    quality_data = {"overview": sess.get("quality_overview", {}), "tables": quality}
    try:
        fp = generate_markdown_export(schema_data, quality_data) if req.format == "markdown" else generate_json_export(schema_data, quality_data)
        return FileResponse(fp, filename=os.path.basename(fp), media_type="application/octet-stream")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/artifacts")
def get_artifacts(): return {"artifacts": list_artifacts()}

@app.delete("/session/{session_id}")
def delete_session(session_id: str):
    for d in [_sessions, _chat_history]:
        d.pop(session_id, None)
    _schema_snapshots.pop(f"{session_id}_initial", None)
    return {"status": "disconnected"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
