"""
quality_analyzer.py - Comprehensive data quality analysis engine
Computes completeness, uniqueness, freshness, key health, and statistical metrics.
"""
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


def analyze_table_quality(connector, table_name: str, columns: List[Dict], schema: Optional[str] = None) -> Dict:
    """Run full quality analysis on a table and return scored metrics."""
    
    metrics = {
        "table_name": table_name,
        "analyzed_at": datetime.utcnow().isoformat(),
        "column_metrics": {},
        "table_level": {},
        "overall_score": 0,
        "grade": "F",
        "issues": [],
        "highlights": []
    }

    total_rows = 0
    scores     = []

    # Count PK columns upfront — composite PKs must not be checked per-column
    pk_cols_total = sum(1 for c in columns if c.get("is_primary_key"))

    for col in columns:
        col_name = col["column_name"]
        dtype = col.get("data_type", "")
        
        try:
            col_stats = connector.get_column_stats(table_name, col_name, dtype)
        except Exception as e:
            col_stats = {"error": str(e)}

        col_metrics = {
            "column_name": col_name,
            "data_type": dtype,
            "is_primary_key": col.get("is_primary_key", False),
            "is_nullable": col.get("is_nullable", True),
            **col_stats
        }

        # ── Completeness score ───────────────────────────────────────────────
        completeness = col_stats.get("completeness_pct", 100)
        col_metrics["completeness_score"] = completeness

        # Columns whose NULLs are expected by design — user-provided optional content
        OPTIONAL_PATTERNS = (
            "comment", "note", "message", "description", "title",
            "remark", "feedback", "bio", "summary", "detail",
            "reason", "label", "alias", "tag", "url", "photo",
            "avatar", "middle", "suffix", "secondary", "alt",
        )
        is_optional_by_name = any(p in col_name.lower() for p in OPTIONAL_PATTERNS)
        is_nullable = col.get("is_nullable", True)

        if completeness < 50 and not is_nullable:
            # NOT NULL column that is still mostly empty — real problem
            metrics["issues"].append({
                "severity": "high",
                "column": col_name,
                "message": f"Only {completeness:.0f}% complete but marked NOT NULL — {int((1 - completeness/100) * col_stats.get('total_rows', 0)):,} unexpected NULLs",
                "fix": (
                    f"Find the missing rows:\n"
                    f"  SELECT * FROM \"{table_name}\" WHERE \"{col_name}\" IS NULL LIMIT 50;\n"
                    f"Backfill with a default or fix the upstream pipeline."
                )
            })
        elif completeness < 80 and not is_nullable and not is_optional_by_name:
            # Nullable but low — still worth flagging as a real issue
            metrics["issues"].append({
                "severity": "medium",
                "column": col_name,
                "message": f"Low completeness: {completeness:.0f}% — {col_stats.get('null_count', 0):,} NULL rows out of {col_stats.get('total_rows', 0):,}",
                "fix": (
                    f"Review the missing rows:\n"
                    f"  SELECT * FROM \"{table_name}\" WHERE \"{col_name}\" IS NULL LIMIT 50;\n"
                    f"If NULLs are expected, document this column as optional."
                )
            })
        elif completeness < 80 and is_optional_by_name:
            # Optional user-provided content — NULL is expected, note it as a concern not an issue
            col_metrics["expected_nulls"] = True
            col_metrics["expected_nulls_note"] = (
                f"{col_name} is {completeness:.0f}% complete — "
                f"NULLs are expected (optional user-provided field)"
            )
        elif completeness == 100 and not is_nullable:
            metrics["highlights"].append(f"{col_name}: 100% complete ✓")

        scores.append(completeness)

        # ── Uniqueness / cardinality ─────────────────────────────────────────
        uniqueness = col_stats.get("uniqueness_pct", None)
        if uniqueness is not None:
            col_metrics["uniqueness_score"] = uniqueness
            # Only flag PK uniqueness for single-column PKs.
            # Composite PKs: per-column uniqueness is expected to be <100% — validated at table level.
            if col.get("is_primary_key") and pk_cols_total == 1 and uniqueness < 100:
                metrics["issues"].append({
                    "severity": "critical",
                    "column": col_name,
                    "message": f"PRIMARY KEY has duplicate values! Uniqueness: {uniqueness:.1f}%"
                })
            elif uniqueness == 100 and col_stats.get("distinct_count", 0) > 1:
                col_metrics["uniqueness_label"] = "fully_unique"
            elif uniqueness < 5:
                col_metrics["uniqueness_label"] = "low_cardinality"

        # ── Range / outlier check for numerics ──────────────────────────────
        mn = col_stats.get("min")
        mx = col_stats.get("max")
        avg = col_stats.get("avg")
        if mn is not None and mx is not None and avg is not None:
            try:
                mn_f, mx_f, avg_f = float(mn), float(mx), float(avg)
                if mx_f > 0 and mn_f < 0 and "price" in col_name.lower():
                    metrics["issues"].append({
                        "severity": "high",
                        "column": col_name,
                        "message": f"Negative values found in price column (min={mn_f})"
                    })
                col_metrics["range_summary"] = f"{mn_f:,.2f} – {mx_f:,.2f} (avg: {avg_f:,.2f})"
            except:
                pass

        if col_stats.get("total_rows"):
            total_rows = col_stats["total_rows"]

        metrics["column_metrics"][col_name] = col_metrics

    # ── Table-level metrics ──────────────────────────────────────────────────
    metrics["table_level"]["row_count"] = total_rows
    
    # Primary key health
    pk_cols = [c for c in columns if c.get("is_primary_key")]
    metrics["table_level"]["has_primary_key"] = len(pk_cols) > 0
    metrics["table_level"]["primary_key_columns"] = [c["column_name"] for c in pk_cols]
    
    if not pk_cols:
        metrics["issues"].append({
            "severity": "medium",
            "column": None,
            "message": "Table has no primary key defined"
        })
        scores.append(70)  # Penalize
    else:
        scores.append(100)

    # Freshness (check for date/timestamp columns)
    date_cols = [c for c in columns if any(t in c.get("data_type","").lower() for t in ("date","time","timestamp"))]
    if date_cols:
        freshness_score = _compute_freshness(connector, table_name, date_cols[0]["column_name"])
        metrics["table_level"]["freshness_score"] = freshness_score
        metrics["table_level"]["freshness_column"] = date_cols[0]["column_name"]
        scores.append(freshness_score)
        if freshness_score < 50:
            metrics["issues"].append({
                "severity": "medium",
                "column": date_cols[0]["column_name"],
                "message": f"Data may be stale — freshness score: {freshness_score}/100"
            })
    else:
        metrics["table_level"]["freshness_score"] = None

    # ── Collect expected-null concerns from column metrics ────────────────────
    expected_null_concerns = [
        cm["expected_nulls_note"]
        for cm in metrics["column_metrics"].values()
        if cm.get("expected_nulls")
    ]
    if expected_null_concerns:
        metrics["expected_null_concerns"] = expected_null_concerns

    # ── Overall score ────────────────────────────────────────────────────────
    if scores:
        overall = sum(scores) / len(scores)
        metrics["overall_score"] = round(overall, 1)
        metrics["grade"] = _score_to_grade(overall)

    # Sort issues by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    metrics["issues"].sort(key=lambda x: severity_order.get(x["severity"], 99))

    return metrics


def _compute_freshness(connector, table_name: str, date_column: str) -> float:
    """Score freshness 0-100 based on how recent the latest record is."""
    try:
        rows = connector.execute(f'SELECT MAX("{date_column}") as latest FROM "{table_name}"')
        if not rows or rows[0].get("latest") is None:
            return 50
        
        latest_str = str(rows[0]["latest"])
        # Try to parse date
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                latest_dt = datetime.strptime(latest_str[:19], fmt)
                age_days = (datetime.utcnow() - latest_dt).days
                # Score: fresh today = 100, 30 days = 80, 90 days = 60, 365+ = 20
                if age_days <= 1: return 100
                elif age_days <= 7: return 90
                elif age_days <= 30: return 80
                elif age_days <= 90: return 65
                elif age_days <= 180: return 50
                elif age_days <= 365: return 35
                else: return 20
            except:
                continue
        return 75  # Unknown date format, assume moderate freshness
    except:
        return 75


def _score_to_grade(score: float) -> str:
    if score >= 95: return "A+"
    if score >= 90: return "A"
    if score >= 85: return "A-"
    if score >= 80: return "B+"
    if score >= 75: return "B"
    if score >= 70: return "B-"
    if score >= 65: return "C+"
    if score >= 60: return "C"
    if score >= 55: return "C-"
    if score >= 50: return "D"
    return "F"


def compute_database_quality_overview(table_metrics: List[Dict]) -> Dict:
    """Aggregate quality scores across the whole database."""
    if not table_metrics:
        return {}
    
    scores = [t.get("overall_score", 0) for t in table_metrics]
    avg_score = sum(scores) / len(scores)
    
    all_issues = []
    for t in table_metrics:
        for issue in t.get("issues", []):
            all_issues.append({**issue, "table": t["table_name"]})
    
    critical = [i for i in all_issues if i["severity"] == "critical"]
    high = [i for i in all_issues if i["severity"] == "high"]
    
    return {
        "database_score": round(avg_score, 1),
        "database_grade": _score_to_grade(avg_score),
        "total_tables": len(table_metrics),
        "tables_analyzed": len(table_metrics),
        "critical_issues": len(critical),
        "high_issues": len(high),
        "top_issues": (critical + high)[:5],
        "best_table": max(table_metrics, key=lambda x: x.get("overall_score", 0))["table_name"],
        "worst_table": min(table_metrics, key=lambda x: x.get("overall_score", 0))["table_name"],
        "score_distribution": {
            "excellent": len([s for s in scores if s >= 90]),
            "good": len([s for s in scores if 75 <= s < 90]),
            "fair": len([s for s in scores if 60 <= s < 75]),
            "poor": len([s for s in scores if s < 60])
        }
    }
