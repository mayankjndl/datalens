"""
quality_analyzer.py - Comprehensive data quality analysis engine
Computes completeness, uniqueness, freshness, key health, and statistical metrics.
Every issue includes a concrete suggested fix.
"""
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


def analyze_table_quality(connector, table_name: str, columns: List[Dict], schema: Optional[str] = None) -> Dict:
    """Run full quality analysis on a table and return scored metrics."""

    metrics = {
        "table_name":   table_name,
        "analyzed_at":  datetime.utcnow().isoformat(),
        "column_metrics": {},
        "table_level":  {},
        "overall_score": 0,
        "grade":        "F",
        "issues":       [],
        "highlights":   []
    }

    total_rows = 0
    scores     = []

    # Count PK columns upfront — needed for composite PK logic
    pk_cols_total = sum(1 for c in columns if c.get("is_primary_key"))

    for col in columns:
        col_name = col["column_name"]
        dtype    = col.get("data_type", "")

        try:
            col_stats = connector.get_column_stats(table_name, col_name, dtype)
        except Exception as e:
            col_stats = {"error": str(e)}

        col_metrics = {
            "column_name":    col_name,
            "data_type":      dtype,
            "is_primary_key": col.get("is_primary_key", False),
            "is_nullable":    col.get("is_nullable", True),
            **col_stats
        }

        # ── Completeness ─────────────────────────────────────────────────────
        completeness = col_stats.get("completeness_pct", 100)
        null_count   = col_stats.get("null_count", 0)
        total        = col_stats.get("total_rows", 0)
        col_metrics["completeness_score"] = completeness

        if completeness < 50 and not col.get("is_nullable", True):
            metrics["issues"].append({
                "severity": "high",
                "column":   col_name,
                "message": (
                    f"{completeness:.0f}% complete — {null_count:,} NULL values "
                    f"in a NOT NULL column"
                ),
                "fix": (
                    f"Inspect the missing rows:\n"
                    f"  SELECT * FROM \"{table_name}\" WHERE \"{col_name}\" IS NULL LIMIT 50;\n"
                    f"Then backfill with a sensible default:\n"
                    f"  UPDATE \"{table_name}\" SET \"{col_name}\" = '<default>'"
                    f" WHERE \"{col_name}\" IS NULL;\n"
                    f"Or relax the NOT NULL constraint in your schema if NULLs are valid."
                )
            })
        elif completeness < 80:
            metrics["issues"].append({
                "severity": "medium",
                "column":   col_name,
                "message": (
                    f"Low completeness: {completeness:.0f}% "
                    f"({null_count:,} NULL rows out of {total:,})"
                ),
                "fix": (
                    f"Find and review the missing rows:\n"
                    f"  SELECT * FROM \"{table_name}\" WHERE \"{col_name}\" IS NULL LIMIT 50;\n"
                    f"If NULLs are expected, document this column as optional. "
                    f"Otherwise trace back to the ingestion pipeline to understand why values are missing."
                )
            })
        elif completeness == 100 and not col.get("is_nullable", True):
            metrics["highlights"].append(f"{col_name}: 100% complete ✓")

        scores.append(completeness)

        # ── Uniqueness / PK ──────────────────────────────────────────────────
        uniqueness = col_stats.get("uniqueness_pct", None)
        if uniqueness is not None:
            col_metrics["uniqueness_score"] = uniqueness

            # Only flag individual-column PK duplicates for single-column PKs.
            # Composite PKs are validated at table level below.
            if col.get("is_primary_key") and pk_cols_total == 1 and uniqueness < 100:
                dup_count = (col_stats.get("total_rows", 0)
                             - col_stats.get("distinct_count", 0))
                metrics["issues"].append({
                    "severity": "critical",
                    "column":   col_name,
                    "message": (
                        f"PRIMARY KEY has duplicate values — "
                        f"uniqueness is {uniqueness:.1f}% "
                        f"(~{dup_count:,} duplicate rows)"
                    ),
                    "fix": (
                        f"Find the duplicates:\n"
                        f"  SELECT \"{col_name}\", COUNT(*) as cnt\n"
                        f"  FROM \"{table_name}\"\n"
                        f"  GROUP BY \"{col_name}\" HAVING cnt > 1;\n"
                        f"Keep only one row per key:\n"
                        f"  DELETE FROM \"{table_name}\" WHERE rowid NOT IN (\n"
                        f"    SELECT MIN(rowid) FROM \"{table_name}\" GROUP BY \"{col_name}\"\n"
                        f"  );\n"
                        f"Then fix the upstream pipeline to prevent duplicate inserts."
                    )
                })
            elif uniqueness == 100 and col_stats.get("distinct_count", 0) > 1:
                col_metrics["uniqueness_label"] = "fully_unique"
            elif uniqueness < 5:
                col_metrics["uniqueness_label"] = "low_cardinality"

        # ── Negative values in price columns ─────────────────────────────────
        mn  = col_stats.get("min")
        mx  = col_stats.get("max")
        avg = col_stats.get("avg")
        if mn is not None and mx is not None and avg is not None:
            try:
                mn_f, mx_f, avg_f = float(mn), float(mx), float(avg)
                if mx_f > 0 and mn_f < 0 and "price" in col_name.lower():
                    metrics["issues"].append({
                        "severity": "high",
                        "column":   col_name,
                        "message": (
                            f"Negative values in price column "
                            f"(min = {mn_f:,.2f}, avg = {avg_f:,.2f})"
                        ),
                        "fix": (
                            f"Inspect the negative rows:\n"
                            f"  SELECT * FROM \"{table_name}\" "
                            f"WHERE \"{col_name}\" < 0 LIMIT 50;\n"
                            f"If these represent refunds, move them to a dedicated refunds table. "
                            f"If they are data errors, correct the source and add a constraint:\n"
                            f"  -- ALTER TABLE \"{table_name}\" "
                            f"ADD CHECK (\"{col_name}\" >= 0);"
                        )
                    })
                col_metrics["range_summary"] = (
                    f"{mn_f:,.2f} \u2013 {mx_f:,.2f} (avg: {avg_f:,.2f})"
                )
            except Exception:
                pass

        if col_stats.get("total_rows"):
            total_rows = col_stats["total_rows"]

        metrics["column_metrics"][col_name] = col_metrics

    # ── Table-level metrics ───────────────────────────────────────────────────
    metrics["table_level"]["row_count"] = total_rows

    pk_cols = [c for c in columns if c.get("is_primary_key")]
    metrics["table_level"]["has_primary_key"]     = len(pk_cols) > 0
    metrics["table_level"]["primary_key_columns"] = [c["column_name"] for c in pk_cols]

    if not pk_cols:
        metrics["issues"].append({
            "severity": "medium",
            "column":   None,
            "message":  "Table has no primary key — rows cannot be uniquely identified",
            "fix": (
                f"If a natural unique column exists (e.g. an ID field), declare it as PK "
                f"in your schema. Otherwise add a surrogate key:\n"
                f"  ALTER TABLE \"{table_name}\" "
                f"ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT;"
            )
        })
        scores.append(70)
    else:
        # Composite PK — validate combination uniqueness
        if len(pk_cols) > 1:
            pk_col_names = [c["column_name"] for c in pk_cols]
            quoted       = ", ".join(f'"{c}"' for c in pk_col_names)
            try:
                result = connector.execute(
                    f'SELECT COUNT(*) as c FROM ('
                    f'SELECT {quoted} FROM "{table_name}" '
                    f'GROUP BY {quoted} HAVING COUNT(*) > 1)'
                )
                dup_combos = result[0]["c"] if result else 0
                if dup_combos > 0:
                    metrics["issues"].append({
                        "severity": "critical",
                        "column":   ", ".join(pk_col_names),
                        "message": (
                            f"Composite PRIMARY KEY ({', '.join(pk_col_names)}) "
                            f"has {dup_combos:,} duplicate combinations"
                        ),
                        "fix": (
                            f"Find the duplicate combinations:\n"
                            f"  SELECT {quoted}, COUNT(*) as cnt\n"
                            f"  FROM \"{table_name}\"\n"
                            f"  GROUP BY {quoted} HAVING cnt > 1;\n"
                            f"Deduplicate by keeping the most recent or most complete row, "
                            f"then fix the upstream pipeline."
                        )
                    })
                else:
                    metrics["highlights"].append(
                        f"Composite PK ({', '.join(pk_col_names)}): "
                        f"{total_rows:,} rows, all combinations unique \u2713"
                    )
            except Exception:
                pass
        scores.append(100)

    # ── Freshness ─────────────────────────────────────────────────────────────
    date_cols = [
        c for c in columns
        if any(t in c.get("data_type", "").lower()
               for t in ("date", "time", "timestamp"))
    ]
    if date_cols:
        freshness_col   = date_cols[0]["column_name"]
        freshness_score = _compute_freshness(connector, table_name, freshness_col)
        metrics["table_level"]["freshness_score"]  = freshness_score
        metrics["table_level"]["freshness_column"] = freshness_col
        scores.append(freshness_score)
        if freshness_score < 50:
            metrics["issues"].append({
                "severity": "medium",
                "column":   freshness_col,
                "message": (
                    f"Data may be stale — freshness score {freshness_score}/100 "
                    f"based on most recent value in '{freshness_col}'"
                ),
                "fix": (
                    f"Check the most recent record:\n"
                    f"  SELECT MAX(\"{freshness_col}\") as latest "
                    f"FROM \"{table_name}\";\n"
                    f"If the ETL pipeline has stopped, investigate the scheduled job. "
                    f"If this is a historical archive, document it as intentionally static."
                )
            })
    else:
        metrics["table_level"]["freshness_score"] = None

    # ── Overall score ──────────────────────────────────────────────────────────
    if scores:
        overall = sum(scores) / len(scores)
        metrics["overall_score"] = round(overall, 1)
        metrics["grade"]         = _score_to_grade(overall)

    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    metrics["issues"].sort(key=lambda x: severity_order.get(x["severity"], 99))

    return metrics


def _compute_freshness(connector, table_name: str, date_column: str) -> float:
    """Score freshness 0-100 based on how recent the latest record is."""
    try:
        rows = connector.execute(
            f'SELECT MAX("{date_column}") as latest FROM "{table_name}"'
        )
        if not rows or rows[0].get("latest") is None:
            return 50

        latest_str = str(rows[0]["latest"])
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                latest_dt = datetime.strptime(latest_str[:19], fmt)
                age_days  = (datetime.utcnow() - latest_dt).days
                if age_days <= 1:    return 100
                elif age_days <= 7:  return 90
                elif age_days <= 30: return 80
                elif age_days <= 90: return 65
                elif age_days <= 180: return 50
                elif age_days <= 365: return 35
                else:                return 20
            except Exception:
                continue
        return 75
    except Exception:
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

    scores    = [t.get("overall_score", 0) for t in table_metrics]
    avg_score = sum(scores) / len(scores)

    all_issues = []
    for t in table_metrics:
        for issue in t.get("issues", []):
            all_issues.append({**issue, "table": t["table_name"]})

    critical = [i for i in all_issues if i["severity"] == "critical"]
    high     = [i for i in all_issues if i["severity"] == "high"]

    return {
        "database_score":   round(avg_score, 1),
        "database_grade":   _score_to_grade(avg_score),
        "total_tables":     len(table_metrics),
        "tables_analyzed":  len(table_metrics),
        "critical_issues":  len(critical),
        "high_issues":      len(high),
        "top_issues":       (critical + high)[:5],
        "best_table":       max(table_metrics, key=lambda x: x.get("overall_score", 0))["table_name"],
        "worst_table":      min(table_metrics, key=lambda x: x.get("overall_score", 0))["table_name"],
        "score_distribution": {
            "excellent": len([s for s in scores if s >= 90]),
            "good":      len([s for s in scores if 75 <= s < 90]),
            "fair":      len([s for s in scores if 60 <= s < 75]),
            "poor":      len([s for s in scores if s < 60])
        }
    }