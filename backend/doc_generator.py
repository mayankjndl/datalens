"""
doc_generator.py - Export data dictionaries in JSON and Markdown formats.

Design philosophy (post mentor feedback):
- Lead with what matters: issues and fixes, not raw schema
- Engineers already know their column names and types
- Every issue must come with a concrete suggested fix
- AI business context is more valuable than metadata repetition
"""
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "..", "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

SEVERITY_ICON = {
    "critical": "🔴",
    "high":     "🟠",
    "medium":   "🟡",
    "low":      "🟢",
}
SEVERITY_LABEL = {
    "critical": "CRITICAL",
    "high":     "HIGH",
    "medium":   "MEDIUM",
    "low":      "LOW",
}
GRADE_ICON = {
    "A+": "🟢", "A": "🟢", "A-": "🟢",
    "B+": "🔵", "B": "🔵", "B-": "🔵",
    "C+": "🟡", "C": "🟡", "C-": "🟡",
    "D":  "🟠", "F": "🔴",
}


# ─────────────────────────────────────────────────────────────────────────────
# JSON EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def generate_json_export(
    schema_data: Dict, quality_data: Dict, filename: Optional[str] = None
) -> str:
    """
    Export a focused, actionable JSON data dictionary.
    Omits raw schema noise; keeps AI context, quality issues with fixes,
    relationships, and column-level statistics.
    """
    overview   = quality_data.get("overview", {})
    all_tables = schema_data.get("tables", [])

    # ── Collect all issues across the whole database ──────────────────────
    all_issues = []
    for tbl in all_tables:
        tbl_quality = quality_data.get("tables", {}).get(tbl["table_name"], {})
        for issue in tbl_quality.get("issues", []):
            all_issues.append({
                "table":    tbl["table_name"],
                "severity": issue["severity"],
                "column":   issue.get("column"),
                "message":  issue["message"],
                "fix":      issue.get("fix", ""),
            })

    # Sort: critical → high → medium → low
    sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_issues.sort(key=lambda x: sev_order.get(x["severity"], 99))

    export = {
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "generator":    "DataLens — Intelligent Data Dictionary Agent",
            "database":     schema_data.get("database_name", "unknown"),
            "db_type":      schema_data.get("db_type", "unknown"),
            "total_tables": len(all_tables),
        },
        "database_health": {
            "quality_score":  overview.get("database_score"),
            "grade":          overview.get("database_grade"),
            "critical_issues": overview.get("critical_issues", 0),
            "high_issues":    overview.get("high_issues", 0),
            "best_table":     overview.get("best_table"),
            "worst_table":    overview.get("worst_table"),
            "score_distribution": overview.get("score_distribution", {}),
        },
        # All issues in one place, sorted by severity — the most actionable section
        "all_issues": all_issues,
        "tables": [],
    }

    for tbl in all_tables:
        tbl_name    = tbl["table_name"]
        tbl_quality = quality_data.get("tables", {}).get(tbl_name, {})
        ai          = tbl.get("ai_summary", {})

        table_entry = {
            "table_name":  tbl_name,
            "row_count":   tbl.get("row_count", 0),

            # AI context — the valuable part
            "ai_context": {
                "business_summary":  ai.get("business_summary", ""),
                "business_usage":    ai.get("business_usage", []),
                "domain":            ai.get("domain", ""),
                "data_sensitivity":  ai.get("data_sensitivity", ""),
                "tags":              ai.get("tags", []),
                "common_join_patterns": ai.get("common_join_patterns", []),
            },

            # Quality summary
            "quality": {
                "score":      tbl_quality.get("overall_score"),
                "grade":      tbl_quality.get("grade"),
                "highlights": tbl_quality.get("highlights", []),
            },

            # Issues with fixes — this is what an engineer acts on
            "issues": [
                {
                    "severity": issue["severity"],
                    "column":   issue.get("column"),
                    "message":  issue["message"],
                    "fix":      issue.get("fix", ""),
                }
                for issue in tbl_quality.get("issues", [])
            ],

            # Relationships are genuinely useful context
            "relationships": tbl.get("foreign_keys", []),

            # Column descriptions + key stats only (no raw type/nullable noise)
            "columns": [],
        }

        for col in tbl.get("columns", []):
            col_name    = col["column_name"]
            col_quality = tbl_quality.get("column_metrics", {}).get(col_name, {})
            table_entry["columns"].append({
                "name":             col_name,
                "is_primary_key":   col.get("is_primary_key", False),
                "description":      col.get("ai_description", ""),
                "completeness_pct": col_quality.get("completeness_pct"),
                "uniqueness_pct":   col_quality.get("uniqueness_pct"),
                "null_count":       col_quality.get("null_count"),
                "distinct_count":   col_quality.get("distinct_count"),
                "range_summary":    col_quality.get("range_summary"),
                "top_values":       col_quality.get("top_values", []),
            })

        export["tables"].append(table_entry)

    filename = filename or (
        f"datalens_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    )
    filepath = os.path.join(EXPORTS_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2, default=str, ensure_ascii=False)

    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# MARKDOWN EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def generate_markdown_export(
    schema_data: Dict, quality_data: Dict, filename: Optional[str] = None
) -> str:
    """
    Export a human-readable, issues-first Markdown data dictionary.
    Structure:
      1. Header & database health snapshot
      2. ⚠️  All Issues — sorted by severity, with concrete SQL fix for each
      3. Per-table section: AI summary → issues (with fix) → column AI descriptions
         (no raw schema dump — engineers already know their types)
    """
    db_name  = schema_data.get("database_name", "Database")
    db_type  = schema_data.get("db_type", "").upper()
    overview = quality_data.get("overview", {})
    tables   = schema_data.get("tables", [])
    now_str  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    grade       = overview.get("database_grade", "N/A")
    grade_icon  = GRADE_ICON.get(grade, "⚪")
    score       = overview.get("database_score", "N/A")
    n_critical  = overview.get("critical_issues", 0)
    n_high      = overview.get("high_issues", 0)

    lines = [
        f"# DataLens Data Dictionary — {db_name}",
        "",
        f"> **Generated:** {now_str}  ",
        f"> **Database:** {db_name} ({db_type})  ",
        f"> **Overall Quality:** {grade_icon} {score}/100 — Grade **{grade}**  ",
        f"> **Tables:** {len(tables)}  ",
        f"> **Issues:** {n_critical} critical · {n_high} high",
        "",
        "---",
        "",
    ]

    # ── Section 1: Full issues list ─────────────────────────────────────────
    all_issues = []
    for tbl in tables:
        tbl_quality = quality_data.get("tables", {}).get(tbl["table_name"], {})
        for issue in tbl_quality.get("issues", []):
            all_issues.append({**issue, "table": tbl["table_name"]})

    sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_issues.sort(key=lambda x: sev_order.get(x["severity"], 99))

    if all_issues:
        lines += [
            "## ⚠️ Issues & Recommended Fixes",
            "",
            "_All issues across the database, sorted by severity. "
            "Each entry includes a ready-to-run SQL fix._",
            "",
        ]
        for issue in all_issues:
            sev      = issue["severity"]
            icon     = SEVERITY_ICON.get(sev, "⚪")
            label    = SEVERITY_LABEL.get(sev, sev.upper())
            table    = issue.get("table", "")
            col      = issue.get("column")
            msg      = issue["message"]
            fix      = issue.get("fix", "")
            location = f"`{table}`" + (f" › `{col}`" if col else "")

            lines += [
                f"### {icon} {label} — {location}",
                "",
                f"**Problem:** {msg}",
                "",
            ]
            if fix:
                lines += [
                    "**Suggested Fix:**",
                    "",
                    f"```sql",
                    fix,
                    "```",
                    "",
                ]
            else:
                lines.append("")
    else:
        lines += [
            "## ✅ No Issues Found",
            "",
            "All tables passed quality checks.",
            "",
        ]

    lines += ["---", ""]

    # ── Section 2: Table of Contents ────────────────────────────────────────
    lines += ["## Table of Contents", ""]
    for tbl in tables:
        tbl_name    = tbl["table_name"]
        tbl_quality = quality_data.get("tables", {}).get(tbl_name, {})
        grade       = tbl_quality.get("grade", "?")
        g_icon      = GRADE_ICON.get(grade, "⚪")
        n_issues    = len(tbl_quality.get("issues", []))
        issue_str   = f" · {n_issues} issue{'s' if n_issues != 1 else ''}" if n_issues else ""
        anchor      = tbl_name.lower().replace("_", "-")
        lines.append(
            f"- [{tbl_name}](#{anchor}) — {g_icon} {grade}{issue_str}"
        )
    lines += ["", "---", ""]

    # ── Section 3: Per-table detail ─────────────────────────────────────────
    for tbl in tables:
        tbl_name    = tbl["table_name"]
        tbl_quality = quality_data.get("tables", {}).get(tbl_name, {})
        ai          = tbl.get("ai_summary", {})
        grade       = tbl_quality.get("grade", "?")
        score       = tbl_quality.get("overall_score", "N/A")
        g_icon      = GRADE_ICON.get(grade, "⚪")
        row_count   = tbl.get("row_count", 0)
        issues      = tbl_quality.get("issues", [])
        highlights  = tbl_quality.get("highlights", [])
        fks         = tbl.get("foreign_keys", [])
        join_pats   = ai.get("common_join_patterns", [])
        tags        = ai.get("tags", [])

        lines += [
            f"## {tbl_name}",
            "",
            f"{g_icon} **Grade {grade}** ({score}/100) · "
            f"{row_count:,} rows · "
            f"{len(tbl.get('columns', []))} columns",
            "",
        ]

        # AI business summary
        if ai.get("business_summary"):
            lines += [ai["business_summary"], ""]

        # Business usage bullets
        if ai.get("business_usage"):
            lines += ["**Common uses:**", ""]
            for use in ai["business_usage"]:
                lines.append(f"- {use}")
            lines.append("")

        # Relationships
        if fks:
            lines += ["**Relationships:**", ""]
            for fk in fks:
                lines.append(
                    f"- `{fk['column']}` → `{fk['referenced_table']}.{fk['referenced_column']}`"
                )
            lines.append("")

        # Tags
        if tags:
            lines += [
                "**Tags:** " + " ".join(f"`{t}`" for t in tags),
                "",
            ]

        # Highlights (green flags)
        if highlights:
            lines += ["**Quality highlights:**", ""]
            for h in highlights:
                lines.append(f"- ✅ {h}")
            lines.append("")

        # Issues with fixes — the most important part of each table section
        if issues:
            lines += [
                f"### ⚠️ Issues ({len(issues)})",
                "",
            ]
            for issue in issues:
                sev   = issue["severity"]
                icon  = SEVERITY_ICON.get(sev, "⚪")
                label = SEVERITY_LABEL.get(sev, sev.upper())
                col   = issue.get("column")
                msg   = issue["message"]
                fix   = issue.get("fix", "")
                col_str = f" › `{col}`" if col else ""

                lines += [
                    f"#### {icon} {label}{col_str}",
                    "",
                    f"{msg}",
                    "",
                ]
                if fix:
                    lines += [
                        "<details>",
                        "<summary>Suggested Fix</summary>",
                        "",
                        "```sql",
                        fix,
                        "```",
                        "",
                        "</details>",
                        "",
                    ]

        # Column AI descriptions — no raw type/nullable table, just meaning
        cols = tbl.get("columns", [])
        if cols:
            lines += ["### Columns", ""]
            for col in cols:
                col_name = col["column_name"]
                desc     = col.get("ai_description", "")
                pk_flag  = " 🔑" if col.get("is_primary_key") else ""
                col_quality = (
                    tbl_quality.get("column_metrics", {}).get(col_name, {})
                )
                completeness = col_quality.get("completeness_pct")
                comp_str = (
                    f" · {completeness:.0f}% complete" if completeness is not None else ""
                )
                lines.append(
                    f"- **`{col_name}`**{pk_flag}{comp_str}"
                    + (f" — {desc}" if desc else "")
                )
            lines.append("")

        # Common join patterns as SQL code block
        if join_pats:
            lines += ["### Common Join Patterns", "", "```sql"]
            for pat in join_pats[:2]:
                lines.append(pat)
            lines += ["```", ""]

        lines += ["---", ""]

    # Footer
    lines += [
        "## About This Report",
        "",
        "_Generated by **DataLens — Intelligent Data Dictionary Agent**._  ",
        "_AI-generated descriptions should be validated with domain experts "
        "before use in production documentation._",
    ]

    content  = "\n".join(lines)
    filename = filename or (
        f"datalens_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md"
    )
    filepath = os.path.join(EXPORTS_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# LIST ARTIFACTS
# ─────────────────────────────────────────────────────────────────────────────

def list_artifacts() -> List[Dict]:
    """List all previously generated documentation artifacts."""
    artifacts = []
    for fname in sorted(os.listdir(EXPORTS_DIR), reverse=True):
        if fname.endswith((".json", ".md")):
            fpath = os.path.join(EXPORTS_DIR, fname)
            stat  = os.stat(fpath)
            artifacts.append({
                "filename":   fname,
                "format":     fname.split(".")[-1].upper(),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "path":       fpath,
            })
    return artifacts