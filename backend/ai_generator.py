"""
ai_generator.py - AI-powered metadata enrichment using Groq (free tier)
Generates business-friendly descriptions, usage recommendations, and chat responses.

Free API: https://console.groq.com  (no credit card required)
Set env var: GROQ_API_KEY="gsk_..."

Model mapping (replaces Anthropic):
  claude-opus-4-6           -> llama-3.3-70b-versatile  (complex reasoning)
  claude-haiku-4-5-20251001 -> llama-3.1-8b-instant     (fast bulk tasks)
"""
import json
import os
from typing import Any, Dict, List, Optional
from groq import Groq

_client = None

def _get_client() -> Groq:
    global _client
    if _client is None:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY environment variable not set. "
                "Get a free key at https://console.groq.com"
            )
        _client = Groq(api_key=api_key)
    return _client


def _chat(model: str, system: str, user: str, max_tokens: int) -> str:
    """Unified helper — calls Groq chat completions."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})

    response = _get_client().chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=messages,
    )
    return response.choices[0].message.content.strip()


def _chat_multi(model: str, system: str, messages: List[Dict], max_tokens: int) -> str:
    """Helper for multi-turn conversations."""
    full_messages = []
    if system:
        full_messages.append({"role": "system", "content": system})
    full_messages.extend(messages)

    response = _get_client().chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=full_messages,
    )
    return response.choices[0].message.content.strip()


def _strip_fences(text: str) -> str:
    """
    Extract a JSON object from a model response robustly.

    Groq models sometimes emit explanatory text before or after the JSON
    object (e.g. "Here is the answer:\n{...}"), which causes json.loads to
    fail and the entire raw string ends up rendered as the chat answer.

    Strategy:
    1. Strip markdown code fences (```json ... ```)
    2. If the result still doesn't start with '{', use regex to pull out
       the first complete {...} block — handles any surrounding prose.
    """
    import re
    text = text.strip()

    # Remove ```json ... ``` or ``` ... ``` fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    # If there's still prose before the JSON object, extract the {...} block
    if not text.startswith("{"):
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            text = m.group(0)

    return text.strip()


def generate_table_description(
    table_name: str,
    columns: List[Dict],
    sample_data: List[Dict],
    row_count: int,
    foreign_keys: List[Dict],
    existing_summary: Optional[str] = None
) -> Dict[str, Any]:
    """Generate AI-powered business description and recommendations for a table."""

    col_descriptions = "\n".join([
        f"  - {c['column_name']} ({c['data_type']}, {'nullable' if c.get('is_nullable') else 'not null'}"
        + (", PK" if c.get('is_primary_key') else "") + ")"
        for c in columns
    ])

    fk_descriptions = ""
    if foreign_keys:
        fk_descriptions = "\nForeign key relationships:\n" + "\n".join([
            f"  - {fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}"
            for fk in foreign_keys
        ])

    sample_str = ""
    if sample_data:
        sample_str = f"\nSample data (first {len(sample_data)} rows):\n{json.dumps(sample_data[:3], indent=2, default=str)}"

    prompt = f"""You are a senior data architect creating a data dictionary entry for a database table.

Table: {table_name}
Row count: {row_count:,}
Columns:
{col_descriptions}
{fk_descriptions}
{sample_str}

Produce a JSON object with these exact keys:
{{
  "business_summary": "2-3 sentence plain-English description of what this table stores and its business purpose",
  "domain": "one of: customer_data, transactions, product_catalog, marketing, analytics, reference_data, audit_log, configuration",
  "business_usage": ["list", "of", "3-5 specific ways analysts/business users typically use this table"],
  "key_relationships": ["brief description of important relationships with other tables"],
  "data_sensitivity": "one of: public, internal, confidential, restricted",
  "common_join_patterns": ["example SQL join hints like 'JOIN orders ON customers.customer_id = orders.customer_id'"],
  "potential_issues": ["list any data quality concerns, null columns that seem critical, etc."],
  "tags": ["5-8 searchable tags for this table"]
}}

Return ONLY valid JSON, no markdown fences."""

    text = _chat(
        model="llama-3.3-70b-versatile",
        system="You are a senior data architect. Return only valid JSON, no explanation.",
        user=prompt,
        max_tokens=1024,
    )
    text = _strip_fences(text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "business_summary": text[:500],
            "domain": "unknown",
            "business_usage": [],
            "key_relationships": [],
            "data_sensitivity": "internal",
            "common_join_patterns": [],
            "potential_issues": [],
            "tags": []
        }


def generate_column_description(
    table_name: str,
    column_name: str,
    data_type: str,
    stats: Dict,
    sample_values: List[Any]
) -> str:
    """Generate a concise business-friendly description for a single column."""
    prompt = f"""For column '{column_name}' in table '{table_name}':
- Data type: {data_type}
- Stats: {json.dumps(stats, default=str)}
- Sample values: {sample_values[:5]}

Write ONE sentence (max 20 words) describing what this column contains and its business meaning.
Return only the sentence, no prefixes."""

    return _chat(
        model="llama-3.1-8b-instant",
        system="You are a data documentation assistant. Return only a single sentence.",
        user=prompt,
        max_tokens=100,
    )


def chat_with_schema(
    user_question: str,
    schema_context: Dict,
    conversation_history: List[Dict]
) -> Dict[str, Any]:
    """Answer natural language questions about the database schema."""

    tables_info = []
    for tbl in schema_context.get("tables", []):
        cols = ", ".join([f"{c['column_name']}({c['data_type']})" for c in tbl.get("columns", [])])
        tables_info.append(f"- {tbl['table_name']} [{tbl.get('row_count',0):,} rows]: {cols}")

    schema_str = "\n".join(tables_info)

    ai_descriptions = []
    for tbl in schema_context.get("tables", []):
        if tbl.get("ai_summary"):
            ai_descriptions.append(f"- {tbl['table_name']}: {tbl['ai_summary'].get('business_summary','')}")

    system_prompt = f"""You are an expert data analyst assistant with deep knowledge of this database schema.

DATABASE SCHEMA:
{schema_str}

AI-GENERATED BUSINESS CONTEXT:
{chr(10).join(ai_descriptions)}

You help users understand the data, write SQL queries, analyze relationships, and explore data quality.
When asked to write SQL, use correct syntax for SQLite unless told otherwise.
Always be specific and reference actual table/column names from the schema above.

Respond with a JSON object:
{{
  "answer": "your detailed answer in markdown format",
  "sql_query": "SQL query if relevant, otherwise null",
  "referenced_tables": ["list of table names mentioned"],
  "confidence": "high|medium|low",
  "follow_up_suggestions": ["2-3 related questions the user might want to ask"]
}}

Return ONLY valid JSON, no markdown fences."""

    messages = []
    for h in conversation_history[-6:]:  # Last 6 turns for context
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_question})

    text = _chat_multi(
        model="llama-3.3-70b-versatile",
        system=system_prompt,
        messages=messages,
        max_tokens=2048,
    )
    text = _strip_fences(text)

    try:
        return json.loads(text)
    except:
        return {
            "answer": text,
            "sql_query": None,
            "referenced_tables": [],
            "confidence": "medium",
            "follow_up_suggestions": []
        }


def generate_quality_summary(table_name: str, quality_metrics: Dict) -> str:
    """Generate a human-readable quality assessment."""
    prompt = f"""Table '{table_name}' data quality metrics:
{json.dumps(quality_metrics, indent=2, default=str)}

Write a 2-3 sentence business-friendly data quality assessment.
Highlight the most important issues and strengths. Be specific with numbers.
Return only the assessment text."""

    return _chat(
        model="llama-3.1-8b-instant",
        system="You are a data quality analyst. Return only the assessment text.",
        user=prompt,
        max_tokens=200,
    )


def suggest_sql_query(question: str, table_name: str, columns: List[Dict]) -> str:
    """Generate a SQL query suggestion based on user question."""
    col_list = ", ".join([f"{c['column_name']} ({c['data_type']})" for c in columns])
    prompt = f"""Write a SQLite query to answer: "{question}"
Table: {table_name}
Columns: {col_list}
Return ONLY the SQL, no explanation."""

    return _chat(
        model="llama-3.1-8b-instant",
        system="You are a SQL expert. Return only the SQL query.",
        user=prompt,
        max_tokens=300,
    )