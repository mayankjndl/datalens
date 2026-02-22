# DataLens — Intelligent Data Dictionary Agent

> *Connect any database. Understand it instantly.*

DataLens is a full-stack AI-powered data dictionary that automatically extracts, enriches, and documents any database — turning raw schema into living, intelligent documentation in under 2 minutes. No manual writing. No outdated docs. Just connect and go.

**Live Demo → [datalens-delta.vercel.app](https://datalens-delta.vercel.app)**

---

## The Problem

Most engineering teams have no documentation for their databases. Schema knowledge lives in the heads of senior engineers — not in any system. Onboarding takes weeks. Data quality issues go undetected. And manual documentation is always out of date the moment a schema changes.

DataLens eliminates this entirely.

---

## What it does

Connect a database and DataLens will:
- Extract the complete schema — all tables, columns, foreign keys, indexes, and row counts
- Run statistical quality analysis grading every table from **A+ to F**
- Generate AI-written business summaries, column descriptions, domain tags, and usage recommendations for every table
- Visualise all foreign key relationships as an interactive force-directed graph
- Let you query your data in plain English via a natural language chat interface
- Export a complete, professional data dictionary as Markdown or JSON

---

## Live Demo

**Try it at [datalens-delta.vercel.app](https://datalens-delta.vercel.app)** — upload any `.db` file and DataLens documents your entire database automatically.

**Validated on:** Olist Brazilian E-Commerce dataset — 9 tables, 569,774 rows
**Quality Score:** 97/100 — Grade A+
**Enrichment time:** ~90 seconds on Groq free tier

<img width="1920" height="1080" alt="DataLens Screenshot" src="https://github.com/user-attachments/assets/e7e37158-6374-41c6-9d75-a321ec20b04a" />

---

## Features

### AI Enrichment
- Business-friendly table summaries written by Llama 3.3 70B via Groq
- Column-level AI descriptions with domain context
- Semantic tags specific to each table's actual content — not generic labels
- Common join patterns and usage recommendations
- Data sensitivity classification (public / internal / confidential / restricted)

### Quality Analysis Engine
- Completeness scoring per column with NULL counts
- Uniqueness validation with composite primary key awareness
- Freshness detection on date/timestamp columns
- Context-aware issue detection — optional user-provided fields (comments, notes, messages) are classified as expected behaviour, not issues
- Every detected issue includes a concrete, copy-pasteable SQL fix
- Grading: A+ / A / B+ / B / C / D / F

### Interactive Lineage Graph
- D3.js force-directed graph of all table relationships
- Drag, zoom, and click to navigate
- Per-table mini relationship graph scoped to directly connected tables

### Natural Language Chat
- Ask questions about your database in plain English
- AI generates SQL, executes it live, and returns real results
- Full conversation history with suggested starter questions

### Sample Data Preview
- One click to see live rows from any table
- No SQL knowledge required

### ⌘K Semantic Search
- Instant search across table names, column names, AI descriptions, and tags

### Export
- **Markdown** — issues-first format with SQL fixes, AI summaries, and quality grades
- **JSON** — structured export with full quality metrics and relationships for downstream tooling

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python · FastAPI · Uvicorn |
| Frontend | React · D3.js · Vanilla HTML/CSS |
| AI | Groq API · Llama 3.3 70B Versatile |
| Databases | SQLite · PostgreSQL · Snowflake · SQL Server |
| Quality Engine | Custom ANSI-SQL statistics engine |
| Deployment | Vercel (frontend) · Render (backend) |

---

## Running Locally

### Prerequisites
- Python 3.10+
- A [Groq API key](https://console.groq.com) (free tier works)

### Setup

```bash
git clone https://github.com/mayankjndl/datalens.git
cd datalens
pip install -r backend/requirements.txt
```

### Run

```bash
# Set your Groq API key
export GROQ_API_KEY="gsk_your_key_here"        # Mac/Linux
$env:GROQ_API_KEY="gsk_your_key_here"          # Windows PowerShell

# Start the backend
cd backend
python main.py
```

Then open `frontend/index.html` in your browser.

> For the hosted version, no setup needed — just visit [datalens-delta.vercel.app](https://datalens-delta.vercel.app) and upload a `.db` file.

---

## Project Structure

```
datalens/
├── backend/
│   ├── main.py               # FastAPI app — all API endpoints
│   ├── db_connectors.py      # SQLite, PostgreSQL, Snowflake connectors
│   ├── quality_analyzer.py   # Quality scoring engine
│   ├── ai_generator.py       # Groq AI enrichment
│   └── doc_generator.py      # Markdown + JSON export
├── frontend/
│   └── index.html            # Single-file React app
└── README.md
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| POST | `/upload-db` | Upload a SQLite `.db` file |
| POST | `/connect` | Connect to a database |
| GET | `/tables/{session_id}` | List all tables with quality scores |
| GET | `/table/{session_id}/{table}` | Full table detail with AI context |
| POST | `/enrich` | AI-enrich tables |
| POST | `/analyze-quality/{session_id}` | Run quality analysis |
| POST | `/execute-sql` | Execute a SQL query |
| POST | `/chat` | Natural language chat |
| POST | `/search` | Semantic search across schema |
| POST | `/export` | Export data dictionary |

---

## Author

**Mayank Jindal** — B.S. Applied AI & Data Science, IIT Jodhpur

[GitHub](https://github.com/mayankjndl) · [LinkedIn](https://linkedin.com/in/mayankjndl)

---

## License

MIT
