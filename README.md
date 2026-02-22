# DataLens — Intelligent Data Dictionary Agent

> *Connect any database. Understand it instantly.*

DataLens is a full-stack AI-powered data dictionary agent that automatically extracts, enriches, and documents any database — turning raw schema into living, intelligent documentation in under 2 minutes.

Built for the **GDG Cloud New Delhi — AI Accelerator Hub Hackathon** by **Quantum Quad**.

---

## What it does

Most teams document their databases manually — or not at all. DataLens eliminates that entirely.

Connect a database and DataLens will:
- Extract the complete schema across all tables, columns, foreign keys, and indexes
- Run statistical quality analysis grading every table from **A+ to F**
- Generate AI-written business summaries, column descriptions, domain tags, and usage recommendations for every table
- Visualise all foreign key relationships as an interactive force-directed graph
- Let you query your data in plain English via an AI chat interface
- Export a complete, professional data dictionary as Markdown or JSON — ready for GitHub, Confluence, or any wiki

---

## Demo

**Database:** Olist Brazilian E-Commerce (9 tables, 569,774 rows)
**Quality Score:** 97/100 — Grade A+
**Enrichment time:** 90-120 seconds on free-tier Groq API

<img width="1920" height="1080" alt="Screenshot (437)" src="https://github.com/user-attachments/assets/e7e37158-6374-41c6-9d75-a321ec20b04a" />


---

## Features

### AI Enrichment
- Business-friendly table summaries written by Llama 3.3 70B via Groq
- Column-level AI descriptions with domain context
- Semantic tags specific to each table's actual content
- Common join patterns and usage recommendations
- Data sensitivity classification (public / internal / confidential / restricted)

### Quality Analysis Engine
- Completeness scoring per column with NULL counts
- Uniqueness validation with composite PK awareness
- Freshness detection on date/timestamp columns
- Context-aware issue detection — optional user-provided fields (comments, notes) are classified as expected behaviour, not issues
- Every issue includes a concrete, copy-pasteable SQL fix
- Grading: A+ / A / B+ / B / C / D / F

### Interactive Lineage Graph
- D3.js force-directed graph of all table relationships
- Drag, zoom, and click to navigate
- Per-table mini relationship graph inside Table Detail view

### Natural Language Chat
- Ask questions about your database in plain English
- AI generates SQL, executes it, and returns real results
- Full conversation history with suggested starter questions

### Sample Data Preview
- One click to see 20 live rows from any table
- No SQL required

### ⌘K Semantic Search
- Search across all tables, columns, descriptions, and tags instantly

### Export
- **Markdown** — issues-first format with SQL fixes, AI summaries, and quality grades. Ready for GitHub READMEs and Confluence
- **JSON** — structured export with full quality metrics, AI context, and relationships for downstream tooling

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python · FastAPI · Uvicorn |
| Frontend | React · D3.js · Vanilla HTML/CSS |
| AI | Groq API · Llama 3.3 70B Versatile |
| Databases | SQLite · PostgreSQL · Snowflake · SQL Server |
| Quality Engine | Custom ANSI-SQL statistics engine (BaseConnector) |

---

## Getting Started

### Prerequisites
- Python 3.10+
- A [Groq API key](https://console.groq.com) (free tier works)

### Installation

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

### Connect a database

On the connect screen, select **SQLite** and enter the path to any `.db` file, or enter credentials for PostgreSQL or Snowflake.

A demo SQLite database is not included in the repo due to file size limits. You can use any SQLite database — or create one using the Olist dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

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

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/connect` | Connect to a database |
| GET | `/tables/{session_id}` | List all tables with quality scores |
| GET | `/table/{session_id}/{table}` | Full table detail with AI context |
| POST | `/enrich` | AI-enrich a single table |
| POST | `/analyze-quality/{session_id}` | Run quality analysis |
| POST | `/execute-sql` | Execute a SQL query |
| POST | `/chat` | Natural language chat with schema context |
| GET | `/search/{session_id}` | Semantic search across schema |
| GET | `/export/{session_id}` | Export data dictionary |

---

## Team

**Quantum Quad** — GDG Cloud New Delhi

- Mayank Jindal
- Sunidhi Thakur

---

## Hackathon

Built during the **GDG Cloud New Delhi AI Accelerator Hub Hackathon**
Round 3 — Development Phase · February 21–22, 2026

---

## License

MIT
