# Suisse Bid Match

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](./LICENSE)
[![Stack](https://img.shields.io/badge/stack-Next.js%20%7C%20FastAPI%20%7C%20PostgreSQL%20%7C%20MySQL-blue)](#architecture)

An end-to-end tender matching platform that turns unstructured lighting tender packs into ranked, explainable supplier product candidates.

**Why this matters:** procurement teams lose time in manual cross-checking. Suisse Bid Match creates a repeatable, auditable, and faster path from tender requirements to shortlist-ready product options.

## Who This Is For

- Procurement engineering teams building bid automation workflows
- Solution architects evaluating AI-assisted matching systems
- Platform teams needing a governed LLM + SQL decision flow

## Core Business Outcomes

- **Faster evaluation cycles:** automate extraction, filtering, and ranking across multi-step tender logic
- **Higher consistency:** enforce hard/soft rule contracts instead of ad-hoc spreadsheet decisions
- **Audit-friendly decisions:** preserve job timelines, step outputs, events, and rule version history

## Product Differentiators

- **Structured 7-step core pipeline** with explicit contracts between extraction, rule merge, SQL build/execute, and ranking
- **Hard/soft constraint strategy** to balance strict filtering and candidate recall
- **Rule Copilot with human approval** (generate preview first, publish only after manual review)
- **Rule snapshot binding per job** so running jobs are stable even after later rule changes
- **Realtime telemetry** via SSE event stream with polling fallback behavior on disconnect

### What’s Possible Next

Suisse Bid Match is already production-minded, and is well positioned to evolve toward:

- role-based access and multi-tenant governance
- advanced analytics and SLA monitoring dashboards
- deeper model orchestration and domain-specific evaluation harnesses
- enterprise integration connectors (ERP/procurement suites)

## Architecture

```mermaid
flowchart LR
  U[User] --> FE[Next.js Frontend\nTask Console / Rules / Stats]
  FE -->|REST + SSE| BE[FastAPI Backend]

  BE --> PG[(PostgreSQL\nJobs / Steps / Events / Rule Versions / App Settings)]
  BE --> CORE[Core Pipeline Runtime\n7-step flow]

  CORE --> MYSQL[(MySQL Supplier Product DB\nvia vw_bid_products + vw_bid_specs)]
  INIT1[mysql-init\nimport pim.sql once] --> MYSQL
  INIT2[mysql-views-init\ncreate/update views] --> MYSQL

  CORE --> KB[src/prepare/upload_corpus_kb\nVector Store Bootstrap]
```

### Runtime Split

- **MySQL** stores supplier product catalog data used by core SQL matching steps.
- **PostgreSQL** stores application metadata for the web service (`jobs`, `job_steps`, `job_events`, `rule_versions`, `app_settings`).

## Execution Model

### Job lifecycle

`created -> uploading -> ready -> running -> succeeded | failed`

### Core step flow (conceptual)

1. KB bootstrap / vector store check
2. Requirement extraction from tender files
3. External field rule determination
4. Requirement + hardness merge
5. SQL generation from hard constraints
6. SQL execution on supplier DB views
7. Candidate ranking by soft constraints

### Realtime monitoring

- `GET /api/v1/jobs/{job_id}/events` provides SSE event streaming
- Uses incremental event IDs and supports reconnect with `Last-Event-ID`
- Heartbeat is emitted when idle; frontend can fall back to polling

## Quickstart (Docker-first)

### Prerequisites

- Docker + Docker Compose v2
- OpenAI API key

### 1) Configure environment

Edit `.env` at repo root and set at least:

- `OPENAI_API_KEY`
- optional: `OPENAI_MODEL` (`gpt-5-mini` or `gpt-5.4`)

> Security: never commit real API keys or production credentials.

### 2) Start the platform

```bash
docker compose up --build
```

### 3) Service endpoints

- Frontend: `http://localhost:3000`
- Backend API: `http://localhost:8000/api/v1`
- Health check: `http://localhost:8000/health`

### Startup sequence

1. `mysql` starts
2. `mysql-init` imports `src/prepare/pim.sql` if target tables are missing
3. `mysql-views-init` creates/refreshes `vw_bid_products` and `vw_bid_specs`
4. `backend` starts after DB dependencies are ready
5. `frontend` starts

## First-Run Validation Checklist

- Health check:

```bash
curl -s http://localhost:8000/health
```

- Model settings available:

```bash
curl -s http://localhost:8000/api/v1/settings/model
```

- UI reachable:
  - open `http://localhost:3000`
  - create a job
  - upload one PDF/DOCX/XLSX file or ZIP
  - start job and verify step/event updates in job detail page

## Configuration Reference

### OpenAI

| Variable | Purpose | Default |
|---|---|---|
| `OPENAI_API_KEY` | API key for step2/step7 + rules copilot | empty |
| `OPENAI_MODEL` | Default model snapshot at startup | `gpt-5-mini` |
| `OPENAI_BASE_URL` | OpenAI-compatible endpoint | `https://api.openai.com/v1` |

### MySQL (supplier product source)

| Variable | Purpose | Default |
|---|---|---|
| `MYSQL_ROOT_PASSWORD` | MySQL root password (compose) | `root` |
| `PIM_MYSQL_HOST` | MySQL host for backend/core | `mysql` in compose |
| `PIM_MYSQL_PORT` | MySQL port | `3306` |
| `PIM_MYSQL_USER` | MySQL user | `root` |
| `PIM_MYSQL_PASSWORD` | MySQL password | `root` |
| `PIM_MYSQL_DB` | Supplier DB name | `pim_raw` |
| `PIM_SCHEMA_TABLES` | Schema source tables/views | `vw_bid_products,vw_bid_specs` |
| `PIM_CHECK_TABLE` | Init check table for import script | `articles` |
| `FORCE_REBUILD` | Force drop/reimport on mysql-init | `0` |

### App runtime / frontend wiring

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string (backend) | `postgresql+psycopg://suisse:suisse@postgres:5432/suisse_bid_match` |
| `CORE_SKIP_KB_BOOTSTRAP` | Skip step1 KB bootstrap for faster local runs | `false` |
| `NEXT_PUBLIC_API_BASE` | Frontend API base URL | `http://localhost:8000/api/v1` |

## API Capability Map

### Jobs

- `POST /api/v1/jobs`
- `GET /api/v1/jobs`
- `POST /api/v1/jobs/{job_id}/file`
- `POST /api/v1/jobs/{job_id}/archive`
- `POST /api/v1/jobs/{job_id}/start`
- `GET /api/v1/jobs/{job_id}`
- `GET /api/v1/jobs/{job_id}/result`

### Events (SSE)

- `GET /api/v1/jobs/{job_id}/events`
- Event types include job lifecycle updates, step updates, and LLM progress signals

### Rules

- `GET /api/v1/rules/current`
- `GET /api/v1/rules/versions`
- `POST /api/v1/rules/draft`
- `POST /api/v1/rules/generate` (compat path)
- `POST /api/v1/rules/generate/stream` (copilot preview stream)
- `POST /api/v1/rules/{version_id}/publish`

### Model settings

- `GET /api/v1/settings/model`
- `PUT /api/v1/settings/model`

### Statistics

- `GET /api/v1/stats/dashboard`

## Operational Notes

### MySQL import idempotency + force rebuild

Default behavior: import runs only when initialization checks indicate data is missing.

Force a full rebuild:

```bash
docker compose run --rm -e FORCE_REBUILD=1 mysql-init
```

### `.next` permission issue in local frontend runs

If you encounter `EACCES` errors around `.next` artifacts (often after container/user mismatch):

```bash
rm -rf src/web/frontend/.next
# if ownership is wrong, fix ownership first
# sudo chown -R "$USER":"$USER" src/web/frontend/.next
```

### Common troubleshooting

- **OpenAI key missing (422 on start/generate):** set `OPENAI_API_KEY` and restart backend
- **MySQL not ready / schema mismatch:** check `mysql-init` and `mysql-views-init` container logs
- **Upload rejected:** validate file type/size constraints (PDF/DOCX/XLSX or ZIP containing supported files)

## Local Development

### Backend

```bash
cd src/web/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r ../../requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend

```bash
cd src/web/frontend
npm install
npm run dev
```

## Testing

```bash
python3 -m pytest tests src/web/backend/tests
```

## Contributing Direction

Contributions are welcome, especially around reliability, explainability, and operator UX. Prefer small, test-backed changes that preserve step contracts and keep API behavior stable.

## License

MIT License. See [LICENSE](./LICENSE).
