# suisse-bid-match

End-to-end tender matching system for lighting bids.

## What is included

- Existing `core` pipeline (`src/core`) with 7-step matching flow.
- New FastAPI backend (`src/web/backend`) that wraps core as asynchronous jobs.
- New Next.js frontend (`src/web/frontend`) for upload, progress tracking, and rules management.
- PostgreSQL for web-service metadata (`jobs`, `events`, `rule_versions`, etc.).
- Docker Compose for `frontend + backend + postgres + mysql + mysql-init + mysql-views-init`.

## Important runtime split

- **MySQL** is still the supplier product database used by core SQL steps.
- **PostgreSQL** stores web app data (jobs, files, steps, events, rules).
- `src/prepare/upload_corpus_kb` is the KB source folder used by core Step1 (configured in `src/pipeline.yaml` with project-relative path).

## Start with Docker Compose

```bash
docker compose up --build
```

Startup sequence:

1. `mysql` starts.
2. `mysql-init` waits for MySQL and imports `src/prepare/pim.sql` only when `pim_raw.articles` does not exist.
3. `mysql-views-init` creates/refreshes `vw_bid_products` and `vw_bid_specs`.
4. `backend` starts after view init completes successfully.
5. `frontend` starts.

Services:

- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000`
- Backend health: `http://localhost:8000/health`

## Required environment for MySQL/OpenAI

Compose reads these variables (defaults are provided for local usage):

- `MYSQL_ROOT_PASSWORD`
- `PIM_MYSQL_HOST`
- `PIM_MYSQL_PORT`
- `PIM_MYSQL_USER`
- `PIM_MYSQL_PASSWORD`
- `PIM_MYSQL_DB`
- `PIM_SCHEMA_TABLES` (defaults to `vw_bid_products,vw_bid_specs`)
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_BASE_URL`

Compose internal defaults are wired to the MySQL container:

- `PIM_MYSQL_HOST=mysql`
- `PIM_MYSQL_PORT=3306`
- `PIM_MYSQL_USER=root`
- `PIM_MYSQL_CONTAINER=mysql` (compat field for core env override path)
- `PIM_SCHEMA_TABLES=vw_bid_products,vw_bid_specs`

MySQL is not exposed to host ports; it is only reachable inside the Compose network.
Core SQL steps read through `vw_bid_products` and `vw_bid_specs` views.

## Optional: force rebuild supplier MySQL

By default, SQL import runs once (first initialization).  
If you need a hard reset and re-import:

```bash
docker compose run --rm -e FORCE_REBUILD=1 mysql-init
```

## Run backend locally (without Compose)

```bash
cd src/web/backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r ../../requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Run frontend locally

```bash
cd src/web/frontend
npm install
npm run dev
```

## API summary

- `POST /api/v1/jobs`
- `GET /api/v1/jobs`
- `POST /api/v1/jobs/{job_id}/file`
- `POST /api/v1/jobs/{job_id}/archive`
- `POST /api/v1/jobs/{job_id}/start`
- `GET /api/v1/jobs/{job_id}`
- `GET /api/v1/jobs/{job_id}/events` (SSE)
- `GET /api/v1/jobs/{job_id}/result`
- `GET /api/v1/rules/current`
- `GET /api/v1/rules/versions`
- `POST /api/v1/rules/draft`
- `POST /api/v1/rules/generate`
- `POST /api/v1/rules/{version_id}/publish`

SSE event types include:

- `job_created`, `job_started`, `step_update`, `job_completed`, `job_failed`
- `llm_progress` (live LLM status + reasoning summary deltas for Step2/Step7)

## Tests

Core + backend tests:

```bash
python -m pytest tests src/web/backend/tests
```
