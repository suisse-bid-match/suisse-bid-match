# Suisse Bid Match (SIMAP-only, Stable-first)

Bidder-side copilot MVP for:
- SIMAP tender ingest/search
- Retrieval-grounded Q&A with citations
- Eligibility/checklist extraction
- Notice change impact monitoring

## Stack (current MVP)
- Backend: FastAPI
- DB: PostgreSQL (only)
- Migrations: Alembic
- Vector DB: Qdrant
- UI: Next.js (TypeScript + Tailwind CSS)

## Scope notes
- Implemented: SIMAP connector + ingest + reindex + chat + checklist + changes
- Placeholder only: TED / Apify endpoints (`501`)
- Not in this stage: Celery / Redis / MinIO

## Quickstart (Docker Compose)

### 1) Prepare env
```bash
cp .env.example .env
```
`DB_URL` in `.env` is for host/local runs. Docker Compose API uses
`DB_URL_DOCKER` (defaulting to `postgresql+psycopg://suisse:suisse@postgres:5432/suisse_bid_match`).

### 2) Start all services
```bash
docker compose up -d --build
```

### 3) Verify
```bash
docker compose ps
curl http://localhost:8000/health
curl http://localhost:6333/collections
```

### 4) Seed and reindex
```bash
docker compose exec api python3 scripts/seed_demo.py
docker compose exec api python3 scripts/reindex.py
```

### 5) Open
- API docs: `http://localhost:8000/docs`
- UI: `http://localhost:3000`

## Local run (without dockerized API/UI)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# infra only
docker compose up -d postgres qdrant

# apply schema migrations
alembic upgrade head

# start app
uvicorn apps.api.main:app --reload

# in another terminal, start web ui
cd apps/web
npm install
npm run dev
```

## Database and migrations
- Runtime is Postgres-only (`DB_REQUIRE_POSTGRES=true` by default).
- Schema is managed via Alembic.
- `api` container runs `alembic upgrade head` before starting uvicorn.

## Key env vars
- `DB_URL=postgresql+psycopg://...`
- `DB_URL_DOCKER=postgresql+psycopg://...@postgres:5432/...` (optional compose override)
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `QDRANT_URL`
- `EMBEDDING_BACKEND=local` (default; use local model for vectors)
- `OPENAI_CHAT_API_KEY` (recommended for chat/checklist LLM answers)
- `OPENAI_EMBEDDING_API_KEY` (only needed when `EMBEDDING_BACKEND=openai`)
- `OPENAI_API_KEY` (legacy fallback for both chat and embeddings)
- `SIMAP_PUBLICATIONS_PATH=/api/publications/v2/project/project-search`
- `SIMAP_PUBLICATION_DETAIL_PATH=/api/publications/v1/project/{projectId}/publication-details/{publicationId}`

Recommended low-cost setup:
- `EMBEDDING_BACKEND=local`
- `OPENAI_CHAT_API_KEY=...`

## Useful commands
```bash
# run migrations manually
alembic upgrade head

# inspect qdrant collection
curl http://localhost:6333/collections/tender_chunks

# count points
curl -X POST http://localhost:6333/collections/tender_chunks/points/count \
  -H "Content-Type: application/json" \
  -d '{"exact": true}'
```
