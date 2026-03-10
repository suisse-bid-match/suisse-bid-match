# SwissTender Copilot

AI-assisted tender matching copilot for Swiss public tenders.

The engine supports mixed tender packages (`DOCX + XLSX + PDF`) and focuses on
document-to-SQL matching (no autofill).

Primary workflow (MVP):
1. Ingest tender package (zip or multi-file upload).
2. Parse documents and build context evidence snippets.
3. Extract structured tender requirements.
4. Map requirements to whitelisted DB schema fields (rule-first + optional LLM disambiguation).
5. Generate validated read-only SQL (`SELECT` only, whitelisted tables/fields, required `LIMIT`).
6. Execute and rank products (hard constraints first, soft constraints weighted).
7. Return Top-K candidates + SQL + mapping rationale + audit trail.

## GitHub Description
AI copilot for Swiss tender applications: parse tender documents and match requirements to internal products with explainable SQL.

## What is implemented
- FastAPI backend + separate Next.js frontend (TypeScript + Tailwind CSS)
- Step-based frontend workbench for ingest and matching
- Seed company profile stored as JSON and editable in UI
- PDF deep extraction (sections, deadlines, required docs, criteria)
- Reference chunk indexing for document context
- Match workflow orchestration (`parse -> extract -> map -> SQL -> query -> rank -> audit`)
- SQL guardrails (`SELECT` only, whitelist tables/fields, required `LIMIT`)
- Top-K candidate scoring with hard-constraint first filtering
- Ingestion via browser upload (`zip`, multiple files, or full folder recursive upload)
- Metadata persisted in Postgres + files persisted in S3-compatible blob storage (MinIO in dev)

## API
- `POST /api/packages/ingest`
- `POST /api/packages/ingest/start` (async ingest job with progress)
- `GET /api/packages/ingest/{job_id}` (ingest progress/status)
- `GET /api/packages/{id}/fields`
- `POST /api/match/run`
- `GET /api/match/{run_id}`
- `GET /api/match/{run_id}/audit`
- `GET /api/profile`
- `PUT /api/profile`

## Quickstart (Docker)
```bash
docker compose up --build -d
```

Open:
- Frontend: `http://127.0.0.1:3000`
- Backend API: `http://127.0.0.1:8000`
- API docs: `http://127.0.0.1:8000/docs`
- MinIO console: `http://127.0.0.1:9001` (`minioadmin` / `minioadmin`)

In the frontend, use `Ingest upload`; path-based ingest is intentionally removed.

## PIM Match Datasource (MySQL)
Match queries run against an independent PIM datasource (`PIM_DATABASE_URL`) and keep app runtime/audit storage separated.

Default compose wiring:
- `pim-mysql` service (`mysql:8`)
- app env: `PIM_DATABASE_URL=mysql+pymysql://pim_reader:pim_reader@pim-mysql:3306/pim_raw`
- optional asset root mount: `PIM_ASSETS_ROOT=/pim-assets`

Rebuild flow (manual, on demand):
```bash
# Optional: import dump + rebuild match views
./scripts/pim_rebuild_match_layer.sh

# Rebuild views only (skip dump import)
SKIP_DUMP_IMPORT=1 ./scripts/pim_rebuild_match_layer.sh
```

The match query layer SQL is versioned at:
- `data/match/pim_match_views.sql`

## Optional LLM fallback
Set environment variables before running:
```bash
export OPENAI_API_KEY=...
export OPENAI_MODEL=gpt-5.4
export MATCH_MAPPER_MODEL=gpt-5.4
```

If `OPENAI_API_KEY` is missing, LLM-powered requirement extraction and mapper disambiguation are disabled.

## Tests
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

By default tests use this Uster path as sample data:
`/home/daz/all_things_for_genai_hackathon/real_tenders/20260220_Upload_simap_BKP_233-20260306T200217Z-3-001/20260220_Upload_simap_BKP_233`

Override with:
```bash
export USTER_SOURCE_PATH=/your/path/to/20260220_Upload_simap_BKP_233
```

## License
See [LICENSE](./LICENSE).
