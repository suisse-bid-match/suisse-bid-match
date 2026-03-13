from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text

from .api.jobs import router as jobs_router
from .api.rules import router as rules_router
from .api.settings import router as settings_router
from .api.stats import router as stats_router
from .config import get_settings
from .db import Base, SessionLocal, engine
from .models import RuleSource, RuleStatus
from .repositories.app_settings import AppSettingsRepository
from .repositories.rules import RuleRepository
from .services.rules import allowed_fields_from_schema, fetch_schema_payload, validate_rule_payload

settings = get_settings()
app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


app.include_router(jobs_router, prefix=settings.api_prefix)
app.include_router(rules_router, prefix=settings.api_prefix)
app.include_router(settings_router, prefix=settings.api_prefix)
app.include_router(stats_router, prefix=settings.api_prefix)


def _extract_rule_fields(payload: dict) -> set[str]:
    fields: set[str] = set()
    for row in payload.get("field_rules", []):
        if isinstance(row, dict):
            text = str(row.get("field") or "").strip().lower()
            if text:
                fields.add(text)
    return fields


def _is_legacy_or_invalid_payload(payload: dict, allowed_fields: set[str]) -> bool:
    fields = _extract_rule_fields(payload if isinstance(payload, dict) else {})
    if not fields:
        return True
    if any(field.startswith("match_") for field in fields):
        return True
    return any(field not in allowed_fields for field in fields)



def _seed_rule_if_needed() -> None:
    default_rules_path: Path = settings.default_field_rules_path
    if not default_rules_path.exists():
        return

    seed_payload_raw = json.loads(default_rules_path.read_text(encoding="utf-8"))
    schema_payload = fetch_schema_payload(settings)
    allowed_fields = allowed_fields_from_schema(schema_payload)
    normalized_seed, seed_report = validate_rule_payload(seed_payload_raw, allowed_fields)

    with SessionLocal() as db:
        repo = RuleRepository(db)
        versions = repo.list_versions()
        active_versions = [row for row in versions if row.status != RuleStatus.archived]

        should_reseed = not active_versions
        if not should_reseed:
            published = next((row for row in active_versions if row.status == RuleStatus.published), None)
            if published is None:
                should_reseed = True
            else:
                try:
                    validate_rule_payload(published.payload, allowed_fields)
                    should_reseed = _is_legacy_or_invalid_payload(published.payload, allowed_fields)
                except Exception:
                    should_reseed = True

        if not should_reseed:
            return

        for row in active_versions:
            row.status = RuleStatus.archived
            row.published_at = None
            db.add(row)
        db.commit()

        repo.create_version(
            payload=normalized_seed,
            status=RuleStatus.published,
            source=RuleSource.seed,
            validation_report=seed_report,
            note="seeded from src/field_rules.json after schema migration",
        )


def _ensure_runtime_indexes() -> None:
    dialect = engine.dialect.name
    if dialect == "postgresql":
        statements = [
            "CREATE INDEX IF NOT EXISTS ix_jobs_status_updated_at_desc ON jobs (status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_jobs_updated_at_desc ON jobs (updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rule_versions_status_version_number_desc ON rule_versions (status, version_number DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rule_versions_source_version_number_desc ON rule_versions (source, version_number DESC)",
        ]
    else:
        statements = [
            "CREATE INDEX IF NOT EXISTS ix_jobs_status_updated_at_desc ON jobs (status, updated_at)",
            "CREATE INDEX IF NOT EXISTS ix_jobs_updated_at_desc ON jobs (updated_at)",
            "CREATE INDEX IF NOT EXISTS ix_rule_versions_status_version_number_desc ON rule_versions (status, version_number)",
            "CREATE INDEX IF NOT EXISTS ix_rule_versions_source_version_number_desc ON rule_versions (source, version_number)",
        ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def _ensure_runtime_schema_extensions() -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "rule_versions" not in tables:
        return
    columns = {column["name"] for column in inspector.get_columns("rule_versions")}
    if "copilot_log" in columns:
        return
    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE rule_versions ADD COLUMN copilot_log JSON"))


def _ensure_app_settings_defaults() -> None:
    with SessionLocal() as db:
        repo = AppSettingsRepository(db)
        repo.get_current_openai_model(
            default_model=settings.openai_model,
            allowed_models=settings.allowed_openai_models,
        )


@app.on_event("startup")
def on_startup() -> None:
    settings.jobs_root.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    _ensure_runtime_schema_extensions()
    _ensure_runtime_indexes()
    _ensure_app_settings_defaults()
    _seed_rule_if_needed()
