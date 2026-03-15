from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import sys

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .core_bridge import repo_root_from_here


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Heidi Tender API"
    api_prefix: str = "/api/v1"
    database_url: str = "postgresql+psycopg://suisse:suisse@postgres:5432/suisse_bid_match"

    project_root: Path = Field(default_factory=repo_root_from_here)
    core_main_path: Path | None = None
    core_pipeline_config_path: Path | None = None
    default_field_rules_path: Path | None = None
    core_python_executable: str = sys.executable

    jobs_root: Path | None = None

    upload_file_limit_bytes: int = 50 * 1024 * 1024
    upload_zip_limit_bytes: int = 200 * 1024 * 1024
    upload_uncompressed_limit_bytes: int = 500 * 1024 * 1024
    upload_max_files: int = 1000

    max_concurrent_jobs: int = 2
    scan_interval_seconds: float = 1.0
    sse_heartbeat_seconds: int = 15

    # Core MySQL access (direct connection)
    pim_mysql_host: str = "127.0.0.1"
    pim_mysql_port: int = 3306
    pim_mysql_user: str = "root"
    pim_mysql_password: str = "root"
    pim_mysql_db: str = "pim_raw"
    pim_schema_tables: str = "vw_bid_products,vw_bid_specs"

    openai_api_key: str | None = None
    openai_base_url: str = "https://api.openai.com/v1"
    openai_model: str = "gpt-5-mini"

    core_skip_kb_bootstrap: bool = False

    @field_validator("project_root", mode="before")
    @classmethod
    def _normalize_project_root(cls, value: str | Path) -> Path:
        return Path(value).resolve()

    @field_validator("core_main_path", mode="before")
    @classmethod
    def _default_core_main(cls, value: str | Path | None, info) -> Path:
        if value:
            return Path(value).resolve()
        project_root = Path(info.data.get("project_root") or repo_root_from_here())
        return (project_root / "src" / "core" / "main.py").resolve()

    @field_validator("core_pipeline_config_path", mode="before")
    @classmethod
    def _default_pipeline_config(cls, value: str | Path | None, info) -> Path:
        if value:
            return Path(value).resolve()
        project_root = Path(info.data.get("project_root") or repo_root_from_here())
        return (project_root / "src" / "pipeline.yaml").resolve()

    @field_validator("default_field_rules_path", mode="before")
    @classmethod
    def _default_field_rules(cls, value: str | Path | None, info) -> Path:
        if value:
            return Path(value).resolve()
        project_root = Path(info.data.get("project_root") or repo_root_from_here())
        return (project_root / "src" / "field_rules.json").resolve()

    @field_validator("jobs_root", mode="before")
    @classmethod
    def _default_jobs_root(cls, value: str | Path | None, info) -> Path:
        if value:
            return Path(value).resolve()
        project_root = Path(info.data.get("project_root") or repo_root_from_here())
        return (project_root / "src" / "web" / "backend" / "data" / "jobs").resolve()

    @property
    def mysql_schema_tables(self) -> list[str]:
        tables = [item.strip() for item in self.pim_schema_tables.split(",") if item.strip()]
        # Force migration target tables even when a stale .env still points to legacy match_* tables.
        if any(name.startswith("match_") for name in tables):
            return ["vw_bid_products", "vw_bid_specs"]
        return tables

    @property
    def allowed_openai_models(self) -> list[str]:
        return ["gpt-5.4", "gpt-5-mini"]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.jobs_root.mkdir(parents=True, exist_ok=True)
    return settings
