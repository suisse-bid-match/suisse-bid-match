from functools import lru_cache

from pydantic import Field, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Suisse Bid Match"
    app_env: str = "dev"
    log_level: str = "INFO"

    db_require_postgres: bool = True
    db_url: str = "postgresql+psycopg://suisse:suisse@postgres:5432/suisse_bid_match"

    qdrant_url: str = "http://localhost:6333"
    qdrant_collection: str = "tender_chunks"
    qdrant_timeout: int = 30
    qdrant_recreate_collection: bool = False

    embedding_backend: str = "local"  # auto|openai|local
    openai_api_key: str | None = None
    openai_chat_api_key: str | None = None
    openai_embedding_api_key: str | None = None
    openai_embedding_model: str = "text-embedding-3-small"
    local_embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    openai_chat_model: str = "gpt-5-mini"
    llm_matching_enabled: bool = True
    llm_match_pool_size: int = Field(default=12, ge=3, le=30)

    simap_base_url: str = "https://www.simap.ch"
    simap_token: str | None = None
    simap_timeout_seconds: int = 20
    simap_rps: float = 1.0
    simap_publications_path: str = "/api/publications/v2/project/project-search"
    simap_publication_detail_path: str = "/api/publications/v1/project/{projectId}/publication-details/{publicationId}"

    docs_storage_root: str = "./data/docs"

    dense_weight: float = Field(default=0.7, ge=0.0, le=1.0)
    bm25_weight: float = Field(default=0.3, ge=0.0, le=1.0)
    default_top_k: int = Field(default=8, ge=1, le=20)
    dense_candidates: int = Field(default=30, ge=5, le=200)
    chat_max_retrieval_rounds: int = Field(default=1, ge=1, le=3)

    enable_debug_chat: bool = True
    preload_local_embedding_on_startup: bool = True

    @field_validator("db_url")
    @classmethod
    def validate_db_url(cls, value: str, info: ValidationInfo) -> str:
        require_postgres = bool(info.data.get("db_require_postgres", True))
        app_env = str(info.data.get("app_env", "dev")).lower()
        if require_postgres and app_env != "test":
            valid_prefix = ("postgresql://", "postgresql+psycopg://")
            if not value.startswith(valid_prefix):
                raise ValueError("DB_URL must be PostgreSQL when DB_REQUIRE_POSTGRES=true")
        return value

    @property
    def resolved_openai_chat_api_key(self) -> str | None:
        return self.openai_chat_api_key or self.openai_api_key

    @property
    def resolved_openai_embedding_api_key(self) -> str | None:
        return self.openai_embedding_api_key or self.openai_api_key


@lru_cache
def get_settings() -> Settings:
    return Settings()
