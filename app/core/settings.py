from __future__ import annotations

import os
from pathlib import Path


class Settings:
    def __init__(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self.repo_root = repo_root
        self.data_dir = repo_root / "data"
        self.match_data_dir = self.data_dir / "match"
        self.runtime_dir = repo_root / "runtime"
        self.packages_dir = self.runtime_dir / "packages"
        self.runs_dir = self.runtime_dir / "runs"
        self.matches_dir = self.runtime_dir / "matches"
        self.blob_dir = self.runtime_dir / "blob"
        self.profile_path = self.data_dir / "profile.json"
        self.seed_profile_path = self.data_dir / "seed_profile.json"
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "")
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-5.4")
        self.openai_timeout_sec = float(os.getenv("OPENAI_TIMEOUT_SEC", "25"))
        self.openai_max_retries = int(os.getenv("OPENAI_MAX_RETRIES", "1"))
        self.openai_web_search_enabled = os.getenv("OPENAI_WEB_SEARCH_ENABLED", "true").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.openai_web_search_external = os.getenv("OPENAI_WEB_SEARCH_EXTERNAL", "true").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.openai_web_search_tool_choice = os.getenv("OPENAI_WEB_SEARCH_TOOL_CHOICE", "auto").lower()
        self.database_url = os.getenv(
            "DATABASE_URL",
            f"sqlite:///{(self.runtime_dir / 'app.db').as_posix()}",
        )
        self.pim_database_url = os.getenv("PIM_DATABASE_URL", "")
        self.pim_assets_root = os.getenv("PIM_ASSETS_ROOT", "")
        self.docling_url = os.getenv("DOCLING_URL", "http://docling:8020")
        self.docling_timeout_sec = float(os.getenv("DOCLING_TIMEOUT_SEC", "30"))
        self.docling_connect_timeout_sec = float(os.getenv("DOCLING_CONNECT_TIMEOUT_SEC", "0.5"))
        self.docling_max_lines = int(os.getenv("DOCLING_MAX_LINES", "240"))
        self.docling_full_xlsx_max_lines = int(os.getenv("DOCLING_FULL_XLSX_MAX_LINES", "50000"))
        self.docling_pdf_split_enabled = os.getenv("DOCLING_PDF_SPLIT_ENABLED", "true").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.docling_pdf_chunk_pages = int(os.getenv("DOCLING_PDF_CHUNK_PAGES", "6"))
        self.docling_pdf_max_chunks = int(os.getenv("DOCLING_PDF_MAX_CHUNKS", "6"))
        self.docling_response_format = os.getenv("DOCLING_RESPONSE_FORMAT", "markdown").strip().lower()
        self.storage_backend = os.getenv("STORAGE_BACKEND", "local").lower()
        self.s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "")
        self.s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID", "")
        self.s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY", "")
        self.s3_region = os.getenv("S3_REGION", "us-east-1")
        self.s3_bucket = os.getenv("S3_BUCKET", "suisse-bid-match")
        self.s3_secure = os.getenv("S3_SECURE", "false").lower() in {"1", "true", "yes"}
        self.s3_connect_timeout_sec = float(os.getenv("S3_CONNECT_TIMEOUT_SEC", "5"))
        self.s3_read_timeout_sec = float(os.getenv("S3_READ_TIMEOUT_SEC", "60"))
        self.s3_max_retries = int(os.getenv("S3_MAX_RETRIES", "3"))
        self.match_default_domain = os.getenv("MATCH_DEFAULT_DOMAIN", "lighting")
        self.match_default_top_k = int(os.getenv("MATCH_DEFAULT_TOP_K", "5"))
        self.match_query_timeout_sec = float(os.getenv("MATCH_QUERY_TIMEOUT_SEC", "15"))
        self.match_llm_confidence_threshold = float(os.getenv("MATCH_LLM_CONFIDENCE_THRESHOLD", "0.75"))
        self.match_hard_min_confidence = float(os.getenv("MATCH_HARD_MIN_CONFIDENCE", "0.60"))
        self.match_mapping_mode = os.getenv("MATCH_MAPPING_MODE", "hybrid").strip().lower() or "hybrid"
        self.match_mapper_model = os.getenv("MATCH_MAPPER_MODEL", self.openai_model)
        self.match_max_context_lines = int(os.getenv("MATCH_MAX_CONTEXT_LINES", "1200"))
        self.match_llm_timeout_sec = float(os.getenv("MATCH_LLM_TIMEOUT_SEC", "45"))
        self.match_llm_max_retries = int(os.getenv("MATCH_LLM_MAX_RETRIES", "0"))
        self.match_sql_model = os.getenv("MATCH_SQL_MODEL", self.openai_model)
        self.match_sql_max_output_tokens = int(os.getenv("MATCH_SQL_MAX_OUTPUT_TOKENS", "800"))
        self.match_sql_repair_max_rounds = int(os.getenv("MATCH_SQL_REPAIR_MAX_ROUNDS", "2"))
        self.match_extract_max_chars = int(os.getenv("MATCH_EXTRACT_MAX_CHARS", "0"))
        self.match_extract_max_output_tokens = int(
            os.getenv("MATCH_EXTRACT_MAX_OUTPUT_TOKENS", "3000")
        )
        self.match_mapper_max_output_tokens = int(
            os.getenv("MATCH_MAPPER_MAX_OUTPUT_TOKENS", "220")
        )
        self.doc_classifier_model = os.getenv("DOC_CLASSIFIER_MODEL", self.openai_model)
        self.doc_classifier_timeout_sec = float(os.getenv("DOC_CLASSIFIER_TIMEOUT_SEC", "20"))
        self.doc_classifier_max_retries = int(os.getenv("DOC_CLASSIFIER_MAX_RETRIES", "0"))
        self.doc_classifier_max_lines = int(os.getenv("DOC_CLASSIFIER_MAX_LINES", "260"))
        self.doc_classifier_max_chars = int(os.getenv("DOC_CLASSIFIER_MAX_CHARS", "50000"))
        self.doc_classifier_max_output_tokens = int(
            os.getenv("DOC_CLASSIFIER_MAX_OUTPUT_TOKENS", "180")
        )


settings = Settings()
