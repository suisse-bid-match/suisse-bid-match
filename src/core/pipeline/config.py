from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
import yaml


class OpenAIConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str = "gpt-5-mini"
    base_url: str = "https://api.openai.com/v1"
    file_purpose: str = "user_data"
    enable_web_search: bool = False


class KBConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_dir: str
    vector_store_name: str = "lighting_kb"
    kb_key: str = "lighting_kb"
    description: str = "Swiss lighting tender knowledge base."
    max_num_results: int = Field(default=12, ge=1)


class DBConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    container: str
    user: str
    password: str
    database: str
    tables: list[str]
    join_key: str = "product_id"


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_dir: str = "runtime"
    keep_intermediate: bool = True


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    openai: OpenAIConfig
    kb: KBConfig
    db: DBConfig
    runtime: RuntimeConfig


def load_pipeline_config(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw) or {}
    parsed = PipelineConfig.model_validate(data)
    return parsed.model_dump(mode="python")

