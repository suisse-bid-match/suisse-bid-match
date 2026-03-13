from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .models import JobStatus, RuleSource, RuleStatus


class JobCreateResponse(BaseModel):
    id: str
    status: JobStatus
    created_at: datetime


class JobFileResponse(BaseModel):
    id: str
    relative_path: str
    size_bytes: int
    extension: str
    created_at: datetime


class JobStepResponse(BaseModel):
    step_name: str
    step_status: str
    payload: dict[str, Any]
    updated_at: datetime


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    runtime_dir: str | None = None
    final_output_path: str | None = None
    error_message: str | None = None
    rule_version_id: str | None = None
    warnings: list[str] = Field(default_factory=list)
    file_count: int = 0
    step_count: int = 0
    files: list[JobFileResponse] = Field(default_factory=list)
    steps: list[JobStepResponse] = Field(default_factory=list)


class StartJobRequest(BaseModel):
    rule_version_id: str | None = None


class RulePayload(BaseModel):
    field_rules: list[dict[str, Any]]


class CopilotExecutionSummary(BaseModel):
    step_name: str
    request_started_at: str | None = None
    request_finished_at: str | None = None
    duration_ms: int | None = None
    final_status: str
    response_received: bool
    fallback_used: bool = False
    failure_message: str | None = None
    reasoning_summary: str | None = None
    reasoning_chars: int = 0
    stream_event_counts: dict[str, int] = Field(default_factory=dict)
    status_events: list[str] = Field(default_factory=list)


class CopilotLogPayload(BaseModel):
    prompt: str = Field(min_length=1, max_length=2000)
    model: str
    reasoning_summary: str | None = None
    execution_summary: CopilotExecutionSummary


class SaveRuleDraftRequest(BaseModel):
    payload: RulePayload
    note: str | None = None
    source: RuleSource = RuleSource.manual
    copilot_log: CopilotLogPayload | None = None


class GenerateRulesRequest(BaseModel):
    note: str | None = None


class GenerateRulesStreamRequest(BaseModel):
    prompt: str = Field(default="", max_length=2000)


class RuleVersionResponse(BaseModel):
    id: str
    version_number: int
    status: RuleStatus
    source: RuleSource
    payload: dict[str, Any]
    validation_report: dict[str, Any]
    copilot_log: dict[str, Any] | None = None
    note: str | None = None
    created_at: datetime
    published_at: datetime | None = None


class PublishRuleResponse(BaseModel):
    id: str
    status: RuleStatus
    published_at: datetime


class ModelSettingsResponse(BaseModel):
    current_model: str
    allowed_models: list[str]
    has_api_key: bool


class SetModelRequest(BaseModel):
    model: str


class StatsOverviewResponse(BaseModel):
    window_from: datetime
    window_to: datetime
    job_count: int
    succeeded_count: int
    failed_count: int
    avg_job_duration_ms: float | None = None
    p50_job_duration_ms: int | None = None
    p90_job_duration_ms: int | None = None
    avg_extracted_products: float | None = None


class JobDurationStatRow(BaseModel):
    job_id: str
    status: JobStatus
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    extracted_products: int | None = None


class StepDurationStatRow(BaseModel):
    step_name: str
    sample_count: int
    avg_duration_ms: float | None = None
    p50_duration_ms: int | None = None
    p90_duration_ms: int | None = None


class FieldFrequencyStatRow(BaseModel):
    field: str
    count: int


class StatsDashboardResponse(BaseModel):
    overview: StatsOverviewResponse
    job_durations: list[JobDurationStatRow]
    step_durations: list[StepDurationStatRow]
    field_frequency: list[FieldFrequencyStatRow]
