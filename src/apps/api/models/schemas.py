from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class DateRangeFilter(BaseModel):
    start: datetime | None = None
    end: datetime | None = None


class ChatFilters(BaseModel):
    source: str | None = None
    cpv: list[str] | None = None
    date_range: DateRangeFilter | None = None
    buyer: str | None = None
    canton: str | None = None
    language: str | None = None


class ChatRequest(BaseModel):
    question: str = Field(min_length=3)
    filters: ChatFilters | None = None
    top_k: int | None = Field(default=None, ge=1, le=20)
    debug: bool = False


class MatchScoreBreakdown(BaseModel):
    dense_score: float
    bm25_score: float
    final_score: float


class MatchEvidence(BaseModel):
    matched_terms: list[str] = Field(default_factory=list)
    matched_sentences: list[str] = Field(default_factory=list)
    score_breakdown: MatchScoreBreakdown
    llm_reason: str | None = None
    matching_points: list[str] = Field(default_factory=list)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)


class Citation(BaseModel):
    title: str | None = None
    url: str | None = None
    doc_url: str | None = None
    snippet: str
    score: float
    notice_id: str
    match_evidence: MatchEvidence | None = None


class ChatDebug(BaseModel):
    plan: dict[str, Any]
    queries: list[str]
    timings: dict[str, float]
    retrieval_stats: dict[str, Any]


class ChatResponse(BaseModel):
    answer: str
    citations: list[Citation]
    used_filters: dict[str, Any]
    citation_count_insufficient: bool = False
    debug: ChatDebug | None = None


class IngestSimapRequest(BaseModel):
    updated_since: datetime | None = None
    limit: int = Field(default=50, ge=1, le=200)


class IngestResponse(BaseModel):
    fetched: int
    upserted: int
    versions_created: int
    docs_discovered: int
    errors: int
    elapsed_ms: int


class ReindexRequest(BaseModel):
    notice_ids: list[str] | None = None
    full: bool = False


class ReindexResponse(BaseModel):
    notices: int
    documents: int
    chunks: int
    vectors_upserted: int
    elapsed_ms: int


class DocumentRefOut(BaseModel):
    doc_id: str
    url: str
    filename: str | None = None
    mime_type: str | None = None
    fetched_at: datetime | None = None
    sha256: str | None = None
    pages: int | None = None
    raw_bytes_path: str | None = None


class TenderNoticeOut(BaseModel):
    id: str
    source: str
    source_id: str
    title: str | None = None
    description: str | None = None
    buyer_name: str | None = None
    buyer_location: str | None = None
    cpv_codes: list[str] | None = None
    procedure_type: str | None = None
    publication_date: datetime | None = None
    deadline_date: datetime | None = None
    languages: list[str] | None = None
    region: str | None = None
    url: str | None = None
    documents: list[DocumentRefOut] = Field(default_factory=list)


class ChecklistStructured(BaseModel):
    eligibility: list[str] = Field(default_factory=list)
    required_documents: list[str] = Field(default_factory=list)
    key_dates: list[str] = Field(default_factory=list)
    scoring_criteria: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)


class ChecklistResponse(BaseModel):
    notice_id: str
    structured: ChecklistStructured
    summary: str
    evidence_citations: list[Citation] = Field(default_factory=list)


class NoticeVersionOut(BaseModel):
    version_id: str
    version_ts: datetime
    content_hash: str


class ChangeItem(BaseModel):
    field: str
    old: Any
    new: Any
    type: Literal["added", "removed", "changed"]


class ChangesResponse(BaseModel):
    notice_id: str
    versions: list[NoticeVersionOut]
    diffs: list[ChangeItem]
    impact_label: Literal["low", "med", "high"]
    impact_reasons: list[str]
