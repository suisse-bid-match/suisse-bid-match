from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


class CompanyInfo(BaseModel):
    name: str
    legal_form: str
    uid_vat: str
    address_line: str
    postal_code: str
    city: str
    country: str = "CH"
    website: str | None = None


class ContactInfo(BaseModel):
    primary_name: str
    primary_phone: str
    primary_email: str


class ReferenceProject(BaseModel):
    project_name: str
    client: str
    city: str
    year: int
    amount_chf: float
    contact_person: str
    contact_phone: str


class CommercialDefaults(BaseModel):
    discount_percent: float
    skonto_percent: float
    general_deduction_percent: float
    vat_percent: float
    currency: str = "CHF"


class ProductSpec(BaseModel):
    product_key: str
    name: str
    unit_price_chf: float
    specs: dict[str, Any] = Field(default_factory=dict)


class CompanyProfile(BaseModel):
    company: CompanyInfo
    contacts: ContactInfo
    certifications: list[str] = Field(default_factory=list)
    references: list[ReferenceProject] = Field(default_factory=list)
    commercial_defaults: CommercialDefaults
    product_catalog: list[ProductSpec] = Field(default_factory=list)


class DocumentInfo(BaseModel):
    doc_id: str
    name: str
    relative_path: str
    kind: Literal["doc", "docx", "docm", "xlsx", "pdf"]
    template_id: str | None = None
    template_name: str | None = None
    template_confidence: float | None = None
    role: Literal["MUST_FILL", "REFERENCE_ONLY", "IGNORE", "MUST_FILL_CANDIDATE"] = "REFERENCE_ONLY"
    role_confidence: float = 0.0
    role_reasons: list[str] = Field(default_factory=list)
    submit_required: bool | None = None
    anchor_candidates_count: int = 0
    field_understanding_status: Literal["skipped", "ok", "failed"] | None = None
    field_understanding_reason: str | None = None


class FieldCandidate(BaseModel):
    field_id: str
    doc_id: str
    source_kind: Literal["docx", "docm", "xlsx"]
    semantic_key: str
    label: str
    location: str
    internal_ref: str
    anchor_type: str | None = None
    anchor_metadata: dict[str, Any] = Field(default_factory=dict)
    extraction_confidence: float = 0.0
    should_fill_decision_source: str | None = None
    decision_reason: str | None = None
    decision_confidence: float = 0.0
    evidence_anchor_refs: list[str] = Field(default_factory=list)
    value_type: str | None = None
    required_level: Literal["critical", "required", "optional", "none"] = "optional"
    required: bool = False
    critical: bool = False
    amount_related: bool = False
    doc_role: Literal["MUST_FILL", "REFERENCE_ONLY", "IGNORE", "MUST_FILL_CANDIDATE"] | None = None


class ReferenceChunk(BaseModel):
    chunk_id: str
    doc_id: str
    doc_name: str
    section_path: str
    page_or_anchor: str
    text: str
    tokens: int


class DoclingLine(BaseModel):
    evidence_ref: str
    text: str


class DoclingDocument(BaseModel):
    doc_id: str
    used: bool = False
    error: str | None = None
    lines: list[DoclingLine] = Field(default_factory=list)


class PdfInsight(BaseModel):
    title: str
    deadline_lines: list[str] = Field(default_factory=list)
    required_document_lines: list[str] = Field(default_factory=list)
    criteria_lines: list[str] = Field(default_factory=list)
    sections: list[dict[str, Any]] = Field(default_factory=list)


class PackageIndex(BaseModel):
    package_id: str
    created_at: datetime
    source_name: str
    root_dir: str
    documents: list[DocumentInfo]
    fields: list[FieldCandidate]
    pdf_insight: PdfInsight | None = None
    reference_chunks: list[ReferenceChunk] = Field(default_factory=list)
    docling_documents: list[DoclingDocument] = Field(default_factory=list)


class APIIngestResponse(BaseModel):
    package_id: str
    source_name: str
    document_count: int
    field_count: int


class TenderRequirement(BaseModel):
    requirement_id: str
    param_key: str
    operator: Literal["eq", "gte", "lte", "gt", "lt", "between", "in", "contains", "bool_true", "bool_false"]
    value: Any = None
    unit: str | None = None
    is_hard: bool = True
    product_key: str | None = None
    product_name: str | None = None
    quantity: float | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    raw_text: str | None = None
    confidence: float = 0.0


class ProductRequirementScope(BaseModel):
    product_key: str
    product_name: str | None = None
    quantity: float | None = None
    requirements: list[TenderRequirement] = Field(default_factory=list)


class RequirementSet(BaseModel):
    package_id: str
    domain: str
    requirements: list[TenderRequirement] = Field(default_factory=list)
    product_scopes: list[ProductRequirementScope] = Field(default_factory=list)
    generated_at: datetime


class SchemaMapping(BaseModel):
    requirement_id: str
    param_key: str
    mapped_table: str | None = None
    mapped_field: str | None = None
    operator: str
    value: Any = None
    is_hard: bool
    status: Literal["mapped", "unmapped", "ambiguous"] = "unmapped"
    confidence: float = 0.0
    reason: str = ""
    candidate_fields: list[str] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)


class SQLPlan(BaseModel):
    domain: str
    sql: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    hard_clause_count: int = 0
    soft_clause_count: int = 0
    limit: int = 5
    validated: bool = False
    validation_errors: list[str] = Field(default_factory=list)
    blocked: bool = False
    block_reason: str | None = None


class MatchCandidate(BaseModel):
    product_id: str
    product_name: str
    score: float
    hard_passed: bool
    soft_score: float = 0.0
    matched_requirements: list[str] = Field(default_factory=list)
    unmet_requirements: list[str] = Field(default_factory=list)
    hard_violations: list[str] = Field(default_factory=list)
    score_breakdown: dict[str, float] = Field(default_factory=dict)
    request_product_key: str | None = None
    request_product_name: str | None = None
    row: dict[str, Any] = Field(default_factory=dict)


class ProductMatchResult(BaseModel):
    product_key: str
    product_name: str | None = None
    quantity: float | None = None
    blocked: bool = False
    requirements: list[TenderRequirement] = Field(default_factory=list)
    mapped_conditions: list[SchemaMapping] = Field(default_factory=list)
    sql_plan: SQLPlan | None = None
    candidates: list[MatchCandidate] = Field(default_factory=list)
    unmet_constraints: list[str] = Field(default_factory=list)


class AuditEvent(BaseModel):
    step: str
    status: Literal["ok", "failed", "blocked"]
    started_at: datetime
    finished_at: datetime
    summary: str
    input_snapshot: dict[str, Any] = Field(default_factory=dict)
    output_snapshot: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None


class DocumentClassification(BaseModel):
    doc_id: str
    doc_name: str
    is_application_form: bool
    confidence: float
    reason: str
    evidence_refs: list[str] = Field(default_factory=list)
    parse_failed: bool = False


class MatchRun(BaseModel):
    run_id: str
    package_id: str
    domain: str
    created_at: datetime
    top_k: int
    strict_hard_constraints: bool = True
    blocked: bool = False
    doc_classifications: list[DocumentClassification] = Field(default_factory=list)
    requirements: RequirementSet
    mapped_conditions: list[SchemaMapping] = Field(default_factory=list)
    sql_plan: SQLPlan
    candidates: list[MatchCandidate] = Field(default_factory=list)
    product_results: list[ProductMatchResult] = Field(default_factory=list)
    unmet_constraints: list[str] = Field(default_factory=list)
    audit_trail: list[AuditEvent] = Field(default_factory=list)


class APIMatchRunResponse(BaseModel):
    run_id: str
    package_id: str
    domain: str
    blocked: bool
    candidate_count: int


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
