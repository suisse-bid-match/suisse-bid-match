from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


ALLOWED_OPERATORS = {
    "eq",
    "gte",
    "lte",
    "gt",
    "lt",
    "between",
    "in",
    "contains",
    "bool_true",
    "bool_false",
}

_OPERATOR_ALIASES = {
    "=": "eq",
    "==": "eq",
    "equal": "eq",
    "equals": "eq",
    ">=": "gte",
    "greater or equal": "gte",
    "greater than or equal": "gte",
    "<=": "lte",
    "less or equal": "lte",
    "less than or equal": "lte",
    ">": "gt",
    "greater": "gt",
    "greater than": "gt",
    "<": "lt",
    "less": "lt",
    "less than": "lt",
    "true": "bool_true",
    "false": "bool_false",
}

_SQL_COMMENT_PATTERN = re.compile(r"(--|#|/\*|\*/)")
_SQL_FORBIDDEN_PATTERN = re.compile(
    r"\b("
    r"insert|update|delete|drop|alter|create|truncate|replace|"
    r"grant|revoke|merge|call|do|handler|load|lock|unlock|set|use|"
    r"show|describe|desc|explain|analyze|optimize|repair|flush|kill"
    r")\b",
    re.IGNORECASE,
)
_SQL_FROM_OR_JOIN_PATTERN = re.compile(
    r"\b(?:from|join)\s+([`\"]?[a-zA-Z_][\w$]*[`\"]?(?:\.[`\"]?[a-zA-Z_][\w$]*[`\"]?)?)",
    re.IGNORECASE,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_field(value: str) -> str:
    return " ".join(value.strip().lower().split())


def normalize_operator(value: str) -> str:
    normalized = _OPERATOR_ALIASES.get(" ".join(value.strip().lower().split()), value.strip().lower())
    if normalized not in ALLOWED_OPERATORS:
        raise ValueError(f"unsupported operator '{value}'")
    return normalized


def _normalize_table_name(raw_name: str) -> str:
    parts = [p.strip("`\"") for p in raw_name.split(".") if p]
    return parts[-1] if parts else raw_name.strip("`\"")


def _extract_table_names(sql: str) -> list[str]:
    names: list[str] = []
    for match in _SQL_FROM_OR_JOIN_PATTERN.finditer(sql):
        names.append(_normalize_table_name(match.group(1)))
    return names


def validate_safe_select_sql(sql: str, allowed_tables: set[str] | None = None) -> str:
    if not isinstance(sql, str):
        raise ValueError("sql must be a string")
    statement = sql.strip()
    if not statement:
        raise ValueError("sql is empty")
    if _SQL_COMMENT_PATTERN.search(statement):
        raise ValueError("sql comments are not allowed")
    if statement.endswith(";"):
        statement = statement[:-1].strip()
    if ";" in statement:
        raise ValueError("multiple SQL statements are not allowed")
    if not re.match(r"(?is)^select\b", statement):
        raise ValueError("only SELECT statements are allowed")
    if re.search(r"(?is)\binto\s+(outfile|dumpfile)\b", statement):
        raise ValueError("SELECT ... INTO OUTFILE/DUMPFILE is not allowed")
    if _SQL_FORBIDDEN_PATTERN.search(statement):
        raise ValueError("forbidden SQL keyword detected")
    table_names = _extract_table_names(statement)
    if not table_names:
        raise ValueError("SQL must reference at least one table")
    if allowed_tables is not None:
        unknown = sorted({name for name in table_names if name not in allowed_tables})
        if unknown:
            raise ValueError(f"SQL references tables outside allowlist: {', '.join(unknown)}")
    return statement


class ErrorItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class SchemaColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    type: str = Field(min_length=1)


class SchemaTable(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    columns: list[SchemaColumn] = Field(default_factory=list)


class SchemaPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tables: list[SchemaTable] = Field(default_factory=list)


class RequirementSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    file_name: str | None = None
    snippet: str | None = None


class Step2Requirement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirement_id: str | None = None
    field: str = Field(min_length=1)
    value: Any = None
    unit: str | None = None
    source: RequirementSource | None = None
    extraction_confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    @field_validator("field", mode="before")
    @classmethod
    def _normalize_field(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("field must be string")
        normalized = normalize_field(value)
        if "." not in normalized:
            raise ValueError("field must be table.column")
        return normalized


class Step2TenderProduct(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_key: str = Field(min_length=1)
    product_name: str | None = None
    quantity: int | float | str | None = None
    requirements: list[Step2Requirement] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_requirement_fields(self):
        seen: set[str] = set()
        duplicates: set[str] = set()
        for requirement in self.requirements:
            key = requirement.field
            if key in seen:
                duplicates.add(key)
            seen.add(key)
        if duplicates:
            text = ", ".join(sorted(duplicates))
            raise ValueError(
                f"duplicate field in product {self.product_key}: {text}. "
                "Step2 must not emit duplicate fields per product."
            )
        return self


class LLMExecutionSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step_name: str = Field(min_length=1)
    request_started_at: str | None = None
    request_finished_at: str | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    final_status: str = Field(pattern="^(succeeded|failed)$")
    response_received: bool = False
    fallback_used: bool = False
    failure_message: str | None = None
    reasoning_summary: str | None = None
    reasoning_chars: int = Field(default=0, ge=0)
    stream_event_counts: dict[str, int] = Field(default_factory=dict)
    status_events: list[str] = Field(default_factory=list)


class Step2Data(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_snapshot: SchemaPayload
    tender_products: list[Step2TenderProduct] = Field(default_factory=list)
    llm_execution: LLMExecutionSummary | None = None

    @model_validator(mode="after")
    def _unique_product_keys(self):
        seen: set[str] = set()
        duplicates: set[str] = set()
        for product in self.tender_products:
            if product.product_key in seen:
                duplicates.add(product.product_key)
            seen.add(product.product_key)
        if duplicates:
            text = ", ".join(sorted(duplicates))
            raise ValueError(f"duplicate product_key in Step2 output: {text}")
        return self


class FieldRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(min_length=1)
    operator: str
    is_hard: bool
    operator_confidence: float = Field(ge=0.0, le=1.0)
    hardness_confidence: float = Field(ge=0.0, le=1.0)
    rationale: str | None = None

    @field_validator("field", mode="before")
    @classmethod
    def _normalize_field(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("field must be string")
        normalized = normalize_field(value)
        if "." not in normalized:
            raise ValueError("field must be table.column")
        return normalized

    @field_validator("operator", mode="before")
    @classmethod
    def _normalize_operator(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("operator must be string")
        return normalize_operator(value)


class Step3Data(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_rules: list[FieldRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_fields(self):
        seen: set[str] = set()
        duplicates: set[str] = set()
        for rule in self.field_rules:
            key = rule.field
            if key in seen:
                duplicates.add(key)
            seen.add(key)
        if duplicates:
            text = ", ".join(sorted(duplicates))
            raise ValueError(f"duplicate field in step3 field_rules: {text}")
        return self


class MergedRequirement(Step2Requirement):
    model_config = ConfigDict(extra="forbid")

    operator: str | None = None
    is_hard: bool | None = None
    operator_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    hardness_confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    @field_validator("operator", mode="before")
    @classmethod
    def _normalize_optional_operator(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("operator must be string when present")
        return normalize_operator(value)


class MergedTenderProduct(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_key: str = Field(min_length=1)
    product_name: str | None = None
    quantity: int | float | str | None = None
    requirements: list[MergedRequirement] = Field(default_factory=list)


class SkippedRequirement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_key: str
    requirement_id: str | None = None
    requirement_index: int = Field(ge=0)
    field: str
    reason: str


class Step4Data(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tender_products: list[MergedTenderProduct] = Field(default_factory=list)
    skipped_requirements: list[SkippedRequirement] = Field(default_factory=list)


class HardConstraintUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    operator: str
    value: Any = None

    @field_validator("operator", mode="before")
    @classmethod
    def _normalize_operator(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("operator must be string")
        return normalize_operator(value)


class SQLQueryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query_id: str
    product_key: str
    hard_constraints_used: list[HardConstraintUsed] = Field(default_factory=list)
    sql: str

    @field_validator("sql", mode="before")
    @classmethod
    def _validate_sql(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("sql must be string")
        return validate_safe_select_sql(value)


class Step5Data(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queries: list[SQLQueryItem] = Field(default_factory=list)


class SQLResultItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query_id: str
    product_key: str
    sql: str
    row_count: int = Field(ge=0)
    elapsed_ms: int = Field(ge=0)
    rows: list[dict[str, Any]] = Field(default_factory=list)


class Step6Data(BaseModel):
    model_config = ConfigDict(extra="forbid")

    results: list[SQLResultItem] = Field(default_factory=list)


class RankedCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rank: int = Field(ge=1)
    db_product_id: str | int | None = None
    db_product_name: str | None = None
    passes_hard: bool
    soft_match_score: float | None = None
    matched_soft_constraints: list[str] = Field(default_factory=list)
    unmet_soft_constraints: list[str] = Field(default_factory=list)
    explanation: str | None = None


class Step7MatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_key: str
    candidates: list[RankedCandidate] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_candidate_key(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if "candidates" not in normalized and "ranked_candidates" in normalized:
            normalized["candidates"] = normalized.pop("ranked_candidates")
        else:
            normalized.pop("ranked_candidates", None)
        return normalized


class Step7Data(BaseModel):
    model_config = ConfigDict(extra="forbid")

    match_results: list[Step7MatchResult] = Field(default_factory=list)
    llm_execution: LLMExecutionSummary | None = None


class StepEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step: str
    version: str = "1.0"
    run_id: str
    status: str
    created_at: str
    data: dict[str, Any]
    uncertainties: list[str] = Field(default_factory=list)
    errors: list[ErrorItem] = Field(default_factory=list)

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str) -> str:
        if value not in {"ok", "partial", "error"}:
            raise ValueError("status must be one of ok|partial|error")
        return value


def build_step_envelope(
    *,
    step: str,
    run_id: str,
    data: dict[str, Any],
    status: str = "ok",
    uncertainties: list[str] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict:
    payload = StepEnvelope.model_validate(
        {
            "step": step,
            "version": "1.0",
            "run_id": run_id,
            "status": status,
            "created_at": utc_now_iso(),
            "data": data,
            "uncertainties": uncertainties or [],
            "errors": errors or [],
        }
    )
    return payload.model_dump(mode="python")


def schema_column_set(schema_payload: dict) -> set[str]:
    parsed = SchemaPayload.model_validate(schema_payload)
    output: set[str] = set()
    for table in parsed.tables:
        for column in table.columns:
            output.add(f"{table.name.strip().lower()}.{column.name.strip().lower()}")
    return output


def validate_step2_data(payload: dict) -> dict:
    parsed = Step2Data.model_validate(payload)
    return parsed.model_dump(mode="python")


def validate_step3_data(payload: dict, *, allowed_fields: set[str] | None = None) -> dict:
    parsed = Step3Data.model_validate(payload)
    normalized = parsed.model_dump(mode="python")
    if allowed_fields is None:
        return normalized
    unknown = sorted({row["field"] for row in normalized.get("field_rules", []) if row["field"] not in allowed_fields})
    if unknown:
        raise ValueError("step3 field_rules contains non-schema fields: " + ", ".join(unknown))
    return normalized


def validate_step4_data(payload: dict) -> dict:
    parsed = Step4Data.model_validate(payload)
    return parsed.model_dump(mode="python")


def validate_step5_data(payload: dict, *, allowed_tables: set[str] | None = None) -> dict:
    parsed = Step5Data.model_validate(payload)
    normalized = parsed.model_dump(mode="python")
    if allowed_tables is None:
        return normalized
    for item in normalized.get("queries", []):
        item["sql"] = validate_safe_select_sql(item["sql"], allowed_tables=allowed_tables)
    return normalized


def validate_step6_data(payload: dict) -> dict:
    parsed = Step6Data.model_validate(payload)
    return parsed.model_dump(mode="python")


def validate_step7_data(payload: dict) -> dict:
    parsed = Step7Data.model_validate(payload)
    return parsed.model_dump(mode="python")


def validation_error(prefix: str, exc: ValidationError) -> ValueError:
    details: list[str] = []
    for issue in exc.errors():
        loc = ".".join(str(x) for x in issue.get("loc", ()))
        msg = issue.get("msg", "validation error")
        details.append(f"{loc}: {msg}")
    return ValueError(f"{prefix}: {'; '.join(details)}")
