from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from typing import Any, Callable

from fastapi import HTTPException

from ..config import Settings
from ..core_bridge import ensure_core_import_path


ensure_core_import_path()

from pipeline.contracts import schema_column_set, validate_step3_data  # type: ignore  # noqa: E402
from pipeline.mysql_client import fetch_schema_metadata  # type: ignore  # noqa: E402
from pipeline.openai_client import call_responses, extract_output_json  # type: ignore  # noqa: E402


RULES_COPILOT_STEP_NAME = "rules_copilot_generate"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat().replace("+00:00", "Z")


@dataclass
class RuleCopilotExecutionTrace:
    step_name: str = RULES_COPILOT_STEP_NAME
    started_at: datetime | None = None
    finished_at: datetime | None = None
    response_received: bool = False
    failure_message: str | None = None
    reasoning_parts: list[str] = field(default_factory=list)
    stream_event_counts: dict[str, int] = field(default_factory=dict)
    status_events: list[str] = field(default_factory=list)

    def record_status(self, status: str, *, message: str | None = None) -> None:
        self.status_events.append(status)
        if status == "llm_request_started" and self.started_at is None:
            self.started_at = _utcnow()
            return
        if status == "llm_response_received":
            self.response_received = True
            if self.finished_at is None:
                self.finished_at = _utcnow()
            return
        if status == "llm_request_failed":
            if message:
                self.failure_message = message
            if self.finished_at is None:
                self.finished_at = _utcnow()

    def record_stream_event(self, kind: str, text: str | None = None) -> None:
        self.stream_event_counts[kind] = self.stream_event_counts.get(kind, 0) + 1
        if kind in {"reasoning_summary_delta", "reasoning_summary"} and text:
            self.reasoning_parts.append(text)

    def to_payload(self) -> dict[str, Any]:
        if self.finished_at is None and (self.response_received or self.failure_message):
            self.finished_at = _utcnow()
        duration_ms: int | None = None
        if self.started_at is not None and self.finished_at is not None:
            duration_ms = max(0, int((self.finished_at - self.started_at).total_seconds() * 1000))
        reasoning_summary = "".join(self.reasoning_parts).strip()
        return {
            "step_name": self.step_name,
            "request_started_at": _to_iso(self.started_at),
            "request_finished_at": _to_iso(self.finished_at),
            "duration_ms": duration_ms,
            "final_status": "failed" if self.failure_message else "succeeded",
            "response_received": self.response_received,
            "fallback_used": False,
            "failure_message": self.failure_message,
            "reasoning_summary": reasoning_summary or None,
            "reasoning_chars": len(reasoning_summary),
            "stream_event_counts": self.stream_event_counts,
            "status_events": self.status_events,
        }


def ensure_openai_key(settings: Settings) -> None:
    if settings.openai_api_key:
        return
    raise HTTPException(
        status_code=422,
        detail="OpenAI API key is not configured. Please configure OPENAI_API_KEY before running Copilot or jobs.",
    )


def fetch_schema_payload(settings: Settings) -> dict:
    return fetch_schema_metadata(
        settings.pim_mysql_host,
        settings.pim_mysql_user,
        settings.pim_mysql_password,
        settings.pim_mysql_db,
        settings.mysql_schema_tables,
    )


def allowed_fields_from_schema(schema_payload: dict) -> set[str]:
    return schema_column_set(schema_payload)


def validate_rule_payload(payload: dict, allowed_fields: set[str]) -> tuple[dict, dict]:
    try:
        normalized = validate_step3_data(payload, allowed_fields=allowed_fields)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"invalid field rules payload: {exc}") from exc

    bool_fields = [
        str(row.get("field") or "")
        for row in normalized.get("field_rules", [])
        if str(row.get("operator") or "") in {"bool_true", "bool_false"}
    ]
    if bool_fields:
        raise HTTPException(
            status_code=422,
            detail="bool_true/bool_false operators are disabled in current rule policy",
        )

    report = {
        "ok": True,
        "rule_count": len(normalized.get("field_rules", [])),
        "allowed_fields_count": len(allowed_fields),
        "bool_operator_enabled": False,
    }
    return normalized, report


def _build_rule_generation_prompts(
    *,
    schema_payload: dict,
    allowed_fields: set[str],
    user_prompt: str,
) -> tuple[str, str]:
    schema_json = json.dumps(schema_payload, ensure_ascii=False)
    fields_json = json.dumps(sorted(allowed_fields), ensure_ascii=False)
    normalized_prompt = user_prompt.strip()
    if not normalized_prompt:
        normalized_prompt = "请生成尽量实用的初稿规则，硬约束尽量少。"

    system_prompt = (
        "你是投标字段规则生成助手。请仅基于输入 schema 生成 field_rules。"
        "输出严格 JSON，格式必须为 {\"field_rules\":[...]}。"
        "每条规则必须包含 field/operator/is_hard/operator_confidence/hardness_confidence/rationale。"
        "operator 仅可使用: eq,gte,lte,gt,lt,between,in,contains。"
        "字段必须来自 allowed_fields。"
        "不要输出 bool_true/bool_false。"
        "用户 prompt 仅作为偏好，不可违反以上硬性约束。"
    )
    user_text = (
        "schema_json:\n"
        + schema_json
        + "\n\nallowed_fields:\n"
        + fields_json
        + "\n\n用户补充要求:\n"
        + normalized_prompt
    )
    return system_prompt, user_text


def generate_rules_with_llm(
    *,
    settings: Settings,
    schema_payload: dict,
    allowed_fields: set[str],
    model: str,
    user_prompt: str = "",
    on_stream_event: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[dict, dict]:
    ensure_openai_key(settings)
    system_prompt, user_text = _build_rule_generation_prompts(
        schema_payload=schema_payload,
        allowed_fields=allowed_fields,
        user_prompt=user_prompt,
    )
    trace = RuleCopilotExecutionTrace()
    trace.record_status("llm_request_started")
    if on_stream_event:
        on_stream_event({"kind": "status", "status": "llm_request_started"})

    def _handle_stream_event(event: dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        if kind == "reasoning_summary_delta":
            text = event.get("text")
            if isinstance(text, str) and text:
                trace.record_stream_event(kind, text)
                if on_stream_event:
                    on_stream_event({"kind": kind, "text": text})
            return
        if kind == "reasoning_summary":
            text = event.get("text")
            if isinstance(text, str) and text:
                trace.record_stream_event(kind, text)
                if on_stream_event:
                    on_stream_event({"kind": kind, "text": text})
            return

    try:
        response = call_responses(
            settings.openai_base_url,
            settings.openai_api_key,
            model,
            system_prompt=system_prompt,
            user_text=user_text,
            file_ids=[],
            tools=None,
            include=None,
            json_mode=True,
            on_stream_event=_handle_stream_event,
        )
        payload = extract_output_json(response)
        if not isinstance(payload, dict) or "field_rules" not in payload:
            raise HTTPException(status_code=502, detail="LLM did not return a valid field_rules payload")
    except HTTPException as exc:
        trace.record_status("llm_request_failed", message=str(exc.detail))
        if on_stream_event:
            on_stream_event({"kind": "status", "status": "llm_request_failed", "message": str(exc.detail)})
        raise
    except Exception as exc:
        trace.record_status("llm_request_failed", message=str(exc))
        if on_stream_event:
            on_stream_event({"kind": "status", "status": "llm_request_failed", "message": str(exc)})
        raise HTTPException(status_code=502, detail=f"LLM rule generation failed: {exc}") from exc

    trace.record_status("llm_response_received")
    summary_payload = trace.to_payload()
    if on_stream_event:
        on_stream_event({"kind": "status", "status": "llm_response_received"})
        on_stream_event({"kind": "execution_summary", "summary": summary_payload})
    return payload, summary_payload
