from __future__ import annotations

import json
import re
import uuid
from typing import Any

from app.core.models import SQLPlan, TenderRequirement, utcnow
from app.core.openai_web_search import build_web_search_kwargs, extract_web_search_info
from app.core.settings import settings


def _parse_json(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(0))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _coerce_params(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return {str(k): raw[k] for k in raw}
    return {}


def _dump_llm_output(payload: dict[str, Any]) -> str | None:
    try:
        out_dir = settings.runtime_dir / "llm_outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        file_id = str(uuid.uuid4())
        step = str(payload.get("step") or "llm")
        out_path = out_dir / f"{file_id}_{step}.json"
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        return str(out_path)
    except Exception:
        return None


def _response_to_dict(response: Any) -> dict[str, Any]:
    if response is None:
        return {}
    if isinstance(response, dict):
        return response
    for attr in ("model_dump", "dict"):
        handler = getattr(response, attr, None)
        if callable(handler):
            try:
                return handler()
            except Exception:
                pass
    handler = getattr(response, "json", None)
    if callable(handler):
        try:
            return json.loads(handler())
        except Exception:
            return {}
    return {}


def _extract_output_text(response: Any) -> str:
    raw_text = getattr(response, "output_text", None)
    if isinstance(raw_text, str) and raw_text.strip():
        return raw_text.strip()

    payload = _response_to_dict(response)
    if not payload:
        return ""

    texts: list[str] = []

    def _collect(node: Any) -> None:
        if isinstance(node, dict):
            node_type = node.get("type")
            if node_type in {"output_text", "text"}:
                text_val = node.get("text")
                if isinstance(text_val, str):
                    texts.append(text_val)
            for value in node.values():
                _collect(value)
            return
        if isinstance(node, list):
            for item in node:
                _collect(item)

    _collect(payload.get("output", payload))
    return "\n".join([item for item in texts if item]).strip()


class LLMSQLGenerator:
    def __init__(self) -> None:
        self.enabled = bool(settings.openai_api_key)
        self._client = None
        self.last_web_search: dict[str, Any] | None = None
        self.last_raw_output: str | None = None
        self.last_output_path: str | None = None
        if self.enabled:
            try:
                from openai import OpenAI

                self._client = OpenAI(
                    api_key=settings.openai_api_key,
                    timeout=settings.match_llm_timeout_sec,
                    max_retries=settings.match_llm_max_retries,
                )
            except Exception:
                self.enabled = False
                self._client = None

    def _responses_create(
        self,
        *,
        model: str,
        system_text: str,
        user_payload: dict[str, Any],
        max_output_tokens: int,
        enable_web_search: bool = True,
        payload_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled or self._client is None:
            raise RuntimeError("OPENAI_API_KEY not configured for SQL generation")
        attempt = 0
        parsed: dict[str, Any] = {}
        while attempt < 2:
            attempt += 1
            try:
                web_kwargs = build_web_search_kwargs() if enable_web_search else {}
                response = self._client.responses.create(
                    model=model,
                    input=[
                        {"role": "system", "content": system_text},
                        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                    ],
                    max_output_tokens=max_output_tokens,
                    reasoning={"effort": "low"},
                    text={"format": {"type": "json_object"}},
                    **web_kwargs,
                )
            except Exception as exc:
                self.last_raw_output = None
                self.last_output_path = _dump_llm_output(
                    {
                        "step": str(payload_meta.get("step") if payload_meta else "generate_sql"),
                        "created_at": utcnow().isoformat(),
                        "model": model,
                        "parse_ok": False,
                        "error": str(exc),
                        "payload_meta": payload_meta or {},
                        "attempt": attempt,
                    }
                )
                raise
            self.last_web_search = extract_web_search_info(response)
            raw_text = _extract_output_text(response)
            response_dump = None
            try:
                response_dump = response.model_dump()
            except Exception:
                try:
                    response_dump = response.to_dict()
                except Exception:
                    response_dump = {"repr": repr(response)}

            parsed = _parse_json(raw_text)
            self.last_raw_output = raw_text

            status = None
            incomplete_reason = None
            if isinstance(response_dump, dict):
                status = response_dump.get("status")
                incomplete = response_dump.get("incomplete_details")
                if isinstance(incomplete, dict):
                    incomplete_reason = incomplete.get("reason")

            retry_reason = None
            if not raw_text:
                retry_reason = "empty_raw_text"
            if status == "incomplete":
                retry_reason = retry_reason or "status_incomplete"
            if not raw_text and incomplete_reason:
                retry_reason = f"{retry_reason}:{incomplete_reason}" if retry_reason else incomplete_reason

            self.last_output_path = _dump_llm_output(
                {
                    "step": str(payload_meta.get("step") if payload_meta else "generate_sql"),
                    "created_at": utcnow().isoformat(),
                    "model": model,
                    "parse_ok": bool(parsed),
                    "raw_text": raw_text,
                    "payload_meta": payload_meta or {},
                    "response_dump": response_dump,
                    "attempt": attempt,
                    "retry_reason": retry_reason,
                }
            )

            if attempt < 2 and (not raw_text or status == "incomplete"):
                continue
            return parsed
        return parsed

    def generate(
        self,
        *,
        domain: str,
        requirements: list[TenderRequirement],
        schema_metadata: dict[str, Any],
        required_fields: list[str],
        top_k: int,
    ) -> SQLPlan:
        self.last_web_search = None
        if not self.enabled or self._client is None:
            return SQLPlan(
                domain=domain,
                sql="",
                params={},
                hard_clause_count=0,
                soft_clause_count=0,
                limit=top_k,
                validated=False,
                validation_errors=["LLM unavailable"],
                blocked=True,
                block_reason="LLM unavailable for SQL generation",
            )

        req_payload = [
            {
                "param_key": item.param_key,
                "operator": item.operator,
                "value": item.value,
                "unit": item.unit,
                "is_hard": item.is_hard,
                "product_key": item.product_key,
                "product_name": item.product_name,
                "quantity": item.quantity,
            }
            for item in requirements
        ]
        allowed_tables = schema_metadata.get("allowed_tables", [])
        allowed_fields = schema_metadata.get("allowed_fields", [])
        tables = schema_metadata.get("tables", [])

        payload = {
            "task": (
                "Generate a single SELECT-only SQL query to retrieve candidate products "
                "matching the tender requirements."
            ),
            "domain": domain,
            "requirements": req_payload,
            "schema": {
                "tables": tables,
                "allowed_tables": allowed_tables,
                "allowed_fields": allowed_fields,
            },
            "required_select_fields": required_fields,
            "limit": top_k,
            "output_schema": {
                "sql": "string, SELECT-only, must include LIMIT :limit",
                "params": "object of bind params, include limit",
            },
            "constraints": [
                "Return JSON only, no markdown",
                "Use only allowed tables and columns",
                "Do not use SELECT *",
                "Include all required_select_fields in the SELECT list",
                "Use LIMIT :limit and include it in params",
                "Prefer joins on product_id when needed",
                "No subqueries that modify data",
            ],
        }

        try:
            parsed = self._responses_create(
                model=settings.match_sql_model,
                system_text=(
                    "You generate safe SQL for tender matching. "
                    "Only output strict JSON with sql and params. "
                    "Use web search when you need external definitions."
                ),
                user_payload=payload,
                max_output_tokens=max(200, settings.match_sql_max_output_tokens),
                enable_web_search=False,
                payload_meta={
                    "step": "generate_sql",
                    "domain": domain,
                    "requirements_count": len(requirements),
                    "required_fields_count": len(required_fields),
                },
            )
        except Exception as exc:
            return SQLPlan(
                domain=domain,
                sql="",
                params={},
                hard_clause_count=0,
                soft_clause_count=0,
                limit=top_k,
                validated=False,
                validation_errors=[str(exc)],
                blocked=True,
                block_reason=f"LLM SQL generation failed: {exc}",
            )

        sql = str(parsed.get("sql") or "").strip()
        params = _coerce_params(parsed.get("params"))
        params.setdefault("limit", top_k)

        hard_count = sum(1 for item in requirements if item.is_hard)
        soft_count = sum(1 for item in requirements if not item.is_hard)

        if not sql:
            return SQLPlan(
                domain=domain,
                sql="",
                params=params,
                hard_clause_count=hard_count,
                soft_clause_count=soft_count,
                limit=top_k,
                validated=False,
                validation_errors=["missing sql"],
                blocked=True,
                block_reason="LLM returned empty SQL",
            )

        return SQLPlan(
            domain=domain,
            sql=sql,
            params=params,
            hard_clause_count=hard_count,
            soft_clause_count=soft_count,
            limit=top_k,
            validated=False,
            validation_errors=[],
            blocked=False,
            block_reason=None,
        )

    def repair(
        self,
        *,
        domain: str,
        previous_sql: str,
        validation_errors: list[str],
        requirements: list[TenderRequirement],
        schema_metadata: dict[str, Any],
        required_fields: list[str],
        top_k: int,
    ) -> SQLPlan | None:
        if not self.enabled or self._client is None:
            return None

        req_payload = [
            {
                "param_key": item.param_key,
                "operator": item.operator,
                "value": item.value,
                "unit": item.unit,
                "is_hard": item.is_hard,
                "product_key": item.product_key,
                "product_name": item.product_name,
                "quantity": item.quantity,
            }
            for item in requirements
        ]

        payload = {
            "task": "Fix the SQL to satisfy validation errors while keeping intent.",
            "domain": domain,
            "previous_sql": previous_sql,
            "validation_errors": validation_errors,
            "requirements": req_payload,
            "schema": schema_metadata,
            "required_select_fields": required_fields,
            "limit": top_k,
            "output_schema": {
                "sql": "fixed SELECT-only SQL with LIMIT :limit",
                "params": "object of bind params, include limit",
            },
            "constraints": [
                "Return JSON only, no markdown",
                "Use only allowed tables and columns",
                "Do not use SELECT *",
                "Include all required_select_fields in the SELECT list",
                "Use LIMIT :limit and include it in params",
            ],
        }

        try:
            parsed = self._responses_create(
                model=settings.match_sql_model,
                system_text=(
                    "You repair SQL safely. "
                    "Only output strict JSON with sql and params. "
                    "Use web search when you need external definitions."
                ),
                user_payload=payload,
                max_output_tokens=max(200, settings.match_sql_max_output_tokens),
                enable_web_search=False,
                payload_meta={
                    "step": "repair_sql",
                    "domain": domain,
                    "requirements_count": len(requirements),
                    "required_fields_count": len(required_fields),
                },
            )
        except Exception:
            return None

        sql = str(parsed.get("sql") or "").strip()
        if not sql:
            return None

        params = _coerce_params(parsed.get("params"))
        params.setdefault("limit", top_k)

        hard_count = sum(1 for item in requirements if item.is_hard)
        soft_count = sum(1 for item in requirements if not item.is_hard)

        return SQLPlan(
            domain=domain,
            sql=sql,
            params=params,
            hard_clause_count=hard_count,
            soft_clause_count=soft_count,
            limit=top_k,
            validated=False,
            validation_errors=[],
            blocked=False,
            block_reason=None,
        )
