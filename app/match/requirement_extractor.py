from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

from app.core.models import ProductRequirementScope, RequirementSet, TenderRequirement, utcnow
from app.core.openai_web_search import build_web_search_kwargs, extract_web_search_info
from app.core.settings import settings

from .context import ContextLine
from .metadata import DomainMetadata, MappingDefinition, normalize_text, term_matches_text

_VALID_OPERATORS = {
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
_NUMERIC_OPERATORS = {"eq", "gte", "lte", "gt", "lt", "between"}
_BOOLEAN_OPERATORS = {"eq", "bool_true", "bool_false"}

_NUMBER_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*([a-zA-Z]{1,10})?")
_BETWEEN_RE = re.compile(
    r"(?:between|zwischen|在)\s*(-?\d+(?:\.\d+)?)\s*(?:and|und|到|至)\s*(-?\d+(?:\.\d+)?)",
    re.I,
)
_IP_RE = re.compile(r"\bIP\s*([0-9]{2})\b", re.I)
_BOOL_NEGATIVE_HINTS = (
    " not ",
    " without ",
    "kein",
    "nicht",
    "无需",
    "不需要",
    "无",
)


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


def _coerce_confidence(raw: Any, fallback: float = 0.0) -> float:
    try:
        value = float(raw)
    except Exception:
        value = fallback
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return round(value, 4)


def _coerce_bool(raw: Any, fallback: bool) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    if isinstance(raw, str):
        text = raw.strip().lower()
        if text in {"true", "yes", "1"}:
            return True
        if text in {"false", "no", "0"}:
            return False
    return fallback


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


def _normalize_product_key(raw: Any, index: int) -> str:
    if isinstance(raw, str):
        value = raw.strip().lower()
    else:
        value = ""
    value = re.sub(r"[^a-z0-9_:-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        return f"product_{index:02d}"
    return value[:64]


def _contains_any(text: str, options: list[str]) -> bool:
    return any(term_matches_text(text, item) for item in options if item)


def _is_hard_requirement(line_norm: str, meta: DomainMetadata, default_hard: bool) -> bool:
    if _contains_any(line_norm, meta.hard_keywords):
        return True
    if _contains_any(line_norm, meta.soft_keywords):
        return False
    return default_hard


def _extract_numeric(line_text: str) -> tuple[float | None, str | None]:
    match = _NUMBER_RE.search(line_text)
    if not match:
        return None, None
    try:
        value = float(match.group(1))
    except ValueError:
        return None, None
    unit = (match.group(2) or "").strip().lower() or None
    return value, unit


def _extract_operator(line_text: str, line_norm: str, meta: DomainMetadata) -> str:
    if ">=" in line_text:
        return "gte"
    if "<=" in line_text:
        return "lte"
    if " > " in line_text:
        return "gt"
    if " < " in line_text:
        return "lt"

    for op in ("gte", "lte", "gt", "lt"):
        terms = meta.comparison_keywords.get(op, [])
        if _contains_any(line_norm, terms):
            return op
    return "eq"


def _extract_requirement_value(
    *,
    line_text: str,
    line_norm: str,
    mapping_value_type: str,
    mapped_unit: str | None,
    meta: DomainMetadata,
) -> tuple[str, object, str | None, float]:
    between_match = _BETWEEN_RE.search(line_text)
    if between_match and mapping_value_type in {"number", "integer", "float"}:
        low = float(between_match.group(1))
        high = float(between_match.group(2))
        return "between", [low, high], mapped_unit, 0.85

    if mapping_value_type in {"boolean", "bool"}:
        negative = any(token in f" {line_norm} " for token in _BOOL_NEGATIVE_HINTS)
        return ("bool_false", False, None, 0.92) if negative else ("bool_true", True, None, 0.92)

    if "ip" in line_norm and mapping_value_type in {"text", "string"}:
        ip_match = _IP_RE.search(line_text)
        if ip_match:
            return "contains", f"IP{ip_match.group(1)}", None, 0.9

    number, unit = _extract_numeric(line_text)
    if number is not None:
        operator = _extract_operator(line_text, line_norm, meta)
        normalized_value, normalized_unit = meta.normalize_unit_value(number, unit, mapped_unit)
        return operator, normalized_value, normalized_unit, 0.82

    if any(sep in line_text for sep in [",", "/", "、"]):
        parts = [x.strip() for x in re.split(r"[,/、]", line_text) if x.strip()]
        if 2 <= len(parts) <= 8:
            return "in", parts, mapped_unit, 0.6

    return "eq", line_text.strip(), mapped_unit, 0.45


def _line_window(lines: list[ContextLine], *, max_chars: int) -> list[ContextLine]:
    if max_chars <= 0:
        return lines
    out: list[ContextLine] = []
    size = 0
    for line in lines:
        text = line.text.strip()
        if not text:
            continue
        row_cost = len(text) + 32
        if out and size + row_cost > max_chars:
            break
        out.append(line)
        size += row_cost
    return out


def _resolve_mapping(meta: DomainMetadata, raw_param_key: str) -> MappingDefinition | None:
    direct = meta.mapping_by_key(raw_param_key)
    if direct is not None:
        return direct
    candidates = meta.candidate_mappings(raw_param_key)
    if len(candidates) == 1:
        return candidates[0]
    return None


def _normalize_numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


@dataclass
class _ValidatedLLMRequirement:
    param_key: str
    operator: str
    value: Any
    unit: str | None
    is_hard: bool
    confidence: float
    evidence_refs: list[str]
    raw_text: str | None


def _validate_llm_requirement(
    *,
    item: dict[str, Any],
    meta: DomainMetadata,
    allowed_evidence: set[str],
) -> _ValidatedLLMRequirement | None:
    raw_param_key = str(item.get("param_key") or "").strip()
    mapping = _resolve_mapping(meta, raw_param_key)
    if mapping is None:
        return None

    operator = str(item.get("operator") or "eq").strip().lower()
    if operator not in _VALID_OPERATORS:
        operator = "eq"
    if operator not in mapping.operators:
        if mapping.value_type.lower().strip() in {"boolean", "bool"}:
            operator = "bool_true"
        elif "eq" in mapping.operators:
            operator = "eq"
        else:
            return None
        if operator not in mapping.operators:
            return None

    value = item.get("value")
    unit = (str(item.get("unit") or "").strip().lower() or None)
    value_type = mapping.value_type.lower().strip()

    if value_type in {"boolean", "bool"}:
        if operator == "bool_true":
            value = True
        elif operator == "bool_false":
            value = False
        else:
            value = _coerce_bool(value, fallback=True)
        unit = None
    elif operator in _NUMERIC_OPERATORS and value_type in {"number", "integer", "float"}:
        if operator == "between":
            if not isinstance(value, list) or len(value) != 2:
                return None
            low = _normalize_numeric_value(value[0])
            high = _normalize_numeric_value(value[1])
            if low is None or high is None:
                return None
            normalized_low, normalized_unit = meta.normalize_unit_value(low, unit, mapping.unit)
            normalized_high, normalized_unit = meta.normalize_unit_value(high, unit, mapping.unit)
            value = [normalized_low, normalized_high]
            unit = normalized_unit
        else:
            numeric = _normalize_numeric_value(value)
            if numeric is None:
                return None
            normalized_value, normalized_unit = meta.normalize_unit_value(numeric, unit, mapping.unit)
            value = normalized_value
            unit = normalized_unit
    else:
        if operator == "between":
            return None
        if operator == "in" and not isinstance(value, list):
            return None
        if value is None:
            return None
        if mapping.unit and unit is None:
            unit = mapping.unit

    is_hard = _coerce_bool(item.get("is_hard"), fallback=mapping.hard_default)
    confidence = _coerce_confidence(item.get("confidence"), fallback=0.55)

    evidence_refs: list[str] = []
    raw_evidence = item.get("evidence_refs", [])
    if isinstance(raw_evidence, list):
        for row in raw_evidence:
            ref = str(row).strip()
            if not ref:
                continue
            if ref in allowed_evidence:
                evidence_refs.append(ref)
    evidence_refs = evidence_refs[:8]

    raw_text = str(item.get("raw_text") or "").strip() or None

    return _ValidatedLLMRequirement(
        param_key=mapping.canonical_key,
        operator=operator,
        value=value,
        unit=unit,
        is_hard=is_hard,
        confidence=confidence,
        evidence_refs=evidence_refs,
        raw_text=raw_text,
    )


class LLMRequirementExtractor:
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
        payload_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled or self._client is None:
            raise RuntimeError("OPENAI_API_KEY not configured for requirement extraction")
        attempt = 0
        parsed: dict[str, Any] = {}
        while attempt < 2:
            attempt += 1
            try:
                web_kwargs = build_web_search_kwargs()
                if web_kwargs:
                    web_kwargs = {}
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
                        "step": "extract_requirements",
                        "created_at": utcnow().isoformat(),
                        "model": model,
                        "package_id": user_payload.get("package_id"),
                        "domain": user_payload.get("domain"),
                        "parse_ok": False,
                        "error": str(exc),
                        "payload_meta": payload_meta or {},
                        "attempt": attempt,
                    }
                )
                raise
            self.last_web_search = extract_web_search_info(response)
            raw_text = (response.output_text or "").strip()
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
            retry_reason = None
            status = None
            incomplete_reason = None
            if isinstance(response_dump, dict):
                status = response_dump.get("status")
                incomplete = response_dump.get("incomplete_details")
                if isinstance(incomplete, dict):
                    incomplete_reason = incomplete.get("reason")

            if not raw_text:
                retry_reason = "empty_raw_text"
            if status == "incomplete":
                retry_reason = retry_reason or "status_incomplete"
            if not raw_text and incomplete_reason:
                retry_reason = f"{retry_reason}:{incomplete_reason}" if retry_reason else incomplete_reason

            self.last_output_path = _dump_llm_output(
                {
                    "step": "extract_requirements",
                    "created_at": utcnow().isoformat(),
                    "model": model,
                    "package_id": user_payload.get("package_id"),
                    "domain": user_payload.get("domain"),
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

    def _extract_chunk(
        self,
        *,
        model: str,
        system_text: str,
        user_payload: dict[str, Any],
        max_output_tokens: int,
        payload_meta: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            return self._responses_create(
                model=model,
                system_text=system_text,
                user_payload=user_payload,
                max_output_tokens=max_output_tokens,
                payload_meta=payload_meta,
            )
        except Exception:
            return None

    def extract(
        self,
        *,
        package_id: str,
        domain: str,
        meta: DomainMetadata,
        context_lines: list[ContextLine],
    ) -> RequirementSet | None:
        self.last_web_search = None
        if not self.enabled or self._client is None or not context_lines:
            return None

        max_chars = settings.match_extract_max_chars
        if max_chars <= 0:
            max_chars = 0
        else:
            max_chars = max(10_000, max_chars)
        trimmed = _line_window(
            context_lines,
            max_chars=max_chars,
        )
        if not trimmed:
            return None
        allowed_evidence = {line.evidence_ref for line in trimmed}

        # Compact representation keeps full semantic text while reducing JSON overhead.
        content_lines = [
            {
                "evidence_ref": line.evidence_ref,
                "doc_id": line.doc_id,
                "text": line.text,
            }
            for line in trimmed
        ]
        document_excerpt = "\n".join(
            f"[{row['evidence_ref']}] ({row['doc_id']}) {row['text']}"
            for row in content_lines
        )
        payload_meta = {
            "line_count": len(trimmed),
            "doc_count": len({line.doc_id for line in trimmed}),
            "excerpt_chars": len(document_excerpt),
            "max_chars": max_chars,
        }

        payload = {
            "task": (
                "Extract product requirements from a tender LV/BOQ/price schedule. "
                "Primary output: product types + quantities. "
                "Secondary output: technical constraints (IP, UGR, CRI, wattage, CCT, etc.) if present. "
                "The file may contain multiple product types; detect all product scopes."
            ),
            "package_id": package_id,
            "domain": domain,
            "document_excerpt": document_excerpt,
            "output_schema": {
                "products": [
                    {
                        "product_key": "stable key, such as typ_01 / line_item_01",
                        "product_name": "human readable product name",
                        "quantity": "number or null",
                        "requirements": [
                            {
                                "param_key": "must map to canonical key",
                                "operator": "eq|gte|lte|gt|lt|between|in|contains|bool_true|bool_false",
                                "value": "json value",
                                "unit": "unit or null",
                                "is_hard": "boolean",
                                "confidence": "0..1",
                                "evidence_refs": ["must copy refs that appear inside [ref] in document_excerpt"],
                                "raw_text": "supporting source text",
                            }
                        ],
                    }
                ],
                "global_requirements": [
                    {
                        "param_key": "canonical key",
                        "operator": "same enum as above",
                        "value": "json value",
                        "unit": "unit or null",
                        "is_hard": "boolean",
                        "confidence": "0..1",
                        "evidence_refs": ["must copy refs that appear inside [ref] in document_excerpt"],
                        "raw_text": "source text",
                    }
                ],
            },
            "constraints": [
                "Return JSON only, no markdown",
                "param_key should be a short semantic label from the document (e.g., IP rating, wattage, lumen)",
                "If multiple products exist, split requirements by product",
                "If uncertain, skip low-confidence requirements instead of guessing",
                "If technical parameters are missing, still output product_name and quantity at product level",
                "Treat 'Typ XX' / 'Leuchte Typ XX' blocks as product scopes",
                "Use nearby Beschreibung/Description lines as product_name when available",
                "Use Stückzahl/quantité/quantity columns to populate quantity",
                "Limit output size: at most 40 products",
                "For each product, at most 30 requirements",
            ],
        }

        system_text = (
            "You extract LV/BOQ price schedules for lighting tenders. "
            "You understand German/English/French technical language and output deterministic JSON. "
            "Always capture product_name and quantity if present, even when technical specs are missing. "
            "No canonical mapping list is provided; infer semantic labels directly from the text."
        )

        parsed = self._extract_chunk(
            model=settings.openai_model,
            system_text=system_text,
            user_payload=payload,
            max_output_tokens=max(500, settings.match_extract_max_output_tokens),
            payload_meta=payload_meta,
        )

        if not parsed:
            parsed = {}

        raw_products = parsed.get("products", [])
        if not isinstance(raw_products, list) or not raw_products:
            # Chunked fallback: split by product scopes detected in text markers.
            markers = ("typ ", "leuchte typ", "type ")
            chunks: list[list[ContextLine]] = []
            current: list[ContextLine] = []
            for line in trimmed:
                text_norm = normalize_text(line.text)
                if current and any(marker in text_norm for marker in markers):
                    chunks.append(current)
                    current = []
                current.append(line)
            if current:
                chunks.append(current)

            if len(chunks) > 1:
                merged: dict[str, Any] = {"products": [], "global_requirements": []}
                for idx, chunk in enumerate(chunks, start=1):
                    chunk_excerpt = "\n".join(
                        f"[{line.evidence_ref}] ({line.doc_id}) {line.text}"
                        for line in chunk
                    )
                    chunk_payload = {
                        **payload,
                        "document_excerpt": chunk_excerpt,
                    }
                    chunk_meta = {
                        "line_count": len(chunk),
                        "doc_count": len({line.doc_id for line in chunk}),
                        "excerpt_chars": len(chunk_excerpt),
                        "max_chars": max_chars,
                        "chunk_index": idx,
                        "chunk_total": len(chunks),
                    }
                    chunk_parsed = self._extract_chunk(
                        model=settings.openai_model,
                        system_text=system_text,
                        user_payload=chunk_payload,
                        max_output_tokens=max(500, settings.match_extract_max_output_tokens),
                        payload_meta=chunk_meta,
                    )
                    if not chunk_parsed:
                        continue
                    chunk_products = chunk_parsed.get("products", [])
                    if isinstance(chunk_products, list):
                        merged["products"].extend(chunk_products)
                    chunk_global = chunk_parsed.get("global_requirements", [])
                    if isinstance(chunk_global, list):
                        merged["global_requirements"].extend(chunk_global)
                parsed = merged

        raw_products = parsed.get("products", [])
        raw_global = parsed.get("global_requirements", [])

        product_scopes: list[ProductRequirementScope] = []
        requirements: list[TenderRequirement] = []
        req_counter = 0
        dedup: set[str] = set()

        if isinstance(raw_products, list):
            for product_idx, product in enumerate(raw_products, start=1):
                if not isinstance(product, dict):
                    continue
                product_key = _normalize_product_key(product.get("product_key"), product_idx)
                product_name = str(product.get("product_name") or "").strip() or None
                quantity_raw = product.get("quantity")
                quantity = _normalize_numeric_value(quantity_raw)

                scope_requirements: list[TenderRequirement] = []
                raw_requirements = product.get("requirements", [])
                if isinstance(raw_requirements, list):
                    for item in raw_requirements:
                        if not isinstance(item, dict):
                            continue
                        validated = _validate_llm_requirement(
                            item=item,
                            meta=meta,
                            allowed_evidence=allowed_evidence,
                        )
                        if validated is None:
                            continue
                        dedup_key = (
                            f"{product_key}|{validated.param_key}|{validated.operator}|"
                            f"{validated.value}|{validated.is_hard}"
                        )
                        if dedup_key in dedup:
                            continue
                        dedup.add(dedup_key)
                        req_counter += 1
                        req = TenderRequirement(
                            requirement_id=f"{product_key}_req_{req_counter:03d}",
                            param_key=validated.param_key,
                            operator=validated.operator,  # type: ignore[arg-type]
                            value=validated.value,
                            unit=validated.unit,
                            is_hard=validated.is_hard,
                            product_key=product_key,
                            product_name=product_name,
                            quantity=quantity,
                            evidence_refs=validated.evidence_refs,
                            raw_text=validated.raw_text,
                            confidence=validated.confidence,
                        )
                        scope_requirements.append(req)
                        requirements.append(req)

                if scope_requirements:
                    product_scopes.append(
                        ProductRequirementScope(
                            product_key=product_key,
                            product_name=product_name,
                            quantity=quantity,
                            requirements=scope_requirements,
                        )
                    )

        if isinstance(raw_global, list):
            for item in raw_global:
                if not isinstance(item, dict):
                    continue
                validated = _validate_llm_requirement(
                    item=item,
                    meta=meta,
                    allowed_evidence=allowed_evidence,
                )
                if validated is None:
                    continue
                dedup_key = (
                    f"global|{validated.param_key}|{validated.operator}|"
                    f"{validated.value}|{validated.is_hard}"
                )
                if dedup_key in dedup:
                    continue
                dedup.add(dedup_key)
                req_counter += 1
                requirements.append(
                    TenderRequirement(
                        requirement_id=f"req_{req_counter:03d}",
                        param_key=validated.param_key,
                        operator=validated.operator,  # type: ignore[arg-type]
                        value=validated.value,
                        unit=validated.unit,
                        is_hard=validated.is_hard,
                        evidence_refs=validated.evidence_refs,
                        raw_text=validated.raw_text,
                        confidence=validated.confidence,
                    )
                )

        if not requirements and not product_scopes:
            return None

        return RequirementSet(
            package_id=package_id,
            domain=domain,
            requirements=requirements,
            product_scopes=product_scopes,
            generated_at=utcnow(),
        )


def _extract_requirements_rule_based(
    *,
    package_id: str,
    domain: str,
    meta: DomainMetadata,
    context_lines: list[ContextLine],
) -> RequirementSet:
    requirements: list[TenderRequirement] = []
    seen: set[str] = set()
    req_counter = 0

    for line in context_lines:
        line_text = line.text.strip()
        if not line_text:
            continue
        line_norm = normalize_text(line_text)
        if len(line_norm) < 3:
            continue

        for mapping in meta.matching_mappings_for_text(line_norm):
            operator, value, unit, confidence = _extract_requirement_value(
                line_text=line_text,
                line_norm=line_norm,
                mapping_value_type=mapping.value_type,
                mapped_unit=mapping.unit,
                meta=meta,
            )
            is_hard = _is_hard_requirement(
                line_norm=line_norm,
                meta=meta,
                default_hard=mapping.hard_default,
            )
            dedup_key = f"{mapping.canonical_key}|{operator}|{value}|{is_hard}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            req_counter += 1
            requirements.append(
                TenderRequirement(
                    requirement_id=f"req_{req_counter:03d}",
                    param_key=mapping.canonical_key,
                    operator=operator,  # type: ignore[arg-type]
                    value=value,
                    unit=unit,
                    is_hard=is_hard,
                    evidence_refs=[line.evidence_ref],
                    raw_text=line_text,
                    confidence=round(confidence, 4),
                )
            )

    return RequirementSet(
        package_id=package_id,
        domain=domain,
        requirements=requirements,
        generated_at=utcnow(),
    )


def extract_requirements(
    *,
    package_id: str,
    domain: str,
    meta: DomainMetadata,
    context_lines: list[ContextLine],
) -> RequirementSet | None:
    llm = LLMRequirementExtractor()
    return llm.extract(
        package_id=package_id,
        domain=domain,
        meta=meta,
        context_lines=context_lines,
    )
