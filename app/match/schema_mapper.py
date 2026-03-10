from __future__ import annotations

import json

from app.core.models import SchemaMapping, TenderRequirement
from app.core.settings import settings

from .metadata import DomainMetadata, MappingDefinition

_NUMERIC_OPERATORS = {"eq", "gte", "lte", "gt", "lt", "between"}
_BOOLEAN_OPERATORS = {"bool_true", "bool_false", "eq"}
_VALID_MAPPING_MODES = {"hybrid", "llm_only"}


def _coerce_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except Exception:
        return None


def _value_kind_error(requirement: TenderRequirement, mapping: MappingDefinition) -> str | None:
    if requirement.operator not in mapping.operators:
        return (
            f"operator '{requirement.operator}' is not allowed for '{mapping.canonical_key}' "
            f"(allowed={mapping.operators})"
        )

    value_type = mapping.value_type.lower().strip()
    if value_type in {"boolean", "bool"}:
        if requirement.operator not in _BOOLEAN_OPERATORS:
            return f"boolean mapping requires one of {_BOOLEAN_OPERATORS}, got '{requirement.operator}'"
        if requirement.operator == "eq":
            if isinstance(requirement.value, bool):
                return None
            if isinstance(requirement.value, (int, float)) and requirement.value in {0, 1}:
                return None
            if isinstance(requirement.value, str) and requirement.value.strip().lower() in {
                "true",
                "false",
                "yes",
                "no",
                "0",
                "1",
            }:
                return None
            return "boolean eq expects true/false-style value"
        return None

    if value_type in {"number", "integer", "float"} and requirement.operator in _NUMERIC_OPERATORS:
        if requirement.operator == "between":
            if not isinstance(requirement.value, list) or len(requirement.value) != 2:
                return "between requires two numeric values"
            vals = [_coerce_float(requirement.value[0]), _coerce_float(requirement.value[1])]
            if any(item is None for item in vals):
                return "between contains non-numeric values"
            numeric_values = [float(vals[0]), float(vals[1])]
        else:
            numeric = _coerce_float(requirement.value)
            if numeric is None:
                return "numeric operator requires numeric value"
            numeric_values = [numeric]

        if mapping.min_value is not None and min(numeric_values) < mapping.min_value:
            return f"value below min_value={mapping.min_value}"
        if mapping.max_value is not None and max(numeric_values) > mapping.max_value:
            return f"value above max_value={mapping.max_value}"

    return None


def _mapping_guardrail_error(
    requirement: TenderRequirement,
    mapping: MappingDefinition,
) -> str | None:
    if requirement.is_hard and requirement.confidence < settings.match_hard_min_confidence:
        return (
            f"hard requirement confidence too low "
            f"({requirement.confidence:.2f} < {settings.match_hard_min_confidence:.2f})"
        )
    return _value_kind_error(requirement, mapping)


def _unmapped_due_to_guardrail(
    req: TenderRequirement,
    *,
    reason: str,
    candidate_fields: list[str],
) -> SchemaMapping:
    return SchemaMapping(
        requirement_id=req.requirement_id,
        param_key=req.param_key,
        operator=req.operator,
        value=req.value,
        is_hard=req.is_hard,
        status="unmapped",
        confidence=req.confidence,
        reason=reason,
        candidate_fields=candidate_fields,
        evidence_refs=req.evidence_refs,
    )


def _mapped(
    req: TenderRequirement,
    *,
    mapped_field: str,
    confidence: float,
    reason: str,
    candidate_fields: list[str],
) -> SchemaMapping:
    return SchemaMapping(
        requirement_id=req.requirement_id,
        param_key=req.param_key,
        mapped_table=mapped_field.split(".", 1)[0],
        mapped_field=mapped_field,
        operator=req.operator,
        value=req.value,
        is_hard=req.is_hard,
        status="mapped",
        confidence=confidence,
        reason=reason,
        candidate_fields=candidate_fields,
        evidence_refs=req.evidence_refs,
    )


def _append_hard_unmet(
    req: TenderRequirement,
    unmet_constraints: list[str],
    *,
    message: str,
) -> None:
    if req.is_hard:
        unmet_constraints.append(f"{req.requirement_id}: {message} ({req.param_key})")


def _normalize_mapping_mode() -> str:
    mode = settings.match_mapping_mode.strip().lower()
    if mode not in _VALID_MAPPING_MODES:
        return "hybrid"
    return mode


class LLMDisambiguator:
    def __init__(self) -> None:
        self.enabled = bool(settings.openai_api_key)
        self._client = None
        self._cache: dict[str, tuple[str | None, float, str]] = {}
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

    def choose_field(
        self,
        *,
        requirement: TenderRequirement,
        candidate_mappings: list[MappingDefinition],
    ) -> tuple[str | None, float, str]:
        candidate_fields = [item.field for item in candidate_mappings]
        if not self.enabled or self._client is None:
            return None, 0.0, "LLM unavailable"
        cache_key = json.dumps(
            {
                "param_key": requirement.param_key,
                "operator": requirement.operator,
                "value": requirement.value,
                "is_hard": requirement.is_hard,
                "candidate_fields": candidate_fields,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        payload = {
            "task": "choose the best database field for one tender requirement",
            "requirement": requirement.model_dump(mode="json"),
            "candidate_fields": [
                {
                    "field": item.field,
                    "canonical_key": item.canonical_key,
                    "synonyms": item.synonyms,
                    "value_type": item.value_type,
                    "unit": item.unit,
                    "operators": item.operators,
                    "min_value": item.min_value,
                    "max_value": item.max_value,
                }
                for item in candidate_mappings
            ],
            "output_schema": {
                "field": "must be one value from candidate_fields.field",
                "confidence": "0..1 float",
                "reason": "short reason",
            },
            "constraints": [
                "Return JSON only",
                "Do not invent fields",
                "Use multilingual semantic matching based on requirement text and value semantics",
            ],
        }

        try:
            response = self._client.responses.create(
                model=settings.match_mapper_model,
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You map tender requirements to database fields. "
                            "Prioritize semantic and unit compatibility. Return strict JSON only."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(payload, ensure_ascii=False),
                    },
                ],
                max_output_tokens=max(80, settings.match_mapper_max_output_tokens),
            )
            text = response.output_text.strip()
            parsed = json.loads(text)
            field = str(parsed.get("field", "")).strip()
            confidence = float(parsed.get("confidence", 0.0))
            reason = str(parsed.get("reason", "LLM disambiguation"))
            if field not in candidate_fields:
                result = (None, 0.0, "LLM selected out-of-candidate field")
                self._cache[cache_key] = result
                return result
            if confidence < settings.match_llm_confidence_threshold:
                result = (None, confidence, f"LLM confidence below threshold ({confidence:.2f})")
                self._cache[cache_key] = result
                return result
            result = (field, confidence, reason)
            self._cache[cache_key] = result
            return result
        except Exception as exc:
            result = (None, 0.0, f"LLM disambiguation failed: {exc}")
            self._cache[cache_key] = result
            return result


def _map_requirement_hybrid(
    *,
    req: TenderRequirement,
    meta: DomainMetadata,
    by_field: dict[str, MappingDefinition],
    disambiguator: LLMDisambiguator,
    unmet_constraints: list[str],
) -> SchemaMapping:
    direct = meta.mapping_by_key(req.param_key)
    if direct is not None:
        guardrail_error = _mapping_guardrail_error(req, direct)
        if guardrail_error is not None:
            _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
            return _unmapped_due_to_guardrail(
                req,
                reason=f"mapping guardrail failed: {guardrail_error}",
                candidate_fields=[direct.field],
            )
        return _mapped(
            req,
            mapped_field=direct.field,
            confidence=max(req.confidence, 0.9),
            reason="rule match on canonical key",
            candidate_fields=[direct.field],
        )

    candidates = meta.candidate_mappings(req.param_key)
    candidate_fields = [item.field for item in candidates]
    if len(candidates) == 1:
        chosen = candidates[0]
        guardrail_error = _mapping_guardrail_error(req, chosen)
        if guardrail_error is not None:
            _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
            return _unmapped_due_to_guardrail(
                req,
                reason=f"mapping guardrail failed: {guardrail_error}",
                candidate_fields=[chosen.field],
            )
        return _mapped(
            req,
            mapped_field=chosen.field,
            confidence=max(req.confidence, 0.78),
            reason="rule match on synonym",
            candidate_fields=[chosen.field],
        )

    if len(candidates) >= 2:
        chosen_field, confidence, reason = disambiguator.choose_field(
            requirement=req,
            candidate_mappings=candidates,
        )
        if chosen_field:
            chosen_meta = by_field.get(chosen_field)
            if chosen_meta is None:
                _append_hard_unmet(req, unmet_constraints, message="hard requirement mapped to unknown field")
                return SchemaMapping(
                    requirement_id=req.requirement_id,
                    param_key=req.param_key,
                    operator=req.operator,
                    value=req.value,
                    is_hard=req.is_hard,
                    status="unmapped",
                    confidence=confidence,
                    reason="LLM selected field not present in metadata map",
                    candidate_fields=candidate_fields,
                    evidence_refs=req.evidence_refs,
                )
            guardrail_error = _mapping_guardrail_error(req, chosen_meta)
            if guardrail_error is not None:
                _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
                return _unmapped_due_to_guardrail(
                    req,
                    reason=f"mapping guardrail failed: {guardrail_error}",
                    candidate_fields=candidate_fields,
                )
            return _mapped(
                req,
                mapped_field=chosen_field,
                confidence=confidence,
                reason=reason,
                candidate_fields=candidate_fields,
            )

        _append_hard_unmet(req, unmet_constraints, message="hard requirement ambiguous mapping")
        return SchemaMapping(
            requirement_id=req.requirement_id,
            param_key=req.param_key,
            operator=req.operator,
            value=req.value,
            is_hard=req.is_hard,
            status="ambiguous",
            confidence=confidence,
            reason=reason,
            candidate_fields=candidate_fields,
            evidence_refs=req.evidence_refs,
        )

    _append_hard_unmet(req, unmet_constraints, message="hard requirement unmapped")
    return SchemaMapping(
        requirement_id=req.requirement_id,
        param_key=req.param_key,
        operator=req.operator,
        value=req.value,
        is_hard=req.is_hard,
        status="unmapped",
        confidence=0.0,
        reason="no mapping candidate found",
        candidate_fields=[],
        evidence_refs=req.evidence_refs,
    )


def _map_requirement_llm_only(
    *,
    req: TenderRequirement,
    meta: DomainMetadata,
    all_candidates: list[MappingDefinition],
    by_field: dict[str, MappingDefinition],
    disambiguator: LLMDisambiguator,
    unmet_constraints: list[str],
) -> SchemaMapping:
    direct = meta.mapping_by_key(req.param_key)
    if direct is not None:
        guardrail_error = _mapping_guardrail_error(req, direct)
        if guardrail_error is not None:
            _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
            return _unmapped_due_to_guardrail(
                req,
                reason=f"mapping guardrail failed: {guardrail_error}",
                candidate_fields=[direct.field],
            )
        return _mapped(
            req,
            mapped_field=direct.field,
            confidence=max(req.confidence, 0.85),
            reason="canonical key resolved in llm_only mode",
            candidate_fields=[direct.field],
        )

    narrowed_candidates = meta.candidate_mappings(req.param_key)
    if len(narrowed_candidates) == 1:
        chosen = narrowed_candidates[0]
        guardrail_error = _mapping_guardrail_error(req, chosen)
        if guardrail_error is not None:
            _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
            return _unmapped_due_to_guardrail(
                req,
                reason=f"mapping guardrail failed: {guardrail_error}",
                candidate_fields=[chosen.field],
            )
        return _mapped(
            req,
            mapped_field=chosen.field,
            confidence=max(req.confidence, 0.8),
            reason="single semantic candidate in llm_only mode",
            candidate_fields=[chosen.field],
        )

    llm_candidates = narrowed_candidates if len(narrowed_candidates) >= 2 else all_candidates
    candidate_fields = [item.field for item in llm_candidates]
    chosen_field, confidence, reason = disambiguator.choose_field(
        requirement=req,
        candidate_mappings=llm_candidates,
    )
    if not chosen_field:
        _append_hard_unmet(req, unmet_constraints, message="hard requirement ambiguous mapping")
        return SchemaMapping(
            requirement_id=req.requirement_id,
            param_key=req.param_key,
            operator=req.operator,
            value=req.value,
            is_hard=req.is_hard,
            status="ambiguous",
            confidence=confidence,
            reason=f"llm_only mapping failed: {reason}",
            candidate_fields=candidate_fields,
            evidence_refs=req.evidence_refs,
        )

    chosen_meta = by_field.get(chosen_field)
    if chosen_meta is None:
        _append_hard_unmet(req, unmet_constraints, message="hard requirement mapped to unknown field")
        return SchemaMapping(
            requirement_id=req.requirement_id,
            param_key=req.param_key,
            operator=req.operator,
            value=req.value,
            is_hard=req.is_hard,
            status="unmapped",
            confidence=confidence,
            reason=f"llm_only selected field not present in metadata: {chosen_field}",
            candidate_fields=candidate_fields,
            evidence_refs=req.evidence_refs,
        )

    guardrail_error = _mapping_guardrail_error(req, chosen_meta)
    if guardrail_error is not None:
        _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
        return _unmapped_due_to_guardrail(
            req,
            reason=f"mapping guardrail failed: {guardrail_error}",
            candidate_fields=candidate_fields,
        )

    return _mapped(
        req,
        mapped_field=chosen_field,
        confidence=confidence,
        reason=f"llm_only semantic match: {reason}",
        candidate_fields=candidate_fields,
    )


def map_requirements_to_schema(
    requirements: list[TenderRequirement],
    meta: DomainMetadata,
) -> tuple[list[SchemaMapping], list[str]]:
    mapped: list[SchemaMapping] = []
    unmet_constraints: list[str] = []
    for req in requirements:
        direct = meta.mapping_by_key(req.param_key)
        if direct is None:
            _append_hard_unmet(req, unmet_constraints, message="hard requirement unmapped")
            mapped.append(
                SchemaMapping(
                    requirement_id=req.requirement_id,
                    param_key=req.param_key,
                    operator=req.operator,
                    value=req.value,
                    is_hard=req.is_hard,
                    status="unmapped",
                    confidence=req.confidence,
                    reason="canonical key not found in metadata",
                    candidate_fields=[],
                    evidence_refs=req.evidence_refs,
                )
            )
            continue

        guardrail_error = _mapping_guardrail_error(req, direct)
        if guardrail_error is not None:
            _append_hard_unmet(req, unmet_constraints, message="hard requirement blocked by guardrail")
            mapped.append(
                _unmapped_due_to_guardrail(
                    req,
                    reason=f"mapping guardrail failed: {guardrail_error}",
                    candidate_fields=[direct.field],
                )
            )
            continue

        mapped.append(
            _mapped(
                req,
                mapped_field=direct.field,
                confidence=max(req.confidence, 0.9),
                reason="canonical key mapping",
                candidate_fields=[direct.field],
            )
        )

    return mapped, unmet_constraints
