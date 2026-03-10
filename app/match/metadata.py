from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field

from app.core.settings import settings


class UnitRule(BaseModel):
    canonical: str
    multiplier: float


class MappingDefinition(BaseModel):
    canonical_key: str
    synonyms: list[str] = Field(default_factory=list)
    field: str
    field_expr: str
    operators: list[str] = Field(default_factory=list)
    value_type: str = "text"
    unit: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    hard_default: bool = True
    weight: float = 1.0


class DomainMetadata(BaseModel):
    domain: str
    table_whitelist: list[str] = Field(default_factory=list)
    field_whitelist: list[str] = Field(default_factory=list)
    sql_template: str
    mappings: list[MappingDefinition] = Field(default_factory=list)
    units: dict[str, UnitRule] = Field(default_factory=dict)
    hard_keywords: list[str] = Field(default_factory=list)
    soft_keywords: list[str] = Field(default_factory=list)
    comparison_keywords: dict[str, list[str]] = Field(default_factory=dict)

    def mapping_by_key(self, key: str) -> MappingDefinition | None:
        normalized = normalize_text(key)
        for mapping in self.mappings:
            if normalize_text(mapping.canonical_key) == normalized:
                return mapping
        return None

    def matching_mappings_for_text(self, text: str) -> list[MappingDefinition]:
        normalized = normalize_text(text)
        hits: list[MappingDefinition] = []
        for mapping in self.mappings:
            terms = [mapping.canonical_key, *mapping.synonyms]
            if any(term_matches_text(normalized, term) for term in terms):
                hits.append(mapping)
        return hits

    def candidate_mappings(self, term: str) -> list[MappingDefinition]:
        normalized = normalize_text(term)
        hits: list[MappingDefinition] = []
        for mapping in self.mappings:
            if normalize_text(mapping.canonical_key) == normalized:
                hits.append(mapping)
                continue
            for synonym in mapping.synonyms:
                if term_matches_text(normalized, synonym):
                    hits.append(mapping)
                    break
        return hits

    def normalize_unit_value(
        self,
        value: object,
        from_unit: str | None,
        target_unit: str | None,
    ) -> tuple[object, str | None]:
        if not isinstance(value, (int, float)):
            return value, target_unit or from_unit

        if not from_unit and not target_unit:
            return value, None

        source = normalize_text(from_unit or "")
        target = normalize_text(target_unit or "")
        if not source:
            return value, target_unit
        source_rule = self.units.get(source)
        if source_rule is None:
            return value, target_unit or from_unit

        base_value = float(value) * source_rule.multiplier
        if not target:
            return base_value, source_rule.canonical

        target_rule = self.units.get(target)
        if target_rule is None or target_rule.canonical != source_rule.canonical:
            return value, target_unit or from_unit

        converted = base_value / target_rule.multiplier
        return converted, target_rule.canonical


def normalize_text(text: str) -> str:
    lowered = text.lower().strip()
    lowered = re.sub(r"[^a-z0-9_äöüß\u4e00-\u9fff]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered


_TOKEN_RE = re.compile(r"[a-z0-9_äöüß]+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text)


def term_matches_text(text: str, term: str) -> bool:
    text_norm = normalize_text(text)
    term_norm = normalize_text(term)
    if not text_norm or not term_norm:
        return False

    # CJK terms are matched as substrings because token boundaries are not whitespace-delimited.
    if _CJK_RE.search(term_norm):
        return term_norm in text_norm

    text_tokens = _tokens(text_norm)
    term_tokens = _tokens(term_norm)
    if not text_tokens or not term_tokens:
        return False

    # Single-token terms require exact token hit. This prevents short-term false positives
    # such as "ra" matching inside "rating".
    if len(term_tokens) == 1:
        return term_tokens[0] in set(text_tokens)

    window = len(term_tokens)
    for idx in range(len(text_tokens) - window + 1):
        if text_tokens[idx : idx + window] == term_tokens:
            return True
    return False


def _metadata_path_for_domain(domain: str) -> Path:
    return settings.match_data_dir / f"{domain}_metadata.json"


def _load_metadata_file(path: Path) -> DomainMetadata:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return DomainMetadata.model_validate(payload)


def _validate_metadata(meta: DomainMetadata) -> list[str]:
    errors: list[str] = []
    if "{hard_filters}" not in meta.sql_template:
        errors.append("sql_template must include {hard_filters}")
    if "LIMIT" not in meta.sql_template.upper():
        errors.append("sql_template must include LIMIT")

    table_set = set(meta.table_whitelist)
    field_set = set(meta.field_whitelist)
    if not table_set:
        errors.append("table_whitelist must not be empty")
    if not field_set:
        errors.append("field_whitelist must not be empty")

    seen: set[str] = set()
    for mapping in meta.mappings:
        key = normalize_text(mapping.canonical_key)
        if not key:
            errors.append("mapping canonical_key is empty")
            continue
        if key in seen:
            errors.append(f"duplicate canonical_key={mapping.canonical_key}")
        seen.add(key)
        if mapping.field not in field_set:
            errors.append(f"mapping field not in field_whitelist: {mapping.field}")
        table_name = mapping.field.split(".", 1)[0] if "." in mapping.field else ""
        if table_name and table_name not in table_set:
            errors.append(f"mapping table not in table_whitelist: {table_name}")
        if not mapping.operators:
            errors.append(f"mapping operators empty: {mapping.canonical_key}")
        if (
            mapping.min_value is not None
            and mapping.max_value is not None
            and mapping.min_value > mapping.max_value
        ):
            errors.append(f"mapping min_value > max_value: {mapping.canonical_key}")

    return errors


@lru_cache(maxsize=8)
def load_domain_metadata(domain: str) -> DomainMetadata:
    path = _metadata_path_for_domain(domain)
    if not path.exists():
        raise FileNotFoundError(f"metadata file not found for domain={domain}: {path}")
    metadata = _load_metadata_file(path)
    errors = _validate_metadata(metadata)
    if errors:
        raise ValueError(f"metadata validation failed for domain={domain}: {'; '.join(errors)}")
    return metadata


def validate_all_domain_metadata() -> dict[str, list[str]]:
    results: dict[str, list[str]] = {}
    for file_path in sorted(settings.match_data_dir.glob("*_metadata.json")):
        try:
            metadata = _load_metadata_file(file_path)
            errors = _validate_metadata(metadata)
        except Exception as exc:
            results[file_path.name] = [str(exc)]
            continue
        results[file_path.name] = errors
    return results
