from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import get_pim_engine
from app.core.models import MatchCandidate, SQLPlan, SchemaMapping
from app.core.settings import settings

from .metadata import DomainMetadata
from .sql_validator import sanitize_query_result_row


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _compare(row_value: Any, operator: str, expected: Any) -> bool:
    if operator == "eq":
        return str(row_value) == str(expected)
    if operator == "bool_true":
        return bool(row_value) is True
    if operator == "bool_false":
        return bool(row_value) is False
    if operator in {"gte", "lte", "gt", "lt"}:
        left = _coerce_number(row_value)
        right = _coerce_number(expected)
        if left is None or right is None:
            return False
        if operator == "gte":
            return left >= right
        if operator == "lte":
            return left <= right
        if operator == "gt":
            return left > right
        if operator == "lt":
            return left < right
    if operator == "between":
        if not isinstance(expected, list) or len(expected) != 2:
            return False
        left = _coerce_number(row_value)
        low = _coerce_number(expected[0])
        high = _coerce_number(expected[1])
        if left is None or low is None or high is None:
            return False
        return low <= left <= high
    if operator == "in":
        if not isinstance(expected, list):
            return False
        return str(row_value) in {str(x) for x in expected}
    if operator == "contains":
        if row_value is None:
            return False
        return str(expected).lower() in str(row_value).lower()
    return False


def _column_name(field: str) -> str:
    return field.split(".", 1)[-1]


def _weight_for_mapping(meta: DomainMetadata, field: str | None) -> float:
    if not field:
        return 0.0
    for item in meta.mappings:
        if item.field == field:
            return item.weight
    return 0.0


def _attach_asset_root(row: dict[str, Any]) -> dict[str, Any]:
    asset_root = settings.pim_assets_root.strip()
    if not asset_root:
        return row

    normalized = dict(row)
    for key, value in row.items():
        if not isinstance(value, str):
            continue
        if not key.endswith("_path"):
            continue
        if not value or value.startswith(("http://", "https://")):
            continue
        if Path(value).is_absolute():
            continue
        normalized[key] = str(Path(asset_root) / value)
    return normalized


def execute_and_rank(
    *,
    sql_plan: SQLPlan,
    mappings: list[SchemaMapping],
    meta: DomainMetadata,
    top_k: int,
    strict_hard_constraints: bool,
) -> tuple[list[MatchCandidate], list[str], dict[str, Any]]:
    if sql_plan.blocked:
        return [], [], {"rows_fetched": 0, "query_ms": 0}

    engine = get_pim_engine()
    stats: dict[str, Any] = {"rows_fetched": 0, "query_ms": 0}
    rows: list[dict[str, Any]] = []

    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            timeout_ms = int(max(1, settings.match_query_timeout_sec) * 1000)
            conn.exec_driver_sql(f"SET LOCAL statement_timeout = {timeout_ms}")
        if engine.dialect.name in {"mysql", "mariadb"}:
            timeout_ms = int(max(1, settings.match_query_timeout_sec) * 1000)
            conn.exec_driver_sql(f"SET SESSION MAX_EXECUTION_TIME={timeout_ms}")
        result = conn.execute(text(sql_plan.sql), sql_plan.params)
        rows = [_attach_asset_root(sanitize_query_result_row(item)) for item in result.fetchall()]

    stats["rows_fetched"] = len(rows)

    candidates: list[MatchCandidate] = []
    unmet_constraints: list[str] = []

    for row in rows:
        matched: list[str] = []
        unmet: list[str] = []
        hard_violations: list[str] = []
        soft_score = 0.0
        hard_passed = True
        breakdown: dict[str, float] = {}

        for mapping in mappings:
            if mapping.status != "mapped" or not mapping.mapped_field:
                if mapping.is_hard:
                    hard_passed = False
                    hard_violations.append(mapping.requirement_id)
                continue

            column = _column_name(mapping.mapped_field)
            value = row.get(column)
            passes = _compare(value, mapping.operator, mapping.value)
            if mapping.is_hard:
                if passes:
                    matched.append(mapping.requirement_id)
                    breakdown[mapping.requirement_id] = 1.0
                else:
                    hard_passed = False
                    unmet.append(mapping.requirement_id)
                    hard_violations.append(mapping.requirement_id)
                    breakdown[mapping.requirement_id] = -1.0
            else:
                weight = _weight_for_mapping(meta, mapping.mapped_field)
                if passes:
                    soft_score += weight
                    matched.append(mapping.requirement_id)
                    breakdown[mapping.requirement_id] = weight
                else:
                    unmet.append(mapping.requirement_id)
                    breakdown[mapping.requirement_id] = 0.0

        if strict_hard_constraints and not hard_passed:
            continue

        base_score = 100.0 if hard_passed else 0.0
        score = round(base_score + soft_score * 100.0, 4)
        product_id = str(row.get("product_id") or row.get("id") or "")
        product_name = str(row.get("product_name") or row.get("name") or product_id)
        if not product_id:
            product_id = product_name

        candidates.append(
            MatchCandidate(
                product_id=product_id,
                product_name=product_name,
                score=score,
                hard_passed=hard_passed,
                soft_score=round(soft_score, 4),
                matched_requirements=matched,
                unmet_requirements=unmet,
                hard_violations=hard_violations,
                score_breakdown=breakdown,
                row=row,
            )
        )

    candidates.sort(key=lambda item: (item.hard_passed, item.score), reverse=True)
    candidates = candidates[:top_k]

    if strict_hard_constraints and not candidates:
        unmet_constraints.append("no products satisfy hard constraints")

    return candidates, unmet_constraints, stats
