from __future__ import annotations

import re
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None
    return None


def _is_unknown_numeric(value: float) -> bool:
    return abs(value) < 1e-12


def _eval(requirement: dict, row: dict) -> tuple[bool, bool]:
    field = requirement.get("field")
    operator = requirement.get("operator")
    value = requirement.get("value")
    if not isinstance(field, str) or "." not in field:
        return False, False
    if not isinstance(operator, str):
        return False, False
    _, column_name = field.split(".", 1)
    row_value = row.get(column_name)
    if row_value is None:
        return False, False

    if operator in {"bool_true", "bool_false"}:
        # Bool constraints are intentionally not used in current rule strategy.
        return False, False

    left_num = _to_float(row_value)
    if operator in {"eq", "gte", "lte", "gt", "lt", "between", "in"} and left_num is not None:
        if _is_unknown_numeric(left_num):
            return False, False
        if operator == "between":
            if not isinstance(value, list) or len(value) != 2:
                return False, False
            low = _to_float(value[0])
            high = _to_float(value[1])
            if low is None or high is None:
                return False, False
            return low <= left_num <= high, True
        if operator == "in":
            if not isinstance(value, list):
                return False, False
            candidates = [_to_float(v) for v in value]
            candidates = [c for c in candidates if c is not None]
            if not candidates:
                return False, False
            return any(abs(left_num - c) < 1e-6 for c in candidates), True
        right_num = _to_float(value)
        if right_num is None:
            return False, False
        if operator == "eq":
            return abs(left_num - right_num) < 1e-6, True
        if operator == "gte":
            return left_num >= right_num, True
        if operator == "lte":
            return left_num <= right_num, True
        if operator == "gt":
            return left_num > right_num, True
        if operator == "lt":
            return left_num < right_num, True
        return False, False

    left_text = str(row_value).strip().lower()
    if operator == "contains":
        target = str(value or "").strip().lower()
        return (target in left_text, True) if target else (False, False)
    if operator == "in" and isinstance(value, list):
        targets = [str(v).strip().lower() for v in value if str(v).strip()]
        return (left_text in targets, True) if targets else (False, False)
    if operator == "eq":
        target = str(value or "").strip().lower()
        return (left_text == target, True) if target else (False, False)
    return False, False


def build_fallback_step7(step4_data: dict, step6_data: dict) -> dict:
    result_map = {
        item.get("product_key"): item.get("rows") or []
        for item in step6_data.get("results", [])
        if isinstance(item, dict)
    }
    match_results: list[dict] = []
    for product in step4_data.get("tender_products", []):
        if not isinstance(product, dict):
            continue
        product_key = product.get("product_key")
        if not isinstance(product_key, str):
            continue
        requirements = product.get("requirements") or []
        rows = result_map.get(product_key, [])

        scored: list[dict] = []
        for row in rows:
            soft_total = 0
            soft_matched = 0
            unmet_soft: list[str] = []
            for requirement in requirements:
                if not isinstance(requirement, dict):
                    continue
                if requirement.get("is_hard") is True:
                    continue
                if not isinstance(requirement.get("operator"), str):
                    continue
                matched, measurable = _eval(requirement, row)
                if not measurable:
                    continue
                soft_total += 1
                field_name = str(requirement.get("field") or "unknown")
                if matched:
                    soft_matched += 1
                else:
                    unmet_soft.append(field_name)
            soft_score = (soft_matched / soft_total) if soft_total else 0.0
            scored.append(
                {
                    "row": row,
                    "soft_match_score": soft_score,
                    "matched_soft_count": soft_matched,
                    "soft_total": soft_total,
                    "unmet_soft": unmet_soft,
                }
            )
        scored.sort(key=lambda item: (item["soft_match_score"], item["matched_soft_count"]), reverse=True)

        candidates: list[dict] = []
        for rank, item in enumerate(scored, start=1):
            row = item["row"]
            candidates.append(
                {
                    "rank": rank,
                    "db_product_id": row.get("product_id"),
                    "db_product_name": row.get("product_name"),
                    "passes_hard": True,
                    "soft_match_score": round(item["soft_match_score"], 4),
                    "matched_soft_constraints": [],
                    "unmet_soft_constraints": item["unmet_soft"],
                    "explanation": (
                        f"Fallback ranking: matched {item['matched_soft_count']}/"
                        f"{item['soft_total']} measurable soft constraints."
                    ),
                }
            )

        match_results.append({"product_key": product_key, "candidates": candidates})
    return {"match_results": match_results}
