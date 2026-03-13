from __future__ import annotations

from copy import deepcopy
import re
from typing import Any

from .contracts import normalize_field

PRODUCT_VIEW = "vw_bid_products"
SPECS_VIEW = "vw_bid_specs"


def _split_table_column(field: str) -> tuple[str, str] | None:
    if "." not in field:
        return None
    table, column = field.split(".", 1)
    table = table.strip()
    column = column.strip()
    if not table or not column:
        return None
    return table, column


def _alias_for_table(table_name: str) -> str | None:
    if table_name == PRODUCT_VIEW:
        return "bp"
    if table_name == SPECS_VIEW:
        return "bs"
    return None


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


def _sql_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _guard_unknown_numeric(ref: str, condition_sql: str) -> str:
    return f"({ref} IS NOT NULL AND {ref} <> 0 AND {condition_sql})"


def _build_numeric_condition(ref: str, operator: str, value: Any) -> str | None:
    if operator in {"bool_true", "bool_false"}:
        # V1 rule policy excludes bool operators.
        return None

    if operator == "between":
        if isinstance(value, list) and len(value) == 2:
            low = _to_float(value[0])
            high = _to_float(value[1])
            if low is not None and high is not None:
                return _guard_unknown_numeric(ref, f"{ref} BETWEEN {low:g} AND {high:g}")
        return None

    if operator == "in":
        if not isinstance(value, list):
            return None
        nums = [num for num in (_to_float(x) for x in value) if num is not None]
        if not nums:
            return None
        return _guard_unknown_numeric(ref, f"{ref} IN ({', '.join(f'{num:g}' for num in nums)})")

    num = _to_float(value)
    if num is None:
        return None
    op_map = {"eq": "=", "gte": ">=", "lte": "<=", "gt": ">", "lt": "<"}
    sql_op = op_map.get(operator)
    if not sql_op:
        return None
    return _guard_unknown_numeric(ref, f"{ref} {sql_op} {num:g}")


def _build_text_condition(ref: str, operator: str, value: Any) -> str | None:
    if operator == "contains":
        text = str(value or "").strip()
        if not text:
            return None
        safe_text = text.replace("%", "%%").replace("'", "''")
        return f"{ref} LIKE '%{safe_text}%'"
    if operator == "in":
        if not isinstance(value, list):
            return None
        values = [str(item).strip() for item in value if str(item).strip()]
        if not values:
            return None
        return f"{ref} IN ({', '.join(_sql_quote(v) for v in values)})"
    if operator == "eq":
        text = str(value or "").strip()
        if not text:
            return None
        return f"{ref} = {_sql_quote(text)}"
    return None


def _build_condition(requirement: dict, schema_columns: set[str]) -> str | None:
    field = normalize_field(str(requirement.get("field") or ""))
    operator = requirement.get("operator")
    value = requirement.get("value")
    if "." not in field:
        return None
    if not isinstance(operator, str):
        return None

    parsed = _split_table_column(field)
    if not parsed:
        return None
    table_name, column_name = parsed
    if field not in schema_columns:
        return None
    alias = _alias_for_table(table_name)
    if not alias:
        return None
    ref = f"{alias}.{column_name}"
    return _build_numeric_condition(ref, operator, value) or _build_text_condition(ref, operator, value)


def _collect_select_exprs(requirements: list[dict], schema_columns: set[str]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()

    def add(expr: str):
        if expr in seen:
            return
        seen.add(expr)
        selected.append(expr)

    baseline = (
        "bp.product_id",
        "bp.article_number",
        "bp.product_name",
        "bp.manufacturer_name",
        "bp.tender_description",
    )
    for expr in baseline:
        table_alias, col = expr.split(".", 1)
        table_name = PRODUCT_VIEW if table_alias == "bp" else ""
        field = f"{table_name}.{col}"
        if field in schema_columns:
            add(expr)

    for requirement in requirements:
        field = normalize_field(str(requirement.get("field") or ""))
        parsed = _split_table_column(field)
        if not parsed:
            continue
        table_name, column_name = parsed
        alias = _alias_for_table(table_name)
        if not alias:
            continue
        if field not in schema_columns:
            continue
        if column_name in {"product_id", "is_current"}:
            continue
        add(f"{alias}.{column_name}")

    return selected or ["bp.product_id"]


def build_step4_merged(step2_data: dict, step3_data: dict) -> dict:
    merged = deepcopy(step2_data)
    products = merged.get("tender_products")
    if not isinstance(products, list):
        return {"tender_products": [], "skipped_requirements": []}

    rule_map: dict[str, dict] = {}
    for rule in step3_data.get("field_rules", []):
        if not isinstance(rule, dict):
            continue
        field = normalize_field(str(rule.get("field") or ""))
        if not field:
            continue
        rule_map[field] = rule

    skipped_requirements: list[dict] = []
    for product in products:
        if not isinstance(product, dict):
            continue
        product_key = str(product.get("product_key") or "")
        requirements = product.get("requirements")
        if not isinstance(requirements, list):
            product["requirements"] = []
            continue
        for idx, requirement in enumerate(requirements):
            if not isinstance(requirement, dict):
                continue
            field = normalize_field(str(requirement.get("field") or ""))
            rule = rule_map.get(field)
            if rule is None:
                skipped_requirements.append(
                    {
                        "product_key": product_key,
                        "requirement_id": requirement.get("requirement_id"),
                        "requirement_index": idx,
                        "field": field,
                        "reason": "missing_step3_field_rule",
                    }
                )
                continue
            requirement["operator"] = rule.get("operator")
            requirement["is_hard"] = bool(rule.get("is_hard"))
            requirement["operator_confidence"] = rule.get("operator_confidence")
            requirement["hardness_confidence"] = rule.get("hardness_confidence")
    return {"tender_products": products, "skipped_requirements": skipped_requirements}


def build_step5_sql(step4_data: dict, schema_payload: dict, *, join_key: str = "product_id") -> dict:
    schema_columns: set[str] = set()
    table_map: dict[str, set[str]] = {}
    for table in schema_payload.get("tables", []):
        if not isinstance(table, dict):
            continue
        table_name = str(table.get("name") or "").strip().lower()
        if not table_name:
            continue
        columns: set[str] = set()
        for column in table.get("columns", []):
            if not isinstance(column, dict):
                continue
            col_name = str(column.get("name") or "").strip().lower()
            if not col_name:
                continue
            columns.add(col_name)
            schema_columns.add(f"{table_name}.{col_name}")
        table_map[table_name] = columns

    queries: list[dict] = []
    products = step4_data.get("tender_products") or []
    for p_idx, product in enumerate(products):
        if not isinstance(product, dict):
            continue
        product_key = str(product.get("product_key") or f"item_{p_idx+1:03d}")
        requirements = product.get("requirements") or []
        select_exprs = _collect_select_exprs(requirements, schema_columns)

        where_clauses: list[str] = []
        if "is_current" in table_map.get(PRODUCT_VIEW, set()):
            where_clauses.append("bp.is_current = 1")
        if "is_current" in table_map.get(SPECS_VIEW, set()):
            where_clauses.append("bs.is_current = 1")

        hard_constraints_used: list[dict] = []
        for requirement in requirements:
            if not isinstance(requirement, dict):
                continue
            operator = requirement.get("operator")
            is_hard = requirement.get("is_hard")
            if not isinstance(operator, str):
                continue
            if is_hard is not True:
                continue
            condition = _build_condition(requirement, schema_columns)
            if not condition:
                continue
            where_clauses.append(condition)
            hard_constraints_used.append(
                {
                    "field": normalize_field(str(requirement.get("field") or "")),
                    "operator": operator,
                    "value": requirement.get("value"),
                }
            )

        if not where_clauses:
            where_clauses.append("1 = 1")

        sql = (
            "SELECT "
            + ", ".join(select_exprs)
            + f" FROM {PRODUCT_VIEW} bp "
            + f"JOIN {SPECS_VIEW} bs ON bp.{join_key} = bs.{join_key} "
            + "WHERE "
            + " AND ".join(f"({item})" for item in where_clauses)
            + ";"
        )
        queries.append(
            {
                "query_id": f"q_{product_key}",
                "product_key": product_key,
                "hard_constraints_used": hard_constraints_used,
                "sql": sql,
            }
        )

    return {"queries": queries}
