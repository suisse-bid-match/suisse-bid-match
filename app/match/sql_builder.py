from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.models import SQLPlan, SchemaMapping

from .metadata import DomainMetadata
from .sql_validator import validate_readonly_select


@dataclass
class SQLBuildResult:
    plan: SQLPlan
    unmet_constraints: list[str]


def _add_predicate(
    *,
    field_expr: str,
    operator: str,
    value: object,
    param_prefix: str,
    params: dict[str, object],
) -> str:
    condition = _build_condition(
        field_expr=field_expr,
        operator=operator,
        value=value,
        param_prefix=param_prefix,
        params=params,
    )
    return f" AND {condition}"


def _build_condition(
    *,
    field_expr: str,
    operator: str,
    value: object,
    param_prefix: str,
    params: dict[str, object],
) -> str:
    if operator == "eq":
        key = f"{param_prefix}_eq"
        params[key] = value
        return f"{field_expr} = :{key}"
    if operator == "gte":
        key = f"{param_prefix}_gte"
        params[key] = value
        return f"{field_expr} >= :{key}"
    if operator == "lte":
        key = f"{param_prefix}_lte"
        params[key] = value
        return f"{field_expr} <= :{key}"
    if operator == "gt":
        key = f"{param_prefix}_gt"
        params[key] = value
        return f"{field_expr} > :{key}"
    if operator == "lt":
        key = f"{param_prefix}_lt"
        params[key] = value
        return f"{field_expr} < :{key}"
    if operator == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError("between operator requires a two-item list value")
        low_key = f"{param_prefix}_low"
        high_key = f"{param_prefix}_high"
        params[low_key] = value[0]
        params[high_key] = value[1]
        return f"{field_expr} BETWEEN :{low_key} AND :{high_key}"
    if operator == "in":
        if not isinstance(value, list) or not value:
            raise ValueError("in operator requires non-empty list value")
        parts: list[str] = []
        for idx, item in enumerate(value):
            key = f"{param_prefix}_in_{idx}"
            params[key] = item
            parts.append(f":{key}")
        return f"{field_expr} IN ({', '.join(parts)})"
    if operator == "contains":
        key = f"{param_prefix}_contains"
        params[key] = f"%{value}%"
        return f"{field_expr} LIKE :{key}"
    if operator == "bool_true":
        key = f"{param_prefix}_bool"
        params[key] = True
        return f"{field_expr} = :{key}"
    if operator == "bool_false":
        key = f"{param_prefix}_bool"
        params[key] = False
        return f"{field_expr} = :{key}"
    raise ValueError(f"unsupported operator: {operator}")


def _inject_order_by(sql: str, order_by_clause: str) -> str:
    limit_tail = re.search(r"\sLIMIT\s+:[A-Za-z_][A-Za-z0-9_]*\s*$", sql, flags=re.I)
    if limit_tail is not None:
        return f"{sql[:limit_tail.start()]}{order_by_clause}{sql[limit_tail.start():]}"
    return f"{sql}{order_by_clause}"


def build_sql_plan(
    *,
    domain: str,
    mappings: list[SchemaMapping],
    meta: DomainMetadata,
    top_k: int,
    strict_hard_constraints: bool,
) -> SQLBuildResult:
    params: dict[str, object] = {"limit": top_k}
    hard_filters: list[str] = []
    soft_score_terms: list[str] = []
    unmet_constraints: list[str] = []

    mapping_by_field = {item.field: item for item in meta.mappings}

    hard_count = 0
    soft_count = 0

    for item in mappings:
        if item.status != "mapped" or not item.mapped_field:
            if strict_hard_constraints and item.is_hard:
                unmet_constraints.append(
                    f"{item.requirement_id}: hard requirement not mapped ({item.param_key})"
                )
            continue

        mapped = mapping_by_field.get(item.mapped_field)
        if mapped is None:
            if strict_hard_constraints and item.is_hard:
                unmet_constraints.append(
                    f"{item.requirement_id}: mapping metadata missing for {item.mapped_field}"
                )
            continue

        param_prefix = item.requirement_id
        try:
            predicate = _add_predicate(
                field_expr=mapped.field_expr,
                operator=item.operator,
                value=item.value,
                param_prefix=param_prefix,
                params=params,
            )
        except Exception as exc:
            if strict_hard_constraints and item.is_hard:
                unmet_constraints.append(f"{item.requirement_id}: invalid hard predicate ({exc})")
            continue

        if item.is_hard:
            hard_count += 1
            hard_filters.append(predicate)
        else:
            soft_count += 1
            if mapped.weight <= 0:
                continue
            try:
                soft_condition = _build_condition(
                    field_expr=mapped.field_expr,
                    operator=item.operator,
                    value=item.value,
                    param_prefix=f"{param_prefix}_soft",
                    params=params,
                )
            except Exception:
                continue
            soft_score_terms.append(
                f"(CASE WHEN {soft_condition} THEN {mapped.weight} ELSE 0 END)"
            )

    sql = meta.sql_template.format(hard_filters="".join(hard_filters), soft_filters="")
    if soft_score_terms:
        sql = _inject_order_by(sql, f" ORDER BY ({' + '.join(soft_score_terms)}) DESC")

    validation_errors = validate_readonly_select(
        sql=sql,
        table_whitelist=set(meta.table_whitelist),
        field_whitelist=set(meta.field_whitelist),
    )

    blocked = len(validation_errors) > 0 or (strict_hard_constraints and len(unmet_constraints) > 0)
    block_reason = None
    if validation_errors:
        block_reason = f"SQL validation failed ({len(validation_errors)} errors)"
    elif strict_hard_constraints and unmet_constraints:
        block_reason = f"{len(unmet_constraints)} hard constraints unmet before query"

    plan = SQLPlan(
        domain=domain,
        sql=sql,
        params=params,
        hard_clause_count=hard_count,
        soft_clause_count=soft_count,
        limit=top_k,
        validated=len(validation_errors) == 0,
        validation_errors=validation_errors,
        blocked=blocked,
        block_reason=block_reason,
    )
    return SQLBuildResult(plan=plan, unmet_constraints=unmet_constraints)
