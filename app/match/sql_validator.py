from __future__ import annotations

import re
from typing import Any

_NAMED_BIND_RE = re.compile(r"(?<!:):[A-Za-z_][A-Za-z0-9_]*")


def _sql_for_parse(sql: str) -> str:
    # sqlglot does not accept SQLAlchemy-style named binds in every clause (e.g. LIMIT :limit).
    # Replace binds with a numeric literal for AST validation only.
    return _NAMED_BIND_RE.sub("1", sql)


def _naive_validate(sql: str, table_whitelist: set[str]) -> list[str]:
    errors: list[str] = []
    stripped = sql.strip()
    if not stripped.lower().startswith("select"):
        errors.append("only SELECT statements are allowed")
    if ";" in stripped.rstrip(";"):
        errors.append("multiple statements are not allowed")
    if " limit " not in f" {stripped.lower()} ":
        errors.append("LIMIT clause is required")

    for forbidden in ("insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "create "):
        if forbidden in f" {stripped.lower()} ":
            errors.append("only read-only SELECT is allowed")
            break

    table_hits = re.findall(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)", stripped, flags=re.I)
    table_hits += re.findall(r"\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)", stripped, flags=re.I)
    for table in table_hits:
        if table not in table_whitelist:
            errors.append(f"table not allowed: {table}")
    return errors


def validate_readonly_select(
    *,
    sql: str,
    table_whitelist: set[str],
    field_whitelist: set[str],
) -> list[str]:
    try:
        import sqlglot
        from sqlglot import exp
    except Exception:
        return _naive_validate(sql, table_whitelist)

    errors: list[str] = []
    try:
        tree = sqlglot.parse_one(_sql_for_parse(sql))
    except Exception as exc:
        return [f"SQL parse error: {exc}"]

    if not isinstance(tree, exp.Select):
        errors.append("only SELECT statements are allowed")

    if tree.find(exp.Insert) or tree.find(exp.Update) or tree.find(exp.Delete) or tree.find(exp.Drop):
        errors.append("DML/DDL statements are not allowed")

    if tree.args.get("limit") is None:
        errors.append("LIMIT clause is required")

    alias_to_table: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        table_name = table.name
        if table_name not in table_whitelist:
            errors.append(f"table not allowed: {table_name}")
        alias = table.alias_or_name
        if alias:
            alias_to_table[alias] = table_name

    for star in tree.find_all(exp.Star):
        errors.append("wildcard select is not allowed")
        break

    for column in tree.find_all(exp.Column):
        table_alias = column.table
        column_name = column.name
        if not column_name:
            continue
        if not table_alias:
            # Untyped columns are allowed when they match any whitelisted field suffix.
            suffix = f".{column_name}"
            if not any(item.endswith(suffix) for item in field_whitelist):
                errors.append(f"field not allowed: {column_name}")
            continue

        table_name = alias_to_table.get(table_alias, table_alias)
        field = f"{table_name}.{column_name}"
        if field not in field_whitelist:
            errors.append(f"field not allowed: {field}")

    # Guard against stacked statements not represented in tree.
    if ";" in sql.strip().rstrip(";"):
        errors.append("multiple statements are not allowed")

    dedup: list[str] = []
    seen: set[str] = set()
    for item in errors:
        if item in seen:
            continue
        seen.add(item)
        dedup.append(item)
    return dedup


def sanitize_query_result_row(row: Any) -> dict[str, Any]:
    if hasattr(row, "_mapping"):
        out = dict(row._mapping)
    elif isinstance(row, dict):
        out = dict(row)
    else:
        out = dict(row)
    return {str(k): out[k] for k in out}
