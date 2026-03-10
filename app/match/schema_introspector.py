from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text

from app.core.database import get_pim_engine


def _columns_from_information_schema(
    *,
    table_whitelist: list[str],
) -> dict[str, list[dict[str, str]]]:
    engine = get_pim_engine()
    dialect = engine.dialect.name
    columns_by_table: dict[str, list[dict[str, str]]] = {}

    if dialect not in {"mysql", "mariadb", "postgresql"}:
        return columns_by_table

    with engine.begin() as conn:
        if dialect in {"mysql", "mariadb"}:
            schema = engine.url.database
            if not schema:
                schema = conn.execute(text("SELECT DATABASE()")).scalar()
            if not schema:
                return columns_by_table
            placeholders: list[str] = []
            params: dict[str, Any] = {"schema": schema}
            for idx, table in enumerate(table_whitelist):
                key = f"t{idx}"
                params[key] = table
                placeholders.append(f":{key}")
            if not placeholders:
                return columns_by_table
            query = (
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME IN ("
                + ", ".join(placeholders)
                + ")"
            )
        else:
            placeholders = []
            params = {}
            for idx, table in enumerate(table_whitelist):
                key = f"t{idx}"
                params[key] = table
                placeholders.append(f":{key}")
            if not placeholders:
                return columns_by_table
            query = (
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME IN ("
                + ", ".join(placeholders)
                + ")"
            )

        rows = conn.execute(text(query), params).fetchall()
        for table_name, column_name, data_type in rows:
            table_name = str(table_name)
            column_name = str(column_name)
            data_type = str(data_type)
            columns_by_table.setdefault(table_name, []).append(
                {"name": column_name, "type": data_type}
            )

    return columns_by_table


def _columns_from_inspector(
    *,
    table_whitelist: list[str],
) -> dict[str, list[dict[str, str]]]:
    engine = get_pim_engine()
    insp = inspect(engine)
    columns_by_table: dict[str, list[dict[str, str]]] = {}

    available_tables = set(insp.get_table_names()) | set(insp.get_view_names())
    for table in table_whitelist:
        if table not in available_tables:
            continue
        cols = insp.get_columns(table)
        columns_by_table[table] = [
            {"name": str(col.get("name")), "type": str(col.get("type"))} for col in cols
        ]
    return columns_by_table


def fetch_schema_metadata(
    *,
    table_whitelist: list[str],
    field_whitelist: list[str],
) -> dict[str, Any]:
    columns_by_table = _columns_from_information_schema(table_whitelist=table_whitelist)
    if not columns_by_table:
        columns_by_table = _columns_from_inspector(table_whitelist=table_whitelist)

    allowed_fields_set = set(field_whitelist)
    tables_out: list[dict[str, Any]] = []
    allowed_fields: list[str] = []

    for table in table_whitelist:
        cols = columns_by_table.get(table, [])
        if not cols:
            continue
        if allowed_fields_set:
            cols = [col for col in cols if f"{table}.{col['name']}" in allowed_fields_set]
        if not cols:
            continue
        tables_out.append({"name": table, "columns": cols})
        for col in cols:
            allowed_fields.append(f"{table}.{col['name']}")

    if not tables_out:
        raise ValueError("no allowed tables/columns found in PIM schema")

    return {
        "tables": tables_out,
        "allowed_tables": [item["name"] for item in tables_out],
        "allowed_fields": allowed_fields,
    }
