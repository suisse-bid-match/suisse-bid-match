from __future__ import annotations

import os
from time import perf_counter
from typing import Any

import pymysql



def _connection_settings(host_or_container: str, user: str, password: str, database: str) -> tuple[str, int, str, str, str]:
    host = os.getenv("PIM_MYSQL_HOST", host_or_container)
    port = int(os.getenv("PIM_MYSQL_PORT", "3306"))
    resolved_user = os.getenv("PIM_MYSQL_USER", user)
    resolved_password = os.getenv("PIM_MYSQL_PASSWORD", password)
    resolved_database = os.getenv("PIM_MYSQL_DB", database)
    return host, port, resolved_user, resolved_password, resolved_database



def _to_tsv(cursor: pymysql.cursors.Cursor, rows: list[tuple[Any, ...]]) -> str:
    headers = [str(column[0]).strip().lower() for column in (cursor.description or [])]
    if not headers:
        return ""
    lines = ["\t".join(headers)]
    for row in rows:
        values: list[str] = []
        for item in row:
            if item is None:
                values.append("NULL")
            else:
                values.append(str(item))
        lines.append("\t".join(values))
    return "\n".join(lines) + "\n"



def run_mysql_query(host_or_container: str, user: str, password: str, database: str, sql: str) -> tuple[str, int]:
    host, port, resolved_user, resolved_password, resolved_database = _connection_settings(
        host_or_container, user, password, database
    )
    start = perf_counter()
    connection: pymysql.connections.Connection | None = None
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=resolved_user,
            password=resolved_password,
            database=resolved_database,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.Cursor,
        )
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = list(cursor.fetchall())
            output = _to_tsv(cursor, rows)
    except Exception as exc:
        raise RuntimeError(f"MySQL query failed: {exc}") from exc
    finally:
        elapsed_ms = int((perf_counter() - start) * 1000)
        if connection is not None:
            connection.close()
    return output, elapsed_ms



def parse_mysql_tsv(output: str) -> list[dict[str, Any]]:
    lines = [line for line in output.splitlines() if line.strip()]
    if not lines:
        return []
    headers = [header.strip().lower() for header in lines[0].split("\t")]
    rows: list[dict[str, Any]] = []
    for line in lines[1:]:
        values = line.split("\t")
        rows.append(dict(zip(headers, values)))
    return rows



def fetch_schema_metadata(
    host_or_container: str,
    user: str,
    password: str,
    database: str,
    tables: list[str],
) -> dict:
    if not tables:
        return {"tables": []}
    table_list = ",".join([f"'{t}'" for t in tables])
    sql = (
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{database}' AND table_name IN ({table_list}) "
        "ORDER BY table_name, ordinal_position"
    )
    output, _ = run_mysql_query(host_or_container, user, password, database, sql)
    rows = parse_mysql_tsv(output)
    table_map: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        table_name = row.get("table_name")
        column_name = row.get("column_name")
        data_type = row.get("data_type")
        if not table_name or not column_name or not data_type:
            continue
        table_map.setdefault(table_name, []).append({"name": column_name, "type": data_type})
    return {"tables": [{"name": table_name, "columns": columns} for table_name, columns in table_map.items()]}
