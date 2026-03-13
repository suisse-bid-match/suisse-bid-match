from __future__ import annotations

import subprocess
from time import perf_counter
from typing import Any


def run_mysql_query(container: str, user: str, password: str, database: str, sql: str) -> tuple[str, int]:
    cmd = [
        "docker",
        "exec",
        container,
        "mysql",
        f"-u{user}",
        f"-p{password}",
        "-D",
        database,
        "--batch",
        "--raw",
        "-e",
        sql,
    ]
    start = perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=False, check=False)
    elapsed_ms = int((perf_counter() - start) * 1000)
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(f"MySQL query failed: {stderr.strip()}")
    return stdout, elapsed_ms


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
    container: str,
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
    output, _ = run_mysql_query(container, user, password, database, sql)
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

