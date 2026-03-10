#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VIEW_SQL_PATH="${VIEW_SQL_PATH:-$ROOT_DIR/data/match/pim_match_views.sql}"
DUMP_PATH="${PIM_SQL_DUMP_PATH:-$ROOT_DIR/../pim.sql}"

PIM_DB_HOST="${PIM_DB_HOST:-127.0.0.1}"
PIM_DB_PORT="${PIM_DB_PORT:-3306}"
PIM_DB_NAME="${PIM_DB_NAME:-pim_raw}"
PIM_DB_USER="${PIM_DB_USER:-root}"
PIM_DB_PASSWORD="${PIM_DB_PASSWORD:-}"

if ! command -v mysql >/dev/null 2>&1; then
  echo "mysql client is required but was not found in PATH" >&2
  exit 1
fi

if [[ ! -f "$VIEW_SQL_PATH" ]]; then
  echo "view SQL not found: $VIEW_SQL_PATH" >&2
  exit 1
fi

MYSQL_CMD=(
  mysql
  --host="$PIM_DB_HOST"
  --port="$PIM_DB_PORT"
  --user="$PIM_DB_USER"
  --database="$PIM_DB_NAME"
  --default-character-set=utf8mb4
)

if [[ -n "$PIM_DB_PASSWORD" ]]; then
  export MYSQL_PWD="$PIM_DB_PASSWORD"
fi

if [[ "${SKIP_DUMP_IMPORT:-0}" != "1" ]]; then
  if [[ ! -f "$DUMP_PATH" ]]; then
    echo "pim dump not found: $DUMP_PATH" >&2
    exit 1
  fi
  echo "[1/2] Importing PIM dump into $PIM_DB_NAME ..."
  "${MYSQL_CMD[@]}" < "$DUMP_PATH"
else
  echo "[1/2] SKIP_DUMP_IMPORT=1, skipping dump import."
fi

echo "[2/2] Rebuilding match views ..."
"${MYSQL_CMD[@]}" < "$VIEW_SQL_PATH"

echo "PIM match layer rebuild completed."
