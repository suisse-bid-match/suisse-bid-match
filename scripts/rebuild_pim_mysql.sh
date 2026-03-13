#!/usr/bin/env sh
set -eu

MYSQL_HOST="${PIM_MYSQL_HOST:-mysql}"
MYSQL_PORT="${PIM_MYSQL_PORT:-3306}"
MYSQL_USER="${PIM_MYSQL_USER:-root}"
MYSQL_PASSWORD="${PIM_MYSQL_PASSWORD:-root}"
MYSQL_DB="${PIM_MYSQL_DB:-pim_raw}"
SQL_FILE="${PIM_SQL_FILE:-/prepare/pim.sql}"
CHECK_TABLE="${PIM_CHECK_TABLE:-articles}"
FORCE_REBUILD="${FORCE_REBUILD:-0}"

if [ ! -f "$SQL_FILE" ]; then
  echo "[mysql-init] SQL file not found: $SQL_FILE" >&2
  exit 1
fi

echo "[mysql-init] Waiting for MySQL at ${MYSQL_HOST}:${MYSQL_PORT} ..."
ATTEMPT=0
until mysqladmin ping -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" --silent >/dev/null 2>&1; do
  ATTEMPT=$((ATTEMPT + 1))
  if [ "$ATTEMPT" -ge 120 ]; then
    echo "[mysql-init] Timed out waiting for MySQL" >&2
    exit 1
  fi
  sleep 2
done

echo "[mysql-init] MySQL is ready"

mysql_exec() {
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$@"
}

if [ "$FORCE_REBUILD" = "1" ]; then
  echo "[mysql-init] FORCE_REBUILD=1 -> recreating database ${MYSQL_DB}"
  mysql_exec -e "DROP DATABASE IF EXISTS \`${MYSQL_DB}\`; CREATE DATABASE \`${MYSQL_DB}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
fi

mysql_exec -e "CREATE DATABASE IF NOT EXISTS \`${MYSQL_DB}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"

TABLE_EXISTS="0"
if [ -n "$CHECK_TABLE" ]; then
  TABLE_EXISTS="$(mysql_exec -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${MYSQL_DB}' AND table_name='${CHECK_TABLE}';")"
fi

if [ "$FORCE_REBUILD" != "1" ] && [ "${TABLE_EXISTS}" != "0" ]; then
  echo "[mysql-init] ${MYSQL_DB}.${CHECK_TABLE} already exists, skipping import"
  exit 0
fi

EXISTING_TABLES="$(mysql_exec -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${MYSQL_DB}';")"
if [ "$FORCE_REBUILD" != "1" ] && [ "${EXISTING_TABLES}" != "0" ]; then
  if [ -n "$CHECK_TABLE" ]; then
    echo "[mysql-init] ${MYSQL_DB} has existing tables but ${CHECK_TABLE} not found, skipping import"
  else
    echo "[mysql-init] ${MYSQL_DB} already has tables, skipping import"
  fi
  exit 0
fi

echo "[mysql-init] Importing ${SQL_FILE} into ${MYSQL_DB}"
mysql_exec "$MYSQL_DB" < "$SQL_FILE"
echo "[mysql-init] Import completed"
