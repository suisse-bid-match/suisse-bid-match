#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

docker compose up -d postgres qdrant

echo "Running DB migrations"
alembic upgrade head

echo "Starting API on http://localhost:8000"
uvicorn apps.api.main:app --reload &
API_PID=$!

cleanup() {
  kill "$API_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "Starting Next.js UI on http://localhost:3000"
cd apps/web
if [ ! -d node_modules ]; then
  npm install
fi
npm run dev

kill "$API_PID" >/dev/null 2>&1 || true
