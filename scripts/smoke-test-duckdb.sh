#!/usr/bin/env bash
# smoke-test-duckdb.sh — Runs DuckDB against Iceberg tables stored in MinIO
# Uses a one-shot Python container on the lakehouse Docker network.
# No persistent DuckDB service needed — DuckDB is embedded in the Python process.
# Usage: ./scripts/smoke-test-duckdb.sh
# Prerequisites: docker compose up -d (MinIO must be healthy with Iceberg data)
set -euo pipefail

NETWORK="lakehouse_lakehouse"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)/duckdb"

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

log "Running DuckDB Iceberg query via Python container..."
log "(Installing duckdb + boto3 — first run may take ~30s)"
echo ""

docker run --rm \
  --network "${NETWORK}" \
  -v "${SCRIPT_DIR}:/app:ro" \
  -w /app \
  -e MINIO_ENDPOINT="minio:9000" \
  -e MINIO_ACCESS_KEY="lakehouse" \
  -e MINIO_SECRET_KEY="lakehouse123" \
  python:3.12-slim \
  bash -c "pip install duckdb boto3 -q && python query.py" 2>&1 \
  || fail "DuckDB query failed. Check that Iceberg data exists in MinIO (run: make flink-submit smoke-flink first)."

echo ""
echo "==> PASS: DuckDB smoke test completed."
