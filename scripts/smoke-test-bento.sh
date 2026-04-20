#!/usr/bin/env bash
# smoke-test-bento.sh — Verifies the Bento streaming pipeline is working
# Checks: Bento running → PyIceberg writer healthy → CH rows → Iceberg files in MinIO
# Usage: ./scripts/smoke-test-bento.sh
# Prerequisites: docker compose --profile bento up -d (make bento-up)
set -euo pipefail

MINIO_USER="${MINIO_ROOT_USER:-lakehouse}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-lakehouse123}"
MAX_WAIT=90
POLL_INTERVAL=5

log()  { echo "[INFO]  $*"; }
pass() { echo "[PASS]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

mc() {
  docker compose run --rm --no-deps \
    --entrypoint mc \
    -e MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@minio:9000" \
    minio-init "$@" 2>/dev/null
}

# ---------------------------------------------------------------------------
# STEP 1: Bento container is running
# ---------------------------------------------------------------------------
log "STEP 1: Checking Bento container..."
STATE=$(docker inspect --format='{{.State.Running}}' lakehouse-bento 2>/dev/null || echo "false")
[ "$STATE" = "true" ] \
  || fail "Bento container not running. Run: docker compose --profile bento up -d"
pass "Bento container is running."

# ---------------------------------------------------------------------------
# STEP 2: PyIceberg writer health
# ---------------------------------------------------------------------------
log "STEP 2: Checking PyIceberg writer health..."
HEALTH=$(curl -sf http://localhost:8001/health 2>/dev/null || echo "")
echo "$HEALTH" | grep -q '"ok"' \
  || fail "PyIceberg writer not healthy at :8001. Response: ${HEALTH}"
pass "PyIceberg writer is healthy."

# ---------------------------------------------------------------------------
# STEP 3: ClickHouse rows from Bento
# ---------------------------------------------------------------------------
log "STEP 3: Waiting up to ${MAX_WAIT}s for rows in ClickHouse..."
ELAPSED=0
CH_COUNT=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
  CH_COUNT=$(curl -sf -G http://localhost:8123/ \
    --data-urlencode "query=SELECT count() FROM web_logs.web_logs" \
    2>/dev/null | tr -d '[:space:]' || echo "0")
  [ "${CH_COUNT:-0}" -gt "0" ] 2>/dev/null && break
  log "  ClickHouse rows: ${CH_COUNT:-0} — waiting ${POLL_INTERVAL}s..."
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done
[ "${CH_COUNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "No rows in ClickHouse after ${MAX_WAIT}s. Check: docker compose logs bento"
pass "ClickHouse rows: ${CH_COUNT} (written by Bento sql_insert output)"

# ---------------------------------------------------------------------------
# STEP 4: Iceberg files in MinIO
# ---------------------------------------------------------------------------
log "STEP 4: Checking for Iceberg files in MinIO (written by PyIceberg writer)..."
ELAPSED=0
PARQUET_COUNT=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
  # Bento uses s3://lakehouse/warehouse-bento/ (separate from Flink's warehouse/)
  PARQUET_COUNT=$(mc ls --recursive local/lakehouse/warehouse-bento/ 2>/dev/null \
    | grep -c "\.parquet" 2>/dev/null || echo "0")
  [ "${PARQUET_COUNT:-0}" -gt "0" ] 2>/dev/null && break
  log "  Parquet files: ${PARQUET_COUNT:-0} — waiting ${POLL_INTERVAL}s..."
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done
[ "${PARQUET_COUNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "No Parquet files in MinIO after ${MAX_WAIT}s. Check: docker compose logs pyiceberg-writer"
pass "Iceberg Parquet files in MinIO: ${PARQUET_COUNT}"

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo " BENTO SMOKE TEST — PASSED"
echo "============================================================"
echo "  Engine:          Warpstream Bento"
echo "  Consumer group:  bento-web-logs"
echo "  ClickHouse rows: ${CH_COUNT}"
echo "  Parquet files:   ${PARQUET_COUNT}"
echo ""
echo "  Bento logs:     docker compose logs bento"
echo "  Writer logs:    docker compose logs pyiceberg-writer"
echo "============================================================"
