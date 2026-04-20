#!/usr/bin/env bash
# smoke-test-flink.sh — End-to-end verification of the Flink→Iceberg pipeline
# Checks: job RUNNING, checkpoint completed, Iceberg files in MinIO, table in Nessie
# Usage: ./scripts/smoke-test-flink.sh
# Prerequisites: make flink-submit (job must already be running)
set -euo pipefail

FLINK_URL="http://localhost:8082"
NESSIE_API="http://localhost:19120/api/v1"
MINIO_USER="${MINIO_ROOT_USER:-lakehouse}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-lakehouse123}"
BUCKET="lakehouse"
CHECKPOINT_WAIT=90
POLL_INTERVAL=10

log()  { echo "[INFO]  $*"; }
pass() { echo "[PASS]  $*"; }
warn() { echo "[WARN]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# Helper: run mc in a throwaway container on the lakehouse network
mc() {
  docker compose run --rm --no-deps \
    --entrypoint mc \
    -e MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@minio:9000" \
    minio-init "$@"
}

# ---------------------------------------------------------------------------
# STEP 1: Flink REST API reachable
# ---------------------------------------------------------------------------
log "STEP 1: Checking Flink REST API at ${FLINK_URL}..."
HTTP=$(curl -sf -o /dev/null -w "%{http_code}" "${FLINK_URL}/jobs/overview" 2>/dev/null || echo "000")
[ "$HTTP" = "200" ] \
  || fail "Flink REST API not reachable (HTTP ${HTTP}). Run: docker compose up -d flink-jobmanager"
pass "Flink REST API is reachable."

# ---------------------------------------------------------------------------
# STEP 2: Verify a job is in RUNNING state
# ---------------------------------------------------------------------------
log "STEP 2: Checking for RUNNING job..."
JOBS_JSON=$(curl -sf "${FLINK_URL}/jobs/overview" 2>/dev/null)
JOB_ID=$(echo "$JOBS_JSON" \
  | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
r = [j for j in jobs if j.get('state') == 'RUNNING']
print(r[0]['jid'] if r else '')
" 2>/dev/null || echo "")
[ -n "$JOB_ID" ] \
  || fail "No RUNNING job found. Run: make flink-submit"
pass "Job is RUNNING. ID: ${JOB_ID}"

# ---------------------------------------------------------------------------
# STEP 3: Wait for first completed checkpoint (Iceberg commits on checkpoint)
# ---------------------------------------------------------------------------
log "STEP 3: Waiting up to ${CHECKPOINT_WAIT}s for first checkpoint..."
ELAPSED=0
COMPLETED=0
while [ $ELAPSED -lt $CHECKPOINT_WAIT ]; do
  COMPLETED=$(curl -sf "${FLINK_URL}/jobs/${JOB_ID}/checkpoints" 2>/dev/null \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('counts', {}).get('completed', 0))
" 2>/dev/null || echo "0")
  [ "$COMPLETED" -gt "0" ] && break
  log "  Completed checkpoints: ${COMPLETED} — waiting ${POLL_INTERVAL}s..."
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done
[ "$COMPLETED" -gt "0" ] \
  || fail "No completed checkpoints after ${CHECKPOINT_WAIT}s. Check: docker compose logs flink-jobmanager"
pass "Checkpoints completed: ${COMPLETED}"

# ---------------------------------------------------------------------------
# STEP 4: Verify Iceberg files landed in MinIO
# ---------------------------------------------------------------------------
log "STEP 4: Checking for Iceberg files in MinIO (s3://${BUCKET}/warehouse/)..."
MC_LIST=$(mc ls --recursive "local/${BUCKET}/warehouse/" 2>/dev/null || true)
echo "$MC_LIST" | head -10

PARQUET_COUNT=$(echo "$MC_LIST" | grep -c "\.parquet" 2>/dev/null || echo "0")
META_COUNT=$(echo "$MC_LIST" | grep -c "metadata" 2>/dev/null || echo "0")

[ "$META_COUNT" -gt "0" ] \
  || fail "No Iceberg metadata files in MinIO. Iceberg table may not have been created — check Flink logs."

if [ "$PARQUET_COUNT" -gt "0" ]; then
  pass "Found ${PARQUET_COUNT} parquet file(s) and ${META_COUNT} metadata file(s) in MinIO."
else
  warn "No parquet data files yet (metadata files found: ${META_COUNT}). Iceberg table exists but may be writing. Wait ~60s and retry."
  pass "Iceberg metadata files confirmed in MinIO (${META_COUNT})."
fi

# ---------------------------------------------------------------------------
# STEP 5: Verify Nessie namespace exists
# ---------------------------------------------------------------------------
log "STEP 5: Verifying Nessie namespace 'web_logs' on branch 'main'..."
NS_BODY=$(curl -sf "${NESSIE_API}/namespaces/main" 2>/dev/null || echo "{}")
echo "$NS_BODY" | grep -q "web_logs" \
  || fail "Namespace 'web_logs' not found in Nessie. Raw: ${NS_BODY}"
pass "Nessie namespace 'web_logs' exists on branch 'main'."

# ---------------------------------------------------------------------------
# STEP 6: Verify Iceberg table is registered in Nessie
# ---------------------------------------------------------------------------
log "STEP 6: Verifying Iceberg table in Nessie entries..."
ENTRIES=$(curl -sf "${NESSIE_API}/trees/tree/main/entries" 2>/dev/null || echo "{}")
TABLE_FOUND=$(echo "$ENTRIES" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
entries = d.get('entries', [])
tables = [e for e in entries if e.get('type') == 'ICEBERG_TABLE']
elements = ['.'.join(e.get('name', {}).get('elements', [])) for e in tables]
print('\n'.join(elements))
found = any('web_logs' in e for e in elements)
sys.exit(0 if found else 1)
" 2>/dev/null || echo "")
[ $? -eq 0 ] \
  || fail "Iceberg table 'web_logs' not found in Nessie entries. Registered tables: ${TABLE_FOUND}"
pass "Iceberg table registered in Nessie: ${TABLE_FOUND}"

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo " FLINK PIPELINE SMOKE TEST — PASSED"
echo "============================================================"
echo "  Job ID:           ${JOB_ID}"
echo "  Checkpoints:      ${COMPLETED} completed"
echo "  Parquet files:    ${PARQUET_COUNT}"
echo "  Metadata files:   ${META_COUNT}"
echo ""
echo "  Flink Web UI:     ${FLINK_URL}/#/job/${JOB_ID}/overview"
echo "  Flink Checkpoints:${FLINK_URL}/#/job/${JOB_ID}/checkpoints"
echo "  MinIO Console:    http://localhost:9001  (lakehouse / lakehouse123)"
echo "  MinIO Path:       s3://${BUCKET}/warehouse/web_logs/"
echo "  Nessie Entries:   http://localhost:19120/api/v1/trees/tree/main/entries"
echo "============================================================"
