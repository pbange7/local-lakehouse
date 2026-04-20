#!/usr/bin/env bash
# submit-flink-job.sh — Submit the Flink SQL streaming job (Kafka → Iceberg)
# Idempotent: skips submission if a RUNNING job already exists.
# Usage: ./scripts/submit-flink-job.sh
# Prerequisites: make up (all services including Flink healthy)
set -euo pipefail

FLINK_URL="http://localhost:8082"
SQL_FILE="/opt/flink/sql/web-logs-to-iceberg.sql"
MAX_WAIT=120
POLL_INTERVAL=5

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Pre-flight: JobManager REST API reachable
# ---------------------------------------------------------------------------
log "Checking Flink JobManager REST API at ${FLINK_URL}..."
HTTP=$(curl -sf -o /dev/null -w "%{http_code}" "${FLINK_URL}/jobs/overview" 2>/dev/null || echo "000")
[ "$HTTP" = "200" ] \
  || fail "JobManager not reachable (HTTP ${HTTP}). Run: docker compose up -d flink-jobmanager"
log "JobManager is reachable."

# ---------------------------------------------------------------------------
# 2. Idempotency guard: skip if a RUNNING job already exists
# ---------------------------------------------------------------------------
RUNNING=$(curl -sf "${FLINK_URL}/jobs/overview" 2>/dev/null \
  | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
print(len([j for j in jobs if j.get('state') == 'RUNNING']))
" 2>/dev/null || echo "0")

if [ "$RUNNING" -gt "0" ]; then
  EXISTING=$(curl -sf "${FLINK_URL}/jobs/overview" 2>/dev/null \
    | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
r = [j for j in jobs if j.get('state') == 'RUNNING']
print(r[0]['jid'] if r else '')
" 2>/dev/null || echo "")
  log "Job already RUNNING — skipping submission (use 'docker compose exec flink-jobmanager /opt/flink/bin/flink cancel ${EXISTING}' to stop)"
  echo ""
  echo "==> RUNNING JOB: ${EXISTING}"
  echo "    Flink Web UI: ${FLINK_URL}/#/job/${EXISTING}/overview"
  exit 0
fi

# ---------------------------------------------------------------------------
# 3. Check free task slots
# ---------------------------------------------------------------------------
log "Checking TaskManager slot availability..."
FREE=$(curl -sf "${FLINK_URL}/overview" 2>/dev/null \
  | python3 -c "import sys, json; print(json.load(sys.stdin).get('slots-available', 0))" \
  2>/dev/null || echo "0")
[ "$FREE" -gt "0" ] \
  || fail "No free TaskManager slots (available=${FREE}). Check: docker compose logs flink-taskmanager"
log "Free slots: ${FREE}"

# ---------------------------------------------------------------------------
# 4. Submit SQL job via sql-client inside the JobManager container
# ---------------------------------------------------------------------------
log "Submitting SQL job: ${SQL_FILE}"
echo "--- sql-client output ---"
docker compose exec flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f "${SQL_FILE}" 2>&1
echo "--- end sql-client output ---"

# ---------------------------------------------------------------------------
# 5. Poll until a RUNNING job appears (max MAX_WAIT seconds)
# ---------------------------------------------------------------------------
log "Polling for RUNNING job (up to ${MAX_WAIT}s)..."
ELAPSED=0
JOB_ID=""
while [ $ELAPSED -lt $MAX_WAIT ]; do
  JOB_ID=$(curl -sf "${FLINK_URL}/jobs/overview" 2>/dev/null \
    | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
r = [j for j in jobs if j.get('state') == 'RUNNING']
print(r[0]['jid'] if r else '')
" 2>/dev/null || echo "")
  [ -n "$JOB_ID" ] && break
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

[ -n "$JOB_ID" ] \
  || fail "No RUNNING job found after ${MAX_WAIT}s. Check: docker compose logs flink-jobmanager"

echo ""
echo "==> JOB SUBMITTED SUCCESSFULLY"
echo "    Job ID:       ${JOB_ID}"
echo "    Flink Web UI: ${FLINK_URL}/#/job/${JOB_ID}/overview"
echo ""
echo "    Wait ~60s for the first checkpoint, then run: make smoke-flink"
