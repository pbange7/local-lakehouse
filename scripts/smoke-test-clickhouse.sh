#!/usr/bin/env bash
# smoke-test-clickhouse.sh — Verify data is flowing into ClickHouse from Flink
# All queries use the ClickHouse HTTP API (no docker exec needed).
# Usage: ./scripts/smoke-test-clickhouse.sh
# Prerequisites: make clickhouse-submit (ClickHouse Flink job must be running)
set -euo pipefail

CH_URL="http://localhost:8123"
DB="web_logs"
TABLE="web_logs"
MAX_WAIT=60
POLL_INTERVAL=5

log()  { echo "[INFO]  $*"; }
pass() { echo "[PASS]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# Helper: run a ClickHouse query via HTTP API
# Uses -G to pass query as URL parameter (GET), not as form POST body.
ch_query() {
  local query="$1"
  local fmt="${2:-TabSeparated}"
  curl -sf -G "${CH_URL}/" \
    --data-urlencode "query=${query}" \
    --data-urlencode "default_format=${fmt}" \
    2>/dev/null || echo ""
}

# ---------------------------------------------------------------------------
# STEP 1: Health check
# ---------------------------------------------------------------------------
log "STEP 1: Checking ClickHouse health at ${CH_URL}..."
PING=$(curl -sf "${CH_URL}/ping" 2>/dev/null || echo "")
[ "$PING" = "Ok." ] \
  || fail "ClickHouse not responding at ${CH_URL}/ping. Is the stack running? Try: docker compose up -d clickhouse"
pass "ClickHouse is healthy."

# ---------------------------------------------------------------------------
# STEP 2: Table exists
# ---------------------------------------------------------------------------
log "STEP 2: Verifying table ${DB}.${TABLE} exists..."
TABLE_COUNT=$(ch_query "SELECT count() FROM system.tables WHERE database='${DB}' AND name='${TABLE}'")
[ "$TABLE_COUNT" = "1" ] \
  || fail "Table ${DB}.${TABLE} not found (count=${TABLE_COUNT}). Check ClickHouse init logs."
pass "Table ${DB}.${TABLE} exists."

# ---------------------------------------------------------------------------
# STEP 3: Wait for rows to appear
# ---------------------------------------------------------------------------
log "STEP 3: Waiting up to ${MAX_WAIT}s for rows in ${DB}.${TABLE}..."
ELAPSED=0
ROW_COUNT=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
  ROW_COUNT=$(ch_query "SELECT count() FROM ${DB}.${TABLE}" || echo "0")
  ROW_COUNT=$(echo "$ROW_COUNT" | tr -d '[:space:]')
  if [ "${ROW_COUNT:-0}" -gt "0" ] 2>/dev/null; then
    break
  fi
  log "  Row count: ${ROW_COUNT:-0} — waiting ${POLL_INTERVAL}s..."
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

[ "${ROW_COUNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "No rows in ${DB}.${TABLE} after ${MAX_WAIT}s. Is the ClickHouse Flink job running? Run: make clickhouse-submit"
pass "Row count: ${ROW_COUNT}"

# ---------------------------------------------------------------------------
# STEP 4: Sample rows
# ---------------------------------------------------------------------------
log "STEP 4: Sampling 5 rows..."
echo ""
ch_query "SELECT * FROM ${DB}.${TABLE} LIMIT 5 FORMAT JSONEachRow" \
  | head -5
echo ""

# ---------------------------------------------------------------------------
# STEP 5: Aggregate by status_code
# ---------------------------------------------------------------------------
log "STEP 5: Aggregate query — row count by HTTP status code..."
echo ""
AGGS=$(ch_query "SELECT status_code, count() as cnt FROM ${DB}.${TABLE} GROUP BY status_code ORDER BY status_code FORMAT TabSeparated")
echo "$AGGS" | awk '{printf "    status_code=%-6s  rows=%s\n", $1, $2}'
echo ""

STATUS_CODES=$(echo "$AGGS" | wc -l | tr -d '[:space:]')
[ "$STATUS_CODES" -gt "0" ] \
  || fail "Aggregate query returned no rows."
pass "Aggregate over ${STATUS_CODES} distinct status_code value(s)."

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo " CLICKHOUSE SMOKE TEST — PASSED"
echo "============================================================"
echo "  Table:        ${DB}.${TABLE}"
echo "  Total rows:   ${ROW_COUNT}"
echo "  Status codes: ${STATUS_CODES} distinct values"
echo ""
echo "  ClickHouse HTTP API:  ${CH_URL}"
echo "  ClickHouse Play UI:   ${CH_URL}/play"
echo ""
echo "  Example queries (paste into Play UI):"
echo "    SELECT status_code, count(), avg(latency_ms)"
echo "      FROM web_logs.web_logs"
echo "      GROUP BY status_code ORDER BY status_code;"
echo "============================================================"
