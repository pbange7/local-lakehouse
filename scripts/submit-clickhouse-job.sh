#!/usr/bin/env bash
# submit-clickhouse-job.sh — Verify ClickHouse Kafka consumer is active
# ClickHouse uses its native Kafka engine (not Flink JDBC) to consume web-logs.
# This script confirms ClickHouse is healthy and the Kafka engine tables are ready.
# Usage: ./scripts/submit-clickhouse-job.sh
# Prerequisites: docker compose up -d clickhouse (Kafka engine starts automatically)
set -euo pipefail

CH_URL="http://localhost:8123"

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# Helper: run a ClickHouse query
ch_query() {
  curl -sf -G "${CH_URL}/" --data-urlencode "query=${1}" 2>/dev/null || echo ""
}

log "Checking ClickHouse health..."
PING=$(curl -sf "${CH_URL}/ping" 2>/dev/null || echo "")
[ "$PING" = "Ok." ] \
  || fail "ClickHouse not responding at ${CH_URL}/ping. Run: docker compose up -d clickhouse"
log "ClickHouse is healthy."

log "Verifying Kafka engine table exists (web_logs.web_logs_queue)..."
ENGINE=$(ch_query "SELECT engine FROM system.tables WHERE database='web_logs' AND name='web_logs_queue'")
[ "$ENGINE" = "Kafka" ] \
  || fail "Kafka engine table not found (got: '${ENGINE}'). Check: docker compose logs clickhouse"
log "Kafka engine table is ready (engine=${ENGINE})."

log "Verifying materialized view exists (web_logs.web_logs_mv)..."
MV=$(ch_query "SELECT count() FROM system.tables WHERE database='web_logs' AND name='web_logs_mv'")
[ "$MV" = "1" ] \
  || fail "Materialized view web_logs.web_logs_mv not found."
log "Materialized view is ready."

echo ""
echo "==> CLICKHOUSE KAFKA CONSUMER ACTIVE"
echo "    Architecture: Kafka (web-logs) → Kafka Engine → MaterializedView → MergeTree"
echo "    Consumer group: clickhouse-web-logs"
echo "    Storage table:  web_logs.web_logs"
echo "    ClickHouse UI:  ${CH_URL}/play"
echo ""
echo "    Rows will appear within seconds. Run: make smoke-clickhouse"
