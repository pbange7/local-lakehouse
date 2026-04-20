#!/usr/bin/env bash
# smoke-test-trino.sh — Verifies Trino can query Iceberg (via Nessie) and ClickHouse
# Tests: health → catalog list → Iceberg row count → ClickHouse row count → cross-catalog join
# Usage: ./scripts/smoke-test-trino.sh
# Prerequisites: docker compose up -d trino (all dependent services healthy)
set -euo pipefail

TRINO_URL="http://localhost:8080"
TIMEOUT=30   # seconds to wait for each query

log()  { echo "[INFO]  $*"; }
pass() { echo "[PASS]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# Helper: run a query via the Trino CLI (blocks until complete)
trino_query() {
  docker exec lakehouse-trino trino \
    --output-format TSV \
    --execute "$1" \
    2>/dev/null
}

# ---------------------------------------------------------------------------
# STEP 1: Health check
# ---------------------------------------------------------------------------
log "STEP 1: Checking Trino health at ${TRINO_URL}..."
STARTING=$(curl -sf "${TRINO_URL}/v1/info" 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('starting', True))" 2>/dev/null || echo "true")
[ "$STARTING" = "False" ] \
  || fail "Trino is still starting or not reachable. Check: docker compose logs trino"
pass "Trino is ready."

# ---------------------------------------------------------------------------
# STEP 2: List catalogs — verify iceberg and clickhouse are registered
# ---------------------------------------------------------------------------
log "STEP 2: Listing catalogs..."
CATALOGS=$(trino_query "SHOW CATALOGS")
echo "  Available catalogs:"
echo "$CATALOGS" | sed 's/^/    /'

echo "$CATALOGS" | grep -qi "iceberg" \
  || fail "Catalog 'iceberg' not found. Check trino/catalog/iceberg.properties."
echo "$CATALOGS" | grep -qi "clickhouse" \
  || fail "Catalog 'clickhouse' not found. Check trino/catalog/clickhouse.properties."
pass "Both 'iceberg' and 'clickhouse' catalogs registered."

# ---------------------------------------------------------------------------
# STEP 3: Iceberg row count
# ---------------------------------------------------------------------------
log "STEP 3: Querying Iceberg table (SELECT count(*) FROM iceberg.web_logs.web_logs)..."
ICEBERG_COUNT=$(trino_query "SELECT count(*) FROM iceberg.web_logs.web_logs" | tr -d '[:space:]')
[ "${ICEBERG_COUNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "Iceberg returned 0 rows (got: '${ICEBERG_COUNT}'). Is the Flink job running? Run: make flink-submit"
pass "Iceberg row count: ${ICEBERG_COUNT}"

# ---------------------------------------------------------------------------
# STEP 4: ClickHouse row count
# ---------------------------------------------------------------------------
log "STEP 4: Querying ClickHouse table (SELECT count(*) FROM clickhouse.web_logs.web_logs)..."
CH_COUNT=$(trino_query "SELECT count(*) FROM clickhouse.web_logs.web_logs" | tr -d '[:space:]')
[ "${CH_COUNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "ClickHouse returned 0 rows (got: '${CH_COUNT}'). Is ClickHouse Kafka consumer active? Run: make clickhouse-submit"
pass "ClickHouse row count: ${CH_COUNT}"

# ---------------------------------------------------------------------------
# STEP 5: Cross-catalog join — Iceberg vs ClickHouse side by side
# ---------------------------------------------------------------------------
log "STEP 5: Cross-catalog join (Iceberg ⨝ ClickHouse on status_code)..."
echo ""
CROSS=$(trino_query "
SELECT
    i.status_code,
    i.iceberg_rows,
    c.clickhouse_rows
FROM (
    SELECT status_code, count(*) AS iceberg_rows
    FROM iceberg.web_logs.web_logs
    GROUP BY status_code
) i
JOIN (
    SELECT status_code, count(*) AS clickhouse_rows
    FROM clickhouse.web_logs.web_logs
    GROUP BY status_code
) c ON i.status_code = c.status_code
ORDER BY i.status_code
")

printf "  %-14s %-14s %-16s\n" "status_code" "iceberg_rows" "clickhouse_rows"
printf "  %-14s %-14s %-16s\n" "------------" "------------" "---------------"
echo "$CROSS" | awk '{printf "  %-14s %-14s %-16s\n", $1, $2, $3}'
echo ""

MATCHED=$(echo "$CROSS" | grep -c "." || echo "0")
[ "$MATCHED" -gt "0" ] \
  || fail "Cross-catalog join returned no rows."
pass "Cross-catalog join matched ${MATCHED} status_code value(s) across both stores."

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo " TRINO SMOKE TEST — PASSED"
echo "============================================================"
echo "  Iceberg rows:    ${ICEBERG_COUNT}"
echo "  ClickHouse rows: ${CH_COUNT}"
echo "  Cross-join rows: ${MATCHED} status_code buckets matched"
echo ""
echo "  Trino Web UI:    ${TRINO_URL}"
echo "  Example query:"
echo "    SELECT status_code, count(*), avg(latency_ms)"
echo "      FROM iceberg.web_logs.web_logs"
echo "      GROUP BY 1 ORDER BY 1;"
echo "============================================================"
