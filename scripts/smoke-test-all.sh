#!/usr/bin/env bash
# smoke-test-all.sh — Consolidated end-to-end integration test for the local lakehouse.
# Runs 11 checks across all components and prints a single summary table.
# Usage: ./scripts/smoke-test-all.sh
# Prerequisites: make up && make flink-submit && make clickhouse-submit
set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
MINIO_USER="${MINIO_ROOT_USER:-lakehouse}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-lakehouse123}"

# ── Result tracking ──────────────────────────────────────────────────────────
declare -a CHECK_LABELS=()
declare -a CHECK_RESULTS=()
declare -a CHECK_DETAILS=()
PASS_COUNT=0
FAIL_COUNT=0

record() {
  local label="$1" result="$2" detail="$3"
  CHECK_LABELS+=("$label")
  CHECK_RESULTS+=("$result")
  CHECK_DETAILS+=("$detail")
  if [ "$result" = "PASS" ]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    echo "  [PASS] ${label}${detail:+  — ${detail}}"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo "  [FAIL] ${label}${detail:+  — ${detail}}" >&2
  fi
}

# ── Helper: run mc in a throwaway container ──────────────────────────────────
mc() {
  docker compose run --rm --no-deps \
    --entrypoint mc \
    -e MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@minio:9000" \
    minio-init "$@" 2>/dev/null
}

echo ""
echo "Running lakehouse integration checks..."
echo ""

# ── CHECK 1: Kafka ────────────────────────────────────────────────────────────
if docker exec lakehouse-kafka ${KAFKA_BIN}/kafka-topics.sh \
     --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  record "Kafka" "PASS" "broker responding on :9092"
else
  record "Kafka" "FAIL" "kafka-topics.sh --list failed"
fi

# ── CHECK 2: MinIO ───────────────────────────────────────────────────────────
MINIO_HTTP=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:9000/minio/health/live 2>/dev/null || echo "000")
if [ "$MINIO_HTTP" = "200" ]; then
  record "MinIO" "PASS" "health endpoint :9000 → 200"
else
  record "MinIO" "FAIL" "health endpoint returned HTTP ${MINIO_HTTP}"
fi

# ── CHECK 3: Nessie ──────────────────────────────────────────────────────────
NESSIE_BODY=$(curl -sf http://localhost:19120/api/v2/config 2>/dev/null || echo "")
if echo "$NESSIE_BODY" | grep -q "defaultBranch"; then
  BRANCH=$(echo "$NESSIE_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['defaultBranch'])" 2>/dev/null || echo "main")
  record "Nessie" "PASS" "catalog ready, default branch: ${BRANCH}"
else
  record "Nessie" "FAIL" "config endpoint returned unexpected: ${NESSIE_BODY:0:80}"
fi

# ── CHECK 4: Producer ────────────────────────────────────────────────────────
MSGS=$(docker exec lakehouse-kafka ${KAFKA_BIN}/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic web-logs \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 8000 2>/dev/null | tr -d '\r' || echo "")
if echo "$MSGS" | grep -q "request_id"; then
  MSG_COUNT=$(echo "$MSGS" | grep -c "request_id" || echo "0")
  record "Producer" "PASS" "${MSG_COUNT} messages with request_id field"
else
  record "Producer" "FAIL" "no messages with request_id consumed from web-logs"
fi

# ── CHECK 5: Flink streaming jobs ────────────────────────────────────────────
FLINK_JOBS=$(curl -sf http://localhost:8082/jobs/overview 2>/dev/null \
  | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j.get('state') == 'RUNNING']
print(len(running))
" 2>/dev/null || echo "0")
if [ "${FLINK_JOBS:-0}" -ge "1" ]; then
  record "Flink jobs" "PASS" "${FLINK_JOBS} RUNNING job(s)"
else
  record "Flink jobs" "FAIL" "no RUNNING jobs — run: make flink-submit"
fi

# ── CHECK 6: Iceberg data in MinIO ───────────────────────────────────────────
PARQUET_COUNT=$(mc ls --recursive local/lakehouse/warehouse/ 2>/dev/null \
  | grep -c "\.parquet" 2>/dev/null || echo "0")
if [ "${PARQUET_COUNT:-0}" -gt "0" ]; then
  record "Iceberg data" "PASS" "${PARQUET_COUNT} parquet file(s) in MinIO warehouse"
else
  record "Iceberg data" "FAIL" "no parquet files in s3://lakehouse/warehouse/"
fi

# ── CHECK 7: ClickHouse ──────────────────────────────────────────────────────
CH_PING=$(curl -sf http://localhost:8123/ping 2>/dev/null || echo "")
CH_COUNT=$(curl -sf -G http://localhost:8123/ \
  --data-urlencode "query=SELECT count() FROM web_logs.web_logs" 2>/dev/null \
  | tr -d '[:space:]' || echo "0")
if [ "$CH_PING" = "Ok." ] && [ "${CH_COUNT:-0}" -gt "0" ] 2>/dev/null; then
  record "ClickHouse" "PASS" "${CH_COUNT} rows in web_logs.web_logs"
else
  record "ClickHouse" "FAIL" "ping=${CH_PING} rows=${CH_COUNT}"
fi

# ── CHECK 8: Trino → Iceberg ─────────────────────────────────────────────────
TRINO_COUNT=$(docker exec lakehouse-trino trino \
  --output-format TSV \
  --execute "SELECT count(*) FROM iceberg.web_logs.web_logs" \
  2>/dev/null | tr -d '[:space:]' || echo "0")
if [ "${TRINO_COUNT:-0}" -gt "0" ] 2>/dev/null; then
  record "Trino (Iceberg)" "PASS" "${TRINO_COUNT} rows via iceberg catalog"
else
  record "Trino (Iceberg)" "FAIL" "count returned: ${TRINO_COUNT}"
fi

# ── CHECK 9: API health ───────────────────────────────────────────────────────
API_HEALTH=$(curl -sf http://localhost:8000/health 2>/dev/null || echo "")
if echo "$API_HEALTH" | grep -q '"ok"'; then
  record "API health" "PASS" "/health → {status: ok}"
else
  record "API health" "FAIL" "response: ${API_HEALTH:0:80}"
fi

# ── CHECK 10: API → DuckDB ───────────────────────────────────────────────────
API_DUCK=$(curl -sf -X POST http://localhost:8000/query/duckdb \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT count(*) AS n FROM web_logs.web_logs"}' \
  2>/dev/null || echo "")
DUCK_CNT=$(echo "$API_DUCK" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d['rows'][0][0])" 2>/dev/null || echo "0")
if [ "${DUCK_CNT:-0}" -gt "0" ] 2>/dev/null; then
  record "API DuckDB" "PASS" "${DUCK_CNT} rows via /query/duckdb"
else
  record "API DuckDB" "FAIL" "response: ${API_DUCK:0:120}"
fi

# ── CHECK 11: Web UI ─────────────────────────────────────────────────────────
UI_BODY=$(curl -sf http://localhost:3000/ 2>/dev/null || echo "")
if echo "$UI_BODY" | grep -q "Local Lakehouse"; then
  record "Web UI" "PASS" "nginx serving index.html on :3000"
else
  record "Web UI" "FAIL" "expected 'Local Lakehouse' in HTML"
fi

# ── Summary table ────────────────────────────────────────────────────────────
TOTAL=${#CHECK_LABELS[@]}
echo ""
echo "╔════════════════════════════════════════════╗"
echo "║  LAKEHOUSE INTEGRATION TEST RESULTS        ║"
echo "╠══════════════════════╦═════════════════════╣"
for i in "${!CHECK_LABELS[@]}"; do
  label="${CHECK_LABELS[$i]}"
  result="${CHECK_RESULTS[$i]}"
  if [ "$result" = "PASS" ]; then
    printf "║ %-20s ║ PASS                ║\n" "$label"
  else
    printf "║ %-20s ║ FAIL ←              ║\n" "$label"
  fi
done
echo "╠══════════════════════╩═════════════════════╣"
if [ "$FAIL_COUNT" -eq "0" ]; then
  printf "║ OVERALL: PASS  (%d/%d checks passed)       ║\n" "$PASS_COUNT" "$TOTAL"
else
  printf "║ OVERALL: FAIL  (%d/%d checks passed)       ║\n" "$PASS_COUNT" "$TOTAL"
fi
echo "╚════════════════════════════════════════════╝"
echo ""

if [ "$FAIL_COUNT" -gt "0" ]; then
  echo "Failed checks:"
  for i in "${!CHECK_RESULTS[@]}"; do
    if [ "${CHECK_RESULTS[$i]}" = "FAIL" ]; then
      echo "  • ${CHECK_LABELS[$i]}: ${CHECK_DETAILS[$i]}"
    fi
  done
  exit 1
fi
