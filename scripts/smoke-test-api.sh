#!/usr/bin/env bash
# smoke-test-api.sh — Verifies the FastAPI backend is healthy and both query engines work
# Usage: ./scripts/smoke-test-api.sh
# Prerequisites: docker compose up -d api (trino + minio must be healthy)
set -euo pipefail

API="http://localhost:8000"

log()  { echo "[INFO]  $*"; }
pass() { echo "[PASS]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

api_get() {
  curl -sf "${API}${1}" 2>/dev/null || echo ""
}

api_post() {
  curl -sf -X POST "${API}${1}" \
    -H "Content-Type: application/json" \
    -d "${2}" 2>/dev/null || echo ""
}

api_status() {
  # -s only (no -f) so curl doesn't fail on 4xx — we want the status code, not curl's exit code
  curl -s -o /dev/null -w "%{http_code}" -X POST "${API}${1}" \
    -H "Content-Type: application/json" \
    -d "${2}" 2>/dev/null || echo "000"
}

# ---------------------------------------------------------------------------
# STEP 1: Health check
# ---------------------------------------------------------------------------
log "STEP 1: Health check..."
HEALTH=$(api_get "/health")
echo "$HEALTH" | grep -q '"ok"' \
  || fail "Health check failed. Response: ${HEALTH}. Is the API running? docker compose up -d api"
pass "API is healthy."

# ---------------------------------------------------------------------------
# STEP 2: Catalogs
# ---------------------------------------------------------------------------
log "STEP 2: GET /catalogs..."
CATS=$(api_get "/catalogs")
echo "$CATS" | grep -q "duckdb" \
  || fail "Catalog 'duckdb' not found in: ${CATS}"
echo "$CATS" | grep -q "trino" \
  || fail "Catalog 'trino' not found in: ${CATS}"
pass "Both 'duckdb' and 'trino' catalogs returned."

# ---------------------------------------------------------------------------
# STEP 3: Tables — DuckDB
# ---------------------------------------------------------------------------
log "STEP 3: GET /tables/duckdb..."
DBTABLES=$(api_get "/tables/duckdb")
echo "$DBTABLES" | grep -q "web_logs" \
  || fail "No 'web_logs' table found in DuckDB tables: ${DBTABLES}"
pass "DuckDB tables include 'web_logs'."

# ---------------------------------------------------------------------------
# STEP 4: Tables — Trino
# ---------------------------------------------------------------------------
log "STEP 4: GET /tables/trino..."
TTABLES=$(api_get "/tables/trino")
echo "$TTABLES" | grep -q "web_logs" \
  || fail "No 'web_logs' table found in Trino tables: ${TTABLES}"
pass "Trino tables include 'web_logs'."

# ---------------------------------------------------------------------------
# STEP 5: DuckDB query
# ---------------------------------------------------------------------------
log "STEP 5: POST /query/duckdb — SELECT count(*) FROM web_logs.web_logs..."
DRES=$(api_post "/query/duckdb" '{"sql":"SELECT count(*) AS cnt FROM web_logs.web_logs"}')
DCNT=$(echo "$DRES" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows'][0][0])" 2>/dev/null || echo "0")
[ "${DCNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "DuckDB returned 0 rows (response: ${DRES})"
pass "DuckDB row count: ${DCNT}"

# ---------------------------------------------------------------------------
# STEP 6: Trino query
# ---------------------------------------------------------------------------
log "STEP 6: POST /query/trino — SELECT count(*) FROM iceberg.web_logs.web_logs..."
TRES=$(api_post "/query/trino" '{"sql":"SELECT count(*) AS cnt FROM iceberg.web_logs.web_logs"}')
TCNT=$(echo "$TRES" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows'][0][0])" 2>/dev/null || echo "0")
[ "${TCNT:-0}" -gt "0" ] 2>/dev/null \
  || fail "Trino returned 0 rows (response: ${TRES})"
pass "Trino row count: ${TCNT}"

# ---------------------------------------------------------------------------
# STEP 7: Error handling
# ---------------------------------------------------------------------------
log "STEP 7: Error handling — bad SQL should return HTTP 400..."
STATUS=$(api_status "/query/duckdb" '{"sql":"SELECT * FROM definitely_nonexistent_table_xyz"}')
[ "$STATUS" = "400" ] \
  || fail "Expected HTTP 400 for bad SQL, got: ${STATUS}"
pass "Bad SQL correctly returns HTTP 400."

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo " FASTAPI SMOKE TEST — PASSED"
echo "============================================================"
echo "  DuckDB row count: ${DCNT}"
echo "  Trino row count:  ${TCNT}"
echo ""
echo "  API base URL:  ${API}"
echo "  Swagger UI:    ${API}/docs"
echo "  OpenAPI JSON:  ${API}/openapi.json"
echo "============================================================"
