#!/usr/bin/env bash
# smoke-test-ui.sh — Verifies the Web UI is being served and the API proxy works
# Usage: ./scripts/smoke-test-ui.sh
# Prerequisites: docker compose up -d ui (api must be healthy first)
set -euo pipefail

UI="http://localhost:3000"

log()  { echo "[INFO]  $*"; }
pass() { echo "[PASS]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# STEP 1: nginx serves the HTML page
# ---------------------------------------------------------------------------
log "STEP 1: Checking that nginx serves index.html at ${UI}..."
BODY=$(curl -sf "${UI}/" 2>/dev/null || echo "")
[ -n "$BODY" ] \
  || fail "nginx not responding at ${UI}/. Is the stack running? docker compose up -d ui"
echo "$BODY" | grep -q "Local Lakehouse" \
  || fail "Expected 'Local Lakehouse' in HTML. Got: ${BODY:0:200}"
pass "nginx is serving the Web UI."

# ---------------------------------------------------------------------------
# STEP 2: API proxy — health endpoint
# ---------------------------------------------------------------------------
log "STEP 2: Checking API proxy at ${UI}/api/health..."
HEALTH=$(curl -sf "${UI}/api/health" 2>/dev/null || echo "")
echo "$HEALTH" | grep -q '"ok"' \
  || fail "API proxy health check failed. Response: ${HEALTH}"
pass "API proxy is working (/api/health → FastAPI)."

# ---------------------------------------------------------------------------
# STEP 3: API proxy — query endpoint
# ---------------------------------------------------------------------------
log "STEP 3: Checking API proxy query endpoint..."
QRES=$(curl -sf -X POST "${UI}/api/query/duckdb" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT 1 AS n"}' 2>/dev/null || echo "")
echo "$QRES" | grep -q '"columns"' \
  || fail "Query proxy failed. Response: ${QRES}"
pass "Query proxy working (DuckDB SELECT 1 returned columns)."

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo " WEB UI SMOKE TEST — PASSED"
echo "============================================================"
echo "  Web UI:        ${UI}"
echo "  API proxy:     ${UI}/api/"
echo "  Swagger docs:  http://localhost:8000/docs"
echo ""
echo "  Open ${UI} in your browser to use the SQL workbench."
echo "============================================================"
