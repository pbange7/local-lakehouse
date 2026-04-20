#!/usr/bin/env bash
# smoke-test-nessie.sh — verifies the Nessie Iceberg catalog is reachable and functional
# Tests: health → config → create namespace → verify → delete
# Uses the Nessie REST API v1 (compatible with projectnessie/nessie:latest)
# Usage: ./scripts/smoke-test-nessie.sh
# Prerequisites: Docker running, `docker compose up -d` completed
set -euo pipefail

BASE="http://localhost:19120"
API="${BASE}/api/v1"
BRANCH="main"
NS="smoke_test"

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# Helper: make a curl request, return HTTP status code
http_status() {
  curl -sf -o /dev/null -w "%{http_code}" "$@" 2>/dev/null || echo "000"
}

# Helper: make a curl request, return response body
http_body() {
  curl -sf "$@" 2>/dev/null || echo ""
}

log "Checking Nessie health at ${BASE}..."
STATUS=$(http_status "${BASE}/q/health/ready")
[ "$STATUS" = "200" ] \
  || fail "Nessie health check returned HTTP ${STATUS}. Is the stack running? Try: docker compose up -d"
log "Nessie is healthy."

log "Verifying Nessie catalog config..."
BODY=$(http_body "${BASE}/api/v2/config")
echo "$BODY" | grep -q "defaultBranch" \
  || fail "Nessie config endpoint returned unexpected response: ${BODY}"
DEFAULT_BRANCH=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['defaultBranch'])" 2>/dev/null || echo "main")
log "Default branch: ${DEFAULT_BRANCH}"

log "Listing existing namespaces on '${BRANCH}'..."
INITIAL=$(http_body "${API}/namespaces/${BRANCH}")
log "Existing namespaces: $(echo "$INITIAL" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('namespaces',[])), 'namespace(s)')" 2>/dev/null || echo "unknown")"

log "Creating test namespace '${NS}' on '${BRANCH}'..."
STATUS=$(http_status -X PUT "${API}/namespaces/namespace/${BRANCH}/${NS}" \
  -H "Content-Type: application/json" \
  -d "{\"type\":\"NAMESPACE\",\"elements\":[\"${NS}\"],\"properties\":{}}")
[ "$STATUS" = "200" ] \
  || fail "Namespace creation returned HTTP ${STATUS} (expected 200)."
log "Namespace '${NS}' created."

log "Verifying namespace '${NS}' exists..."
BODY=$(http_body "${API}/namespaces/${BRANCH}")
echo "$BODY" | grep -q "${NS}" \
  || fail "Namespace '${NS}' not found in: ${BODY}"
log "Namespace '${NS}' confirmed."

log "Deleting test namespace '${NS}'..."
STATUS=$(http_status -X DELETE "${API}/namespaces/namespace/${BRANCH}/${NS}")
[ "$STATUS" = "204" ] \
  || fail "Namespace deletion returned HTTP ${STATUS} (expected 204)."
log "Namespace '${NS}' deleted."

echo ""
echo "==> PASS: Nessie catalog smoke test completed successfully."
echo ""
echo "    Nessie REST API (v1): ${BASE}/api/v1"
echo "    Nessie REST API (v2): ${BASE}/api/v2"
echo "    Default branch:       ${DEFAULT_BRANCH}"
echo ""
echo "    Phase 5 Flink NessieCatalog config:"
echo "      catalog-impl  = org.apache.iceberg.nessie.NessieCatalog"
echo "      uri           = http://nessie:19120/api/v1"
echo "      ref           = main"
echo "      warehouse     = s3://lakehouse/warehouse"
echo "      s3.endpoint   = http://minio:9000"
echo "      s3.path-style-access = true"
