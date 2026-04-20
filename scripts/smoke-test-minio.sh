#!/usr/bin/env bash
# smoke-test-minio.sh — verifies MinIO S3 API is reachable and the lakehouse bucket exists
# Tests a put/get/delete round-trip to confirm read-write access.
# Usage: ./scripts/smoke-test-minio.sh
# Prerequisites: Docker running, `docker compose up -d` completed
set -euo pipefail

MINIO_ALIAS="local"
MINIO_URL="http://minio:9000"
BUCKET="lakehouse"
TEST_OBJECT="${BUCKET}/smoke-test-$(date +%s).txt"
TEST_CONTENT="smoke-test-$(date +%s)"
MINIO_USER="${MINIO_ROOT_USER:-lakehouse}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-lakehouse123}"

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

# Helper: run mc inside a throwaway container on the lakehouse network.
# --entrypoint mc overrides the minio-init service's custom init entrypoint.
mc() {
  docker compose run --rm --no-deps \
    --entrypoint mc \
    -e MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@minio:9000" \
    minio-init "$@"
}

log "Checking MinIO health at localhost:9000..."
curl -sf http://localhost:9000/minio/health/live \
  || fail "MinIO health check failed. Is the stack running? Try: docker compose up -d"
log "MinIO is healthy."

log "Verifying bucket '$BUCKET' exists..."
mc ls "${MINIO_ALIAS}/" 2>/dev/null | grep -q "${BUCKET}" \
  || fail "Bucket '${BUCKET}' not found. Check minio-init logs: docker compose logs minio-init"
log "Bucket '${BUCKET}' confirmed."

log "Writing test object..."
echo "${TEST_CONTENT}" | mc pipe "${MINIO_ALIAS}/${TEST_OBJECT}" \
  || fail "Failed to write object to ${TEST_OBJECT}"

log "Reading test object back..."
RETRIEVED=$(mc cat "${MINIO_ALIAS}/${TEST_OBJECT}" 2>/dev/null | tr -d '\r\n')
[ "${RETRIEVED}" = "${TEST_CONTENT}" ] \
  || fail "Object content mismatch. Got: '${RETRIEVED}' Expected: '${TEST_CONTENT}'"
log "Read-write round-trip verified."

log "Deleting test object..."
mc rm "${MINIO_ALIAS}/${TEST_OBJECT}" \
  || fail "Failed to delete test object"

echo ""
echo "==> PASS: MinIO smoke test completed successfully."
echo "    S3 endpoint (internal): http://minio:9000"
echo "    S3 endpoint (host):     http://localhost:9000"
echo "    Console UI:             http://localhost:9001"
echo "    Bucket:                 ${BUCKET}"
