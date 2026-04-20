#!/usr/bin/env bash
# verify-producer.sh — consumes 10 messages from the web-logs topic and prints them
# Usage: ./scripts/verify-producer.sh
# Prerequisites: Docker running, `docker compose up -d --build` completed
set -euo pipefail

TOPIC="${KAFKA_TOPIC:-web-logs}"
MAX_MESSAGES=10
TIMEOUT_MS=15000

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

log "Waiting for up to $((TIMEOUT_MS / 1000))s to consume $MAX_MESSAGES messages from '$TOPIC'..."
echo ""

MESSAGES=$(docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages "$MAX_MESSAGES" \
  --timeout-ms "$TIMEOUT_MS" 2>/dev/null) || true

COUNT=$(echo "$MESSAGES" | grep -c '"request_id"' || true)

if [ "$COUNT" -eq 0 ]; then
  fail "No messages received from topic '$TOPIC'. Is the producer running? Check: docker compose logs producer"
fi

echo "$MESSAGES"
echo ""
echo "==> PASS: Consumed $COUNT message(s) from '$TOPIC'."
