#!/usr/bin/env bash
# smoke-test-kafka.sh — verifies Kafka is up and can produce/consume messages
# Usage: ./scripts/smoke-test-kafka.sh
# Prerequisites: Docker running, `docker compose up -d` completed
set -euo pipefail

TOPIC="smoke-test"
MESSAGE="smoke-test-$(date +%s)"
KAFKA_BIN="/opt/kafka/bin"

log()  { echo "[INFO]  $*"; }
fail() { echo "[FAIL]  $*" >&2; exit 1; }

log "Creating topic '$TOPIC'..."
docker compose exec kafka ${KAFKA_BIN}/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

log "Listing topics..."
TOPICS=$(docker compose exec kafka ${KAFKA_BIN}/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list)

echo "$TOPICS" | grep -q "^${TOPIC}$" \
  || fail "Topic '$TOPIC' not found in topic list. Got: $TOPICS"
log "Topic '$TOPIC' confirmed."

log "Producing message: '$MESSAGE'..."
echo "$MESSAGE" | docker compose exec -T kafka ${KAFKA_BIN}/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC"

log "Consuming message (timeout 10s)..."
CONSUMED=$(docker compose exec kafka ${KAFKA_BIN}/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000 2>/dev/null | tr -d '\r')

[ "$CONSUMED" = "$MESSAGE" ] \
  || fail "Consumed message '$CONSUMED' does not match produced message '$MESSAGE'."
log "Round-trip message verified."

log "Cleaning up topic '$TOPIC'..."
docker compose exec kafka ${KAFKA_BIN}/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic "$TOPIC"

echo ""
echo "==> PASS: Kafka smoke test completed successfully."
