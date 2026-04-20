"""
Integration tests: require live Kafka at localhost:9094.
Skipped automatically when Kafka is not reachable (via conftest.kafka_available).
Each test gets a unique temporary topic via the `test_topic` fixture —
production topics are never touched.
"""
import json
import time
import uuid

import pytest

import generator  # resolved via pythonpath = ["producer"] in pyproject.toml

BOOTSTRAP = "localhost:9094"


def _consume_one(topic: str, timeout_s: int = 15) -> dict | None:
    """Helper: consume one message from `topic`, return parsed JSON or None."""
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"test-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    result = None
    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != -191:  # _PARTITION_EOF
                    pytest.fail(f"Consumer error: {msg.error()}")
                continue
            result = msg
            break
    finally:
        consumer.close()
    return result


@pytest.mark.integration
def test_produce_and_consume_single_message(test_topic):
    """Producer writes one message; consumer reads it back with key and payload intact."""
    from confluent_kafka import Producer

    event = generator.generate()
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    producer.produce(test_topic, key=event["request_id"], value=json.dumps(event))
    producer.flush(timeout=10)

    msg = _consume_one(test_topic)
    assert msg is not None, "No message received within 15 seconds"
    assert msg.key().decode() == event["request_id"]
    assert json.loads(msg.value().decode()) == event


@pytest.mark.integration
def test_message_fields_survive_serialization(test_topic):
    """All 8 fields present with correct types after JSON round-trip through Kafka."""
    from confluent_kafka import Producer

    event = generator.generate()
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    producer.produce(test_topic, key=event["request_id"], value=json.dumps(event))
    producer.flush(timeout=10)

    msg = _consume_one(test_topic)
    assert msg is not None, "No message received within 15 seconds"

    payload = json.loads(msg.value().decode())
    assert isinstance(payload["request_id"], str)
    assert isinstance(payload["method"], str)
    assert isinstance(payload["path"], str)
    assert isinstance(payload["status_code"], int)
    assert isinstance(payload["latency_ms"], int)
    assert isinstance(payload["ip"], str)
    assert isinstance(payload["user_agent"], str)
    assert isinstance(payload["ts"], str)
