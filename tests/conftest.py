import socket
import uuid

import pytest

KAFKA_HOST = "localhost"
KAFKA_PORT = 9094
TEST_TOPIC_PREFIX = "test-"


def _kafka_reachable() -> bool:
    try:
        with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=3):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def kafka_available() -> bool:
    """Session-scoped probe — socket check runs once per pytest session."""
    return _kafka_reachable()


@pytest.fixture(scope="function")
def test_topic(kafka_available):
    """
    Creates a unique temporary topic before each integration test and
    deletes it after, regardless of test outcome.
    Skips automatically if Kafka is not reachable.
    """
    if not kafka_available:
        pytest.skip("Kafka not reachable at localhost:9094 — start with: make up")

    from confluent_kafka.admin import AdminClient, NewTopic

    bootstrap = f"{KAFKA_HOST}:{KAFKA_PORT}"
    topic_name = f"{TEST_TOPIC_PREFIX}{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})

    fs = admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
    fs[topic_name].result()  # raises KafkaException on failure

    yield topic_name

    # Best-effort cleanup
    fs = admin.delete_topics([topic_name])
    try:
        fs[topic_name].result()
    except Exception:
        pass
