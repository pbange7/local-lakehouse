import os


class Config:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "web-logs")
    messages_per_second: float = float(os.getenv("MESSAGES_PER_SECOND", "10"))
    client_id: str = os.getenv("PRODUCER_CLIENT_ID", "web-log-producer")
