import json
import signal
import sys
import time

from confluent_kafka import Producer, KafkaException

import generator
from config import Config


running = True


def _handle_stop(signum, frame):
    global running
    print(f"[INFO] Received signal {signum}, shutting down...", flush=True)
    running = False


def _on_delivery(err, msg):
    if err:
        print(f"[ERROR] Delivery failed for {msg.key()}: {err}", file=sys.stderr, flush=True)


def main():
    global running
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    cfg = Config()
    sleep_interval = 1.0 / cfg.messages_per_second

    producer = Producer({
        "bootstrap.servers": cfg.bootstrap_servers,
        "client.id": cfg.client_id,
    })

    print(f"[INFO] Starting producer: topic={cfg.topic} rate={cfg.messages_per_second} msg/s bootstrap={cfg.bootstrap_servers}", flush=True)

    count = 0
    last_msg_summary = ""

    try:
        while running:
            msg = generator.generate()
            key = msg["request_id"]
            value = json.dumps(msg)

            producer.produce(
                cfg.topic,
                key=key,
                value=value,
                on_delivery=_on_delivery,
            )
            producer.poll(0)

            count += 1
            last_msg_summary = f"{msg['status_code']} {msg['method']} {msg['path']}"

            if count % 100 == 0:
                print(f"[INFO] Produced {count} messages to {cfg.topic} (last: {last_msg_summary})", flush=True)

            time.sleep(sleep_interval)

    except KafkaException as e:
        print(f"[ERROR] KafkaException: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        print(f"[INFO] Flushing {producer.len()} buffered messages...", flush=True)
        producer.flush()
        print(f"[INFO] Shutdown complete. Total produced: {count}", flush=True)


if __name__ == "__main__":
    main()
