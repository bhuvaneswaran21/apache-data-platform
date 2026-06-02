#!/usr/bin/env python3
"""
Sample Kafka producer — generates realistic e-commerce events.
Install: pip install kafka-python
Run:     python scripts/kafka_producer.py
"""

import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "user_events"
EVENTS_PER_SEC = 2

EVENT_TYPES = ["purchase", "view", "add_to_cart", "remove_from_cart", "wishlist"]
PRODUCTS = [
    "laptop", "phone", "tablet", "headphones", "keyboard",
    "monitor", "mouse", "webcam", "speaker", "smartwatch",
]
WEIGHTS = [0.15, 0.40, 0.20, 0.08, 0.17]   # purchase is intentionally rare


def make_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=WEIGHTS, k=1)[0]
    return {
        "user_id":    random.randint(1000, 9999),
        "event_type": event_type,
        "product":    random.choice(PRODUCTS),
        "amount":     round(random.uniform(9.99, 1999.99), 2) if event_type == "purchase" else 0.0,
        "ts":         datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z",
    }


def main():
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} ...")
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            break
        except NoBrokersAvailable:
            print(f"  Broker not ready, retrying ({attempt + 1}/10)...")
            time.sleep(5)
    else:
        sys.exit("Could not connect to Kafka after 10 retries.")

    sent = 0

    def shutdown(sig, frame):
        print(f"\nShutting down — sent {sent} events.")
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Producing to topic '{TOPIC}' at ~{EVENTS_PER_SEC} events/sec. Ctrl+C to stop.\n")
    while True:
        event = make_event()
        producer.send(TOPIC, value=event)
        sent += 1
        print(f"[{sent:>6}] {event['ts']}  user={event['user_id']}  "
              f"{event['event_type']:<16} {event['product']:<12}  ${event['amount']:.2f}")
        time.sleep(1 / EVENTS_PER_SEC)


if __name__ == "__main__":
    main()
