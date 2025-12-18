# python/kafka_producer.py

import json
import random
import string
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "sales_events"


REGIONS = ["EU", "US", "APAC", "MENA"]
CHANNELS = ["web", "mobile", "store"]
CURRENCIES = ["EUR", "USD", "GBP"]
STATUSES = ["SUCCESS", "FAILED", "PENDING"]


def random_id(prefix: str, length: int = 8) -> str:
    return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def generate_event() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    amount = round(random.uniform(10, 500), 2)

    event = {
        "event_id": random_id("EVT_"),
        "order_id": random_id("ORD_"),
        "user_id": random.randint(1, 10_000),
        "region": random.choice(REGIONS),
        "channel": random.choice(CHANNELS),
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
        "created_at": now,
    }
    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"Producing events to topic '{TOPIC_NAME}' on {KAFKA_BROKER} ...")

    try:
        while True:
            event = generate_event()
            key = event["region"]  # particionisanje po regionu

            producer.send(TOPIC_NAME, key=key, value=event)
            producer.flush()

            print(f"Sent event: {event}")
            time.sleep(1)  # 1 event u sekundi (možeš kasnije ubrzati/usporiti)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
