"""
Kafka producer - publishes journey.accepted / journey.rejected events.
"""

import json
import logging
import os

from confluent_kafka import Producer

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-eu:9092")

_producer = None


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return _producer


def publish_decision(
    journey_id: str,
    status: str,
    reason: str = "ok",
    route: dict | None = None,
) -> None:
    topic = f"journey.{status}"
    payload = {
        "journey_id": journey_id,
        "status": status,
        "reason": reason,
        "region": REGION,
        "route": route,
    }
    producer = get_producer()
    producer.produce(
        topic,
        key=journey_id,
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()
    logger.info(f"[{REGION.upper()}] Published {topic} for journey {journey_id}")