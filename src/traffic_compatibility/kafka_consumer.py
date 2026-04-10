"""
Kafka consumer - subscribes to journey.requested topic
and triggers compatibility checks.
"""

from datetime import datetime
import time
import json
import logging
import os

from confluent_kafka import Consumer, KafkaError

from src.traffic_compatibility.router import compute_route, check_compatibility
from src.traffic_compatibility.kafka_producer import publish_decision
from src.traffic_compatibility.cassandra_client import upsert_booking, write_journey_segment
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-eu:9092")


def process_booking_request(event: dict) -> None:
    journey_id = event.get("journey_id")
    origin = event.get("origin")
    destination = event.get("destination")
    departure_time = event.get("departure_time")
    vehicle_id = event.get("vehicle_id")
    driver_id = event.get("driver_id")
    date_bucket = event.get("date_bucket", datetime.now().strftime("%Y-%m-%d"))
    logger.info(f"[{REGION.upper()}] Processing journey {journey_id}: {origin} -> {destination}")

    # 1. Compute route
    route = compute_route(origin, destination)
    if route is None:
        logger.warning(f"[{REGION.upper()}] No route found for journey {journey_id}")
        upsert_booking(journey_id, "rejected", date_bucket=date_bucket,
                       vehicle_id=vehicle_id, driver_id=driver_id,
                       origin=origin, destination=destination,
                       departure_time=departure_time)
        publish_decision(journey_id, "rejected", reason="no_route_found",
                         vehicle_id=vehicle_id, date_bucket=date_bucket,
                         driver_id=driver_id)
        return

    # 2. Check compatibility
    accepted, reason = check_compatibility(route, departure_time)
    status = "accepted" if accepted else "rejected"

    # 3. Persist to Cassandra BEFORE publishing Kafka (enforcement needs the row immediately)
    upsert_booking(
        journey_id=journey_id,
        status=status,
        date_bucket=date_bucket,
        vehicle_id=vehicle_id,
        driver_id=driver_id,
        origin=origin,
        destination=destination,
        departure_time=departure_time,
        route=route if accepted else None,
    )
    if accepted:
        for seg in route.get("segments", []):
            write_journey_segment(journey_id, seg["segment_id"], date_bucket)

    # 4. Publish decision (row is already in Cassandra)
    publish_decision(
        journey_id=journey_id,
        status=status,
        reason=reason,
        route=route if accepted else None,
        vehicle_id=vehicle_id,
        date_bucket=date_bucket,
        driver_id=driver_id,
    )

    logger.info(f"[{REGION.upper()}] Journey {journey_id} -> {status} ({reason})")

def start_consumer() -> None:
    retries = 0
    max_retries = 10
    
    while retries < max_retries:
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"traffic-compatibility-{REGION}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            })
            consumer.subscribe(["journey.requested"])
            logger.info(f"[{REGION.upper()}] Subscribed to journey.requested")
            
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    process_booking_request(event)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")

        except Exception as e:
            retries += 1
            wait = 2 ** retries
            logger.warning(f"[{REGION.upper()}] Kafka consumer failed (attempt {retries}/{max_retries}), retrying in {wait}s: {e}")
            time.sleep(wait)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    logger.error(f"[{REGION.upper()}] Kafka consumer failed after {max_retries} retries, giving up.")