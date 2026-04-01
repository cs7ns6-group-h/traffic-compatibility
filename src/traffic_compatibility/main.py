"""
Traffic/Compatibility Service - CS7NS6 Group H
Marta Fraioli

Responsibilities:
- Subscribe to journey.requested events from Kafka
- Compute route using OSM road graph
- Overlay real-time traffic conditions from traffic authorities
- Publish journey.accepted or journey.rejected to Kafka
- Write final booking status to Cassandra
"""

import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from src.traffic_compatibility.kafka_consumer import start_consumer
from src.traffic_compatibility.kafka_producer import publish_decision
from src.traffic_compatibility.cassandra_client import update_booking_status
from src.traffic_compatibility.router import compute_route, check_compatibility

from src.traffic_compatibility.state import road_closures, traffic_conditions
from src.traffic_compatibility.cassandra_client import get_journeys_by_segment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"[{REGION.upper()}] Traffic/Compatibility Service starting...")
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()
    logger.info(f"[{REGION.upper()}] Kafka consumer started.")
    yield
    logger.info(f"[{REGION.upper()}] Shutting down.")


app = FastAPI(
    title="Traffic/Compatibility Service",
    description=f"Region: {REGION.upper()} - Validates journey requests against road conditions.",
    version="1.0.0",
    lifespan=lifespan,
)


# Health check

@app.get("/health")
def health():
    return {"status": "ok", "region": REGION, "service": "traffic-compatibility"}


@app.get("/status")
def status():
    return {
        "region": REGION,
        "active_closures": len(road_closures),
        "monitored_segments": len(traffic_conditions),
    }


# Traffic Authority API

class RoadClosure(BaseModel):
    segment_id: str
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    reason: str
    valid_until: str  # ISO8601


class TrafficUpdate(BaseModel):
    segment_id: str
    congestion_level: int  # 0-100


class EmergencyOverride(BaseModel):
    segment_id: str
    reason: str


@app.post("/authority/closure", summary="Submit road closure")
def submit_closure(closure: RoadClosure):
    road_closures.append(closure)
    logger.info(f"[{REGION.upper()}] Road closure received: {closure.segment_id}")
    return {"status": "accepted", "segment_id": closure.segment_id}


@app.post("/authority/traffic", summary="Submit traffic conditions")
def submit_traffic(update: TrafficUpdate):
    traffic_conditions[update.segment_id] = update.congestion_level
    logger.info(f"[{REGION.upper()}] Traffic update: {update.segment_id} = {update.congestion_level}")
    return {"status": "accepted"}


@app.post("/authority/emergency", summary="Issue emergency override")
def emergency_override(override: EmergencyOverride):
    logger.warning(f"[{REGION.upper()}] Emergency override on segment: {override.segment_id}")
    # TODO: publish journey.cancelled for all active journeys on this segment
    return {"status": "override_issued", "segment_id": override.segment_id}

@app.post("/authority/emergency")
def emergency_override(override: EmergencyOverride):
    from datetime import datetime
    date_bucket = datetime.now().strftime("%Y-%m-%d")
    
    logger.warning(f"[{REGION.upper()}] Emergency override on segment: {override.segment_id}")
    
    # Trova tutti i journey attivi su questo segmento
    journey_ids = get_journeys_by_segment(override.segment_id, date_bucket)
    
    # Pubblica journey.cancelled per ognuno
    for journey_id in journey_ids:
        publish_decision(
            journey_id=journey_id,
            status="cancelled",
            reason=f"emergency_override: {override.reason}"
        )
        logger.warning(f"[{REGION.upper()}] Cancelled journey {journey_id} due to emergency override")
    
    return {
        "status": "override_issued",
        "segment_id": override.segment_id,
        "journeys_cancelled": len(journey_ids)
    }