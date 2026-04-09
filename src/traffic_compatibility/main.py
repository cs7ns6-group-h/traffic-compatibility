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

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.traffic_compatibility.kafka_consumer import start_consumer
from src.traffic_compatibility.kafka_producer import publish_decision
from src.traffic_compatibility.state import road_closures, traffic_conditions
from src.traffic_compatibility.cassandra_client import (
    get_journeys_by_segment,
    save_road_closure,
    delete_road_closure,
    save_traffic_condition,
    load_state_from_cassandra,
    cancel_journey 
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"[{REGION.upper()}] Traffic/Compatibility Service starting...")

    # Load state from Cassandra on startup
    saved_closures, saved_conditions = load_state_from_cassandra()
    road_closures.extend(saved_closures)
    traffic_conditions.update(saved_conditions)
    logger.info(f"[{REGION.upper()}] Loaded {len(road_closures)} closures and {len(traffic_conditions)} conditions from Cassandra.")

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


# ── Models ────────────────────────────────────────────────────────────────────

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


# ── Health ────────────────────────────────────────────────────────────────────

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


# ── Traffic Authority API ─────────────────────────────────────────────────────

@app.post("/authority/closure", summary="Submit road closure")
def submit_closure(closure: RoadClosure):
    road_closures.append(closure)
    save_road_closure(closure)
    logger.info(f"[{REGION.upper()}] Road closure received: {closure.segment_id}")
    return {"status": "accepted", "segment_id": closure.segment_id}

@app.get("/authority/closures", summary="List active road closures")
def list_closures():
    result = []
    for c in road_closures:
        if hasattr(c, 'model_dump'):
            result.append(c.model_dump())
        else:
            result.append({
                "segment_id": c.segment_id,
                "start_lat": c.start_lat,
                "start_lon": c.start_lon,
                "end_lat": c.end_lat,
                "end_lon": c.end_lon,
                "reason": c.reason,
                "valid_until": str(c.valid_until)
            })
    return {"region": REGION, "closures": result}


@app.delete("/authority/closure/{segment_id}", summary="Remove road closure")
def remove_closure(segment_id: str):
    global road_closures
    before = len(road_closures)
    road_closures[:] = [c for c in road_closures if c.segment_id != segment_id]
    removed = before - len(road_closures)
    if removed == 0:
        raise HTTPException(status_code=404, detail=f"No closure found for segment {segment_id}")
    delete_road_closure(segment_id)
    logger.info(f"[{REGION.upper()}] Removed closure for segment {segment_id}")
    return {"status": "removed", "segment_id": segment_id}


@app.post("/authority/traffic", summary="Submit traffic conditions")
def submit_traffic(update: TrafficUpdate):
    traffic_conditions[update.segment_id] = update.congestion_level
    save_traffic_condition(update.segment_id, update.congestion_level)
    logger.info(f"[{REGION.upper()}] Traffic update: {update.segment_id} = {update.congestion_level}")
    return {"status": "accepted"}


@app.post("/authority/emergency", summary="Issue emergency override")
def emergency_override(override: EmergencyOverride):
    from datetime import datetime, timedelta
    today = datetime.now()
    # Search today and the next 7 days to catch journeys regardless of their date_bucket
    date_buckets = [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]

    logger.warning(f"[{REGION.upper()}] Emergency override on segment: {override.segment_id}")

    journey_ids = get_journeys_by_segment(override.segment_id, date_buckets)

    for journey_id in journey_ids:
        publish_decision(
            journey_id=journey_id,
            status="cancelled",
            reason=f"emergency_override: {override.reason}"
        )
        cancel_journey(journey_id)
        logger.warning(f"[{REGION.upper()}] Cancelled journey {journey_id} due to emergency override")

    return {
        "status": "override_issued",
        "segment_id": override.segment_id,
        "journeys_cancelled": len(journey_ids)
    }


@app.get("/authority/segments/{segment_id}/journeys", summary="Get active journeys on segment")
def get_segment_journeys(segment_id: str):
    from datetime import datetime, timedelta
    today = datetime.now()
    date_buckets = [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]
    journey_ids = get_journeys_by_segment(segment_id, date_buckets)
    return {
        "segment_id": segment_id,
        "active_journeys": journey_ids,
        "count": len(journey_ids)
    }