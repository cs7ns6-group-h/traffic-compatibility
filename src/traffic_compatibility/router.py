"""
Router - route computation and compatibility checks.

For the demo, uses a simplified mock implementation.
In production: pre-processed OSM contracted hierarchy graph.
"""

import logging
import os

logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")


def compute_route(origin: dict, destination: dict) -> dict | None:
    """
    Compute optimal route between origin and destination.
    Mock implementation for demo — replace with OSM graph in production.
    """
    if origin is None or destination is None:
        return None

    return {
        "segments": [
            {"segment_id": "seg-001", "from": origin, "to": destination}
        ],
        "estimated_travel_minutes": 30,
        "distance_km": 15.0,
    }


from datetime import datetime

def check_compatibility(route: dict, departure_time: str) -> tuple[bool, str]:
    if route is None:
        return False, "no_route"

    from src.traffic_compatibility.state import road_closures, traffic_conditions

    # Parse departure time
    try:
        departure_dt = datetime.fromisoformat(departure_time)
    except (TypeError, ValueError):
        departure_dt = datetime.now()

    segments = route.get("segments", [])

    for seg in segments:
        sid = seg.get("segment_id")

        # Check road closures — only if closure is still valid at departure time
        for closure in road_closures:
            if closure.segment_id == sid:
                try:
                    valid_until = datetime.fromisoformat(closure.valid_until)
                    if departure_dt <= valid_until:
                        logger.info(f"[{REGION.upper()}] Segment {sid} closed until {valid_until}")
                        return False, "road_closure"
                except (TypeError, ValueError):
                    return False, "road_closure"

        # Check congestion
        congestion = traffic_conditions.get(sid, 0)
        if congestion > 90:
            logger.info(f"[{REGION.upper()}] Segment {sid} too congested ({congestion}%)")
            return False, "high_congestion"

    return True, "ok"