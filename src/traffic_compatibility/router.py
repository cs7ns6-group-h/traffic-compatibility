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


def check_compatibility(route: dict, departure_time: str) -> tuple[bool, str]:
    """
    Check whether a route is compatible with current conditions.
    Reads road_closures and traffic_conditions from main.py shared state.
    Returns (accepted: bool, reason: str)
    """
    if route is None:
        return False, "no_route"

    # Import shared state from main (populated by Traffic Authority API)
    try:
        from src.traffic_compatibility.main import road_closures, traffic_conditions
    except ImportError:
        road_closures = []
        traffic_conditions = {}

    segments = route.get("segments", [])
    closed_segment_ids = {c.segment_id for c in road_closures}

    for seg in segments:
        sid = seg.get("segment_id")

        # Check road closures
        if sid in closed_segment_ids:
            logger.info(f"[{REGION.upper()}] Segment {sid} is closed")
            return False, "road_closure"

        # Check congestion (reject if > 90%)
        congestion = traffic_conditions.get(sid, 0)
        if congestion > 90:
            logger.info(f"[{REGION.upper()}] Segment {sid} too congested ({congestion}%)")
            return False, "high_congestion"

    return True, "ok"