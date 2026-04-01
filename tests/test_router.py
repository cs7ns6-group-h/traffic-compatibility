"""
Tests for Traffic/Compatibility Service
"""

from src.traffic_compatibility.router import compute_route, check_compatibility
from src.traffic_compatibility.state import road_closures, traffic_conditions


def test_compute_route_valid():
    origin = {"lat": 53.3498, "lon": -6.2603}
    destination = {"lat": 53.3388, "lon": -6.2591}
    route = compute_route(origin, destination)
    assert route is not None
    assert "segments" in route
    assert len(route["segments"]) > 0
    assert route["estimated_travel_minutes"] > 0
    assert route["distance_km"] > 0


def test_compute_route_missing_input():
    assert compute_route(None, None) is None


def test_check_compatibility_no_closures():
    road_closures.clear()
    traffic_conditions.clear()
    route = compute_route(
        {"lat": 53.3498, "lon": -6.2603},
        {"lat": 53.3388, "lon": -6.2591}
    )
    accepted, reason = check_compatibility(route, "2026-04-03T10:00:00")
    assert accepted is True
    assert reason == "ok"


def test_check_compatibility_no_route():
    accepted, reason = check_compatibility(None, "2026-04-03T10:00:00")
    assert accepted is False
    assert reason == "no_route"


def test_check_compatibility_high_congestion():
    road_closures.clear()
    traffic_conditions.clear()
    route = compute_route(
        {"lat": 53.3498, "lon": -6.2603},
        {"lat": 53.3388, "lon": -6.2591}
    )
    # Set high congestion on first segment
    first_segment = route["segments"][0]["segment_id"]
    traffic_conditions[first_segment] = 95
    accepted, reason = check_compatibility(route, "2026-04-03T10:00:00")
    assert accepted is False
    assert reason == "high_congestion"
    traffic_conditions.clear()