"""
Tests for Traffic/Compatibility Service
"""

from src.traffic_compatibility.router import compute_route, check_compatibility


def test_compute_route_valid():
    origin = {"lat": 53.3498, "lon": -6.2603}
    destination = {"lat": 53.3388, "lon": -6.2591}
    route = compute_route(origin, destination)
    assert route is not None
    assert "segments" in route
    assert route["estimated_travel_minutes"] > 0


def test_compute_route_missing_input():
    assert compute_route(None, None) is None


def test_check_compatibility_no_closures():
    route = {
        "segments": [{"segment_id": "seg-001"}],
        "estimated_travel_minutes": 30,
    }
    accepted, reason = check_compatibility(route, "2026-04-01T10:00:00")
    assert accepted is True
    assert reason == "ok"


def test_check_compatibility_no_route():
    accepted, reason = check_compatibility(None, "2026-04-01T10:00:00")
    assert accepted is False
    assert reason == "no_route"