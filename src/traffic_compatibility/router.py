"""
Router – route computation and compatibility checks.

Uses OpenStreetMap data via osmnx for real route computation.
Road graph is cached in memory and refreshed on startup.
"""

import logging
import os
import pickle
from datetime import datetime

import networkx as nx
import osmnx as ox

logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")

GRAPH_CACHE_PATH = "road_graph.pkl"

def get_graph():
    global _graph
    if _graph is None:
        # Try loading from disk first
        if os.path.exists(GRAPH_CACHE_PATH):
            logger.info(f"[{REGION.upper()}] Loading road graph from disk cache...")
            with open(GRAPH_CACHE_PATH, "rb") as f:
                _graph = pickle.load(f)
            logger.info(f"[{REGION.upper()}] Road graph loaded from cache: {len(_graph.nodes)} nodes")
        else:
            logger.info(f"[{REGION.upper()}] Downloading road graph from OSM...")
            try:
                _graph = ox.graph_from_place("Dublin, Ireland", network_type="drive")
                # Save to disk for next restart
                with open(GRAPH_CACHE_PATH, "wb") as f:
                    pickle.dump(_graph, f)
                logger.info(f"[{REGION.upper()}] Road graph saved to disk: {len(_graph.nodes)} nodes, {len(_graph.edges)} edges")
            except Exception as e:
                logger.error(f"[{REGION.upper()}] Failed to load road graph: {e}")
                _graph = None
    return _graph


def compute_route(origin: dict, destination: dict) -> dict | None:
    """
    Compute optimal route between origin and destination using OSM graph.
    Falls back to mock route if graph is unavailable.
    """
    if origin is None or destination is None:
        return None

    graph = get_graph()

    if graph is None:
        logger.warning(f"[{REGION.upper()}] Road graph unavailable, using mock route")
        return _mock_route(origin, destination)

    try:
        # Find nearest nodes to origin and destination
        orig_node = ox.distance.nearest_nodes(
            graph, origin["lon"], origin["lat"]
        )
        dest_node = ox.distance.nearest_nodes(
            graph, destination["lon"], destination["lat"]
        )

        # Compute shortest path
        path = nx.shortest_path(graph, orig_node, dest_node, weight="length")

        # Build route segments from path
        segments = []
        total_length = 0.0
        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            edge_data = graph.get_edge_data(u, v)
            if edge_data:
                edge = edge_data.get(0, edge_data)
                segment_id = edge.get("osmid", f"seg-{u}-{v}")
                if isinstance(segment_id, list):
                    segment_id = str(segment_id[0])
                else:
                    segment_id = str(segment_id)
                length = edge.get("length", 0)
                total_length += length
                segments.append({
                    "segment_id": segment_id,
                    "from": {"node": u},
                    "to": {"node": v},
                    "length_m": length
                })

        # Estimate travel time (assume 30 km/h average in city)
        estimated_minutes = (total_length / 1000) / 30 * 60

        return {
            "segments": segments,
            "estimated_travel_minutes": round(estimated_minutes, 1),
            "distance_km": round(total_length / 1000, 2),
        }

    except nx.NetworkXNoPath:
        logger.warning(f"[{REGION.upper()}] No path found between {origin} and {destination}")
        return None
    except Exception as e:
        logger.error(f"[{REGION.upper()}] Route computation failed: {e}, falling back to mock")
        return _mock_route(origin, destination)


def _mock_route(origin: dict, destination: dict) -> dict:
    """Fallback mock route when OSM graph is unavailable."""
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
    Returns (accepted: bool, reason: str)
    """
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
                    valid_until = closure.valid_until
                    if isinstance(valid_until, str):
                        valid_until = datetime.fromisoformat(valid_until)
                    if departure_dt <= valid_until:
                        logger.info(f"[{REGION.upper()}] Segment {sid} closed until {valid_until}")
                        return False, "road_closure"
                except (TypeError, ValueError):
                    return False, "road_closure"

        # Check congestion (reject if > 90%)
        congestion = traffic_conditions.get(sid, 0)
        if congestion > 90:
            logger.info(f"[{REGION.upper()}] Segment {sid} too congested ({congestion}%)")
            return False, "high_congestion"

    return True, "ok"