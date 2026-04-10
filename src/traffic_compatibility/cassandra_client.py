"""
Cassandra client - reads and writes booking status.
"""

import logging
import os
import time
from uuid import UUID
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra-eu").split(",")
CASSANDRA_DC = os.getenv("CASSANDRA_DC", "eu")

_session = None


def _retry(func, retries=3, delay=1):
    """Retry a function on failure with exponential backoff."""
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            logger.warning(f"Cassandra operation failed (attempt {attempt + 1}/{retries}), retrying in {wait}s: {e}")
            time.sleep(wait)


def get_session():
    global _session
    if _session is None:
        cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=5
        )
        _session = cluster.connect()
        _ensure_schema(_session)
    return _session


def _ensure_schema(session) -> None:
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS traffic_service
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """)
    session.set_keyspace("traffic_service")
    session.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            region TEXT,
            date_bucket TEXT,
            journey_id UUID,
            vehicle_id TEXT,
            driver_id TEXT,
            origin TEXT,
            destination TEXT,
            departure_time TIMESTAMP,
            status TEXT,
            route TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY ((region, date_bucket), journey_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS journeys_by_segment (
            segment_id TEXT,
            date_bucket TEXT,
            journey_id UUID,
            region TEXT,
            status TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY ((segment_id, date_bucket), journey_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS road_closures (
            segment_id TEXT,
            valid_until TIMESTAMP,
            start_lat DOUBLE,
            start_lon DOUBLE,
            end_lat DOUBLE,
            end_lon DOUBLE,
            reason TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (segment_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS traffic_conditions (
            segment_id TEXT,
            congestion_level INT,
            updated_at TIMESTAMP,
            PRIMARY KEY (segment_id)
        )
    """)
    logger.info(f"[{REGION.upper()}] Cassandra schema ready.")


def upsert_booking(
    journey_id: str,
    status: str,
    date_bucket: str = None,
    vehicle_id: str = None,
    driver_id: str = None,
    origin: dict = None,
    destination: dict = None,
    departure_time: str = None,
    route: dict = None,
) -> None:
    """Full INSERT (upsert) of a booking row. Must be called BEFORE publishing to Kafka
    so that downstream consumers (enforcement) can immediately look up the row."""
    import json as _json
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        dep_ts = None
        if departure_time:
            try:
                dep_ts = datetime.fromisoformat(departure_time)
            except (TypeError, ValueError):
                pass

        def _write():
            session = get_session()
            session.execute("""
                INSERT INTO traffic_service.bookings
                (region, date_bucket, journey_id, vehicle_id, driver_id,
                 origin, destination, departure_time, status, route,
                 created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        toTimestamp(now()), toTimestamp(now()))
            """, (
                REGION,
                date_bucket,
                UUID(journey_id),
                vehicle_id,
                UUID(driver_id) if driver_id else None,
                _json.dumps(origin) if origin else None,
                _json.dumps(destination) if destination else None,
                dep_ts,
                status,
                _json.dumps(route) if route else None,
            ))
        _retry(_write)
    except Exception as e:
        logger.error(f"Cassandra upsert failed for journey {journey_id}: {e}")


def update_booking_status(journey_id: str, status: str, date_bucket: str = None) -> None:
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        def _write():
            session = get_session()
            session.execute("""
                UPDATE traffic_service.bookings
                SET status = %s, updated_at = toTimestamp(now())
                WHERE region = %s AND date_bucket = %s AND journey_id = %s
            """, (status, REGION, date_bucket, UUID(journey_id)))
        _retry(_write)
    except Exception as e:
        logger.error(f"Cassandra write failed for journey {journey_id}: {e}")


def write_journey_segment(journey_id: str, segment_id: str, date_bucket: str = None) -> None:
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        def _write():
            session = get_session()
            session.execute("""
                INSERT INTO traffic_service.journeys_by_segment
                (segment_id, date_bucket, journey_id, region, status, updated_at)
                VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
            """, (segment_id, date_bucket, UUID(journey_id), REGION, "accepted"))
        _retry(_write)
    except Exception as e:
        logger.error(f"Cassandra segment write failed: {e}")


def get_journeys_by_segment(segment_id: str, date_buckets: list[str] = None) -> list:
    """Return accepted journey IDs on a segment across the given date buckets.

    If date_buckets is None, defaults to today only.
    Pass multiple buckets when the exact booking date is unknown (e.g. enforcement).
    """
    if date_buckets is None:
        date_buckets = [datetime.now().strftime("%Y-%m-%d")]
    try:
        session = get_session()
        journey_ids = []
        for date_bucket in date_buckets:
            rows = session.execute("""
                SELECT journey_id FROM traffic_service.journeys_by_segment
                WHERE segment_id = %s AND date_bucket = %s AND status = 'accepted'
                ALLOW FILTERING
            """, (segment_id, date_bucket))
            journey_ids.extend(str(row.journey_id) for row in rows)
        return journey_ids
    except Exception as e:
        logger.error(f"Cassandra segment query failed: {e}")
        return []


def cancel_journey(journey_id: str, date_bucket: str = None) -> None:
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        def _write():
            session = get_session()
            session.execute("""
                UPDATE traffic_service.bookings
                SET status = 'cancelled', updated_at = toTimestamp(now())
                WHERE region = %s AND date_bucket = %s AND journey_id = %s
            """, (REGION, date_bucket, UUID(journey_id)))
        _retry(_write)
    except Exception as e:
        logger.error(f"Failed to cancel journey {journey_id} in bookings: {e}")

    try:
        def _write_segment():
            session = get_session()
            rows = session.execute("""
                SELECT segment_id, date_bucket FROM traffic_service.journeys_by_segment
                WHERE journey_id = %s
                ALLOW FILTERING
            """, (UUID(journey_id),))
            for row in rows:
                session.execute("""
                    UPDATE traffic_service.journeys_by_segment
                    SET status = 'cancelled'
                    WHERE segment_id = %s AND date_bucket = %s AND journey_id = %s
                """, (row.segment_id, row.date_bucket, UUID(journey_id)))
        _retry(_write_segment)
    except Exception as e:
        logger.error(f"Failed to cancel journey {journey_id} in journeys_by_segment: {e}")


def save_road_closure(closure) -> None:
    try:
        def _write():
            session = get_session()
            session.execute("""
                INSERT INTO traffic_service.road_closures
                (segment_id, valid_until, start_lat, start_lon, end_lat, end_lon, reason, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, toTimestamp(now()))
            """, (
                closure.segment_id,
                datetime.fromisoformat(closure.valid_until),
                closure.start_lat,
                closure.start_lon,
                closure.end_lat,
                closure.end_lon,
                closure.reason
            ))
        _retry(_write)
    except Exception as e:
        logger.error(f"Failed to save road closure: {e}")


def delete_road_closure(segment_id: str) -> None:
    try:
        def _write():
            session = get_session()
            session.execute("""
                DELETE FROM traffic_service.road_closures WHERE segment_id = %s
            """, (segment_id,))
        _retry(_write)
    except Exception as e:
        logger.error(f"Failed to delete road closure: {e}")


def save_traffic_condition(segment_id: str, congestion_level: int) -> None:
    try:
        def _write():
            session = get_session()
            session.execute("""
                INSERT INTO traffic_service.traffic_conditions
                (segment_id, congestion_level, updated_at)
                VALUES (%s, %s, toTimestamp(now()))
            """, (segment_id, congestion_level))
        _retry(_write)
    except Exception as e:
        logger.error(f"Failed to save traffic condition: {e}")


def load_state_from_cassandra() -> tuple[list, dict]:
    """Load road closures and traffic conditions from Cassandra on startup."""
    try:
        session = get_session()
        closures_rows = session.execute("SELECT * FROM traffic_service.road_closures")
        conditions_rows = session.execute("SELECT * FROM traffic_service.traffic_conditions")

        conditions = {
            row.segment_id: int(row.congestion_level)
            for row in conditions_rows
        }

        return list(closures_rows), conditions
    except Exception as e:
        logger.error(f"Failed to load state from Cassandra: {e}")
        return [], {}