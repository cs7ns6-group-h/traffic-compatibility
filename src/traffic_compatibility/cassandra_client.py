"""
Cassandra client - reads and writes booking status.
"""

import logging
import os

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy

from dotenv import load_dotenv
load_dotenv()

from uuid import UUID

logger = logging.getLogger(__name__)

REGION = os.getenv("REGION", "eu")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra-eu").split(",")
CASSANDRA_DC = os.getenv("CASSANDRA_DC", "eu")

_session = None


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



def update_booking_status(journey_id: str, status: str, date_bucket: str = None) -> None:
    from datetime import datetime
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        session = get_session()
        session.execute("""
            UPDATE traffic_service.bookings
            SET status = %s, updated_at = toTimestamp(now())
            WHERE region = %s AND date_bucket = %s AND journey_id = %s
        """, (status, REGION, date_bucket, UUID(journey_id)))
    except Exception as e:
        logger.error(f"Cassandra write failed for journey {journey_id}: {e}")

def write_journey_segment(journey_id: str, segment_id: str, date_bucket: str = None) -> None:
    from datetime import datetime
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        session = get_session()
        session.execute("""
            INSERT INTO traffic_service.journeys_by_segment
            (segment_id, date_bucket, journey_id, region, status, updated_at)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
        """, (segment_id, date_bucket, UUID(journey_id), REGION, "accepted"))
    except Exception as e:
        logger.error(f"Cassandra segment write failed: {e}")


def get_journeys_by_segment(segment_id: str, date_bucket: str = None) -> list:
    from datetime import datetime
    if date_bucket is None:
        date_bucket = datetime.now().strftime("%Y-%m-%d")
    try:
        session = get_session()
        rows = session.execute("""
            SELECT journey_id FROM traffic_service.journeys_by_segment
            WHERE segment_id = %s AND date_bucket = %s AND status = 'accepted'
            ALLOW FILTERING
        """, (segment_id, date_bucket))
        return [str(row.journey_id) for row in rows]
    except Exception as e:
        logger.error(f"Cassandra segment query failed: {e}")
        return []
    
def save_road_closure(closure) -> None:
    from datetime import datetime
    try:
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
    except Exception as e:
        logger.error(f"Failed to save road closure: {e}")


def delete_road_closure(segment_id: str) -> None:
    try:
        session = get_session()
        session.execute("""
            DELETE FROM traffic_service.road_closures WHERE segment_id = %s
        """, (segment_id,))
    except Exception as e:
        logger.error(f"Failed to delete road closure: {e}")


def save_traffic_condition(segment_id: str, congestion_level: int) -> None:
    try:
        session = get_session()
        session.execute("""
            INSERT INTO traffic_service.traffic_conditions
            (segment_id, congestion_level, updated_at)
            VALUES (%s, %s, toTimestamp(now()))
        """, (segment_id, congestion_level))
    except Exception as e:
        logger.error(f"Failed to save traffic condition: {e}")


def load_state_from_cassandra() -> tuple[list, dict]:
    """Load road closures and traffic conditions from Cassandra on startup."""
    try:
        session = get_session()
        closures_rows = session.execute("SELECT * FROM traffic_service.road_closures")
        conditions_rows = session.execute("SELECT * FROM traffic_service.traffic_conditions")
        return list(closures_rows), {row.segment_id: row.congestion_level for row in conditions_rows}
    except Exception as e:
        logger.error(f"Failed to load state from Cassandra: {e}")
        return [], {}