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
    logger.info(f"[{REGION.upper()}] Cassandra schema ready.")



def update_booking_status(journey_id: str, status: str) -> None:
    try:
        session = get_session()
        from datetime import datetime
        date_bucket = datetime.now().strftime("%Y-%m-%d")
        session.execute("""
            UPDATE traffic_service.bookings
            SET status = %s, updated_at = toTimestamp(now())
            WHERE region = %s AND date_bucket = %s AND journey_id = %s
        """, (status, REGION, date_bucket, UUID(journey_id)))
    except Exception as e:
        logger.error(f"Cassandra write failed for journey {journey_id}: {e}")