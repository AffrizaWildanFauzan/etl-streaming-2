"""LOAD (stream): menulis hasil transform stream ke PostgreSQL secara idempotent."""

from collections.abc import Sequence

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from common.points import MonitoringPoint
from processing.stream.incident_transform import Incident
from processing.stream.traffic_transform import TrafficReading

UPSERT_POINTS_SQL = """
INSERT INTO monitoring_points (point_id, corridor, name, latitude, longitude)
VALUES %s
ON CONFLICT (point_id) DO UPDATE SET
    corridor = EXCLUDED.corridor, name = EXCLUDED.name,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
"""

INSERT_READINGS_SQL = """
INSERT INTO traffic_flow (point_id, observed_at, source, frc, current_speed, free_flow_speed,
                          current_travel_time, free_flow_travel_time, confidence, road_closure,
                          speed_ratio, congestion_index, delay_seconds, congestion_level, ingested_at)
VALUES %s
ON CONFLICT (point_id, observed_at) DO NOTHING
RETURNING point_id
"""

# Insiden yang sama muncul di setiap polling selama masih aktif -> update, bukan insert baru
UPSERT_INCIDENTS_SQL = """
INSERT INTO traffic_incidents (incident_id, source, category, category_label, magnitude,
                               magnitude_label, description, road_from, road_to, length_m,
                               delay_seconds, start_time, end_time, latitude, longitude,
                               first_seen_at, last_seen_at)
VALUES %s
ON CONFLICT (incident_id) DO UPDATE SET
    magnitude = EXCLUDED.magnitude, magnitude_label = EXCLUDED.magnitude_label,
    description = EXCLUDED.description, delay_seconds = EXCLUDED.delay_seconds,
    length_m = EXCLUDED.length_m, end_time = EXCLUDED.end_time,
    last_seen_at = GREATEST(traffic_incidents.last_seen_at, EXCLUDED.last_seen_at)
RETURNING (xmax = 0) AS inserted
"""


def upsert_points(conn: PgConnection, points: Sequence[MonitoringPoint]) -> None:
    rows = [(p.point_id, p.corridor, p.name, p.latitude, p.longitude) for p in points]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_POINTS_SQL, rows)


def insert_readings(conn: PgConnection, readings: Sequence[TrafficReading]) -> int:
    unique = {(r.point_id, r.observed_at): r for r in readings}.values()
    rows = [
        (r.point_id, r.observed_at, r.source, r.frc, r.current_speed, r.free_flow_speed,
         r.current_travel_time, r.free_flow_travel_time, r.confidence, r.road_closure,
         r.speed_ratio, r.congestion_index, r.delay_seconds, r.congestion_level, r.ingested_at)
        for r in unique
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        return len(execute_values(cur, INSERT_READINGS_SQL, rows, fetch=True))


def upsert_incidents(conn: PgConnection, incidents: Sequence[Incident]) -> tuple[int, int]:
    """Kembalikan (jumlah insiden baru, jumlah insiden yang diperbarui)."""
    latest = {}
    for inc in incidents:
        if inc.incident_id not in latest or inc.observed_at > latest[inc.incident_id].observed_at:
            latest[inc.incident_id] = inc
    rows = [
        (i.incident_id, i.source, i.category, i.category_label, i.magnitude, i.magnitude_label,
         i.description, i.road_from, i.road_to, i.length_m, i.delay_seconds, i.start_time,
         i.end_time, i.latitude, i.longitude, i.observed_at, i.observed_at)
        for i in latest.values()
    ]
    if not rows:
        return 0, 0
    with conn.cursor() as cur:
        results = execute_values(cur, UPSERT_INCIDENTS_SQL, rows, fetch=True)
    inserted = sum(1 for (flag,) in results if flag)
    return inserted, len(results) - inserted
