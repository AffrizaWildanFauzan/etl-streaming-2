from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

CONN_ID = "city_db"

# Jadwal "5 * * * *" membuat data_interval jatuh di menit ke-5 (mis. 00.05-01.05), bukan jam
# bulat, sedangkan GROUP BY memakai date_trunc('hour', ...). Dipakai apa adanya, 5 menit pertama
# tiap jam tidak pernah terhitung (terbukti: 180 dari 189 jam kehilangan tepat 10 sampel) dan
# data quality check memeriksa bucket yang salah. Karena itu jendela dikunci ke jam penuh.
HOUR_END = "date_trunc('hour', TIMESTAMPTZ '{{ data_interval_end }}')"
HOUR_START = f"({HOUR_END} - interval '1 hour')"
INTERVAL = f"observed_at >= {HOUR_START} AND observed_at < {HOUR_END}"
HOUR_INTERVAL = f"hour >= {HOUR_START} AND hour < {HOUR_END}"

# Polling TomTom tiap 6 menit tidak ada data 20 menit berarti streaming bermasalah
FRESHNESS_SQL = """
SELECT count(*) > 0 FROM traffic_flow
WHERE loaded_at > now() - make_interval(mins => {{ params.freshness_minutes }})
"""

BUILD_SQL = f"""
INSERT INTO traffic_hourly (point_id, hour, avg_speed, avg_free_flow, avg_congestion,
                            max_congestion, avg_delay_seconds, macet_samples, samples)
SELECT point_id, date_trunc('hour', observed_at),
       avg(current_speed), avg(free_flow_speed), avg(congestion_index), max(congestion_index),
       avg(delay_seconds),
       count(*) FILTER (WHERE congestion_level IN ('MACET', 'TUTUP')),
       count(*)
FROM traffic_flow
WHERE {INTERVAL}
GROUP BY point_id, date_trunc('hour', observed_at)
ON CONFLICT (point_id, hour) DO UPDATE SET
    avg_speed = EXCLUDED.avg_speed, avg_free_flow = EXCLUDED.avg_free_flow,
    avg_congestion = EXCLUDED.avg_congestion, max_congestion = EXCLUDED.max_congestion,
    avg_delay_seconds = EXCLUDED.avg_delay_seconds, macet_samples = EXCLUDED.macet_samples,
    samples = EXCLUDED.samples, built_at = now()
"""

RANGE_CHECK_SQL = f"""
SELECT COALESCE(bool_and(avg_congestion BETWEEN 0 AND 1 AND max_congestion BETWEEN 0 AND 1
                         AND avg_speed >= 0 AND avg_speed <= 200 AND samples > 0
                         AND macet_samples <= samples), true)
FROM traffic_hourly WHERE {HOUR_INTERVAL}
"""

# Minimal separuh titik pantau punya data pada jam tersebut
COVERAGE_SQL = f"""
SELECT (SELECT count(DISTINCT point_id) FROM traffic_hourly WHERE {HOUR_INTERVAL})
       >= (SELECT count(*) FROM monitoring_points) * {{{{ params.min_coverage }}}}
"""

with DAG(
    dag_id="traffic_hourly_mart",
    description="Agregasi traffic_flow (streaming) menjadi traffic_hourly + data quality",
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={"freshness_minutes": 20, "min_coverage": 0.5},
    tags=["batch", "traffic", "data-quality", "smart-city"],
) as dag:
    check_stream_freshness = SQLCheckOperator(
        task_id="check_stream_freshness", conn_id=CONN_ID, sql=FRESHNESS_SQL)
    build_traffic_hourly = SQLExecuteQueryOperator(
        task_id="build_traffic_hourly", conn_id=CONN_ID, sql=BUILD_SQL)
    check_value_ranges = SQLCheckOperator(
        task_id="check_value_ranges", conn_id=CONN_ID, sql=RANGE_CHECK_SQL)
    check_point_coverage = SQLCheckOperator(
        task_id="check_point_coverage", conn_id=CONN_ID, sql=COVERAGE_SQL)

    check_stream_freshness >> build_traffic_hourly >> check_value_ranges >> check_point_coverage
