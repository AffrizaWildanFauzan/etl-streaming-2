"""DAG ETL batch: cuaca & kualitas udara Metropolia (Surabaya) dari Open-Meteo, tiap jam.

extract_weather ──▶ transform_weather ──▶ load_weather ──┐
                                                         ├──▶ validate_load
extract_air_quality ▶ transform_air_quality ▶ load_aq ──┘

Dijalankan oleh scheduler Airflow pada menit ke-10 setiap jam (bukan manual trigger).
Data diambil 2 hari ke belakang + 2 hari ke depan; upsert membuat run ulang aman (idempotent)
dan data aktual menimpa nilai prakiraan untuk jam yang sama.
"""

import logging
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.decorators import dag, task

from common.points import CITY, CITY_LATITUDE, CITY_LONGITUDE
from ingestion.batch.open_meteo import AIR_QUALITY_URL, WEATHER_URL, fetch_hourly
from processing.batch.open_meteo_transform import AIR_QUALITY_FIELDS, WEATHER_FIELDS, hourly_rows
from storage.batch_store import connect, upsert_air_quality, upsert_weather

log = logging.getLogger(__name__)
FRESHNESS_LIMIT = timedelta(hours=3)


def _transform(payload: dict, fields: dict[str, str], name: str) -> list[dict]:
    batch = hourly_rows(payload, fields, datetime.now(UTC))
    log.info("%s: %d baris valid, %d nilai di luar rentang dijadikan NULL",
             name, len(batch.rows), batch.rejected_values)
    if not batch.rows:
        raise ValueError(f"{name}: tidak ada baris valid dari Open-Meteo")
    return list(batch.rows)


@dag(
    dag_id="weather_air_quality_hourly",
    description="ETL batch cuaca & kualitas udara (Open-Meteo) ke PostgreSQL",
    schedule="10 * * * *",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["batch", "open-meteo", "smart-city"],
)
def weather_air_quality_hourly():
    @task
    def extract_weather() -> dict:
        return fetch_hourly(WEATHER_URL, tuple(WEATHER_FIELDS), CITY_LATITUDE, CITY_LONGITUDE)

    @task
    def extract_air_quality() -> dict:
        return fetch_hourly(AIR_QUALITY_URL, tuple(AIR_QUALITY_FIELDS), CITY_LATITUDE, CITY_LONGITUDE)

    @task
    def transform_weather(payload: dict) -> list[dict]:
        return _transform(payload, WEATHER_FIELDS, "weather")

    @task
    def transform_air_quality(payload: dict) -> list[dict]:
        return _transform(payload, AIR_QUALITY_FIELDS, "air_quality")

    @task
    def load_weather(rows: list[dict]) -> int:
        with connect() as conn:
            count = upsert_weather(conn, CITY, rows)
            conn.commit()
        return count

    @task
    def load_air_quality(rows: list[dict]) -> int:
        with connect() as conn:
            count = upsert_air_quality(conn, CITY, rows)
            conn.commit()
        return count

    @task
    def validate_load(weather_rows: int, air_quality_rows: int) -> None:
        """Data aktual terbaru harus ada dan tidak lebih tua dari 3 jam."""
        with connect() as conn, conn.cursor() as cur:
            for table in ("weather_hourly", "air_quality_hourly"):
                cur.execute(f"SELECT max(observed_hour) FROM {table} "
                            "WHERE city = %s AND NOT is_forecast", (CITY,))
                (latest,) = cur.fetchone()
                if latest is None or datetime.now(UTC) - latest > FRESHNESS_LIMIT:
                    raise ValueError(f"{table}: data aktual terbaru terlalu lama ({latest})")
        log.info("Dimuat: %d baris cuaca, %d baris kualitas udara", weather_rows, air_quality_rows)

    weather = load_weather(transform_weather(extract_weather()))
    air_quality = load_air_quality(transform_air_quality(extract_air_quality()))
    validate_load(weather, air_quality)


weather_air_quality_hourly()
