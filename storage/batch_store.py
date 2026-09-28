"""LOAD (batch): akses PostgreSQL untuk DAG Airflow (cuaca, kualitas udara, model)."""

from collections.abc import Sequence
from contextlib import closing, contextmanager
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from common.config import postgres_config

WEATHER_COLUMNS = ("temperature", "relative_humidity", "precipitation", "rain",
                   "wind_speed", "weather_code")
AIR_QUALITY_COLUMNS = ("pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide", "us_aqi")
TRAINING_SQL = """
SELECT point_id, hour, avg_congestion, temperature, relative_humidity, precipitation, wind_speed
FROM mart_traffic_features
ORDER BY hour
"""
WEATHER_WINDOW_SQL = """
SELECT observed_hour AS hour, temperature, relative_humidity, precipitation, wind_speed
FROM weather_hourly
WHERE city = %s AND observed_hour >= %s AND observed_hour < %s
ORDER BY observed_hour
"""


@contextmanager
def connect():
    with closing(psycopg2.connect(postgres_config().dsn)) as conn:
        yield conn


def _upsert_hourly(conn: PgConnection, table: str, columns: Sequence[str],
                   city: str, rows: Sequence[dict]) -> int:
    """Upsert per (city, jam). Data aktual menimpa prakiraan sebelumnya untuk jam yang sama."""
    if not rows:
        return 0
    all_columns = ("city", "observed_hour", *columns, "is_forecast")
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in (*columns, "is_forecast"))
    sql = (f"INSERT INTO {table} ({', '.join(all_columns)}) VALUES %s "
           f"ON CONFLICT (city, observed_hour) DO UPDATE SET {updates}, loaded_at = now()")
    values = [(city, r["observed_hour"], *(r.get(c) for c in columns), r["is_forecast"])
              for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    return len(values)


def upsert_weather(conn: PgConnection, city: str, rows: Sequence[dict]) -> int:
    return _upsert_hourly(conn, "weather_hourly", WEATHER_COLUMNS, city, rows)


def upsert_air_quality(conn: PgConnection, city: str, rows: Sequence[dict]) -> int:
    return _upsert_hourly(conn, "air_quality_hourly", AIR_QUALITY_COLUMNS, city, rows)


def _frame(conn: PgConnection, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def load_training_frame(conn: PgConnection) -> pd.DataFrame:
    df = _frame(conn, TRAINING_SQL)
    numeric = ["avg_congestion", "temperature", "relative_humidity", "precipitation", "wind_speed"]
    return df.astype({c: float for c in numeric}) if not df.empty else df


def load_weather_window(conn: PgConnection, city: str, start: datetime, end: datetime) -> pd.DataFrame:
    return _frame(conn, WEATHER_WINDOW_SQL, (city, start, end))


def save_model_run(conn: PgConnection, version: str, train_rows: int, test_rows: int,
                   mae: float, rmse: float, baseline_mae: float, features: Sequence[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO model_runs (model_version, train_rows, test_rows, mae, rmse, "
            "baseline_mae, features) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (version, train_rows, test_rows, mae, rmse, baseline_mae, ",".join(features)),
        )


def save_forecasts(conn: PgConnection, forecast: pd.DataFrame, version: str) -> int:
    rows = [(r.point_id, r.hour, float(r.predicted_congestion), r.predicted_level, version)
            for r in forecast.itertuples()]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO traffic_forecast (point_id, target_hour, predicted_congestion,
                                          predicted_level, model_version)
            VALUES %s
            ON CONFLICT (point_id, target_hour) DO UPDATE SET
                predicted_congestion = EXCLUDED.predicted_congestion,
                predicted_level = EXCLUDED.predicted_level,
                model_version = EXCLUDED.model_version, created_at = now()
        """, rows)
    return len(rows)
