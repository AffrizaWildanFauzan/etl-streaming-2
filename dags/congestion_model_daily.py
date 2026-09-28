"""DAG model prediktif: latih ulang model kemacetan & buat prakiraan 24 jam ke depan.

train_model ──▶ forecast_next_24h

Dijadwalkan setiap hari pukul 00.30 WIB (17.30 UTC). Jika data historis belum cukup,
task di-skip (bukan gagal) sampai data streaming terkumpul.
"""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import joblib
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from common.config import env
from common.points import CITY, MONITORING_POINTS
from modeling.congestion_model import (
    CATEGORICAL_FEATURES, NUMERIC_FEATURES, NotEnoughData, forecast, train_and_evaluate,
)
from storage.batch_store import (
    connect, load_training_frame, load_weather_window, save_forecasts, save_model_run,
)

log = logging.getLogger(__name__)
FORECAST_HOURS = 24
MODEL_FILE = "congestion_model.joblib"


def _model_path() -> Path:
    return Path(env("MODEL_DIR", "/opt/airflow/models")) / MODEL_FILE


def _future_frame(weather: pd.DataFrame, start: datetime) -> pd.DataFrame:
    hours = pd.date_range(start, periods=FORECAST_HOURS, freq="h", tz="UTC")
    grid = pd.DataFrame([(p.point_id, h) for p in MONITORING_POINTS for h in hours],
                        columns=["point_id", "hour"])
    if weather.empty:
        return grid.assign(temperature=None, relative_humidity=None,
                           precipitation=None, wind_speed=None)
    weather = weather.assign(hour=pd.to_datetime(weather["hour"], utc=True))
    return grid.merge(weather, on="hour", how="left")


@dag(
    dag_id="congestion_model_daily",
    description="Training model prediksi kemacetan + prakiraan 24 jam",
    schedule="30 17 * * *",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["batch", "machine-learning", "smart-city"],
)
def congestion_model_daily():
    @task
    def train_model() -> str:
        with connect() as conn:
            history = load_training_frame(conn)
        try:
            result = train_and_evaluate(history)
        except NotEnoughData as exc:
            raise AirflowSkipException(f"Training di-skip: {exc}")

        version = datetime.now(UTC).strftime("v%Y%m%d%H%M")
        path = _model_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(result.pipeline, path)
        with connect() as conn:
            save_model_run(conn, version, result.train_rows, result.test_rows, result.mae,
                           result.rmse, result.baseline_mae,
                           CATEGORICAL_FEATURES + NUMERIC_FEATURES)
            conn.commit()
        log.info("Model %s: MAE=%.4f RMSE=%.4f (baseline MAE=%.4f)",
                 version, result.mae, result.rmse, result.baseline_mae)
        return version

    @task
    def forecast_next_24h(version: str) -> int:
        start = pd.Timestamp.now(tz="UTC").ceil("h").to_pydatetime()
        with connect() as conn:
            weather = load_weather_window(conn, CITY, start, start + timedelta(hours=FORECAST_HOURS))
        future = _future_frame(weather, start)
        predictions = forecast(joblib.load(_model_path()), future.astype(
            {c: float for c in ("temperature", "relative_humidity", "precipitation", "wind_speed")}))
        with connect() as conn:
            count = save_forecasts(conn, predictions, version)
            conn.commit()
        log.info("Prakiraan tersimpan: %d baris (%d titik x %d jam)",
                 count, len(MONITORING_POINTS), FORECAST_HOURS)
        return count

    forecast_next_24h(train_model())


congestion_model_daily()
