"""Model prediktif: indeks kemacetan per titik per jam.

Fitur   : titik pantau, jam (WIB), hari, akhir pekan, suhu, kelembapan, hujan, angin
Target  : avg_congestion (0 = lancar, 1 = berhenti total) dari tabel traffic_hourly
Model   : HistGradientBoostingRegressor (menangani nilai cuaca kosong secara native)
Evaluasi: time-series split (data terbaru sebagai test) -> mencegah data leakage,
          dibandingkan dengan baseline rata-rata historis per (titik, jam).
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from common.points import TIMEZONE
from processing.stream.traffic_transform import congestion_level

CATEGORICAL_FEATURES = ["point_id"]
NUMERIC_FEATURES = ["hour_of_day", "day_of_week", "is_weekend",
                    "temperature", "relative_humidity", "precipitation", "wind_speed"]
TARGET = "avg_congestion"
MIN_TRAIN_ROWS = 48
TEST_FRACTION = 0.2
RANDOM_STATE = 42


class NotEnoughData(ValueError):
    """Data historis belum cukup untuk melatih model."""


@dataclass(frozen=True)
class TrainResult:
    pipeline: Pipeline
    mae: float
    rmse: float
    baseline_mae: float
    train_rows: int
    test_rows: int


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    local = pd.to_datetime(df["hour"], utc=True).dt.tz_convert(TIMEZONE)
    return df.assign(
        hour_of_day=local.dt.hour,
        day_of_week=local.dt.dayofweek,
        is_weekend=(local.dt.dayofweek >= 5).astype(int),
    )


def time_split(df: pd.DataFrame, test_fraction: float = TEST_FRACTION):
    hours = np.sort(df["hour"].unique())
    cutoff = hours[int(len(hours) * (1 - test_fraction))]
    return df[df["hour"] < cutoff], df[df["hour"] >= cutoff]


def build_pipeline() -> Pipeline:
    features = ColumnTransformer([
        ("point", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
        ("numeric", "passthrough", NUMERIC_FEATURES),
    ])
    model = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05,
                                          random_state=RANDOM_STATE)
    return Pipeline([("features", features), ("model", model)])


def baseline_predict(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    profile = train.groupby(["point_id", "hour_of_day"])[TARGET].mean().rename("baseline")
    merged = test.join(profile, on=["point_id", "hour_of_day"])
    return merged["baseline"].fillna(train[TARGET].mean()).to_numpy()


def train_and_evaluate(history: pd.DataFrame) -> TrainResult:
    if len(history) < MIN_TRAIN_ROWS:
        raise NotEnoughData(f"butuh >= {MIN_TRAIN_ROWS} baris, tersedia {len(history)}")
    data = add_time_features(history.dropna(subset=[TARGET]))
    train, test = time_split(data)
    if train.empty or test.empty:
        raise NotEnoughData("rentang waktu terlalu pendek untuk time-series split")

    evaluation = build_pipeline().fit(train[CATEGORICAL_FEATURES + NUMERIC_FEATURES], train[TARGET])
    predicted = np.clip(evaluation.predict(test[CATEGORICAL_FEATURES + NUMERIC_FEATURES]), 0, 1)

    # Model final dilatih ulang dengan seluruh data agar pola terbaru ikut dipelajari
    final = build_pipeline().fit(data[CATEGORICAL_FEATURES + NUMERIC_FEATURES], data[TARGET])
    return TrainResult(
        pipeline=final,
        mae=float(mean_absolute_error(test[TARGET], predicted)),
        rmse=float(np.sqrt(mean_squared_error(test[TARGET], predicted))),
        baseline_mae=float(mean_absolute_error(test[TARGET], baseline_predict(train, test))),
        train_rows=len(train),
        test_rows=len(test),
    )


def forecast(pipeline: Pipeline, future: pd.DataFrame) -> pd.DataFrame:
    features = add_time_features(future)
    predicted = np.clip(pipeline.predict(features[CATEGORICAL_FEATURES + NUMERIC_FEATURES]), 0, 1)
    return future.assign(
        predicted_congestion=predicted,
        predicted_level=[congestion_level(1 - p, False) for p in predicted],
    )
