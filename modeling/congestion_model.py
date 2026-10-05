"""Model prediktif: indeks kemacetan per titik per jam.

Fitur   : titik pantau, jam (WIB), hari, akhir pekan, suhu, kelembapan, hujan, angin,
          lag kemacetan (1j & 24j), serta insiden aktif.
Target  : avg_congestion (0 = lancar, 1 = berhenti total) dari tabel traffic_hourly
Model   : HistGradientBoostingRegressor (menangani nilai cuaca/fitur kosong secara native)
Evaluasi: Time-Series Split (data terbaru sebagai test) & Time-Series Cross Validation.
"""

from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from common.points import TIMEZONE
from processing.stream.traffic_transform import congestion_level

CATEGORICAL_FEATURES = ["point_id"]
NUMERIC_FEATURES = [
    "hour_of_day", "day_of_week", "is_weekend",
    "temperature", "relative_humidity", "precipitation", "wind_speed",
    "lag_1h_congestion", "lag_24h_congestion", "active_incidents"
]
TARGET = "avg_congestion"

# Minimal 3 hari data historis penuh untuk 9 titik pantau (9 titik x 24 jam x 3 hari = 648 baris)
MIN_TRAIN_ROWS = 648
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
    cv_mae_mean: float
    train_rows: int
    test_rows: int


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Membuat fitur waktu lokal (WIB), autokorelasi lag, dan indikator insiden."""
    df = df.sort_values(["point_id", "hour"]).copy()
    local = pd.to_datetime(df["hour"], utc=True).dt.tz_convert(TIMEZONE)

    df["hour_of_day"] = local.dt.hour
    df["day_of_week"] = local.dt.dayofweek
    df["is_weekend"] = (local.dt.dayofweek >= 5).astype(int)

    # Fitur Lag (Autokorelasi Waktu)
    df["lag_1h_congestion"] = df.groupby("point_id")[TARGET].shift(1)
    df["lag_24h_congestion"] = df.groupby("point_id")[TARGET].shift(24)

    # Fallback untuk nilai lag yang kosong pada baris awal
    mean_target = df[TARGET].mean() if (TARGET in df.columns and not df[TARGET].dropna().empty) else 0.1
    df["lag_1h_congestion"] = df["lag_1h_congestion"].fillna(mean_target)
    df["lag_24h_congestion"] = df["lag_24h_congestion"].fillna(mean_target)

    if "active_incidents" not in df.columns:
        df["active_incidents"] = 0
    else:
        df["active_incidents"] = df["active_incidents"].fillna(0)

    return df


# Alias untuk menjaga kompatibilitas dengan pengujian lama
add_time_features = add_engineered_features


def time_split(df: pd.DataFrame, test_fraction: float = TEST_FRACTION):
    hours = np.sort(df["hour"].unique())
    cutoff = hours[int(len(hours) * (1 - test_fraction))]
    return df[df["hour"] < cutoff], df[df["hour"] >= cutoff]


def build_pipeline() -> Pipeline:
    features = ColumnTransformer([
        ("point", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
        ("numeric", "passthrough", NUMERIC_FEATURES),
    ])
    model = HistGradientBoostingRegressor(
        max_iter=300,
        learning_rate=0.03,
        max_depth=6,
        random_state=RANDOM_STATE
    )
    return Pipeline([("features", features), ("model", model)])


def baseline_predict(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    profile = train.groupby(["point_id", "hour_of_day"])[TARGET].mean().rename("baseline")
    merged = test.join(profile, on=["point_id", "hour_of_day"])
    return merged["baseline"].fillna(train[TARGET].mean()).to_numpy()


def train_and_evaluate(history: pd.DataFrame) -> TrainResult:
    if len(history) < MIN_TRAIN_ROWS:
        raise NotEnoughData(f"butuh >= {MIN_TRAIN_ROWS} baris, tersedia {len(history)}")

    data = add_engineered_features(history.dropna(subset=[TARGET]))
    train, test = time_split(data)

    if train.empty or test.empty:
        raise NotEnoughData("rentang waktu terlalu pendek untuk time-series split")

    X_train = train[CATEGORICAL_FEATURES + NUMERIC_FEATURES]
    y_train = train[TARGET]
    X_test = test[CATEGORICAL_FEATURES + NUMERIC_FEATURES]
    y_test = test[TARGET]

    # Time-Series Cross Validation pada data training
    tscv = TimeSeriesSplit(n_splits=3)
    cv_scores = []
    for train_idx, val_idx in tscv.split(X_train):
        fold_pipeline = build_pipeline()
        fold_pipeline.fit(X_train.iloc[train_idx], y_train.iloc[train_idx])
        preds = fold_pipeline.predict(X_train.iloc[val_idx])
        cv_scores.append(mean_absolute_error(y_train.iloc[val_idx], preds))

    # Evaluasi pada Test Set
    eval_pipeline = build_pipeline().fit(X_train, y_train)
    predicted = np.clip(eval_pipeline.predict(X_test), 0, 1)

    # Model Final (dilatih ulang dengan seluruh data)
    final_pipeline = build_pipeline().fit(data[CATEGORICAL_FEATURES + NUMERIC_FEATURES], data[TARGET])

    return TrainResult(
        pipeline=final_pipeline,
        mae=float(mean_absolute_error(y_test, predicted)),
        rmse=float(np.sqrt(mean_squared_error(y_test, predicted))),
        baseline_mae=float(mean_absolute_error(y_test, baseline_predict(train, test))),
        cv_mae_mean=float(np.mean(cv_scores)),
        train_rows=len(train),
        test_rows=len(test),
    )


def forecast(pipeline: Pipeline, future: pd.DataFrame) -> pd.DataFrame:
    features = add_engineered_features(future)
    predicted = np.clip(pipeline.predict(features[CATEGORICAL_FEATURES + NUMERIC_FEATURES]), 0, 1)
    return future.assign(
        predicted_congestion=predicted,
        predicted_level=[congestion_level(1 - p, False) for p in predicted],
    )
