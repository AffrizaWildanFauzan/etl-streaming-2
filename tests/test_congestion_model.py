import numpy as np
import pandas as pd
import pytest

from modeling.congestion_model import (
    NotEnoughData,
    add_engineered_features,
    forecast,
    time_split,
    train_and_evaluate,
)

POINTS = ("AY-01", "DR-02", "MR-03")


def synthetic_history(days: int = 10, seed: int = 0) -> pd.DataFrame:
    """Kemacetan sintetis 10 hari (720 baris) untuk menguji pipeline model (> 648 MIN_TRAIN_ROWS)."""
    rng = np.random.default_rng(seed)
    hours = pd.date_range("2026-09-01", periods=days * 24, freq="h", tz="UTC")
    rows = []
    for point_offset, point in enumerate(POINTS):
        for ts in hours:
            local_hour = (ts.hour + 7) % 24
            rush = np.exp(-((local_hour - 7.5) ** 2) / 2) + np.exp(-((local_hour - 17.5) ** 2) / 2)
            rain = float(rng.random() < 0.2) * rng.uniform(1, 10)
            incidents = int(rng.random() < 0.1)
            congestion = 0.1 + 0.05 * point_offset + 0.5 * rush + 0.02 * rain + 0.1 * incidents + rng.normal(0, 0.02)
            rows.append({
                "point_id": point,
                "hour": ts,
                "avg_congestion": float(np.clip(congestion, 0, 1)),
                "temperature": 29 + rng.normal(),
                "relative_humidity": 70.0,
                "precipitation": rain,
                "wind_speed": 10.0,
                "active_incidents": incidents,
            })
    return pd.DataFrame(rows)


def test_time_features_and_lags_use_local_wib_time():
    df = pd.DataFrame({
        "point_id": ["DR-02", "DR-02"],
        "hour": pd.to_datetime(["2026-09-28T00:00Z", "2026-09-28T01:00Z"]),
        "avg_congestion": [0.2, 0.5],
    })
    out = add_engineered_features(df)
    assert out.loc[0, "hour_of_day"] == 7
    assert out.loc[0, "day_of_week"] == 0
    assert out.loc[0, "is_weekend"] == 0
    assert "lag_1h_congestion" in out.columns
    assert "lag_24h_congestion" in out.columns
    assert "active_incidents" in out.columns


def test_time_split_has_no_leakage():
    df = add_engineered_features(synthetic_history())
    train, test = time_split(df, test_fraction=0.25)
    assert train["hour"].max() < test["hour"].min()
    assert len(train) + len(test) == len(df)


def test_model_learns_rush_hour_pattern_and_beats_constant_guess():
    result = train_and_evaluate(synthetic_history())

    assert result.test_rows > 0
    assert result.mae < 0.05
    assert result.mae < result.baseline_mae
    assert result.cv_mae_mean >= 0


def test_forecast_is_bounded_and_labelled():
    result = train_and_evaluate(synthetic_history())
    future = pd.DataFrame({
        "point_id": ["DR-02", "DR-02"],
        "hour": pd.to_datetime(["2026-09-12T00:30Z", "2026-09-12T18:00Z"]),  # 07.30 & 01.00 WIB
        "temperature": [29.0, 27.0],
        "relative_humidity": [70.0, 80.0],
        "precipitation": [0.0, np.nan],
        "wind_speed": [10.0, 5.0],
        "avg_congestion": [0.4, 0.1],
        "active_incidents": [0, 0],
    })

    out = forecast(result.pipeline, future)

    assert out["predicted_congestion"].between(0, 1).all()
    assert out.loc[0, "predicted_congestion"] > out.loc[1, "predicted_congestion"]
    assert set(out["predicted_level"]) <= {"LANCAR", "PADAT", "MACET"}


def test_too_little_data_raises():
    with pytest.raises(NotEnoughData):
        train_and_evaluate(synthetic_history().head(100))
