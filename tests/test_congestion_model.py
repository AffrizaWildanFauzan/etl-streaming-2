import numpy as np
import pandas as pd
import pytest

from modeling.congestion_model import (
    NotEnoughData, add_time_features, forecast, time_split, train_and_evaluate,
)

POINTS = ("AY-01", "DR-02", "MR-03")


def synthetic_history(days: int = 6, seed: int = 0) -> pd.DataFrame:
    """Kemacetan dengan pola jam sibuk WIB + efek hujan, untuk menguji pipeline model."""
    rng = np.random.default_rng(seed)
    hours = pd.date_range("2026-09-01", periods=days * 24, freq="h", tz="UTC")
    rows = []
    for point_offset, point in enumerate(POINTS):
        for ts in hours:
            local_hour = (ts.hour + 7) % 24
            rush = np.exp(-((local_hour - 7.5) ** 2) / 2) + np.exp(-((local_hour - 17.5) ** 2) / 2)
            rain = float(rng.random() < 0.2) * rng.uniform(1, 10)
            congestion = 0.1 + 0.05 * point_offset + 0.5 * rush + 0.02 * rain + rng.normal(0, 0.02)
            rows.append({"point_id": point, "hour": ts, "avg_congestion": float(np.clip(congestion, 0, 1)),
                         "temperature": 29 + rng.normal(), "relative_humidity": 70.0,
                         "precipitation": rain, "wind_speed": 10.0})
    return pd.DataFrame(rows)


def test_time_features_use_local_wib_time():
    df = pd.DataFrame({"hour": pd.to_datetime(["2026-09-28T00:00Z"])})  # 07.00 WIB, Senin
    out = add_time_features(df)
    assert out.loc[0, "hour_of_day"] == 7
    assert out.loc[0, "day_of_week"] == 0
    assert out.loc[0, "is_weekend"] == 0
    assert "hour_of_day" not in df.columns  # input tidak dimutasi


def test_time_split_has_no_leakage():
    df = add_time_features(synthetic_history())
    train, test = time_split(df, test_fraction=0.25)
    assert train["hour"].max() < test["hour"].min()
    assert len(train) + len(test) == len(df)


def test_model_learns_rush_hour_pattern_and_beats_constant_guess():
    result = train_and_evaluate(synthetic_history())

    assert result.test_rows > 0
    assert result.mae < 0.05
    constant_guess_mae = 0.2  # jauh di atas; model harus menangkap pola jam sibuk
    assert result.mae < constant_guess_mae


def test_forecast_is_bounded_and_labelled():
    result = train_and_evaluate(synthetic_history())
    future = pd.DataFrame({
        "point_id": ["DR-02", "DR-02"],
        "hour": pd.to_datetime(["2026-09-08T00:30Z", "2026-09-08T18:00Z"]),  # 07.30 & 01.00 WIB
        "temperature": [29.0, 27.0], "relative_humidity": [70.0, 80.0],
        "precipitation": [0.0, np.nan], "wind_speed": [10.0, 5.0],
    })

    out = forecast(result.pipeline, future)

    assert out["predicted_congestion"].between(0, 1).all()
    assert out.loc[0, "predicted_congestion"] > out.loc[1, "predicted_congestion"]
    assert set(out["predicted_level"]) <= {"LANCAR", "PADAT", "MACET"}


def test_too_little_data_raises():
    with pytest.raises(NotEnoughData):
        train_and_evaluate(synthetic_history().head(10))
