from datetime import UTC, datetime

import pytest

from processing.batch.open_meteo_transform import AIR_QUALITY_FIELDS, WEATHER_FIELDS, hourly_rows

NOW = datetime(2026, 9, 28, 6, 30, tzinfo=UTC)


def weather_payload(**overrides) -> dict:
    hourly = {
        "time": ["2026-09-28T05:00", "2026-09-28T06:00", "2026-09-28T07:00"],
        "temperature_2m": [29.1, 30.2, 31.0],
        "relative_humidity_2m": [70, 68, 150],   # 150% tidak valid
        "precipitation": [0.0, 0.2, 0.0],
        "rain": [0.0, 0.2, 0.0],
        "wind_speed_10m": [10.5, 12.0, 11.1],
        "weather_code": [1, 61, 2],
        **overrides,
    }
    return {"hourly": hourly}


def test_rows_are_renamed_and_flagged_as_forecast():
    batch = hourly_rows(weather_payload(), WEATHER_FIELDS, NOW)

    assert len(batch.rows) == 3
    first, _, last = batch.rows
    assert first["observed_hour"] == "2026-09-28T05:00:00+00:00"
    assert first["temperature"] == 29.1
    assert first["relative_humidity"] == 70
    assert first["is_forecast"] is False
    assert last["is_forecast"] is True


def test_out_of_range_values_become_null_and_are_counted():
    batch = hourly_rows(weather_payload(), WEATHER_FIELDS, NOW)
    assert batch.rows[2]["relative_humidity"] is None
    assert batch.rejected_values == 1


def test_hours_without_any_value_are_dropped():
    payload = {"hourly": {"time": ["2026-09-28T05:00", "2026-09-28T06:00"],
                          "pm2_5": [None, 20.0], "pm10": [None, 35.1]}}
    batch = hourly_rows(payload, AIR_QUALITY_FIELDS, NOW)
    assert [r["observed_hour"] for r in batch.rows] == ["2026-09-28T06:00:00+00:00"]
    assert batch.rows[0]["us_aqi"] is None   # variabel yang tidak dikirim -> NULL


def test_negative_pollutant_is_rejected():
    payload = {"hourly": {"time": ["2026-09-28T05:00"], "pm2_5": [-3.0], "pm10": [10.0]}}
    batch = hourly_rows(payload, AIR_QUALITY_FIELDS, NOW)
    assert batch.rows[0]["pm2_5"] is None
    assert batch.rejected_values == 1


@pytest.mark.parametrize("payload", [
    {},
    {"hourly": {"temperature_2m": [1.0]}},
    {"hourly": {"time": ["2026-09-28T05:00"], "temperature_2m": [1.0, 2.0]}},
])
def test_malformed_payload_raises(payload):
    with pytest.raises(ValueError):
        hourly_rows(payload, WEATHER_FIELDS, NOW)
