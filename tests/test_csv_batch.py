"""Uji pemuat batch CSV: bentuk baris cuaca & kualitas udara sebelum masuk database."""

from datetime import UTC, datetime

import pytest

from ingestion.batch.csv_batch import (
    AIR_QUALITY_FIELDS,
    WEATHER_FIELDS,
    read_air_quality_rows,
    read_weather_rows,
)

WEATHER_CSV = (
    "city,observed_hour,temperature,relative_humidity,precipitation,rain,wind_speed,"
    "weather_code,is_forecast,loaded_at\n"
    "Surabaya,2026-09-26 00:00:00+00,28.7,68,0,0,19.2,2,f,2026-09-28 14:10:09+00\n"
    "Surabaya,2026-09-26 01:00:00+00,28.1,70,,,18.0,,t,2026-09-28 14:10:09+00\n"
    "Malang,2026-09-26 00:00:00+00,22.4,80,1.2,1.2,8.5,61,f,2026-09-28 14:10:09+00\n"
)
MART_CSV = (
    "point_id,corridor,hour,avg_speed,avg_congestion,max_congestion,avg_delay_seconds,samples,"
    "temperature,relative_humidity,precipitation,rain,wind_speed,pm2_5,pm10,carbon_monoxide,"
    "nitrogen_dioxide,us_aqi\n"
    "AY-01,Koridor A. Yani,2026-09-28 06:00:00+00,39.3,0.12,0.13,6.6,3,31.4,56,0,0,17.6,"
    "15.9,17.9,352,5.9,82\n"
    "DR-02,Koridor Darmo,2026-09-28 06:00:00+00,30.1,0.33,0.40,22.0,3,31.4,56,0,0,17.6,"
    "15.9,17.9,352,5.9,82\n"
    "AY-01,Koridor A. Yani,2026-09-28 07:00:00+00,35.0,0.22,0.25,12.0,3,32.0,54,0,0,18.0,"
    "16.4,18.5,360,6.1,85\n"
)


@pytest.fixture()
def weather_csv(tmp_path):
    path = tmp_path / "weather_hourly.csv"
    path.write_text(WEATHER_CSV, encoding="utf-8")
    return path


@pytest.fixture()
def mart_csv(tmp_path):
    path = tmp_path / "mart_traffic_features.csv"
    path.write_text(MART_CSV, encoding="utf-8")
    return path


def test_weather_grouped_by_city(weather_csv):
    """Kunci tabel adalah (city, observed_hour), jadi baris harus terpisah per kota."""
    by_city = read_weather_rows(weather_csv)
    assert set(by_city) == {"Surabaya", "Malang"}
    assert len(by_city["Surabaya"]) == 2


def test_weather_row_has_every_column_upsert_expects(weather_csv):
    row = read_weather_rows(weather_csv)["Surabaya"][0]
    assert set(WEATHER_FIELDS) <= set(row), "nama kolom harus sama dengan WEATHER_COLUMNS"
    assert row["observed_hour"] == datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
    assert row["temperature"] == 28.7
    assert row["weather_code"] == 2
    assert row["is_forecast"] is False


def test_empty_cells_become_none_not_zero(weather_csv):
    """Sel kosong berarti tidak ada pengukuran; 0.0 akan terbaca sebagai 'tidak hujan'."""
    row = read_weather_rows(weather_csv)["Surabaya"][1]
    assert row["precipitation"] is None
    assert row["rain"] is None
    assert row["weather_code"] is None
    assert row["is_forecast"] is True


def test_air_quality_collapses_repeated_hours(mart_csv):
    """Di mart, nilai kualitas udara terulang untuk tiap titik pantau pada jam yang sama."""
    rows = read_air_quality_rows(mart_csv)["Surabaya"]
    assert len(rows) == 2, "dua jam berbeda dari tiga baris mart"
    assert [row["observed_hour"].hour for row in rows] == [6, 7]
    assert set(AIR_QUALITY_FIELDS) <= set(rows[0])
    assert rows[0]["us_aqi"] == 82
    assert rows[0]["is_forecast"] is False


def test_air_quality_city_is_configurable(mart_csv):
    """Mart tidak menyimpan kolom city, jadi kota datang dari pemanggil."""
    assert set(read_air_quality_rows(mart_csv, city="Malang")) == {"Malang"}
