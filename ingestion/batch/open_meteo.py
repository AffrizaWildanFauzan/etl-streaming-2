"""EXTRACT (batch): data cuaca & kualitas udara per jam dari Open-Meteo (tanpa API key).

- Weather Forecast API : suhu, kelembapan, curah hujan, angin (data model NWP)
- Air Quality API      : PM2.5, PM10, CO, NO2, US AQI (model CAMS - Copernicus)

Setiap pemanggilan mengambil `past_days` hari ke belakang (data aktual) dan
`forecast_days` hari ke depan (prakiraan, dipakai model prediksi kemacetan).
"""

import requests

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
DEFAULT_PAST_DAYS = 2
DEFAULT_FORECAST_DAYS = 2
TIMEOUT_SECONDS = 30


def fetch_hourly(url: str, variables: tuple[str, ...], latitude: float, longitude: float,
                 past_days: int = DEFAULT_PAST_DAYS, forecast_days: int = DEFAULT_FORECAST_DAYS,
                 session=None) -> dict:
    response = (session or requests).get(url, params={
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(variables),
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "UTC",
    }, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    body = response.json()
    if body.get("error"):
        raise RuntimeError(f"Open-Meteo error: {body.get('reason')}")
    return body
