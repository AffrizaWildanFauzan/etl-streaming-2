"""TRANSFORM (batch): respons hourly Open-Meteo -> baris tabel weather/air quality.

- Mengganti nama variabel API ke nama kolom database
- Validasi rentang fisik; nilai di luar rentang dijadikan NULL dan dihitung
- Jam tanpa satu pun nilai dibuang
- Menandai baris prakiraan (jam > sekarang) dengan is_forecast = True
Baris dikembalikan sebagai dict dengan waktu ISO string agar aman dikirim lewat XCom Airflow.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

# nama variabel API -> nama kolom database
WEATHER_FIELDS = {
    "temperature_2m": "temperature",
    "relative_humidity_2m": "relative_humidity",
    "precipitation": "precipitation",
    "rain": "rain",
    "wind_speed_10m": "wind_speed",
    "weather_code": "weather_code",
}
AIR_QUALITY_FIELDS = {
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "carbon_monoxide": "carbon_monoxide",
    "nitrogen_dioxide": "nitrogen_dioxide",
    "us_aqi": "us_aqi",
}
VALID_RANGES = {
    "temperature": (-30, 60),
    "relative_humidity": (0, 100),
    "precipitation": (0, 500),
    "rain": (0, 500),
    "wind_speed": (0, 300),
    "weather_code": (0, 99),
    "pm2_5": (0, 2000),
    "pm10": (0, 3000),
    "carbon_monoxide": (0, 100000),
    "nitrogen_dioxide": (0, 5000),
    "us_aqi": (0, 500),
}


@dataclass(frozen=True)
class HourlyBatch:
    rows: tuple[dict, ...]
    rejected_values: int


def _validated(column: str, value) -> float | None:
    if value is None:
        return None
    low, high = VALID_RANGES.get(column, (float("-inf"), float("inf")))
    return value if low <= value <= high else None


def _parse_hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def hourly_rows(payload: dict, field_map: dict[str, str], now: datetime) -> HourlyBatch:
    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError("respons Open-Meteo tidak berisi hourly.time")
    times = hourly["time"]
    series = {col: hourly.get(src, [None] * len(times)) for src, col in field_map.items()}
    if any(len(values) != len(times) for values in series.values()):
        raise ValueError("panjang array hourly tidak konsisten")

    rows, rejected = [], 0
    for i, raw_time in enumerate(times):
        hour = _parse_hour(raw_time)
        values = {}
        for col, arr in series.items():
            clean = _validated(col, arr[i])
            rejected += int(arr[i] is not None and clean is None)
            values[col] = clean
        if all(v is None for v in values.values()):
            continue
        rows.append({"observed_hour": hour.isoformat(), **values, "is_forecast": hour > now})
    return HourlyBatch(rows=tuple(rows), rejected_values=rejected)
