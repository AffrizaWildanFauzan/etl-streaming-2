"""EXTRACT + LOAD (batch): memuat ekspor CSV cuaca & kualitas udara ke PostgreSQL.

Pasangan batch dari `ingestion/stream/csv_replay.py`. Dipakai saat Open-Meteo tidak dipanggil
langsung -- misalnya ketika data harus tetap satu rentang waktu dengan ekspor lalu lintas.

Memakai `upsert_weather` / `upsert_air_quality` yang sudah ada di storage/batch_store.py, jadi
tidak ada SQL baru: kunci (city, observed_hour) dan perilaku ON CONFLICT tetap sama seperti
jalur Open-Meteo.

Kualitas udara dibaca dari mart_traffic_features.csv karena tabel air_quality_hourly tidak
ikut diekspor; di berkas itu nilainya terulang untuk setiap titik pantau pada jam yang sama,
sehingga diringkas satu baris per jam.

Jalankan:
  python -m ingestion.batch.csv_batch                 # cuaca + kualitas udara
  python -m ingestion.batch.csv_batch --dry-run       # hitung saja, tanpa menulis ke database
"""

import argparse
import csv
import logging
from pathlib import Path

from ingestion.stream.csv_replay import optional_float, parse_bool, parse_timestamp

log = logging.getLogger("csv_batch")

DEFAULT_WEATHER_CSV = Path("data/raw/weather_hourly.csv")
DEFAULT_AIR_QUALITY_CSV = Path("data/raw/mart_traffic_features.csv")
DEFAULT_CITY = "Surabaya"
WEATHER_FIELDS = ("temperature", "relative_humidity", "precipitation", "rain",
                  "wind_speed", "weather_code")
AIR_QUALITY_FIELDS = ("pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide", "us_aqi")


def read_weather_rows(path: Path) -> dict[str, list[dict]]:
    """Baris cuaca per kota, siap dipakai upsert_weather."""
    by_city: dict[str, list[dict]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            city = (raw.get("city") or DEFAULT_CITY).strip() or DEFAULT_CITY
            row = {"observed_hour": parse_timestamp(raw["observed_hour"]),
                   "is_forecast": parse_bool(raw.get("is_forecast"))}
            row.update({field: optional_float(raw.get(field)) for field in WEATHER_FIELDS})
            by_city.setdefault(city, []).append(row)
    return by_city


def read_air_quality_rows(path: Path, city: str = DEFAULT_CITY) -> dict[str, list[dict]]:
    """Satu baris per jam: di mart, nilai kualitas udara terulang untuk tiap titik pantau."""
    seen: dict[object, dict] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            hour = parse_timestamp(raw["hour"])
            if hour in seen:
                continue
            row = {"observed_hour": hour, "is_forecast": False}
            row.update({field: optional_float(raw.get(field)) for field in AIR_QUALITY_FIELDS})
            seen[hour] = row
    return {city: sorted(seen.values(), key=lambda row: row["observed_hour"])}


def summarize(label: str, by_city: dict[str, list[dict]]) -> None:
    for city, rows in by_city.items():
        hours = [row["observed_hour"] for row in rows]
        log.info("%s %s: %d baris, %s -> %s", label, city, len(rows),
                 min(hours).isoformat(), max(hours).isoformat())


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--weather-csv", type=Path, default=DEFAULT_WEATHER_CSV)
    parser.add_argument("--air-quality-csv", type=Path, default=DEFAULT_AIR_QUALITY_CSV)
    parser.add_argument("--city", default=DEFAULT_CITY,
                        help="kota untuk kualitas udara (mart tidak menyimpan kolom city)")
    parser.add_argument("--skip-air-quality", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="hitung saja, tanpa menulis ke database")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_arg_parser().parse_args(argv)

    weather = read_weather_rows(args.weather_csv)
    summarize("Cuaca", weather)
    air_quality = {}
    if not args.skip_air_quality:
        if args.air_quality_csv.exists():
            air_quality = read_air_quality_rows(args.air_quality_csv, args.city)
            summarize("Kualitas udara", air_quality)
        else:
            log.warning("%s tidak ada, kualitas udara dilewati", args.air_quality_csv)
    if args.dry_run:
        return 0

    # Import di dalam fungsi: modul ini harus bisa diuji tanpa psycopg2 terpasang.
    from storage.batch_store import connect, upsert_air_quality, upsert_weather

    with connect() as conn:
        for city, rows in weather.items():
            log.info("weather_hourly %s: %d baris dimuat", city, upsert_weather(conn, city, rows))
        for city, rows in air_quality.items():
            log.info("air_quality_hourly %s: %d baris dimuat", city,
                     upsert_air_quality(conn, city, rows))
        conn.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
