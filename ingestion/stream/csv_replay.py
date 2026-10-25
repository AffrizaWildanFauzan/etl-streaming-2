"""EXTRACT (stream, replay): memutar ulang ekspor CSV ke Kafka seperti producer sungguhan.

Dipakai untuk mensimulasikan streaming tanpa menyalakan Docker terus-menerus menunggu data
terkumpul. Sumbernya ekspor tabel `traffic_flow` dan `traffic_incidents` (lihat data/raw/).

Yang membedakannya dari `producer.py`: `observed_at` diambil dari kolom timestamp CSV, BUKAN
jam dinding. Kalau ditimpa jam sekarang, rentang historis ekspor (±189 jam) runtuh menjadi
sepanjang durasi replay saja, sehingga `MIN_TRAIN_HOURS = 72` di modeling tidak pernah
terpenuhi dan model gagal dilatih. Processor di hilir tidak perlu diubah: ia menghitung ulang
speed_ratio dan congestion_index dari current_speed & free_flow_speed, dan validasi
`not_in_future()` memang mengizinkan waktu masa lalu.

Posisi pemutaran disimpan di berkas state, jadi setiap pemanggilan melanjutkan dari tempat
berhenti -- itu yang membuat "satu jendela masuk, lihat dashboard, jalankan lagi" bisa manual.

Jalankan:
  python -m ingestion.stream.csv_replay                 # 1 jendela 6 menit lalu berhenti
  python -m ingestion.stream.csv_replay --windows 10    # 10 jendela sekaligus
  python -m ingestion.stream.csv_replay --all           # seluruh CSV
  python -m ingestion.stream.csv_replay --reset         # mulai lagi dari awal CSV
"""

import argparse
import csv
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

log = logging.getLogger("csv_replay")

DEFAULT_FLOW_CSV = Path("data/raw/traffic_flow.csv")
DEFAULT_INCIDENT_CSV = Path("data/raw/traffic_incidents.csv")
DEFAULT_STATE_FILE = Path("data/.replay_state.json")
DEFAULT_WINDOW_SECONDS = 360  # satu jendela = 6 menit waktu rekaman
FLUSH_TIMEOUT_SECONDS = 10
TRUE_VALUES = {"t", "true", "1", "yes", "y"}
OFFSET_WITHOUT_MINUTES = re.compile(r"([+-]\d{2})$")


class ReplayExhausted(RuntimeError):
    """Seluruh baris CSV sudah pernah dikirim."""


def parse_timestamp(value: str) -> datetime:
    """PostgreSQL menulis offset sebagai '+00', fromisoformat butuh '+00:00'."""
    text = str(value).strip().replace("Z", "+00:00")
    text = OFFSET_WITHOUT_MINUTES.sub(r"\1:00", text)
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_bool(value) -> bool:
    return str(value).strip().lower() in TRUE_VALUES


def optional_float(value) -> float | None:
    text = str(value).strip() if value is not None else ""
    return float(text) if text else None


def to_epoch_ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def read_rows(path: Path, time_column: str) -> list[dict]:
    """Baris CSV yang punya timestamp, diurutkan menurut waktu rekaman."""
    with path.open(newline="", encoding="utf-8") as handle:
        rows = [row for row in csv.DictReader(handle) if (row.get(time_column) or "").strip()]
    for row in rows:
        row["_moment"] = parse_timestamp(row[time_column])
    rows.sort(key=lambda row: row["_moment"])
    return rows


def flow_payload(row: dict) -> dict:
    """Bentuk payload sama seperti TomTom Flow Segment Data, agar parse_flow tidak berubah."""
    payload = {
        "currentSpeed": float(row["current_speed"]),
        "freeFlowSpeed": float(row["free_flow_speed"]),
        "currentTravelTime": float(row["current_travel_time"]),
        "freeFlowTravelTime": float(row["free_flow_travel_time"]),
        "roadClosure": parse_bool(row.get("road_closure")),
    }
    confidence = optional_float(row.get("confidence"))
    if confidence is not None:
        payload["confidence"] = confidence
    return payload


def strip_source_prefix(incident_id: str, source: str) -> str:
    """Ekspor menyimpan id yang sudah berawalan sumber; parse_incident menambahkannya lagi."""
    prefix = f"{source}:"
    return incident_id[len(prefix):] if incident_id.startswith(prefix) else incident_id


def incident_payload(row: dict) -> dict:
    """Bentuk payload sama seperti TomTom Incident Details (GeoJSON feature)."""
    source = row["source"]
    properties = {
        "id": strip_source_prefix(row["incident_id"], source),
        "iconCategory": int(float(row["category"])),
        "magnitudeOfDelay": int(float(row["magnitude"])),
        "from": row.get("road_from") or None,
        "to": row.get("road_to") or None,
        "length": optional_float(row.get("length_m")),
        "delay": optional_float(row.get("delay_seconds")),
        "startTime": (row.get("start_time") or "").strip() or None,
        "endTime": (row.get("end_time") or "").strip() or None,
    }
    if (row.get("description") or "").strip():
        properties["events"] = [{"description": row["description"]}]
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {
            "type": "Point",
            "coordinates": [float(row["longitude"]), float(row["latitude"])],
        },
    }


def window_bounds(rows: list[dict], cursor: datetime | None, window_seconds: int,
                  windows: int | None) -> tuple[datetime, datetime]:
    """Jendela berikutnya dalam satuan waktu rekaman, bukan waktu nyata.

    windows=None berarti seluruh sisa CSV. Jangan diganti dengan angka jendela yang
    sangat besar: `timedelta(seconds=360 * 10**9)` melebihi batas datetime (OverflowError).
    """
    if not rows:
        raise ReplayExhausted("CSV tidak punya baris berisi timestamp")
    last = rows[-1]["_moment"]
    start = cursor if cursor is not None else rows[0]["_moment"]
    if start > last:
        raise ReplayExhausted(
            f"posisi replay ({start.isoformat()}) sudah melewati baris terakhir CSV "
            f"({last.isoformat()}); jalankan dengan --reset untuk mulai lagi"
        )
    if windows is None:
        return start, last + timedelta(microseconds=1)
    return start, start + timedelta(seconds=window_seconds * windows)


def rows_in_window(rows: list[dict], start: datetime, end: datetime) -> list[dict]:
    return [row for row in rows if start <= row["_moment"] < end]


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Berkas state tidak terbaca (%s), replay dimulai dari awal CSV", exc)
        return {}


def save_state(path: Path, cursor: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"cursor": cursor.isoformat()}, indent=2), encoding="utf-8")


def state_cursor(state: dict) -> datetime | None:
    raw = state.get("cursor")
    return parse_timestamp(raw) if raw else None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--flow-csv", type=Path, default=DEFAULT_FLOW_CSV)
    parser.add_argument("--incident-csv", type=Path, default=DEFAULT_INCIDENT_CSV)
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--window-seconds", type=int, default=DEFAULT_WINDOW_SECONDS,
                        help="panjang satu jendela dalam detik waktu rekaman (default 360)")
    parser.add_argument("--windows", type=int, default=1,
                        help="berapa jendela dikirim pada pemanggilan ini (default 1)")
    parser.add_argument("--all", action="store_true", help="kirim seluruh sisa CSV sekaligus")
    parser.add_argument("--reset", action="store_true", help="hapus posisi, mulai dari awal CSV")
    parser.add_argument("--dry-run", action="store_true",
                        help="hitung saja, tanpa mengirim ke Kafka dan tanpa menyimpan posisi")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_arg_parser().parse_args(argv)

    if args.reset and args.state_file.exists():
        args.state_file.unlink()
        log.info("Posisi replay dihapus, mulai dari awal CSV")

    flow_rows = read_rows(args.flow_csv, "timestamp_utc")
    incident_rows = (read_rows(args.incident_csv, "first_seen_at")
                     if args.incident_csv.exists() else [])
    if not args.incident_csv.exists():
        log.warning("%s tidak ada, insiden dilewati", args.incident_csv)

    cursor = state_cursor(load_state(args.state_file))
    windows = None if args.all else args.windows
    try:
        start, end = window_bounds(flow_rows, cursor, args.window_seconds, windows)
    except ReplayExhausted as exc:
        log.error("%s", exc)
        return 1

    flows = rows_in_window(flow_rows, start, end)
    incidents = rows_in_window(incident_rows, start, end)
    log.info("Jendela %s -> %s: %d baris flow, %d insiden",
             start.isoformat(), end.isoformat(), len(flows), len(incidents))
    if args.dry_run:
        return 0

    # Import di dalam fungsi: modul ini harus bisa diuji tanpa confluent_kafka terpasang.
    from confluent_kafka import Producer

    from common.config import kafka_config
    from ingestion.stream.producer import envelope, publish

    kafka = kafka_config()
    producer = Producer({
        "bootstrap.servers": kafka.bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "client.id": "producer-csv-replay",
    })
    for row in flows:
        publish(producer, kafka.flow_topic, row["point_id"],
                envelope("flow", row["source"], flow_payload(row),
                         to_epoch_ms(row["_moment"]), point_id=row["point_id"]))
    for row in incidents:
        payload = incident_payload(row)
        publish(producer, kafka.incident_topic, payload["properties"]["id"],
                envelope("incident", row["source"], payload, to_epoch_ms(row["_moment"])))
    producer.flush(FLUSH_TIMEOUT_SECONDS)

    save_state(args.state_file, end)
    remaining = sum(1 for row in flow_rows if row["_moment"] >= end)
    log.info("Terkirim. Posisi berikutnya %s, sisa %d baris flow di CSV", end.isoformat(), remaining)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
