"""Uji pemutar ulang CSV: bentuk payload, pembagian jendela, dan posisi replay."""

from datetime import UTC, datetime, timedelta

import pytest

from ingestion.stream.csv_replay import (
    ReplayExhausted,
    flow_payload,
    incident_payload,
    load_state,
    parse_timestamp,
    read_rows,
    rows_in_window,
    save_state,
    state_cursor,
    strip_source_prefix,
    window_bounds,
)
from processing.stream.traffic_transform import parse_flow

FLOW_HEADER = ("timestamp_utc,point_id,source,current_speed,free_flow_speed,current_travel_time,"
               "free_flow_travel_time,speed_ratio,congestion_index,delay_seconds,congestion_level,"
               "road_closure,confidence\n")
FLOW_ROWS = (
    "2026-09-28 06:00:00.5+00,AY-01,simulator,39,45,55,48,0.8667,0.1333,7,LANCAR,f,0.93\n"
    "2026-09-28 06:05:00+00,DR-02,simulator,20,45,100,48,0.4444,0.5556,52,MACET,f,0.9\n"
    "2026-09-28 06:30:00+00,AY-01,simulator,30,45,70,48,0.6667,0.3333,22,PADAT,f,0.95\n"
)


@pytest.fixture()
def flow_csv(tmp_path):
    path = tmp_path / "traffic_flow.csv"
    path.write_text(FLOW_HEADER + FLOW_ROWS, encoding="utf-8")
    return path


def test_parses_postgres_offset_without_minutes():
    """Ekspor PostgreSQL menulis '+00', fromisoformat sendiri menuntut '+00:00'."""
    assert parse_timestamp("2026-09-28 06:58:42.41+00") == datetime(
        2026, 9, 28, 6, 58, 42, 410000, tzinfo=UTC)


def test_rows_sorted_by_recorded_time(flow_csv):
    rows = read_rows(flow_csv, "timestamp_utc")
    assert [row["point_id"] for row in rows] == ["AY-01", "DR-02", "AY-01"]
    assert rows[0]["_moment"] < rows[-1]["_moment"]


def test_payload_round_trips_through_real_transform(flow_csv):
    """Payload harus berbentuk seperti TomTom agar parse_flow menghasilkan nilai yang sama."""
    row = read_rows(flow_csv, "timestamp_utc")[1]
    envelope = {
        "kind": "flow", "source": row["source"], "point_id": row["point_id"],
        "observed_at": int(row["_moment"].timestamp() * 1000),
        "ingested_at": int(row["_moment"].timestamp() * 1000),
        "payload": flow_payload(row),
    }
    reading = parse_flow(envelope, datetime.now(UTC))

    assert reading.observed_at == row["_moment"], "observed_at wajib dari CSV, bukan jam dinding"
    assert reading.congestion_index == pytest.approx(float(row["congestion_index"]), abs=1e-4)
    assert reading.congestion_level == row["congestion_level"]
    assert reading.delay_seconds == pytest.approx(float(row["delay_seconds"]))


def test_windows_are_contiguous_and_cover_every_row(flow_csv):
    rows = read_rows(flow_csv, "timestamp_utc")
    cursor, seen, windows = None, 0, 0
    while True:
        try:
            start, end = window_bounds(rows, cursor, 360, 1)
        except ReplayExhausted:
            break
        seen += len(rows_in_window(rows, start, end))
        cursor, windows = end, windows + 1
    assert seen == len(rows), "tiap baris tepat satu kali: tanpa bolong, tanpa tumpang tindih"
    assert windows > 1


def test_all_mode_does_not_overflow_datetime(flow_csv):
    """windows=None, bukan angka jendela raksasa: timedelta(seconds=360*10**9) meluap."""
    rows = read_rows(flow_csv, "timestamp_utc")
    start, end = window_bounds(rows, None, 360, None)
    assert len(rows_in_window(rows, start, end)) == len(rows)


def test_cursor_past_last_row_is_exhausted(flow_csv):
    rows = read_rows(flow_csv, "timestamp_utc")
    beyond = rows[-1]["_moment"] + timedelta(seconds=1)
    with pytest.raises(ReplayExhausted, match="--reset"):
        window_bounds(rows, beyond, 360, 1)


def test_state_file_round_trip(tmp_path):
    path = tmp_path / "state" / ".replay_state.json"
    moment = datetime(2026, 9, 28, 6, 6, tzinfo=UTC)
    save_state(path, moment)
    assert state_cursor(load_state(path)) == moment


def test_unreadable_state_starts_from_beginning(tmp_path):
    path = tmp_path / ".replay_state.json"
    path.write_text("{bukan json", encoding="utf-8")
    assert state_cursor(load_state(path)) is None


def test_incident_id_matches_export_exactly():
    """parse_incident menambahkan awalan sumber, jadi ekspor harus dilepas lebih dulu."""
    assert strip_source_prefix("simulator:sim-1", "simulator") == "sim-1"
    assert strip_source_prefix("sim-1", "simulator") == "sim-1"


def test_incident_payload_has_geojson_shape():
    row = {
        "incident_id": "simulator:sim-1", "source": "simulator", "category": "1", "magnitude": "2",
        "description": "Simulated incident", "road_from": "", "road_to": "", "length_m": "",
        "delay_seconds": "249", "start_time": "2026-09-28 07:00:12+00", "end_time": "",
        "latitude": "-7.30095", "longitude": "112.74857",
    }
    payload = incident_payload(row)

    assert payload["geometry"] == {"type": "Point", "coordinates": [112.74857, -7.30095]}
    assert payload["properties"]["id"] == "sim-1"
    assert payload["properties"]["events"][0]["description"] == "Simulated incident"
    assert payload["properties"]["endTime"] is None
    assert payload["properties"]["length"] is None
