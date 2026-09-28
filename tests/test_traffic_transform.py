import json
from datetime import UTC, datetime

import pytest

from processing.stream.traffic_transform import TrafficReading, congestion_level
from processing.stream.transform import transform_message
from processing.stream.validation import InvalidRecord

NOW = datetime(2026, 9, 28, 1, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def flow_envelope(point_id: str = "DR-02", **overrides) -> bytes:
    payload = {"frc": "FRC2", "currentSpeed": 26, "freeFlowSpeed": 52,
               "currentTravelTime": 120, "freeFlowTravelTime": 60,
               "confidence": 0.95, "roadClosure": False, **overrides}
    return json.dumps({"kind": "flow", "source": "tomtom", "point_id": point_id,
                       "observed_at": NOW_MS - 2000, "ingested_at": NOW_MS - 1000,
                       "payload": payload}).encode()


def test_flow_is_enriched_with_congestion_metrics():
    reading = transform_message(flow_envelope(), now=NOW)

    assert isinstance(reading, TrafficReading)
    assert reading.point_id == "DR-02"
    assert reading.speed_ratio == pytest.approx(0.5)
    assert reading.congestion_index == pytest.approx(0.5)
    assert reading.delay_seconds == 60
    assert reading.congestion_level == "PADAT"
    assert reading.observed_at == datetime(2026, 9, 28, 0, 59, 58, tzinfo=UTC)


def test_speed_above_free_flow_is_capped_to_no_congestion():
    reading = transform_message(flow_envelope(currentSpeed=60), now=NOW)
    assert reading.speed_ratio == 1.0
    assert reading.congestion_index == 0.0
    assert reading.delay_seconds == 60  # tetap dihitung dari travel time
    assert reading.congestion_level == "LANCAR"


@pytest.mark.parametrize("ratio, closure, expected", [
    (0.95, False, "LANCAR"),
    (0.80, False, "LANCAR"),
    (0.79, False, "PADAT"),
    (0.50, False, "PADAT"),
    (0.49, False, "MACET"),
    (0.90, True, "TUTUP"),
])
def test_congestion_level_thresholds(ratio, closure, expected):
    assert congestion_level(ratio, closure) == expected


@pytest.mark.parametrize("overrides, reason", [
    ({"freeFlowSpeed": 0}, "freeFlowSpeed"),
    ({"currentSpeed": -1}, "currentSpeed"),
    ({"confidence": 1.5}, "confidence"),
    ({"currentTravelTime": "abc"}, "currentTravelTime"),
])
def test_invalid_flow_values_are_rejected(overrides, reason):
    with pytest.raises(InvalidRecord, match=reason):
        transform_message(flow_envelope(**overrides), now=NOW)


def test_unknown_point_is_rejected():
    with pytest.raises(InvalidRecord, match="point_id"):
        transform_message(flow_envelope(point_id="XX-99"), now=NOW)


def test_road_closure_with_zero_speed_is_valid():
    reading = transform_message(flow_envelope(currentSpeed=0, roadClosure=True), now=NOW)
    assert reading.congestion_level == "TUTUP"
    assert reading.congestion_index == 1.0


@pytest.mark.parametrize("raw", [b"bukan json", b"[]", b'{"kind": "flow"}',
                                 b'{"kind": "cuaca", "source": "x", "observed_at": 1, "ingested_at": 1, "payload": {}}'])
def test_malformed_envelopes_are_rejected(raw):
    with pytest.raises(InvalidRecord):
        transform_message(raw, now=NOW)


def test_future_observation_is_rejected():
    raw = json.loads(flow_envelope())
    raw["observed_at"] = NOW_MS + 60 * 60 * 1000
    with pytest.raises(InvalidRecord, match="masa depan"):
        transform_message(json.dumps(raw).encode(), now=NOW)
