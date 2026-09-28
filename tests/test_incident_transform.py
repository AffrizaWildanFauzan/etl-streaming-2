import json
from datetime import UTC, datetime

import pytest

from processing.stream.incident_transform import Incident
from processing.stream.transform import transform_message
from processing.stream.validation import InvalidRecord

NOW = datetime(2026, 9, 28, 1, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def incident_envelope(**prop_overrides) -> bytes:
    properties = {
        "id": "abc123", "iconCategory": 1, "magnitudeOfDelay": 3,
        "events": [{"description": "Accident", "code": 401}],
        "startTime": "2026-09-28T00:30:00Z", "endTime": None,
        "from": "Jl. Basuki Rahmat", "to": "Jl. Embong Malang",
        "length": 412.5, "delay": 240, "roadNumbers": [],
        **prop_overrides,
    }
    payload = {"type": "Feature",
               "geometry": {"type": "LineString", "coordinates": [[112.7414, -7.2668], [112.7420, -7.2650]]},
               "properties": properties}
    return json.dumps({"kind": "incident", "source": "tomtom", "observed_at": NOW_MS,
                       "ingested_at": NOW_MS, "payload": payload}).encode()


def test_incident_is_normalized():
    incident = transform_message(incident_envelope(), now=NOW)

    assert isinstance(incident, Incident)
    assert incident.incident_id == "tomtom:abc123"
    assert incident.category == 1
    assert incident.category_label == "Kecelakaan"
    assert incident.magnitude_label == "Berat"
    assert incident.description == "Accident"
    assert incident.latitude == pytest.approx(-7.2668)
    assert incident.longitude == pytest.approx(112.7414)
    assert incident.start_time == datetime(2026, 9, 28, 0, 30, tzinfo=UTC)
    assert incident.end_time is None
    assert incident.delay_seconds == 240


def test_point_geometry_is_supported():
    raw = json.loads(incident_envelope())
    raw["payload"]["geometry"] = {"type": "Point", "coordinates": [112.74, -7.26]}
    incident = transform_message(json.dumps(raw).encode(), now=NOW)
    assert (incident.latitude, incident.longitude) == (-7.26, 112.74)


def test_unknown_category_is_labelled_lainnya():
    incident = transform_message(incident_envelope(iconCategory=99), now=NOW)
    assert incident.category_label == "Lainnya"


def test_optional_fields_can_be_missing():
    raw = json.loads(incident_envelope())
    props = raw["payload"]["properties"]
    for key in ("events", "from", "to", "length", "delay", "startTime"):
        props.pop(key)
    incident = transform_message(json.dumps(raw).encode(), now=NOW)
    assert incident.description is None
    assert incident.delay_seconds is None


@pytest.mark.parametrize("mutate, reason", [
    (lambda p: p["properties"].pop("id"), "id"),
    (lambda p: p.pop("geometry"), "geometry"),
    (lambda p: p["geometry"].update(coordinates=[]), "koordinat"),
    (lambda p: p["geometry"].update(coordinates=[[200, 95]]), "koordinat"),
])
def test_invalid_incidents_are_rejected(mutate, reason):
    raw = json.loads(incident_envelope())
    mutate(raw["payload"])
    with pytest.raises(InvalidRecord, match=reason):
        transform_message(json.dumps(raw).encode(), now=NOW)
