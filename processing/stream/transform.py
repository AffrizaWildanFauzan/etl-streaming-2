"""TRANSFORM (stream): dispatcher pesan Kafka ke parser sesuai jenisnya."""

from collections.abc import Callable
from datetime import UTC, datetime

from processing.stream.incident_transform import Incident, parse_incident
from processing.stream.traffic_transform import TrafficReading, parse_flow
from processing.stream.validation import InvalidRecord, decode_envelope

PARSERS: dict[str, Callable[[dict, datetime], TrafficReading | Incident]] = {
    "flow": parse_flow,
    "incident": parse_incident,
}


def transform_message(raw: bytes, now: datetime | None = None) -> TrafficReading | Incident:
    envelope = decode_envelope(raw)
    parser = PARSERS.get(envelope["kind"])
    if parser is None:
        raise InvalidRecord(f"kind tidak dikenal: {envelope['kind']!r}")
    return parser(envelope, now or datetime.now(UTC))
