"""TRANSFORM (stream): insiden lalu lintas TomTom -> skema `Incident`.

Kategori mengikuti iconCategory TomTom Incident Details API v5.
"""

from dataclasses import dataclass
from datetime import datetime

from processing.stream.validation import InvalidRecord, epoch_ms, iso_datetime, number, require

CATEGORY_LABELS = {
    0: "Tidak diketahui", 1: "Kecelakaan", 2: "Kabut", 3: "Kondisi berbahaya", 4: "Hujan",
    5: "Es", 6: "Kemacetan", 7: "Lajur ditutup", 8: "Jalan ditutup", 9: "Perbaikan jalan",
    10: "Angin", 11: "Banjir", 14: "Kendaraan mogok",
}
MAGNITUDE_LABELS = {0: "Tidak diketahui", 1: "Ringan", 2: "Sedang", 3: "Berat", 4: "Tidak terdefinisi"}


@dataclass(frozen=True)
class Incident:
    incident_id: str
    source: str
    category: int
    category_label: str
    magnitude: int
    magnitude_label: str
    description: str | None
    road_from: str | None
    road_to: str | None
    length_m: float | None
    delay_seconds: float | None
    start_time: datetime | None
    end_time: datetime | None
    latitude: float
    longitude: float
    observed_at: datetime


def _first_coordinate(geometry: dict) -> tuple[float, float]:
    coords = geometry.get("coordinates") or []
    if not coords:
        raise InvalidRecord("koordinat insiden kosong")
    first = coords if geometry.get("type") == "Point" else coords[0]
    if not isinstance(first, (list, tuple)) or len(first) < 2:
        raise InvalidRecord(f"koordinat tidak valid: {first!r}")
    lon, lat = first[0], first[1]
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        raise InvalidRecord(f"koordinat tidak valid: {first!r}")
    return float(lat), float(lon)


def _optional_number(value, name: str) -> float | None:
    return None if value is None else number(value, name, 0)


def parse_incident(envelope: dict, now: datetime) -> Incident:
    payload = envelope["payload"]
    properties = require(payload, "properties")
    geometry = require(payload, "geometry")
    raw_id = require(properties, "id")
    latitude, longitude = _first_coordinate(geometry)
    category = int(number(properties.get("iconCategory", 0), "iconCategory", 0))
    magnitude = int(number(properties.get("magnitudeOfDelay", 0), "magnitudeOfDelay", 0))
    events = properties.get("events") or []
    source = str(envelope["source"])

    return Incident(
        incident_id=f"{source}:{raw_id}",
        source=source,
        category=category,
        category_label=CATEGORY_LABELS.get(category, "Lainnya"),
        magnitude=magnitude,
        magnitude_label=MAGNITUDE_LABELS.get(magnitude, "Tidak diketahui"),
        description=events[0].get("description") if events else None,
        road_from=properties.get("from"),
        road_to=properties.get("to"),
        length_m=_optional_number(properties.get("length"), "length"),
        delay_seconds=_optional_number(properties.get("delay"), "delay"),
        start_time=iso_datetime(properties.get("startTime"), "startTime"),
        end_time=iso_datetime(properties.get("endTime"), "endTime"),
        latitude=latitude,
        longitude=longitude,
        observed_at=epoch_ms(envelope["observed_at"], "observed_at"),
    )
