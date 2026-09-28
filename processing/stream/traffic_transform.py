"""TRANSFORM (stream): data arus lalu lintas TomTom -> metrik kemacetan.

speed_ratio      = kecepatan saat ini / kecepatan arus bebas (dibatasi 0..1)
congestion_index = 1 - speed_ratio (0 = lancar, 1 = berhenti total)
delay_seconds    = waktu tempuh saat ini - waktu tempuh arus bebas
"""

from dataclasses import dataclass
from datetime import datetime

from common.points import POINTS_BY_ID
from processing.stream.validation import InvalidRecord, epoch_ms, not_in_future, number, require

LANCAR_MIN_RATIO = 0.8
PADAT_MIN_RATIO = 0.5
MAX_SPEED_KMH = 200


@dataclass(frozen=True)
class TrafficReading:
    point_id: str
    observed_at: datetime
    source: str
    frc: str | None
    current_speed: float
    free_flow_speed: float
    current_travel_time: float
    free_flow_travel_time: float
    confidence: float
    road_closure: bool
    speed_ratio: float
    congestion_index: float
    delay_seconds: float
    congestion_level: str
    ingested_at: datetime


def congestion_level(speed_ratio: float, road_closure: bool) -> str:
    if road_closure:
        return "TUTUP"
    if speed_ratio >= LANCAR_MIN_RATIO:
        return "LANCAR"
    if speed_ratio >= PADAT_MIN_RATIO:
        return "PADAT"
    return "MACET"


def parse_flow(envelope: dict, now: datetime) -> TrafficReading:
    point_id = str(require(envelope, "point_id"))
    if point_id not in POINTS_BY_ID:
        raise InvalidRecord(f"point_id tidak dikenal: {point_id!r}")
    payload = envelope["payload"]

    current = number(require(payload, "currentSpeed"), "currentSpeed", 0, MAX_SPEED_KMH)
    free_flow = number(require(payload, "freeFlowSpeed"), "freeFlowSpeed", 1, MAX_SPEED_KMH)
    current_tt = number(require(payload, "currentTravelTime"), "currentTravelTime", 0)
    free_tt = number(require(payload, "freeFlowTravelTime"), "freeFlowTravelTime", 0)
    confidence = number(payload.get("confidence", 1), "confidence", 0, 1)
    closure = bool(payload.get("roadClosure", False))

    ratio = 0.0 if closure else min(current / free_flow, 1.0)
    return TrafficReading(
        point_id=point_id,
        observed_at=not_in_future(epoch_ms(envelope["observed_at"], "observed_at"), now),
        source=str(envelope["source"]),
        frc=payload.get("frc"),
        current_speed=current,
        free_flow_speed=free_flow,
        current_travel_time=current_tt,
        free_flow_travel_time=free_tt,
        confidence=confidence,
        road_closure=closure,
        speed_ratio=round(ratio, 4),
        congestion_index=round(1 - ratio, 4),
        delay_seconds=max(current_tt - free_tt, 0),
        congestion_level=congestion_level(ratio, closure),
        ingested_at=epoch_ms(envelope["ingested_at"], "ingested_at"),
    )
