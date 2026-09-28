"""Helper validasi bersama untuk transform stream."""

import json
from datetime import UTC, datetime, timedelta

MAX_CLOCK_SKEW = timedelta(minutes=5)
REQUIRED_ENVELOPE_FIELDS = ("kind", "source", "observed_at", "ingested_at", "payload")


class InvalidRecord(ValueError):
    """Record gagal validasi; pesan menjelaskan alasannya (dikirim ke DLQ)."""


def require(payload: dict, key: str):
    if key not in payload or payload[key] is None:
        raise InvalidRecord(f"field wajib '{key}' tidak ada")
    return payload[key]


def number(value, name: str, minimum: float | None = None, maximum: float | None = None) -> float:
    if isinstance(value, bool):
        raise InvalidRecord(f"{name} bukan angka: {value!r}")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidRecord(f"{name} bukan angka: {value!r}") from exc
    if minimum is not None and result < minimum:
        raise InvalidRecord(f"{name} harus >= {minimum}, didapat {value!r}")
    if maximum is not None and result > maximum:
        raise InvalidRecord(f"{name} harus <= {maximum}, didapat {value!r}")
    return result


def epoch_ms(value, name: str) -> datetime:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise InvalidRecord(f"{name} bukan epoch ms valid: {value!r}") from exc


def iso_datetime(value, name: str) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidRecord(f"{name} bukan ISO datetime: {value!r}") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def not_in_future(ts: datetime, now: datetime) -> datetime:
    if ts > now + MAX_CLOCK_SKEW:
        raise InvalidRecord(f"waktu observasi di masa depan: {ts.isoformat()}")
    return ts


def decode_envelope(raw: bytes) -> dict:
    try:
        envelope = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidRecord(f"bukan JSON valid: {exc}") from exc
    if not isinstance(envelope, dict):
        raise InvalidRecord("envelope harus berupa object JSON")
    for key in REQUIRED_ENVELOPE_FIELDS:
        require(envelope, key)
    if not isinstance(envelope["payload"], dict):
        raise InvalidRecord("payload harus berupa object JSON")
    return envelope
