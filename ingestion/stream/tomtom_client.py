"""EXTRACT (stream): client TomTom Traffic API.

- Flow Segment Data  : kecepatan saat ini vs kecepatan arus bebas pada satu ruas jalan
- Incident Details   : kecelakaan, kemacetan, penutupan & perbaikan jalan dalam satu bounding box

Pesan error sengaja tidak menyertakan URL/exception asli karena URL berisi API key.
"""

import requests

FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json"
INCIDENT_URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"
INCIDENT_FIELDS = (
    "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,"
    "magnitudeOfDelay,events{description,code},startTime,endTime,from,to,"
    "length,delay,roadNumbers}}}"
)
DEFAULT_ZOOM = 10
DEFAULT_TIMEOUT_SECONDS = 10
AUTH_ERROR_STATUSES = (401, 403)


class TomTomError(RuntimeError):
    """Request ke TomTom gagal (jaringan, rate limit, server error)."""


class TomTomAuthError(TomTomError):
    """API key tidak valid / tidak punya akses - tidak ada gunanya retry."""


class TomTomClient:
    def __init__(self, api_key: str, zoom: int = DEFAULT_ZOOM,
                 timeout: float = DEFAULT_TIMEOUT_SECONDS, session=None):
        if not api_key:
            raise ValueError("TOMTOM_API_KEY kosong")
        self._api_key = api_key
        self._zoom = zoom
        self._timeout = timeout
        self._session = session or requests.Session()

    def flow(self, latitude: float, longitude: float) -> dict:
        body = self._get(
            FLOW_URL.format(zoom=self._zoom),
            {"point": f"{latitude},{longitude}", "unit": "KMPH"},
        )
        return body.get("flowSegmentData", {})

    def incidents(self, bbox: tuple[float, float, float, float]) -> list[dict]:
        body = self._get(INCIDENT_URL, {
            "bbox": ",".join(str(v) for v in bbox),
            "fields": INCIDENT_FIELDS,
            "language": "en-GB",
            "timeValidityFilter": "present",
        })
        return body.get("incidents", [])

    def _get(self, url: str, params: dict) -> dict:
        try:
            response = self._session.get(
                url, params={**params, "key": self._api_key}, timeout=self._timeout
            )
        except requests.RequestException as exc:
            raise TomTomError(f"koneksi gagal: {type(exc).__name__}") from None
        if response.status_code in AUTH_ERROR_STATUSES:
            raise TomTomAuthError(f"API key ditolak (HTTP {response.status_code})") from None
        if response.status_code != 200:
            raise TomTomError(f"HTTP {response.status_code}") from None
        try:
            return response.json()
        except ValueError:
            raise TomTomError("respons bukan JSON") from None
