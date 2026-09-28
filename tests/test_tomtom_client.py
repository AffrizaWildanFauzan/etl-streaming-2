import pytest
import requests

from ingestion.stream.tomtom_client import TomTomAuthError, TomTomClient, TomTomError

SECRET = "super-secret-key"


class FakeResponse:
    def __init__(self, status: int, payload: dict | None = None):
        self.status_code = status
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self, response: FakeResponse | Exception):
        self.response = response
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: float) -> FakeResponse:
        self.calls.append((url, params))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_flow_returns_segment_data_and_sends_point_and_key():
    session = FakeSession(FakeResponse(200, {"flowSegmentData": {"currentSpeed": 30}}))
    client = TomTomClient(SECRET, session=session)

    data = client.flow(-7.26, 112.74)

    assert data == {"currentSpeed": 30}
    url, params = session.calls[0]
    assert "flowSegmentData/absolute/10/json" in url
    assert params["point"] == "-7.26,112.74"
    assert params["key"] == SECRET


def test_incidents_returns_list_for_bbox():
    session = FakeSession(FakeResponse(200, {"incidents": [{"type": "Feature"}]}))
    client = TomTomClient(SECRET, session=session)

    incidents = client.incidents((112.6, -7.36, 112.85, -7.18))

    assert incidents == [{"type": "Feature"}]
    assert session.calls[0][1]["bbox"] == "112.6,-7.36,112.85,-7.18"


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failure_raises_auth_error(status):
    client = TomTomClient(SECRET, session=FakeSession(FakeResponse(status)))
    with pytest.raises(TomTomAuthError):
        client.flow(-7.26, 112.74)


@pytest.mark.parametrize("response", [
    FakeResponse(429),
    FakeResponse(500),
    requests.ConnectionError(f"HTTPSConnectionPool url: /flow?key={SECRET}"),
])
def test_errors_never_leak_api_key(response):
    client = TomTomClient(SECRET, session=FakeSession(response))
    with pytest.raises(TomTomError) as excinfo:
        client.flow(-7.26, 112.74)
    assert SECRET not in str(excinfo.value)
    # exception asli (berisi URL + key) tidak boleh ikut di traceback
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__


def test_missing_api_key_is_rejected():
    with pytest.raises(ValueError):
        TomTomClient("")
