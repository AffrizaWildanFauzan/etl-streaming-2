"""Simulator lalu lintas - HANYA untuk development/testing tanpa API key.

Menghasilkan payload dengan format yang sama persis dengan TomTom, sehingga seluruh
pipeline bisa diuji. Data ini sintetis: source-nya ditandai "simulator" di database.
Pola: jam sibuk pagi (07.00-08.00 WIB) dan sore (17.00-18.30 WIB).
"""

import math
import random
from datetime import UTC, datetime, timedelta

WIB = timedelta(hours=7)
SEGMENT_LENGTH_M = 600
MORNING_PEAK_HOUR = 7.5
EVENING_PEAK_HOUR = 17.75
PEAK_WIDTH_HOURS = 1.2
INCIDENT_PROBABILITY = 0.15
INCIDENT_LIFETIME_POLLS = 5


def _rush_factor(now: datetime) -> float:
    hour = ((now + WIB).hour + (now + WIB).minute / 60) % 24
    peaks = (MORNING_PEAK_HOUR, EVENING_PEAK_HOUR)
    return max(math.exp(-((hour - p) ** 2) / (2 * PEAK_WIDTH_HOURS ** 2)) for p in peaks)


class SimulatedTrafficClient:
    def __init__(self, seed: int | None = None):
        self._random = random.Random(seed)
        self._incidents: list[tuple[int, dict]] = []
        self._counter = 0

    def flow(self, latitude: float, longitude: float) -> dict:
        now = datetime.now(UTC)
        free_flow = 40 + (abs(hash((round(latitude, 4), round(longitude, 4)))) % 21)
        congestion = min(0.95, 0.1 + 0.6 * _rush_factor(now) + self._random.uniform(-0.08, 0.08))
        current = max(1, round(free_flow * (1 - max(congestion, 0))))
        return {
            "frc": "FRC2",
            "currentSpeed": current,
            "freeFlowSpeed": free_flow,
            "currentTravelTime": round(SEGMENT_LENGTH_M / (current / 3.6)),
            "freeFlowTravelTime": round(SEGMENT_LENGTH_M / (free_flow / 3.6)),
            "confidence": round(self._random.uniform(0.85, 1.0), 2),
            "roadClosure": False,
        }

    def incidents(self, bbox: tuple[float, float, float, float]) -> list[dict]:
        self._incidents = [(age + 1, inc) for age, inc in self._incidents
                           if age + 1 < INCIDENT_LIFETIME_POLLS]
        if self._random.random() < INCIDENT_PROBABILITY:
            self._incidents.append((0, self._new_incident(bbox)))
        return [inc for _, inc in self._incidents]

    def _new_incident(self, bbox: tuple[float, float, float, float]) -> dict:
        self._counter += 1
        min_lon, min_lat, max_lon, max_lat = bbox
        lon = self._random.uniform(min_lon, max_lon)
        lat = self._random.uniform(min_lat, max_lat)
        category = self._random.choice([1, 6, 6, 9, 14])
        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "id": f"sim-{self._counter}",
                "iconCategory": category,
                "magnitudeOfDelay": self._random.randint(1, 3),
                "events": [{"description": "Simulated incident", "code": 0}],
                "startTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime": None,
                "delay": self._random.randint(60, 900),
            },
        }
