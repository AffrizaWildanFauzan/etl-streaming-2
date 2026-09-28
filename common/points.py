"""Titik pantau lalu lintas kota Metropolia (direpresentasikan oleh Surabaya).

Sesuai asumsi proyek: fokus 3 koridor prioritas, masing-masing 3 persimpangan kritis.
Koordinat bersifat perkiraan; TomTom mengembalikan ruas jalan terdekat dari titik ini.
"""

from dataclasses import dataclass

CITY = "Surabaya"
CITY_LATITUDE = -7.2575
CITY_LONGITUDE = 112.7521
# (min_lon, min_lat, max_lon, max_lat) untuk TomTom Incident API
CITY_BBOX = (112.60, -7.36, 112.85, -7.18)
TIMEZONE = "Asia/Jakarta"


@dataclass(frozen=True)
class MonitoringPoint:
    point_id: str
    corridor: str
    name: str
    latitude: float
    longitude: float


MONITORING_POINTS: tuple[MonitoringPoint, ...] = (
    MonitoringPoint("AY-01", "Koridor A. Yani", "Bundaran Waru", -7.3446, 112.7290),
    MonitoringPoint("AY-02", "Koridor A. Yani", "A. Yani - Jemursari", -7.3218, 112.7306),
    MonitoringPoint("AY-03", "Koridor A. Yani", "A. Yani - Royal Plaza", -7.3090, 112.7352),
    MonitoringPoint("DR-01", "Koridor Darmo - Tunjungan", "Raya Darmo - Taman Bungkul", -7.2913, 112.7397),
    MonitoringPoint("DR-02", "Koridor Darmo - Tunjungan", "Basuki Rahmat", -7.2668, 112.7414),
    MonitoringPoint("DR-03", "Koridor Darmo - Tunjungan", "Tunjungan", -7.2601, 112.7390),
    MonitoringPoint("MR-01", "Koridor MERR", "MERR - Kenjeran", -7.2555, 112.7797),
    MonitoringPoint("MR-02", "Koridor MERR", "MERR - Dharmahusada", -7.2790, 112.7818),
    MonitoringPoint("MR-03", "Koridor MERR", "MERR - Rungkut", -7.3195, 112.7803),
)

POINTS_BY_ID: dict[str, MonitoringPoint] = {p.point_id: p for p in MONITORING_POINTS}
