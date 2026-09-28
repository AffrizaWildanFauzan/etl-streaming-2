-- Skema data warehouse Smart City Metropolia (lalu lintas, cuaca, kualitas udara).
-- Semua waktu disimpan dalam UTC (TIMESTAMPTZ); tampilan dashboard dikonversi ke WIB.

-- ================= DIMENSI =================
CREATE TABLE IF NOT EXISTS monitoring_points (
    point_id   TEXT PRIMARY KEY,
    corridor   TEXT             NOT NULL,
    name       TEXT             NOT NULL,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL
);

-- ================= STREAMING (Kafka -> processor) =================
CREATE TABLE IF NOT EXISTS traffic_flow (
    point_id              TEXT        NOT NULL REFERENCES monitoring_points (point_id),
    observed_at           TIMESTAMPTZ NOT NULL,
    source                TEXT        NOT NULL,          -- tomtom / simulator
    frc                   TEXT,                          -- functional road class
    current_speed         REAL        NOT NULL,          -- km/jam
    free_flow_speed       REAL        NOT NULL,          -- km/jam
    current_travel_time   REAL        NOT NULL,          -- detik
    free_flow_travel_time REAL        NOT NULL,          -- detik
    confidence            REAL        NOT NULL,
    road_closure          BOOLEAN     NOT NULL,
    speed_ratio           REAL        NOT NULL CHECK (speed_ratio BETWEEN 0 AND 1),
    congestion_index      REAL        NOT NULL CHECK (congestion_index BETWEEN 0 AND 1),
    delay_seconds         REAL        NOT NULL,
    congestion_level      TEXT        NOT NULL CHECK (congestion_level IN ('LANCAR','PADAT','MACET','TUTUP')),
    ingested_at           TIMESTAMPTZ NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (point_id, observed_at)
);
CREATE INDEX IF NOT EXISTS idx_flow_observed ON traffic_flow (observed_at DESC);

CREATE TABLE IF NOT EXISTS traffic_incidents (
    incident_id     TEXT PRIMARY KEY,
    source          TEXT             NOT NULL,
    category        INT              NOT NULL,
    category_label  TEXT             NOT NULL,
    magnitude       INT              NOT NULL,
    magnitude_label TEXT             NOT NULL,
    description     TEXT,
    road_from       TEXT,
    road_to         TEXT,
    length_m        REAL,
    delay_seconds   REAL,
    start_time      TIMESTAMPTZ,
    end_time        TIMESTAMPTZ,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    first_seen_at   TIMESTAMPTZ      NOT NULL,
    last_seen_at    TIMESTAMPTZ      NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incidents_last_seen ON traffic_incidents (last_seen_at DESC);

-- ================= BATCH (Airflow) =================
CREATE TABLE IF NOT EXISTS weather_hourly (
    city                 TEXT        NOT NULL,
    observed_hour        TIMESTAMPTZ NOT NULL,
    temperature          REAL,        -- °C
    relative_humidity    REAL,        -- %
    precipitation        REAL,        -- mm
    rain                 REAL,        -- mm
    wind_speed           REAL,        -- km/jam
    weather_code         INT,         -- kode WMO
    is_forecast          BOOLEAN     NOT NULL,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, observed_hour)
);

CREATE TABLE IF NOT EXISTS air_quality_hourly (
    city             TEXT        NOT NULL,
    observed_hour    TIMESTAMPTZ NOT NULL,
    pm2_5            REAL,        -- µg/m³
    pm10             REAL,        -- µg/m³
    carbon_monoxide  REAL,        -- µg/m³
    nitrogen_dioxide REAL,        -- µg/m³
    us_aqi           REAL,
    is_forecast      BOOLEAN     NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, observed_hour)
);

-- Mart per jam hasil agregasi traffic_flow (DAG traffic_hourly_mart)
CREATE TABLE IF NOT EXISTS traffic_hourly (
    point_id          TEXT        NOT NULL REFERENCES monitoring_points (point_id),
    hour              TIMESTAMPTZ NOT NULL,
    avg_speed         REAL        NOT NULL,
    avg_free_flow     REAL        NOT NULL,
    avg_congestion    REAL        NOT NULL,
    max_congestion    REAL        NOT NULL,
    avg_delay_seconds REAL        NOT NULL,
    macet_samples     INT         NOT NULL,
    samples           INT         NOT NULL,
    built_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (point_id, hour)
);

-- Dataset siap model: lalu lintas + cuaca + kualitas udara, digabung per jam
CREATE OR REPLACE VIEW mart_traffic_features AS
SELECT t.point_id, p.corridor, t.hour, t.avg_speed, t.avg_congestion, t.max_congestion,
       t.avg_delay_seconds, t.samples,
       w.temperature, w.relative_humidity, w.precipitation, w.rain, w.wind_speed,
       a.pm2_5, a.pm10, a.carbon_monoxide, a.nitrogen_dioxide, a.us_aqi
FROM traffic_hourly t
JOIN monitoring_points p USING (point_id)
LEFT JOIN weather_hourly w     ON w.observed_hour = t.hour
LEFT JOIN air_quality_hourly a ON a.observed_hour = t.hour;

-- ================= MODEL (DAG congestion_model_daily) =================
CREATE TABLE IF NOT EXISTS model_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    model_version TEXT        NOT NULL,
    trained_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    train_rows    INT         NOT NULL,
    test_rows     INT         NOT NULL,
    mae           REAL        NOT NULL,
    rmse          REAL        NOT NULL,
    baseline_mae  REAL        NOT NULL,
    features      TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS traffic_forecast (
    point_id             TEXT        NOT NULL REFERENCES monitoring_points (point_id),
    target_hour          TIMESTAMPTZ NOT NULL,
    predicted_congestion REAL        NOT NULL,
    predicted_level      TEXT        NOT NULL,
    model_version        TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (point_id, target_hour)
);

-- ================= VIEW DASHBOARD =================
CREATE OR REPLACE VIEW v_latest_flow AS
SELECT DISTINCT ON (f.point_id)
       f.point_id, p.corridor, p.name, p.latitude, p.longitude, f.observed_at,
       f.current_speed, f.free_flow_speed, f.congestion_index, f.congestion_level,
       f.delay_seconds, f.source
FROM traffic_flow f
JOIN monitoring_points p USING (point_id)
ORDER BY f.point_id, f.observed_at DESC;

-- Insiden dianggap aktif jika masih terlihat pada polling 15 menit terakhir
CREATE OR REPLACE VIEW v_active_incidents AS
SELECT * FROM traffic_incidents
WHERE last_seen_at > now() - interval '15 minutes';
