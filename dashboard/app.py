"""Dashboard Smart City Metropolia: lalu lintas real-time, insiden, lingkungan, prediksi."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

from common.config import postgres_config
from common.points import CITY, CITY_LATITUDE, CITY_LONGITUDE, TIMEZONE

st.set_page_config(page_title="Metropolia Traffic", layout="wide")

LEVEL_COLORS = {"LANCAR": "#2e9e5b", "PADAT": "#f0a202", "MACET": "#d7263d", "TUTUP": "#5c0a14"}
MAP_ZOOM = 11.3

# Batas waktu data, untuk menentukan posisi slider "jelajah waktu"
BOUNDS_SQL = text("SELECT min(observed_at) AS t_min, max(observed_at) AS t_max FROM traffic_flow")

# Semua query di bawah memakai :t_ref (waktu acuan), BUKAN now().
# Alasannya: data historis tetap bisa ditampilkan kapan pun dashboard dibuka,
# dan waktu acuan bisa digeser untuk menelusuri kondisi kota pada jam tertentu.
LATEST_SQL = text("""
SELECT DISTINCT ON (f.point_id)
       f.point_id, p.corridor, p.name, p.latitude, p.longitude, f.observed_at,
       f.current_speed, f.free_flow_speed, f.congestion_index, f.congestion_level,
       f.delay_seconds, f.source
FROM traffic_flow f JOIN monitoring_points p USING (point_id)
WHERE f.observed_at <= :t_ref
ORDER BY f.point_id, f.observed_at DESC
""")
TREND_SQL = text("""
SELECT f.observed_at, p.name, p.corridor, f.congestion_index, f.current_speed
FROM traffic_flow f JOIN monitoring_points p USING (point_id)
WHERE f.observed_at > :t_ref - make_interval(hours => :hours)
  AND f.observed_at <= :t_ref
ORDER BY f.observed_at
""")
INCIDENTS_SQL = text("""
SELECT category_label AS kategori, magnitude_label AS tingkat, description AS deskripsi,
       road_from AS dari, road_to AS ke, round((delay_seconds / 60.0)::numeric, 1) AS tundaan_menit,
       latitude, longitude, first_seen_at, last_seen_at, source
FROM traffic_incidents
WHERE last_seen_at > :t_ref - interval '15 minutes' AND first_seen_at <= :t_ref
ORDER BY magnitude DESC, delay_seconds DESC NULLS LAST
""")
INCIDENT_STATS_SQL = text("""
SELECT category_label AS kategori, count(*) AS jumlah
FROM traffic_incidents
WHERE first_seen_at > :t_ref - interval '7 days' AND first_seen_at <= :t_ref
GROUP BY 1 ORDER BY 2 DESC
""")
ENVIRONMENT_SQL = text("""
SELECT observed_hour, w.temperature, w.precipitation, w.wind_speed,
       a.pm2_5, a.pm10, a.us_aqi, COALESCE(w.is_forecast, a.is_forecast) AS is_forecast
FROM weather_hourly w FULL JOIN air_quality_hourly a USING (city, observed_hour)
WHERE city = :city
  AND observed_hour BETWEEN :t_ref - interval '48 hours' AND :t_ref + interval '24 hours'
ORDER BY observed_hour
""")
CORRELATION_SQL = text("""
SELECT hour, avg(avg_congestion) AS kemacetan, max(pm2_5) AS pm2_5, max(precipitation) AS hujan
FROM mart_traffic_features GROUP BY hour HAVING max(pm2_5) IS NOT NULL
""")
FORECAST_SQL = text("""
SELECT f.target_hour, p.name, p.corridor, f.predicted_congestion, f.predicted_level
FROM traffic_forecast f JOIN monitoring_points p USING (point_id)
WHERE f.target_hour >= date_trunc('hour', :t_ref)
ORDER BY f.target_hour
""")
MODEL_SQL = text("SELECT * FROM model_runs ORDER BY trained_at DESC LIMIT 1")


@st.cache_resource
def get_engine():
    return create_engine(postgres_config().sqlalchemy_url, pool_pre_ping=True)


def query(sql, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def to_wib(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(TIMEZONE)


def render_kpis(latest: pd.DataFrame, incidents: pd.DataFrame, env: pd.DataFrame) -> None:
    actual = env[~env["is_forecast"].fillna(False)] if not env.empty else env
    last_env = actual.iloc[-1] if not actual.empty else None
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Indeks kemacetan kota", f"{latest['congestion_index'].mean():.2f}")
    c2.metric("Titik macet", f"{(latest['congestion_level'].isin(['MACET', 'TUTUP'])).sum()} / {len(latest)}")
    c3.metric("Insiden aktif", len(incidents))
    c4.metric("US AQI", "-" if last_env is None or pd.isna(last_env.us_aqi) else f"{last_env.us_aqi:.0f}")
    c5.metric("Suhu / hujan", "-" if last_env is None else
              f"{last_env.temperature:.1f}°C / {last_env.precipitation or 0:.1f} mm")


def congestion_map(latest: pd.DataFrame, incidents: pd.DataFrame) -> go.Figure:
    fig = px.scatter_map(
        latest, lat="latitude", lon="longitude", color="congestion_level",
        color_discrete_map=LEVEL_COLORS, hover_name="name",
        hover_data={"corridor": True, "current_speed": ":.0f", "free_flow_speed": ":.0f",
                    "congestion_index": ":.2f", "latitude": False, "longitude": False},
        zoom=MAP_ZOOM, center={"lat": CITY_LATITUDE, "lon": CITY_LONGITUDE},
        map_style="open-street-map", height=520,
    )
    fig.update_traces(marker={"size": 18})
    if not incidents.empty:
        fig.add_trace(go.Scattermap(
            lat=incidents["latitude"], lon=incidents["longitude"], mode="markers",
            marker={"size": 11, "color": "#1b1b3a"}, name="Insiden",
            text=incidents["kategori"] + " - " + incidents["tingkat"],
        ))
    fig.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0}, legend_title_text="Status")
    return fig


def render_live(latest: pd.DataFrame, incidents: pd.DataFrame, hours: int, t_ref) -> None:
    left, right = st.columns([3, 2])
    left.plotly_chart(congestion_map(latest, incidents), use_container_width=True)
    table = latest.assign(observed_at=to_wib(latest["observed_at"]).dt.strftime("%H:%M"))[
        ["point_id", "name", "congestion_level", "current_speed", "free_flow_speed",
         "delay_seconds", "observed_at"]]
    right.dataframe(table, hide_index=True, use_container_width=True)

    trend = query(TREND_SQL, hours=hours, t_ref=t_ref)
    if not trend.empty:
        trend = trend.assign(observed_at=to_wib(trend["observed_at"]))
        corridor = st.selectbox("Koridor", sorted(trend["corridor"].unique()))
        fig = px.line(trend[trend["corridor"] == corridor], x="observed_at", y="congestion_index",
                      color="name", markers=True, range_y=[0, 1],
                      labels={"observed_at": "Waktu (WIB)", "congestion_index": "Indeks kemacetan"})
        st.plotly_chart(fig, use_container_width=True)


def render_incidents(incidents: pd.DataFrame, t_ref) -> None:
    left, right = st.columns([3, 2])
    left.subheader("Insiden aktif")
    if incidents.empty:
        left.info("Tidak ada insiden aktif pada waktu acuan ini. "
                  "Geser 'Jelajah waktu' di sidebar untuk melihat periode lain.")
    else:
        shown = incidents.assign(sejak=to_wib(incidents["first_seen_at"]).dt.strftime("%d/%m %H:%M"))
        left.dataframe(shown.drop(columns=["latitude", "longitude", "first_seen_at", "last_seen_at"]),
                       hide_index=True, use_container_width=True)
    right.subheader("Insiden 7 hari terakhir")
    stats = query(INCIDENT_STATS_SQL, t_ref=t_ref)
    if not stats.empty:
        right.plotly_chart(px.bar(stats, x="jumlah", y="kategori", orientation="h"),
                           use_container_width=True)


def render_environment(env: pd.DataFrame) -> None:
    if env.empty:
        st.info("Belum ada data cuaca/kualitas udara. Tunggu DAG weather_air_quality_hourly berjalan.")
        return
    env = env.assign(observed_hour=to_wib(env["observed_hour"]),
                     jenis=env["is_forecast"].map({True: "Prakiraan", False: "Aktual"}))
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.line(env, x="observed_hour", y=["pm2_5", "pm10"], line_dash="jenis",
                            title="Partikulat (µg/m³)"), use_container_width=True)
    c2.plotly_chart(px.bar(env, x="observed_hour", y="precipitation", color="jenis",
                           title="Curah hujan (mm)"), use_container_width=True)
    corr = query(CORRELATION_SQL)
    if len(corr) >= 3:
        st.plotly_chart(px.scatter(corr, x="pm2_5", y="kemacetan", size=corr["hujan"].fillna(0) + 1,
                                   title="Kemacetan vs PM2.5 per jam (ukuran = hujan)",
                                   trendline=None), use_container_width=True)


def render_forecast(t_ref) -> None:
    model = query(MODEL_SQL)
    forecast = query(FORECAST_SQL, t_ref=t_ref)
    if model.empty or forecast.empty:
        st.info("Model belum dilatih. DAG congestion_model_daily berjalan tiap hari 00.30 WIB "
                "setelah data historis cukup.")
        return
    run = model.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Versi model", run.model_version)
    c2.metric("MAE model", f"{run.mae:.3f}")
    c3.metric("MAE baseline", f"{run.baseline_mae:.3f}",
              delta=f"{run.baseline_mae - run.mae:+.3f} lebih baik", delta_color="normal")
    forecast = forecast.assign(target_hour=to_wib(forecast["target_hour"]))
    fig = px.line(forecast, x="target_hour", y="predicted_congestion", color="name",
                  facet_row="corridor", height=720, range_y=[0, 1],
                  labels={"target_hour": "Jam (WIB)", "predicted_congestion": "Prediksi"})
    st.plotly_chart(fig, use_container_width=True)


st.title("Metropolia Smart Traffic Monitor")
st.caption(f"Representasi kota: {CITY} · TomTom Traffic → Kafka → PostgreSQL · "
           "Open-Meteo → Airflow → PostgreSQL")

@st.cache_data(ttl=30, show_spinner=False)
def data_bounds() -> tuple:
    """Waktu paling awal & paling akhir yang ada di traffic_flow."""
    with get_engine().connect() as conn:
        row = pd.read_sql(BOUNDS_SQL, conn).iloc[0]
    return row["t_min"], row["t_max"]


with st.sidebar:
    st.subheader("Waktu acuan")
    mode = st.radio(
        "Dasar perhitungan 'sekarang'",
        ["Data terbaru", "Jelajah waktu", "Jam dinding"],
        help="Data terbaru: ikuti baris paling akhir di database — dashboard tetap terisi "
             "walau pengumpulan data sudah berhenti.\n\n"
             "Jelajah waktu: geser untuk melihat kondisi kota pada jam tertentu.\n\n"
             "Jam dinding: pakai waktu nyata — hanya cocok saat pipeline sedang berjalan.",
    )

    try:
        t_min, t_max = data_bounds()
    except Exception:
        t_min = t_max = None

    if t_min is None or pd.isna(t_max):
        t_ref = pd.Timestamp.now(tz="UTC")
        st.warning("Database belum berisi data lalu lintas.")
    elif mode == "Jam dinding":
        t_ref = pd.Timestamp.now(tz="UTC")
    elif mode == "Jelajah waktu":
        lo, hi = (pd.Timestamp(t).tz_convert(TIMEZONE).to_pydatetime() for t in (t_min, t_max))
        dipilih = pd.Timestamp(
            st.slider("Posisi waktu (WIB)", min_value=lo, max_value=hi, value=hi,
                      step=pd.Timedelta(minutes=30).to_pytimedelta(), format="DD/MM HH:mm")
        )
        # Streamlit kadang mengembalikan datetime tanpa zona waktu -> pasang WIB dulu
        if dipilih.tzinfo is None:
            dipilih = dipilih.tz_localize(TIMEZONE)
        t_ref = dipilih.tz_convert("UTC")
    else:
        t_ref = pd.Timestamp(t_max).tz_convert("UTC")

    if t_min is not None and not pd.isna(t_max):
        st.caption(f"Data tersedia {pd.Timestamp(t_min).tz_convert(TIMEZONE):%d/%m %H:%M} – "
                   f"{pd.Timestamp(t_max).tz_convert(TIMEZONE):%d/%m %H:%M} WIB")

    st.divider()
    trend_hours = st.slider("Rentang tren (jam)", 1, 48, 6)
    refresh_seconds = st.slider("Auto-refresh (detik)", 10, 120, 30)


NO_TRAFFIC_HELP = """
**Belum ada data lalu lintas.** Jika sudah lebih dari ±1 menit setelah `make up`, kemungkinan besar
producer berhenti. Cek dengan `make logs s=producer`. Penyebab umum:

- `TOMTOM_API_KEY` di `infrastructure/.env` masih kosong atau salah → isi key dari
  https://developer.tomtom.com, lalu jalankan `make up` lagi.
- Hanya ingin mencoba tanpa key → set `TRAFFIC_SOURCE=simulator` (data sintetis), lalu `make up`.

Tab lain (lingkungan, insiden, prediksi) tetap bisa dilihat di bawah.
"""


@st.fragment(run_every=refresh_seconds)
def live_view() -> None:
    try:
        latest = query(LATEST_SQL, t_ref=t_ref)
        incidents = query(INCIDENTS_SQL, t_ref=t_ref)
        env = query(ENVIRONMENT_SQL, city=CITY, t_ref=t_ref)
        if latest.empty:
            st.warning(NO_TRAFFIC_HELP)
        else:
            if (latest["source"] == "simulator").any():
                st.warning("Sebagian data berasal dari SIMULATOR (bukan TomTom). Isi TOMTOM_API_KEY "
                           "dan set TRAFFIC_SOURCE=tomtom untuk data real.")
            render_kpis(latest, incidents, env)
        tabs = st.tabs(["Lalu lintas live", "Insiden", "Lingkungan", "Prediksi"])
        with tabs[0]:
            if latest.empty:
                st.info("Belum ada data lalu lintas sampai waktu acuan ini. "
                        "Geser waktu acuan di sidebar atau tunggu producer & processor berjalan.")
            else:
                render_live(latest, incidents, trend_hours, t_ref)
        with tabs[1]:
            render_incidents(incidents, t_ref)
        with tabs[2]:
            render_environment(env)
        with tabs[3]:
            render_forecast(t_ref)
    except Exception as exc:  # koneksi DB putus dsb. -> tampilkan, jangan crash
        st.error(f"Gagal membaca database: {exc}")


live_view()
