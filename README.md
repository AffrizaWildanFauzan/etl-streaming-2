# Metropolia Smart Traffic — Sistem Analisis Big Data Smart City

Tugas Besar Analisis Big Data (Smart City Planning) — Kelompok 2, ITS.

**Kasus:** kota *Metropolia* (±750.000 penduduk) mengalami kemacetan parah pada jam sibuk.
Pemerintah kota ingin memperlancar arus lalu lintas, menurunkan polusi, dan meningkatkan
keselamatan berbasis data. Sistem ini memantau **3 koridor prioritas × 3 persimpangan kritis**
secara real-time, menggabungkannya dengan data cuaca & kualitas udara, lalu memprediksi
kemacetan 24 jam ke depan.

Metropolia direpresentasikan oleh **Surabaya** (koordinat nyata) agar seluruh data berasal
dari sumber asli.

## Arsitektur

```
                       ETL STREAMING (Apache Kafka)
┌──────────────────────┐   poll /6 mnt   ┌───────────────────────┐    ┌─────────────────────┐
│ TomTom Traffic API   │───────────────▶│ traffic.flow.raw      │───▶│ stream processor    │
│ • Flow (9 titik)     │   producer      │ traffic.incidents.raw │    │ validasi, metrik    │──┐
│ • Incidents (bbox)   │                 └───────────────────────┘    │ kemacetan, dedup    │  │
└──────────────────────┘                    traffic.dlq ◀─────────────┴─────────────────────┘  │
                                                                                               ▼
                       ETL BATCH (Apache Airflow, terjadwal)                        ┌────────────────┐
┌──────────────────────┐  tiap jam :10  ┌────────────────────────────┐              │  PostgreSQL    │
│ Open-Meteo Weather   │──────────────▶│ weather_air_quality_hourly │─────────────▶│  traffic_flow  │
│ Open-Meteo Air Qual. │                └────────────────────────────┘              │  incidents     │
└──────────────────────┘                ┌────────────────────────────┐              │  weather/AQ    │
                          tiap jam :05  │ traffic_hourly_mart        │◀────────────▶│  traffic_hourly│
                                        └────────────────────────────┘              │  forecast      │
                          harian 00.30  ┌────────────────────────────┐              └───────┬────────┘
                          WIB           │ congestion_model_daily (ML)│◀────────────▶        │
                                        └────────────────────────────┘                      ▼
                                                                                 Dashboard Streamlit
```

| Folder | Isi |
|---|---|
| [ingestion/stream/](ingestion/stream/) | Producer Kafka + client TomTom (flow & incidents) + simulator (testing) |
| [ingestion/batch/](ingestion/batch/) | Extract Open-Meteo (cuaca & kualitas udara) |
| [processing/stream/](processing/stream/) | Validasi, transform, metrik kemacetan, stream processor |
| [processing/batch/](processing/batch/) | Transform & validasi data Open-Meteo |
| [storage/](storage/) | Loader PostgreSQL + skema ([storage/schema/](storage/schema/)) |
| [modeling/](modeling/) | Model prediksi kemacetan (scikit-learn) |
| [dags/](dags/) | 3 DAG Airflow terjadwal |
| [dashboard/](dashboard/) | Dashboard Streamlit |
| [infrastructure/](infrastructure/) | Docker Compose, Dockerfile, template `.env` |
| [common/](common/) | Konfigurasi & daftar titik pantau |

## ETL Streaming (Kafka)

**Extract** — [ingestion/stream/producer.py](ingestion/stream/producer.py) memanggil TomTom tiap 6 menit:
- *Flow Segment Data* untuk 9 titik pantau → kecepatan saat ini, kecepatan arus bebas, waktu tempuh.
- *Incident Details* untuk bounding box kota → kecelakaan, kemacetan, penutupan & perbaikan jalan.
- Data mentah dibungkus envelope `{kind, source, observed_at, ingested_at, payload}` dan dikirim ke Kafka
  (key = `point_id` / id insiden). Kuota gratis 2.500 request/hari: 10 request × 240 siklus = 2.400/hari.

**Transform** — [processing/stream/](processing/stream/):
- Validasi (field wajib, rentang kecepatan 0–200 km/jam, confidence 0–1, titik dikenal, waktu tidak di masa depan).
  Record tidak valid → topic `traffic.dlq` beserta alasannya.
- Enrichment: `speed_ratio = current/free_flow`, `congestion_index = 1 − speed_ratio`,
  `delay_seconds`, status **LANCAR** (≥ 0,8) / **PADAT** (0,5–0,8) / **MACET** (< 0,5) / **TUTUP**.
- Insiden dinormalisasi (kategori & tingkat keparahan dalam bahasa Indonesia, koordinat).

**Load** — [storage/stream_store.py](storage/stream_store.py): `traffic_flow` (insert idempotent),
`traffic_incidents` (upsert — insiden yang sama diperbarui selama masih aktif). Offset Kafka di-commit
setelah transaksi database sukses → *at-least-once + idempotent* = tanpa duplikat.

## ETL Batch (Airflow, terjadwal — bukan manual trigger)

| DAG | Jadwal | Isi |
|---|---|---|
| `weather_air_quality_hourly` | tiap jam, menit 10 | Extract Open-Meteo (2 hari lalu + 2 hari prakiraan) → validasi rentang fisik → upsert `weather_hourly` & `air_quality_hourly` → cek kesegaran data |
| `traffic_hourly_mart` | tiap jam, menit 5 | Cek streaming hidup → agregasi `traffic_flow` → `traffic_hourly` → cek rentang nilai & cakupan titik |
| `congestion_model_daily` | harian 00.30 WIB | Latih ulang model → simpan metrik ke `model_runs` → prakiraan 24 jam ke `traffic_forecast` |

View `mart_traffic_features` menggabungkan lalu lintas + cuaca + kualitas udara per jam (dataset siap model).

## Model prediktif

[modeling/congestion_model.py](modeling/congestion_model.py) — `HistGradientBoostingRegressor` memprediksi
indeks kemacetan per titik per jam dari: titik pantau, jam (WIB), hari, akhir pekan, suhu, kelembapan,
curah hujan, dan angin. Evaluasi memakai **time-series split** (20% data terbaru sebagai test, mencegah
data leakage) dan dibandingkan dengan baseline rata-rata historis per (titik, jam). Training otomatis
di-skip sampai data historis di `traffic_hourly` mencapai minimal 48 baris.

## Dashboard

http://localhost:8501 — KPI kota, peta kemacetan & insiden, tren per koridor, cuaca & kualitas udara
(aktual + prakiraan), korelasi kemacetan–PM2.5, serta prediksi kemacetan 24 jam dan performa model.

## Cara menjalankan

Prasyarat: Docker Desktop.

1. Daftar API key gratis di https://developer.tomtom.com (menu *Dashboard → Keys*).
2. Konfigurasi dan jalankan:

```bash
make env     # membuat infrastructure/.env → isi TOMTOM_API_KEY, POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD
make up      # build & jalankan seluruh stack
```

| Layanan | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| Airflow (user `admin`, password dari `.env`) | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| PostgreSQL | `localhost:5434` (db `metropolia`) |

```bash
make ps                                     # status service
make logs s=producer                        # log satu service
make dag-test d=weather_air_quality_hourly  # jalankan satu DAG sekali (debug)
make down                                   # stop (data tetap ada)
make clean                                  # stop + hapus semua data
```

> Tanpa API key, set `TRAFFIC_SOURCE=simulator` untuk menguji pipeline dengan data **sintetis**.
> Data simulator ditandai `source = 'simulator'` di database dan dashboard menampilkan peringatan.
> Untuk laporan/demo gunakan `TRAFFIC_SOURCE=tomtom`.

## Pengujian

```bash
make venv && make test
```

47 unit test: client TomTom (termasuk jaminan API key tidak bocor di pesan error), transform flow &
insiden, transform Open-Meteo, dan model (fitur waktu WIB, time-series split tanpa leakage, akurasi pada
pola jam sibuk, batas prediksi 0–1).

## Catatan data

- Data lalu lintas, cuaca, dan kualitas udara diambil **live**, sehingga **tidak ada modifikasi timestamp**.
  `observed_at` = waktu polling (TomTom tidak mengirim timestamp untuk data flow).
- Koordinat titik pantau adalah perkiraan persimpangan di Surabaya; TomTom mengembalikan ruas jalan terdekat.
- Kualitas udara Open-Meteo berasal dari model CAMS (resolusi ±11 km), sehingga satu nilai mewakili kota.

## Daftar pustaka (IEEE)

[1] TomTom International B.V., "Traffic Flow API – Flow Segment Data," TomTom Developer Portal. [Online]. Available: https://developer.tomtom.com/traffic-api/documentation/traffic-flow/flow-segment-data. [Accessed: Sep. 28, 2026].

[2] TomTom International B.V., "Traffic Incidents API – Incident Details," TomTom Developer Portal. [Online]. Available: https://developer.tomtom.com/traffic-api/documentation/traffic-incidents/incident-details. [Accessed: Sep. 28, 2026].

[3] P. Zippenfenig, "Open-Meteo.com Weather API," Zenodo, 2023, doi: 10.5281/zenodo.7970649. [Online]. Available: https://open-meteo.com/en/docs. [Accessed: Sep. 28, 2026].

[4] Open-Meteo, "Air Quality API," Open-Meteo.com. [Online]. Available: https://open-meteo.com/en/docs/air-quality-api. [Accessed: Sep. 28, 2026].

[5] Copernicus Atmosphere Monitoring Service (CAMS), "CAMS global atmospheric composition forecasts," ECMWF. [Online]. Available: https://atmosphere.copernicus.eu. [Accessed: Sep. 28, 2026].
