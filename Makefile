COMPOSE := docker compose -f infrastructure/docker-compose.yml
PYTHON  := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
d ?= traffic_hourly_mart

.PHONY: env venv up down clean ps logs test dag-test replay replay-reset load-batch

env:            ## buat infrastructure/.env dari template (sekali saja)
	@test -f infrastructure/.env || cp infrastructure/.env.example infrastructure/.env
	@echo "Isi TOMTOM_API_KEY & password di infrastructure/.env lalu jalankan: make up"

venv:           ## virtualenv lokal untuk menjalankan test
	python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

up:             ## jalankan seluruh stack
	$(COMPOSE) up -d --build

down:           ## stop (data tetap ada)
	$(COMPOSE) down

clean:          ## stop + hapus semua data (Kafka, PostgreSQL, model)
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:           ## contoh: make logs s=producer
	$(COMPOSE) logs -f --tail 50 $(s)

test:           ## unit test + coverage
	$(PYTHON) -m pytest --cov=ingestion --cov=processing --cov=modeling --cov-report=term

dag-test:       ## jalankan satu DAG sekali untuk jam berjalan, contoh: make dag-test d=weather_air_quality_hourly
	$(COMPOSE) exec airflow bash -c 'airflow dags test $(d) "$$(date -u -d "+1 hour" +%Y-%m-%dT%H:00:00+00:00)"'

replay:         ## kirim satu jendela 6 menit dari data/raw/traffic_flow.csv ke Kafka (w=3 untuk 3 jendela)
	$(PYTHON) -m ingestion.stream.csv_replay $(if $(w),--windows $(w),)

replay-reset:   ## mulai pemutaran CSV lagi dari awal
	$(PYTHON) -m ingestion.stream.csv_replay --reset --windows 1

load-batch:     ## muat weather_hourly.csv & kualitas udara dari CSV ke PostgreSQL
	$(PYTHON) -m ingestion.batch.csv_batch
