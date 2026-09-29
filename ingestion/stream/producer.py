"""Producer (EXTRACT stream): polling TomTom Traffic API -> Kafka.

Tiap siklus polling:
  1. Flow Segment Data untuk setiap titik pantau  -> topic traffic.flow.raw  (key = point_id)
  2. Incident Details untuk bounding box kota     -> topic traffic.incidents.raw (key = incident id)

Kuota gratis TomTom = 2.500 request/hari. Dengan 9 titik + 1 request insiden = 10 request
per siklus, interval default 360 detik -> 2.400 request/hari.

Jalankan:  TRAFFIC_SOURCE=tomtom TOMTOM_API_KEY=xxx python -m ingestion.stream.producer
           TRAFFIC_SOURCE=simulator python -m ingestion.stream.producer   (tanpa key, data sintetis)
"""

import json
import logging
import signal
import sys
import threading
import time

from confluent_kafka import KafkaError, Message, Producer

from common.config import env, env_int, kafka_config
from common.points import CITY_BBOX, MONITORING_POINTS, MonitoringPoint
from ingestion.stream.simulator import SimulatedTrafficClient
from ingestion.stream.tomtom_client import TomTomAuthError, TomTomClient, TomTomError

log = logging.getLogger("producer")

DEFAULT_POLL_SECONDS = 360
FLUSH_TIMEOUT_SECONDS = 10
EXIT_AUTH_ERROR = 2
EXIT_CONFIG_ERROR = 3


def build_client(source: str):
    if source == "tomtom":
        return TomTomClient(env("TOMTOM_API_KEY", required=True))
    if source == "simulator":
        log.warning("TRAFFIC_SOURCE=simulator -> data SINTETIS, hanya untuk testing")
        return SimulatedTrafficClient()
    raise RuntimeError(f"TRAFFIC_SOURCE tidak dikenal: {source!r} (pilih tomtom / simulator)")


def now_ms() -> int:
    return int(time.time() * 1000)


def envelope(kind: str, source: str, payload: dict, observed_at: int, **extra) -> bytes:
    """Data mentah API dibungkus metadata ingestion, isinya tidak diubah."""
    body = {"kind": kind, "source": source, "observed_at": observed_at,
            "ingested_at": now_ms(), **extra, "payload": payload}
    return json.dumps(body, separators=(",", ":")).encode()


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        log.error("Gagal kirim ke Kafka (%s): %s", msg.topic(), err)


def publish(producer: Producer, topic: str, key: str, value: bytes) -> None:
    try:
        producer.produce(topic, key=key.encode(), value=value, on_delivery=_on_delivery)
    except BufferError:
        producer.poll(1)
        producer.produce(topic, key=key.encode(), value=value, on_delivery=_on_delivery)
    producer.poll(0)


def poll_flows(client, producer: Producer, topic: str, source: str) -> int:
    sent = 0
    observed_at = now_ms()
    for point in MONITORING_POINTS:
        try:
            payload = client.flow(point.latitude, point.longitude)
        except TomTomAuthError:
            raise
        except TomTomError as exc:
            log.warning("Flow %s gagal: %s", point.point_id, exc)
            continue
        publish(producer, topic, point.point_id,
                envelope("flow", source, payload, observed_at, point_id=point.point_id))
        sent += 1
    return sent


def poll_incidents(client, producer: Producer, topic: str, source: str) -> int:
    try:
        incidents = client.incidents(CITY_BBOX)
    except TomTomAuthError:
        raise
    except TomTomError as exc:
        log.warning("Incident gagal: %s", exc)
        return 0
    observed_at = now_ms()
    for incident in incidents:
        key = str(incident.get("properties", {}).get("id", "unknown"))
        publish(producer, topic, key, envelope("incident", source, incident, observed_at))
    return len(incidents)


def run(client, producer: Producer, source: str, poll_seconds: int, stop: threading.Event):
    kafka = kafka_config()
    while not stop.is_set():
        started = time.monotonic()
        flows = poll_flows(client, producer, kafka.flow_topic, source)
        incidents = poll_incidents(client, producer, kafka.incident_topic, source)
        producer.flush(FLUSH_TIMEOUT_SECONDS)
        log.info("Siklus polling: %d/%d titik flow, %d insiden aktif",
                 flows, len(MONITORING_POINTS), incidents)
        stop.wait(max(0, poll_seconds - (time.monotonic() - started)))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    source = env("TRAFFIC_SOURCE", "tomtom")
    poll_seconds = env_int("POLL_SECONDS", DEFAULT_POLL_SECONDS)
    try:
        client = build_client(source)
    except (RuntimeError, ValueError) as exc:
        log.error("%s. Isi TOMTOM_API_KEY di infrastructure/.env (daftar gratis di "
                  "https://developer.tomtom.com), atau set TRAFFIC_SOURCE=simulator untuk testing.",
                  exc)
        sys.exit(EXIT_CONFIG_ERROR)
    producer = Producer({
        "bootstrap.servers": kafka_config().bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "client.id": f"producer-{source}",
    })
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    log.info("Producer %s: %d titik pantau, interval %ds", source, len(MONITORING_POINTS), poll_seconds)
    try:
        run(client, producer, source, poll_seconds, stop)
    except TomTomAuthError as exc:
        log.error("%s - periksa TOMTOM_API_KEY di infrastructure/.env", exc)
        sys.exit(EXIT_AUTH_ERROR)
    finally:
        producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
