"""Stream processor: consume Kafka -> TRANSFORM -> LOAD ke PostgreSQL (micro-batch).

Semantik: at-least-once dari Kafka + load idempotent = hasil akhir tanpa duplikat.
Offset di-commit SETELAH transaksi database berhasil. Jika database gagal, proses
berhenti (exit != 0) dan Docker me-restart-nya dari offset terakhir yang di-commit.
"""

import json
import logging
import signal
from dataclasses import dataclass

import psycopg2
from confluent_kafka import Consumer, KafkaException, Message, Producer

from common.config import env_int, kafka_config, postgres_config
from common.points import MONITORING_POINTS
from processing.stream.incident_transform import Incident
from processing.stream.traffic_transform import TrafficReading
from processing.stream.transform import transform_message
from processing.stream.validation import InvalidRecord
from storage.stream_store import insert_readings, upsert_incidents, upsert_points

log = logging.getLogger("processor")


@dataclass(frozen=True)
class BatchResult:
    received: int
    readings_loaded: int
    incidents_new: int
    incidents_updated: int
    rejected: int


class Shutdown:
    requested = False

    def request(self, *_):
        log.info("Sinyal shutdown diterima")
        self.requested = True


def send_to_dlq(dlq: Producer, topic: str, msg: Message, reason: str) -> None:
    record = {
        "reason": reason,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "raw": (msg.value() or b"").decode("utf-8", errors="replace"),
    }
    dlq.produce(topic, key=msg.key(), value=json.dumps(record).encode())


def split_records(messages: list[Message], dlq: Producer, dlq_topic: str):
    readings, incidents, rejected = [], [], 0
    for msg in messages:
        if msg.error():
            raise KafkaException(msg.error())
        try:
            record = transform_message(msg.value())
        except InvalidRecord as exc:
            rejected += 1
            log.warning("Record ditolak (%s@%s): %s", msg.topic(), msg.offset(), exc)
            send_to_dlq(dlq, dlq_topic, msg, str(exc))
            continue
        if isinstance(record, TrafficReading):
            readings.append(record)
        elif isinstance(record, Incident):
            incidents.append(record)
    dlq.flush()
    return readings, incidents, rejected


def process_batch(messages, conn, dlq: Producer, dlq_topic: str) -> BatchResult:
    readings, incidents, rejected = split_records(messages, dlq, dlq_topic)
    try:
        loaded = insert_readings(conn, readings)
        new, updated = upsert_incidents(conn, incidents)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return BatchResult(len(messages), loaded, new, updated, rejected)


def run(consumer: Consumer, conn, dlq: Producer, dlq_topic: str, shutdown: Shutdown):
    batch_size = env_int("BATCH_SIZE", 200)
    batch_timeout = env_int("BATCH_TIMEOUT_MS", 2000) / 1000
    while not shutdown.requested:
        messages = consumer.consume(num_messages=batch_size, timeout=batch_timeout)
        if not messages:
            continue
        result = process_batch(messages, conn, dlq, dlq_topic)
        consumer.commit(asynchronous=False)
        log.info(
            "batch: diterima=%d flow_dimuat=%d insiden_baru=%d insiden_update=%d ditolak=%d",
            result.received, result.readings_loaded, result.incidents_new,
            result.incidents_updated, result.rejected,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    kafka = kafka_config()
    shutdown = Shutdown()
    signal.signal(signal.SIGINT, shutdown.request)
    signal.signal(signal.SIGTERM, shutdown.request)

    conn = psycopg2.connect(postgres_config().dsn)
    upsert_points(conn, MONITORING_POINTS)
    conn.commit()

    consumer = Consumer({
        "bootstrap.servers": kafka.bootstrap,
        "group.id": kafka.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    dlq = Producer({"bootstrap.servers": kafka.bootstrap, "client.id": "processor-dlq"})
    consumer.subscribe([kafka.flow_topic, kafka.incident_topic])
    log.info("Processor berjalan: %s, %s -> PostgreSQL", kafka.flow_topic, kafka.incident_topic)
    try:
        run(consumer, conn, dlq, kafka.dlq_topic, shutdown)
    finally:
        consumer.close()
        conn.close()
        log.info("Processor berhenti")


if __name__ == "__main__":
    main()
