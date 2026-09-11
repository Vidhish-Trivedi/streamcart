from __future__ import annotations

import json
from dataclasses import dataclass

import requests
from confluent_kafka import KafkaError, Producer

from streamcart.avro_codec import encode_confluent, parse_schema
from streamcart.config import TOPICS, get_settings
from streamcart.logging import log


@dataclass(frozen=True)
class AvroEncoder:
    schema: dict
    schema_id: int

    def encode(self, record: dict) -> bytes:
        return encode_confluent(record, self.schema, self.schema_id)


def _fetch_encoder(registry_url: str, subject: str) -> AvroEncoder:
    resp = requests.get(f"{registry_url}/subjects/{subject}/versions/latest", timeout=15)
    resp.raise_for_status()
    body = resp.json()
    return AvroEncoder(
        schema=parse_schema(json.loads(body["schema"])),
        schema_id=int(body["id"]),
    )


def build_producer() -> tuple[Producer, dict[str, AvroEncoder]]:
    settings = get_settings()
    registry_url = settings.schema_registry_url.rstrip("/")
    encoders = {
        kind: _fetch_encoder(registry_url, f"{topic}-value") for kind, topic in TOPICS.items()
    }
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 8,
            "max.in.flight.requests.per.connection": 5,
            "linger.ms": 20,
            "client.id": "streamcart-generator",
        }
    )
    log("info", "producer_ready", schema_ids={k: v.schema_id for k, v in encoders.items()})
    return producer, encoders


def produce_event(producer: Producer, encoders: dict[str, AvroEncoder], event: dict) -> None:
    kind = event["kind"]
    topic = event["topic"]
    record = event["record"]
    payload = encoders[kind].encode(record)

    def on_delivery(err, msg):
        if err:
            code = err.code() if isinstance(err, KafkaError) else None
            log(
                "error",
                "produce_failed",
                topic=topic,
                error=str(err),
                code=code,
                event_id=record["event_id"],
            )
            return
        log(
            "info",
            "produced",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            event_id=record["event_id"],
            trace_id=record["trace_id"],
            kind=kind,
        )

    producer.produce(
        topic=topic,
        key=event["key"].encode("utf-8"),
        value=payload,
        on_delivery=on_delivery,
        headers=[("trace_id", record["trace_id"].encode("utf-8"))],
    )


def produce_poison(producer: Producer, topic: str) -> None:
    """Invalid Confluent-Avro payload so silver/DLQ paths are exercised."""

    def on_delivery(err, msg):
        if err:
            log("error", "poison_failed", topic=topic, error=str(err))
            return
        log(
            "warn",
            "poison_produced",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )

    producer.produce(
        topic=topic,
        key=b"poison",
        value=b"\x00\x00\x00\x00\x01not-avro",
        on_delivery=on_delivery,
        headers=[("trace_id", b"poison")],
    )
