from __future__ import annotations

from pathlib import Path

from confluent_kafka import KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streamcart.config import get_settings
from streamcart.logging import log

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas" / "avro"

KIND_SCHEMA = {
    "orders": SCHEMA_DIR / "order.avsc",
    "payments": SCHEMA_DIR / "payment.avsc",
    "inventory": SCHEMA_DIR / "inventory.avsc",
    "clickstream": SCHEMA_DIR / "clickstream.avsc",
}


def build_producer() -> tuple[Producer, dict[str, AvroSerializer]]:
    settings = get_settings()
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    serializers = {
        kind: AvroSerializer(
            registry,
            path.read_text(encoding="utf-8"),
            conf={"auto.register.schemas": False},
        )
        for kind, path in KIND_SCHEMA.items()
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
    return producer, serializers


def produce_event(producer: Producer, serializers: dict[str, AvroSerializer], event: dict) -> None:
    kind = event["kind"]
    topic = event["topic"]
    record = event["record"]
    payload = serializers[kind](record, SerializationContext(topic, MessageField.VALUE))

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
