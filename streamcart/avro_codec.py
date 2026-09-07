"""Confluent wire-format Avro helpers (magic byte + schema id + payload)."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import fastavro

MAGIC = 0
HEADER_LEN = 5

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas" / "avro"

SCHEMA_FILES = {
    "orders": "order.avsc",
    "orders_v11": "order_v1_1.avsc",
    "payments": "payment.avsc",
    "inventory": "inventory.avsc",
    "clickstream": "clickstream.avsc",
}


def load_schema(name: str) -> dict[str, Any]:
    path = SCHEMA_DIR / SCHEMA_FILES[name]
    return json.loads(path.read_text(encoding="utf-8"))


def parse_schema(raw: dict[str, Any]) -> dict[str, Any]:
    return fastavro.parse_schema(raw)


def encode_confluent(record: dict[str, Any], schema: dict[str, Any], schema_id: int) -> bytes:
    buf = io.BytesIO()
    buf.write(bytes([MAGIC]))
    buf.write(schema_id.to_bytes(4, "big"))
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def decode_confluent(
    value: bytes,
    writer_schema: dict[str, Any],
    reader_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not value or value[0] != MAGIC:
        raise ValueError("not_confluent_avro")
    if len(value) < HEADER_LEN:
        raise ValueError("truncated_confluent_header")
    payload = io.BytesIO(value[HEADER_LEN:])
    return fastavro.schemaless_reader(payload, writer_schema, reader_schema)


def schema_id_of(value: bytes) -> int:
    if not value or len(value) < HEADER_LEN or value[0] != MAGIC:
        raise ValueError("not_confluent_avro")
    return int.from_bytes(value[1:5], "big")
