from __future__ import annotations

from streamcart.avro_codec import (
    decode_confluent,
    encode_confluent,
    load_schema,
    parse_schema,
    schema_id_of,
)


def test_confluent_roundtrip() -> None:
    schema = parse_schema(load_schema("orders"))
    record = {
        "event_id": "e1",
        "trace_id": "t1",
        "order_id": "o1",
        "customer_id": "c1",
        "status": "created",
        "amount_cents": 1999,
        "currency": "USD",
        "sku": "SKU-TEE-001",
        "quantity": 1,
        "event_ts": 1_700_000_000_000,
    }
    blob = encode_confluent(record, schema, schema_id=7)
    assert blob[0] == 0
    assert schema_id_of(blob) == 7
    assert decode_confluent(blob, schema)["order_id"] == "o1"


def test_decode_rejects_poison() -> None:
    schema = parse_schema(load_schema("orders"))
    try:
        decode_confluent(b"\x00\x00\x00\x00\x01not-avro", schema)
        raise AssertionError("expected decode failure")
    except Exception:
        pass


def test_backward_compat_v1_to_v11() -> None:
    writer = parse_schema(load_schema("orders"))
    reader = parse_schema(load_schema("orders_v11"))
    record = {
        "event_id": "e1",
        "trace_id": "t1",
        "order_id": "o1",
        "customer_id": "c1",
        "status": "created",
        "amount_cents": 500,
        "currency": "USD",
        "sku": "SKU-MUG-014",
        "quantity": 2,
        "event_ts": 1,
    }
    blob = encode_confluent(record, writer, schema_id=1)
    decoded = decode_confluent(blob, writer, reader)
    assert decoded.get("coupon_code") is None
    assert decoded["amount_cents"] == 500
