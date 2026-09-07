from __future__ import annotations

import json
from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas" / "avro"


SCHEMA_FILES = [
    "order.avsc",
    "order_v1_1.avsc",
    "payment.avsc",
    "inventory.avsc",
    "clickstream.avsc",
]


def test_required_schemas_exist() -> None:
    for name in SCHEMA_FILES:
        path = SCHEMA_DIR / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["type"] == "record"
        fields = {f["name"] for f in payload["fields"]}
        assert "event_id" in fields
        assert "event_ts" in fields


def test_order_v11_adds_optional_coupon() -> None:
    v1 = {f["name"] for f in json.loads((SCHEMA_DIR / "order.avsc").read_text())["fields"]}
    v11 = json.loads((SCHEMA_DIR / "order_v1_1.avsc").read_text())
    coupon = next(f for f in v11["fields"] if f["name"] == "coupon_code")
    assert "coupon_code" not in v1
    assert coupon["default"] is None
