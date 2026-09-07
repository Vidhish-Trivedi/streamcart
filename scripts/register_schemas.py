from __future__ import annotations

import json
from pathlib import Path

import requests

from streamcart.logging import log

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas" / "avro"

SUBJECTS = [
    ("ecommerce.orders.v1-value", SCHEMA_DIR / "order.avsc"),
    ("ecommerce.payments.v1-value", SCHEMA_DIR / "payment.avsc"),
    ("ecommerce.inventory.v1-value", SCHEMA_DIR / "inventory.avsc"),
    ("ecommerce.clickstream.v1-value", SCHEMA_DIR / "clickstream.avsc"),
]


def register(url: str, subject: str, schema_text: str) -> int:
    resp = requests.post(
        f"{url}/subjects/{subject}/versions",
        json={"schema": schema_text, "schemaType": "AVRO"},
        timeout=15,
    )
    resp.raise_for_status()
    schema_id = int(resp.json()["id"])
    log("info", "schema_registered", subject=subject, id=schema_id)
    return schema_id


def set_compatibility(url: str, subject: str, level: str = "BACKWARD") -> None:
    resp = requests.put(
        f"{url}/config/{subject}",
        json={"compatibility": level},
        timeout=15,
    )
    resp.raise_for_status()
    log("info", "compatibility_set", subject=subject, level=level)


def main() -> None:
    import os

    url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
    for subject, path in SUBJECTS:
        schema_text = json.dumps(json.loads(path.read_text(encoding="utf-8")))
        register(url, subject, schema_text)
        set_compatibility(url, subject, "BACKWARD")

    order_v11 = SCHEMA_DIR / "order_v1_1.avsc"
    evolved = json.dumps(json.loads(order_v11.read_text(encoding="utf-8")))
    register(url, "ecommerce.orders.v1-value", evolved)
    log(
        "info",
        "schema_evolution_ok",
        subject="ecommerce.orders.v1-value",
        note="v1.1 coupon_code optional",
    )


if __name__ == "__main__":
    main()
