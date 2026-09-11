from __future__ import annotations

import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

from streamcart.config import TOPICS

SKUS = [
    "SKU-TEE-001",
    "SKU-MUG-014",
    "SKU-HAT-003",
    "SKU-BAG-022",
    "SKU-BOT-009",
]
WAREHOUSES = ["wh-east", "wh-west", "wh-central"]
PAGES = ["/home", "/pdp", "/cart", "/checkout", "/search"]


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _ids() -> tuple[str, str]:
    return str(uuid.uuid4()), str(uuid.uuid4())


def make_order(rng: random.Random, *, late_ms: int = 0, bad_amount: bool = False) -> dict[str, Any]:
    event_id, trace_id = _ids()
    order_id = str(uuid.uuid4())
    amount = rng.randint(899, 12999)
    if bad_amount:
        amount = -amount
    return {
        "topic": TOPICS["orders"],
        "key": order_id,
        "record": {
            "event_id": event_id,
            "trace_id": trace_id,
            "order_id": order_id,
            "customer_id": f"cust-{rng.randint(1000, 9999)}",
            "status": rng.choice(["created", "created", "created", "cancelled"]),
            "amount_cents": amount,
            "currency": "USD",
            "sku": rng.choice(SKUS),
            "quantity": rng.randint(1, 3),
            "event_ts": _now_ms() - late_ms,
        },
        "kind": "orders",
    }


def make_payment(rng: random.Random, order: dict[str, Any]) -> dict[str, Any]:
    event_id, _ = _ids()
    rec = order["record"]
    failed = rng.random() < 0.12
    return {
        "topic": TOPICS["payments"],
        "key": rec["order_id"],
        "record": {
            "event_id": event_id,
            "trace_id": rec["trace_id"],
            "payment_id": str(uuid.uuid4()),
            "order_id": rec["order_id"],
            "status": "failed" if failed else rng.choice(["authorized", "captured"]),
            "amount_cents": rec["amount_cents"] if rec["amount_cents"] > 0 else 100,
            "currency": rec["currency"],
            "event_ts": rec["event_ts"] + rng.randint(200, 4000),
        },
        "kind": "payments",
    }


def make_inventory(rng: random.Random, order: dict[str, Any] | None = None) -> dict[str, Any]:
    event_id, trace_id = _ids()
    sku = order["record"]["sku"] if order else rng.choice(SKUS)
    qty = rng.randint(-2, 40)
    if order:
        qty = rng.choice([0, 0, 3, 12, 40])  # occasional stockout
        trace_id = order["record"]["trace_id"]
    return {
        "topic": TOPICS["inventory"],
        "key": sku,
        "record": {
            "event_id": event_id,
            "trace_id": trace_id,
            "sku": sku,
            "warehouse_id": rng.choice(WAREHOUSES),
            "quantity": qty,
            "reason": "sale" if order else rng.choice(["restock", "adjustment"]),
            "event_ts": _now_ms(),
        },
        "kind": "inventory",
    }


def make_click(rng: random.Random) -> dict[str, Any]:
    event_id, trace_id = _ids()
    return {
        "topic": TOPICS["clickstream"],
        "key": str(uuid.uuid4()),
        "record": {
            "event_id": event_id,
            "trace_id": trace_id,
            "session_id": str(uuid.uuid4()),
            "customer_id": f"cust-{rng.randint(1000, 9999)}" if rng.random() > 0.3 else None,
            "page": rng.choice(PAGES),
            "sku": rng.choice(SKUS) if rng.random() > 0.4 else None,
            "event_ts": _now_ms(),
        },
        "kind": "clickstream",
    }


def next_batch(rng: random.Random, chaos: bool) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    order = make_order(
        rng,
        late_ms=rng.randint(5 * 60_000, 25 * 60_000) if chaos and rng.random() < 0.05 else 0,
        bad_amount=chaos and rng.random() < 0.01,
    )
    events.append(order)
    events.append(make_payment(rng, order))
    if rng.random() < 0.7:
        events.append(make_inventory(rng, order))
    events.append(make_click(rng))

    if chaos and rng.random() < 0.03:
        dup = dict(order)
        dup["record"] = dict(order["record"])
        events.append(dup)
    return events


def env_chaos() -> bool:
    return os.getenv("GENERATOR_CHAOS", "1") not in {"0", "false", "False"}
