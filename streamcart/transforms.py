"""Pure-Python silver/gold transforms — unit-tested without Spark.

Spark jobs call these per micro-batch so interview talk tracks match the tests.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

VALID_ORDER_STATUSES = {"created", "paid", "cancelled", "fulfilled"}
VALID_PAYMENT_STATUSES = {"authorized", "captured", "failed"}
WATERMARK_LAG_MS = 30 * 60 * 1000  # 30 minutes — late events beyond this are dropped from joins


def _ts_millis(value: Any) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    return None


def validate_order(event: dict[str, Any]) -> str | None:
    if not event.get("event_id") or not event.get("order_id"):
        return "missing_keys"
    amount = event.get("amount_cents")
    if not isinstance(amount, (int, float)) or amount < 0:
        return "invalid_amount"
    if event.get("status") not in VALID_ORDER_STATUSES:
        return "invalid_status"
    qty = event.get("quantity")
    if not isinstance(qty, int) or qty < 1:
        return "invalid_quantity"
    return None


def validate_payment(event: dict[str, Any]) -> str | None:
    if not event.get("event_id") or not event.get("order_id"):
        return "missing_keys"
    if event.get("status") not in VALID_PAYMENT_STATUSES:
        return "invalid_status"
    amount = event.get("amount_cents")
    if not isinstance(amount, (int, float)) or amount < 0:
        return "invalid_amount"
    return None


def validate_inventory(event: dict[str, Any]) -> str | None:
    if not event.get("event_id") or not event.get("sku") or not event.get("warehouse_id"):
        return "missing_keys"
    if not isinstance(event.get("quantity"), int):
        return "invalid_quantity"
    return None


def validate_click(event: dict[str, Any]) -> str | None:
    if not event.get("event_id") or not event.get("session_id") or not event.get("page"):
        return "missing_keys"
    return None


VALIDATORS = {
    "orders": validate_order,
    "payments": validate_payment,
    "inventory": validate_inventory,
    "clickstream": validate_click,
}


def dedupe_by_event_id(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for event in events:
        eid = str(event.get("event_id", ""))
        if not eid or eid in seen:
            continue
        seen.add(eid)
        out.append(event)
    return out


def drop_late(
    events: Iterable[dict[str, Any]],
    max_event_ts: int,
    lag_ms: int = WATERMARK_LAG_MS,
) -> list[dict[str, Any]]:
    cutoff = max_event_ts - lag_ms
    kept: list[dict[str, Any]] = []
    for event in events:
        ts = _ts_millis(event.get("event_ts"))
        if ts is None or ts < cutoff:
            continue
        kept.append(event)
    return kept


def join_orders_payments(
    orders: Iterable[dict[str, Any]],
    payments: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Latest payment per order_id (by event_ts), left-joined onto orders."""
    latest: dict[str, dict[str, Any]] = {}
    for payment in payments:
        oid = payment["order_id"]
        prev = latest.get(oid)
        if prev is None or int(payment.get("event_ts") or 0) >= int(prev.get("event_ts") or 0):
            latest[oid] = payment

    joined: list[dict[str, Any]] = []
    for order in orders:
        pay = latest.get(order["order_id"])
        row = dict(order)
        row["payment_status"] = pay["status"] if pay else None
        row["payment_event_id"] = pay["event_id"] if pay else None
        joined.append(row)
    return joined


def hour_bucket_ms(event_ts: int) -> int:
    return event_ts - (event_ts % 3_600_000)


def hourly_gmv(orders: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[int, str], dict[str, Any]] = {}
    for order in orders:
        if order.get("status") == "cancelled":
            continue
        ts = int(order["event_ts"])
        currency = str(order.get("currency") or "USD")
        key = (hour_bucket_ms(ts), currency)
        slot = buckets.setdefault(
            key,
            {"hour_ts_ms": key[0], "currency": currency, "gmv_cents": 0, "order_count": 0},
        )
        slot["gmv_cents"] += int(order["amount_cents"])
        slot["order_count"] += 1
    return list(buckets.values())


def payment_failure_stats(payments: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[int, dict[str, int]] = {}
    for payment in payments:
        hour = hour_bucket_ms(int(payment["event_ts"]))
        slot = buckets.setdefault(hour, {"paid": 0, "failed": 0})
        if payment.get("status") == "failed":
            slot["failed"] += 1
        else:
            slot["paid"] += 1
    out: list[dict[str, Any]] = []
    for hour, counts in buckets.items():
        total = counts["paid"] + counts["failed"]
        rate = (counts["failed"] / total) if total else 0.0
        out.append(
            {
                "hour_ts_ms": hour,
                "paid_count": counts["paid"],
                "failed_count": counts["failed"],
                "failure_rate": rate,
            }
        )
    return out


def stockouts(inventory: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in inventory if int(row.get("quantity", 0)) <= 0]


def millis_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
