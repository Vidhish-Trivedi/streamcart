from __future__ import annotations

from streamcart.transforms import (
    dedupe_by_event_id,
    drop_late,
    hourly_gmv,
    join_orders_payments,
    payment_failure_stats,
    stockouts,
    validate_order,
)


def test_validate_order_rejects_negative_amount() -> None:
    bad = {
        "event_id": "e",
        "order_id": "o",
        "status": "created",
        "amount_cents": -1,
        "quantity": 1,
    }
    assert validate_order(bad) == "invalid_amount"


def test_dedupe_keeps_first() -> None:
    rows = [
        {"event_id": "a", "n": 1},
        {"event_id": "a", "n": 2},
        {"event_id": "b", "n": 3},
    ]
    out = dedupe_by_event_id(rows)
    assert [r["n"] for r in out] == [1, 3]


def test_drop_late_applies_watermark() -> None:
    events = [{"event_id": "late", "event_ts": 1}, {"event_id": "ok", "event_ts": 3_600_000}]
    kept = drop_late(events, max_event_ts=3_600_000, lag_ms=60_000)
    assert [e["event_id"] for e in kept] == ["ok"]


def test_join_latest_payment() -> None:
    orders = [
        {
            "event_id": "o1",
            "order_id": "ord",
            "customer_id": "c",
            "status": "created",
            "amount_cents": 100,
            "currency": "USD",
            "event_ts": 10,
        }
    ]
    payments = [
        {"event_id": "p1", "order_id": "ord", "status": "failed", "event_ts": 11},
        {"event_id": "p2", "order_id": "ord", "status": "captured", "event_ts": 12},
    ]
    joined = join_orders_payments(orders, payments)
    assert joined[0]["payment_status"] == "captured"
    assert joined[0]["payment_event_id"] == "p2"


def test_hourly_gmv_skips_cancelled() -> None:
    orders = [
        {"status": "created", "event_ts": 1, "currency": "USD", "amount_cents": 100},
        {"status": "cancelled", "event_ts": 1, "currency": "USD", "amount_cents": 999},
        {"status": "paid", "event_ts": 3_600_000, "currency": "USD", "amount_cents": 50},
    ]
    buckets = {row["hour_ts_ms"]: row for row in hourly_gmv(orders)}
    assert buckets[0]["gmv_cents"] == 100
    assert buckets[0]["order_count"] == 1
    assert buckets[3_600_000]["gmv_cents"] == 50


def test_payment_failure_rate() -> None:
    payments = [
        {"status": "captured", "event_ts": 10},
        {"status": "failed", "event_ts": 11},
        {"status": "failed", "event_ts": 12},
    ]
    stats = payment_failure_stats(payments)
    assert len(stats) == 1
    assert stats[0]["failed_count"] == 2
    assert abs(stats[0]["failure_rate"] - 2 / 3) < 1e-9


def test_stockouts() -> None:
    inv = [
        {"sku": "a", "warehouse_id": "e", "quantity": 0, "event_id": "1"},
        {"sku": "b", "warehouse_id": "e", "quantity": 4, "event_id": "2"},
    ]
    assert len(stockouts(inv)) == 1
    assert stockouts(inv)[0]["sku"] == "a"
