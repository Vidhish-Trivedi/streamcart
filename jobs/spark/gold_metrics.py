"""Gold: hourly GMV, payment failure rate, stockouts → Postgres upserts.

Writes are at-least-once from Kafka; Postgres UNIQUE / PRIMARY KEY + ON CONFLICT
makes the serving layer idempotent on event_id / hour bucket.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from session import build_spark, kafka_bootstrap, lake_path, once_mode
from streamcart.avro_codec import decode_confluent, load_schema, parse_schema
from streamcart.config import TOPICS, get_settings
from streamcart.logging import log
from streamcart.transforms import (
    WATERMARK_LAG_MS,
    dedupe_by_event_id,
    drop_late,
    hourly_gmv,
    join_orders_payments,
    millis_to_dt,
    payment_failure_stats,
    stockouts,
    validate_inventory,
    validate_order,
    validate_payment,
)

ORDER_SCHEMA = parse_schema(load_schema("orders"))
ORDER_READER = parse_schema(load_schema("orders_v11"))
PAYMENT_SCHEMA = parse_schema(load_schema("payments"))
INVENTORY_SCHEMA = parse_schema(load_schema("inventory"))


def _conn():
    return psycopg2.connect(get_settings().postgres_dsn)


def upsert_gold(orders: list[dict], payments: list[dict], inventory: list[dict], batch_id: int) -> None:
    fact = join_orders_payments(orders, payments)
    gmv = hourly_gmv(fact)
    pay_stats = payment_failure_stats(payments)
    outs = stockouts(inventory)
    now = datetime.now(UTC)

    with _conn() as conn:
        with conn.cursor() as cur:
            if fact:
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_orders (
                        event_id, order_id, customer_id, status, payment_status,
                        amount_cents, currency, coupon_code, event_ts, ingest_ts,
                        kafka_partition, kafka_offset
                    ) VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        payment_status = EXCLUDED.payment_status,
                        amount_cents = EXCLUDED.amount_cents,
                        coupon_code = EXCLUDED.coupon_code,
                        updated_at = now()
                    """,
                    [
                        (
                            row["event_id"],
                            row["order_id"],
                            row["customer_id"],
                            row["status"],
                            row.get("payment_status"),
                            int(row["amount_cents"]),
                            row.get("currency") or "USD",
                            row.get("coupon_code"),
                            millis_to_dt(int(row["event_ts"])),
                            now,
                            row.get("kafka_partition"),
                            row.get("kafka_offset"),
                        )
                        for row in fact
                    ],
                )
            if payments:
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_payments (
                        event_id, payment_id, order_id, status, amount_cents, currency, event_ts
                    ) VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        updated_at = now()
                    """,
                    [
                        (
                            row["event_id"],
                            row["payment_id"],
                            row["order_id"],
                            row["status"],
                            int(row["amount_cents"]),
                            row.get("currency") or "USD",
                            millis_to_dt(int(row["event_ts"])),
                        )
                        for row in payments
                    ],
                )
            if gmv:
                execute_values(
                    cur,
                    """
                    INSERT INTO gold_hourly_gmv (hour_ts, currency, gmv_cents, order_count)
                    VALUES %s
                    ON CONFLICT (hour_ts, currency) DO UPDATE SET
                        gmv_cents = gold_hourly_gmv.gmv_cents + EXCLUDED.gmv_cents,
                        order_count = gold_hourly_gmv.order_count + EXCLUDED.order_count,
                        updated_at = now()
                    """,
                    [
                        (millis_to_dt(row["hour_ts_ms"]), row["currency"], row["gmv_cents"], row["order_count"])
                        for row in gmv
                    ],
                )
            if pay_stats:
                execute_values(
                    cur,
                    """
                    INSERT INTO gold_payment_stats (hour_ts, paid_count, failed_count, failure_rate)
                    VALUES %s
                    ON CONFLICT (hour_ts) DO UPDATE SET
                        paid_count = gold_payment_stats.paid_count + EXCLUDED.paid_count,
                        failed_count = gold_payment_stats.failed_count + EXCLUDED.failed_count,
                        failure_rate = CASE
                            WHEN (gold_payment_stats.paid_count + gold_payment_stats.failed_count
                                  + EXCLUDED.paid_count + EXCLUDED.failed_count) = 0 THEN 0
                            ELSE (gold_payment_stats.failed_count + EXCLUDED.failed_count)::float
                                 / (gold_payment_stats.paid_count + gold_payment_stats.failed_count
                                    + EXCLUDED.paid_count + EXCLUDED.failed_count)
                        END,
                        updated_at = now()
                    """,
                    [
                        (
                            millis_to_dt(row["hour_ts_ms"]),
                            row["paid_count"],
                            row["failed_count"],
                            row["failure_rate"],
                        )
                        for row in pay_stats
                    ],
                )
            if outs:
                execute_values(
                    cur,
                    """
                    INSERT INTO gold_stockouts (sku, warehouse_id, quantity, event_id, event_ts)
                    VALUES %s
                    ON CONFLICT (sku, warehouse_id, event_id) DO NOTHING
                    """,
                    [
                        (
                            row["sku"],
                            row["warehouse_id"],
                            int(row["quantity"]),
                            row["event_id"],
                            millis_to_dt(int(row["event_ts"])),
                        )
                        for row in outs
                    ],
                )
            last_ts = None
            if fact:
                last_ts = millis_to_dt(max(int(r["event_ts"]) for r in fact))
            cur.execute(
                """
                INSERT INTO pipeline_watermarks (job_name, last_batch_id, last_event_ts, rows_written, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (job_name) DO UPDATE SET
                    last_batch_id = EXCLUDED.last_batch_id,
                    last_event_ts = EXCLUDED.last_event_ts,
                    rows_written = COALESCE(pipeline_watermarks.rows_written, 0) + EXCLUDED.rows_written,
                    updated_at = now()
                """,
                ("gold", batch_id, last_ts, len(fact)),
            )
        conn.commit()
    log("info", "gold_upsert", batch_id=batch_id, orders=len(fact), gmv_buckets=len(gmv), stockouts=len(outs))


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    orders: list[dict] = []
    payments: list[dict] = []
    inventory: list[dict] = []
    for row in batch_df.collect():
        topic = row["topic"]
        raw = bytes(row["value"]) if row["value"] is not None else b""
        try:
            if topic == TOPICS["orders"]:
                rec = decode_confluent(raw, ORDER_SCHEMA, ORDER_READER)
                if validate_order(rec) is None:
                    rec["kafka_partition"] = int(row["partition"])
                    rec["kafka_offset"] = int(row["offset"])
                    orders.append(rec)
            elif topic == TOPICS["payments"]:
                rec = decode_confluent(raw, PAYMENT_SCHEMA)
                if validate_payment(rec) is None:
                    payments.append(rec)
            elif topic == TOPICS["inventory"]:
                rec = decode_confluent(raw, INVENTORY_SCHEMA)
                if validate_inventory(rec) is None:
                    inventory.append(rec)
        except Exception:
            continue

    max_ts = 0
    for rec in (*orders, *payments, *inventory):
        max_ts = max(max_ts, int(rec["event_ts"]))
    if max_ts:
        orders = drop_late(dedupe_by_event_id(orders), max_ts, WATERMARK_LAG_MS)
        payments = drop_late(dedupe_by_event_id(payments), max_ts, WATERMARK_LAG_MS)
        inventory = drop_late(dedupe_by_event_id(inventory), max_ts, WATERMARK_LAG_MS)

    upsert_gold(orders, payments, inventory, batch_id)


def main() -> None:
    spark = build_spark("streamcart-gold")
    topics = ",".join([TOPICS["orders"], TOPICS["payments"], TOPICS["inventory"]])
    log("info", "gold_start", topics=topics)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("value"),
            current_timestamp().alias("ingest_ts"),
        )
    )
    writer = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", lake_path("checkpoints", "gold"))
        .queryName("gold_metrics")
        .outputMode("update")
    )
    if once_mode():
        writer.trigger(availableNow=True).start().awaitTermination()
    else:
        writer.trigger(processingTime="15 seconds").start().awaitTermination()


if __name__ == "__main__":
    main()
