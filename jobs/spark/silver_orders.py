"""Silver: decode Avro, validate, dedupe, watermark late data, join payments.

Invalid payloads go to Kafka DLQ and MinIO quarantine. Valid rows land as parquet.
Watermark: 30 minutes — events older than that relative to the batch max event_ts
are dropped from the join (see streamcart.transforms.WATERMARK_LAG_MS).
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, date_format, lit
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from session import build_spark, kafka_bootstrap, lake_path, once_mode
from streamcart.avro_codec import decode_confluent, load_schema, parse_schema
from streamcart.config import DLQ_TOPICS, TOPICS
from streamcart.logging import log
from streamcart.transforms import (
    WATERMARK_LAG_MS,
    dedupe_by_event_id,
    drop_late,
    join_orders_payments,
    validate_click,
    validate_inventory,
    validate_order,
    validate_payment,
)

SCHEMAS = {
    TOPICS["orders"]: parse_schema(load_schema("orders")),
    TOPICS["payments"]: parse_schema(load_schema("payments")),
    TOPICS["inventory"]: parse_schema(load_schema("inventory")),
    TOPICS["clickstream"]: parse_schema(load_schema("clickstream")),
}
ORDER_READER = parse_schema(load_schema("orders_v11"))

VALIDATORS = {
    TOPICS["orders"]: validate_order,
    TOPICS["payments"]: validate_payment,
    TOPICS["inventory"]: validate_inventory,
    TOPICS["clickstream"]: validate_click,
}

KIND_BY_TOPIC = {topic: kind for kind, topic in TOPICS.items()}

SILVER_SCHEMA = StructType(
    [
        StructField("kind", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("trace_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("amount_cents", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("event_ts", LongType(), False),
        StructField("ingest_ts", TimestampType(), False),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("topic", StringType(), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("key", StringType(), True),
        StructField("value", BinaryType(), True),
        StructField("ingest_ts", TimestampType(), False),
    ]
)


def _flatten(kind: str, rec: dict, meta: dict) -> dict:
    return {
        "kind": kind,
        "event_id": rec.get("event_id"),
        "trace_id": rec.get("trace_id"),
        "order_id": rec.get("order_id"),
        "customer_id": rec.get("customer_id"),
        "status": rec.get("status"),
        "payment_status": None,
        "amount_cents": rec.get("amount_cents"),
        "currency": rec.get("currency"),
        "sku": rec.get("sku"),
        "quantity": rec.get("quantity"),
        "warehouse_id": rec.get("warehouse_id"),
        "session_id": rec.get("session_id"),
        "page": rec.get("page"),
        "coupon_code": rec.get("coupon_code"),
        "event_ts": int(rec["event_ts"]),
        "ingest_ts": meta["ingest_ts"],
        "kafka_partition": meta["partition"],
        "kafka_offset": meta["offset"],
        "topic": meta["topic"],
    }


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    ingest_ts = datetime.now(UTC)
    valid: list[dict] = []
    quarantine: list[dict] = []

    decoded_by_kind: dict[str, list[dict]] = {"orders": [], "payments": [], "inventory": [], "clickstream": []}

    for row in rows:
        topic = row["topic"]
        raw = row["value"]
        payload = bytes(raw) if raw is not None else b""
        meta = {
            "topic": topic,
            "partition": int(row["partition"]),
            "offset": int(row["offset"]),
            "ingest_ts": ingest_ts,
        }
        schema = SCHEMAS.get(topic)
        kind = KIND_BY_TOPIC.get(topic)
        if schema is None or kind is None:
            quarantine.append({**meta, "reason": "unknown_topic", "key": row["key"], "value": payload})
            continue
        try:
            rec = decode_confluent(
                payload,
                schema,
                ORDER_READER if topic == TOPICS["orders"] else None,
            )
        except Exception as exc:  # noqa: BLE001 — poison / schema mismatch is expected
            quarantine.append({**meta, "reason": str(exc), "key": row["key"], "value": payload})
            continue
        reason = VALIDATORS[topic](rec)
        if reason:
            quarantine.append({**meta, "reason": reason, "key": row["key"], "value": payload})
            continue
        decoded_by_kind[kind].append(_flatten(kind, rec, meta))

    max_ts = 0
    for kind_rows in decoded_by_kind.values():
        for rec in kind_rows:
            max_ts = max(max_ts, rec["event_ts"])

    if max_ts:
        for kind in decoded_by_kind:
            decoded_by_kind[kind] = drop_late(dedupe_by_event_id(decoded_by_kind[kind]), max_ts, WATERMARK_LAG_MS)

    orders = join_orders_payments(decoded_by_kind["orders"], decoded_by_kind["payments"])
    valid.extend(orders)
    valid.extend(decoded_by_kind["payments"])
    valid.extend(decoded_by_kind["inventory"])
    valid.extend(decoded_by_kind["clickstream"])

    if valid:
        silver = spark.createDataFrame(valid, schema=SILVER_SCHEMA)
        silver = silver.withColumn("dt", date_format(col("ingest_ts"), "yyyy-MM-dd"))
        (
            silver.write.mode("append")
            .format("parquet")
            .partitionBy("kind", "dt")
            .save(lake_path("silver", "events"))
        )

    if quarantine:
        qdf = spark.createDataFrame(quarantine, schema=QUARANTINE_SCHEMA)
        qdf = qdf.withColumn("dt", date_format(col("ingest_ts"), "yyyy-MM-dd"))
        (
            qdf.write.mode("append")
            .format("parquet")
            .partitionBy("topic", "dt")
            .save(lake_path("quarantine", "events"))
        )
        dlq = (
            qdf.select(
                col("key"),
                col("value"),
                col("topic"),
                col("reason"),
            )
            .withColumn("headers_reason", col("reason"))
        )
        for topic, dlq_topic in DLQ_TOPICS.items():
            subset = dlq.filter(col("topic") == lit(topic)).select(
                col("key").cast("string").alias("key"),
                col("value"),
            )
            if subset.isEmpty():
                continue
            (
                subset.write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap())
                .option("topic", dlq_topic)
                .save()
            )

    log(
        "info",
        "silver_batch",
        batch_id=batch_id,
        valid=len(valid),
        quarantine=len(quarantine),
        watermark_max_ts=max_ts,
    )


def main() -> None:
    spark = build_spark("streamcart-silver")
    topics = ",".join(TOPICS.values())
    log("info", "silver_start", topics=topics)

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
            col("key").cast("string").alias("key"),
            col("value"),
            current_timestamp().alias("ingest_ts"),
        )
    )

    writer = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", lake_path("checkpoints", "silver"))
        .queryName("silver_events")
        .outputMode("update")
    )
    if once_mode():
        writer.trigger(availableNow=True).start().awaitTermination()
    else:
        writer.trigger(processingTime="10 seconds").start().awaitTermination()


if __name__ == "__main__":
    main()
