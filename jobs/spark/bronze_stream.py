"""Bronze: append-only Kafka landing zone (raw bytes + metadata).

Checkpointing is on MinIO so a job restart resumes without duplicating already
committed files (Spark exactly-once file sink semantics for micro-batches).
"""

from __future__ import annotations

from pyspark.sql.functions import col, current_timestamp, date_format

from session import build_spark, kafka_bootstrap, lake_path, once_mode
from streamcart.config import TOPICS
from streamcart.logging import log


def main() -> None:
    spark = build_spark("streamcart-bronze")
    topics = ",".join(TOPICS.values())
    log("info", "bronze_start", topics=topics)

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
            col("timestamp").alias("kafka_ts"),
            col("key").cast("string").alias("key"),
            col("value"),
            current_timestamp().alias("ingest_ts"),
        )
        .withColumn("dt", date_format(col("ingest_ts"), "yyyy-MM-dd"))
    )

    writer = (
        raw.writeStream.format("parquet")
        .option("path", lake_path("bronze", "events"))
        .option("checkpointLocation", lake_path("checkpoints", "bronze"))
        .partitionBy("topic", "dt")
        .outputMode("append")
        .queryName("bronze_events")
    )
    if once_mode():
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    else:
        query = writer.trigger(processingTime="10 seconds").start()
        query.awaitTermination()


if __name__ == "__main__":
    main()
