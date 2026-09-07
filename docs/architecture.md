# Streamcart architecture

## Data flow

1. **Generator** (`python -m producers.generator`) emits Avro to four topics using an **idempotent producer** (`enable.idempotence=true`, `acks=all`). Chaos mode injects duplicates, late timestamps, invalid amounts, and truncated payloads.
2. **Schema Registry** stores Avro under TopicNameStrategy (`ecommerce.orders.v1-value`, …) with **BACKWARD** compatibility. `order_v1_1.avsc` adds optional `coupon_code` so old consumers keep working.
3. **Bronze Spark job** reads Kafka and writes append-only parquet to `s3a://lake/bronze/events` partitioned by `topic, dt`. Spark checkpoints on MinIO: a restart continues from the last committed micro-batch (file sink exactly-once *within* Spark’s checkpoint, not across independent consumers).
4. **Silver Spark job** (separate consumer group / checkpoint) decodes Confluent Avro (`magic + schema id + payload`), validates, dedupes on `event_id`, drops events older than a **30-minute watermark** relative to the batch max `event_ts`, left-joins the latest payment onto each order, writes parquet to `s3a://lake/silver/events`, and publishes failures to `{topic}.dlq` plus `s3a://lake/quarantine/`.
5. **Gold Spark job** upserts `fact_orders` and `fact_payments` with `ON CONFLICT (event_id)`. Hourly GMV and payment stats are also upserted for low latency. Because Kafka delivery is **at-least-once**, those aggregates can over-count on a failed batch retry — **Airflow `streamcart_backfill_gold` recomputes them from fact tables** (batch = source of truth).
6. **Airflow** (not on the hot path) runs quality expectations, order/payment reconciliation, and the gold rebuild.
7. **Prometheus + kafka-exporter + Grafana** expose broker/topic offsets and consumer-group lag.

## Topics

| Topic | Contents |
| --- | --- |
| `ecommerce.orders.v1` | Order lifecycle |
| `ecommerce.payments.v1` | Authorization / capture / failure |
| `ecommerce.inventory.v1` | Stock by SKU + warehouse |
| `ecommerce.clickstream.v1` | Session page views |
| `*.dlq` | Poison and validation failures |

## Delivery semantics (honest)

| Stage | Guarantee |
| --- | --- |
| Produce | Idempotent producer avoids dupes *inside a session*; chaos still emits duplicate `event_id`s on purpose |
| Spark source | At-least-once unless you enable Kafka transactional / exactly-once sink |
| Bronze parquet | Exactly-once *per checkpointed query* |
| Gold Postgres facts | Idempotent by primary key — safe under at-least-once |
| Gold hourly metrics | Stream = fast & eventually corrected by Airflow rebuild |

## Why watermarks

Payments can arrive minutes after orders, and the generator emits late `event_ts` values. Silver/gold use `drop_late(..., lag_ms=30min)` so a join does not wait forever and extremely late data does not rewrite old gold hours in the streaming path. The batch rebuild still includes all facts.

<!-- ## Interview talking points

- **Medallion**: raw → conformed → serving, with Kafka metadata retained in bronze.
- **Contracts over JSON**: Schema Registry + BACKWARD evolution; show `coupon_code`.
- **Idempotency is a sink property**: keys on `event_id`, not “exactly-once Kafka” slogans.
- **Two-speed gold**: stream for dashboards, batch for correctness (classic Lambda, implemented cheaply).
- **You can fail closed**: quality DAG raises if `event_id` uniqueness or GMV sign breaks.
- **What I would add in production**: Kafka ACLs, IAM for S3, Spark on Kubernetes / EMR, Great Expectations in CI against data contracts, dbt for gold SQL, alerts on lag SLO. -->

## Local ports

8082 Kafka UI · 8081 Schema Registry · 9001 MinIO · 5432 Postgres · 3000 Grafana · 9090 Prometheus · 8085 Airflow · 9092 Kafka (host).
