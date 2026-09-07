-- Serving + Airflow metadata. Idempotent gold tables use natural/event keys.

CREATE DATABASE airflow;

\c streamcart

CREATE TABLE IF NOT EXISTS fact_orders (
    event_id        UUID PRIMARY KEY,
    order_id        TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    status          TEXT NOT NULL,
    payment_status  TEXT,
    amount_cents    BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    coupon_code     TEXT,
    event_ts        TIMESTAMPTZ NOT NULL,
    ingest_ts       TIMESTAMPTZ NOT NULL,
    kafka_partition INT,
    kafka_offset    BIGINT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_orders_order_id_idx ON fact_orders (order_id);
CREATE INDEX IF NOT EXISTS fact_orders_event_ts_idx ON fact_orders (event_ts);

CREATE TABLE IF NOT EXISTS fact_payments (
    event_id        UUID PRIMARY KEY,
    payment_id      TEXT NOT NULL,
    order_id        TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_hourly_gmv (
    hour_ts     TIMESTAMPTZ NOT NULL,
    currency    TEXT NOT NULL,
    gmv_cents   BIGINT NOT NULL,
    order_count BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hour_ts, currency)
);

CREATE TABLE IF NOT EXISTS gold_payment_stats (
    hour_ts       TIMESTAMPTZ PRIMARY KEY,
    paid_count    BIGINT NOT NULL,
    failed_count  BIGINT NOT NULL,
    failure_rate  DOUBLE PRECISION NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_stockouts (
    sku          TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    quantity     INT NOT NULL,
    event_id     UUID NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sku, warehouse_id, event_id)
);

CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    job_name      TEXT PRIMARY KEY,
    last_batch_id BIGINT,
    last_event_ts TIMESTAMPTZ,
    rows_written  BIGINT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    event_id     TEXT,
    reason       TEXT NOT NULL,
    payload      TEXT,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_gmv_last_24h AS
SELECT hour_ts, currency, gmv_cents, order_count
FROM gold_hourly_gmv
WHERE hour_ts >= now() - INTERVAL '24 hours'
ORDER BY hour_ts DESC;
