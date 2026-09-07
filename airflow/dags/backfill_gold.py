"""Rebuild gold aggregates from fact tables (batch is the source of truth).

Streaming gold upserts are low-latency and at-least-once. This DAG recomputes
hourly GMV and payment stats from idempotent fact tables so retries cannot
double-count.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from streamcart.logging import log

REBUILD_SQL = """
DELETE FROM gold_hourly_gmv;
INSERT INTO gold_hourly_gmv (hour_ts, currency, gmv_cents, order_count)
SELECT date_trunc('hour', event_ts), currency, SUM(amount_cents), COUNT(*)
FROM fact_orders
WHERE status <> 'cancelled'
GROUP BY 1, 2;

DELETE FROM gold_payment_stats;
INSERT INTO gold_payment_stats (hour_ts, paid_count, failed_count, failure_rate)
SELECT
    date_trunc('hour', event_ts),
    COUNT(*) FILTER (WHERE status <> 'failed'),
    COUNT(*) FILTER (WHERE status = 'failed'),
    CASE WHEN COUNT(*) = 0 THEN 0
         ELSE COUNT(*) FILTER (WHERE status = 'failed')::float / COUNT(*)
    END
FROM fact_payments
GROUP BY 1;
"""


def _conn():
    return psycopg2.connect(os.environ["STREAMCART_PG_DSN"])


def rebuild() -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(REBUILD_SQL)
        cur.execute("SELECT COUNT(*) FROM gold_hourly_gmv")
        (gmv_rows,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM gold_payment_stats")
        (pay_rows,) = cur.fetchone()
        conn.commit()
    log("info", "gold_backfill_complete", gmv_rows=gmv_rows, payment_stat_rows=pay_rows)


with DAG(
    dag_id="streamcart_backfill_gold",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["streamcart", "backfill"],
) as dag:
    PythonOperator(task_id="rebuild_gold_from_facts", python_callable=rebuild)
