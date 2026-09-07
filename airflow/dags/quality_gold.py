"""Gold-table quality expectations (GE-style checks, SQL-backed)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from streamcart.logging import log
from streamcart.quality import (
    expect_failure_rate_bound,
    expect_gmv_non_negative,
    expect_non_empty,
    expect_unique_ratio,
    expect_watermark_fresh,
    summarize,
)


def _conn():
    return psycopg2.connect(os.environ["STREAMCART_PG_DSN"])


def run_quality() -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM fact_orders")
        count, distinct_ids = cur.fetchone()
        cur.execute("SELECT MIN(gmv_cents) FROM gold_hourly_gmv")
        (min_gmv,) = cur.fetchone()
        cur.execute("SELECT MAX(failure_rate) FROM gold_payment_stats")
        (max_rate,) = cur.fetchone()
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM (now() - updated_at)) FROM pipeline_watermarks WHERE job_name = 'gold'"
        )
        row = cur.fetchone()
        age = float(row[0]) if row and row[0] is not None else None

    if not count:
        log("warn", "quality_skipped_empty")
        return

    results = [
        expect_non_empty(count or 0),
        expect_unique_ratio(distinct_ids or 0, count or 0),
        expect_gmv_non_negative(min_gmv),
        expect_failure_rate_bound(max_rate),
        expect_watermark_fresh(age),
    ]
    summary = summarize(results)
    log("info", "quality_complete", **summary)
    if not summary["passed"]:
        raise RuntimeError(f"quality failed: {summary['failed']}")


with DAG(
    dag_id="streamcart_quality_gold",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["streamcart", "quality"],
) as dag:
    PythonOperator(task_id="run_quality_checks", python_callable=run_quality)
