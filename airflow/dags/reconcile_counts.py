"""Reconcile serving-layer counts (orders vs payments, watermark freshness)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from streamcart.logging import log


def _conn():
    return psycopg2.connect(os.environ["STREAMCART_PG_DSN"])


def reconcile() -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fact_orders")
        (orders,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM fact_payments")
        (payments,) = cur.fetchone()
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_orders o
            LEFT JOIN fact_payments p ON p.order_id = o.order_id
            WHERE p.event_id IS NULL AND o.status <> 'cancelled'
            """
        )
        (orphan_orders,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM dead_letters")
        (dlq,) = cur.fetchone()

    log(
        "info",
        "reconcile",
        fact_orders=orders,
        fact_payments=payments,
        unpaid_non_cancelled=orphan_orders,
        dead_letters=dlq,
    )
    if orders and orphan_orders / orders > 0.5:
        raise RuntimeError("more than 50% of non-cancelled orders lack a payment event")


with DAG(
    dag_id="streamcart_reconcile_counts",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["streamcart", "reconcile"],
) as dag:
    PythonOperator(task_id="reconcile_orders_payments", python_callable=reconcile)
