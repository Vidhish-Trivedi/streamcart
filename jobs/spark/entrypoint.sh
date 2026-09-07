#!/usr/bin/env bash
set -euo pipefail
cd /opt/app/jobs
case "${SPARK_JOB:-bronze}" in
  bronze) exec python bronze_stream.py ;;
  silver) exec python silver_orders.py ;;
  gold) exec python gold_metrics.py ;;
  *) echo "unknown SPARK_JOB=${SPARK_JOB}" >&2; exit 1 ;;
esac
