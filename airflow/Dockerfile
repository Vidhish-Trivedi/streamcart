FROM apache/airflow:2.10.4-python3.12

USER airflow
COPY --chown=airflow:root pyproject.toml README.md /tmp/streamcart/
COPY --chown=airflow:root streamcart /tmp/streamcart/streamcart
WORKDIR /tmp/streamcart
RUN pip install --no-cache-dir .
