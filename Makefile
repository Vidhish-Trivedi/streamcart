COMPOSE ?= docker compose

.PHONY: up down logs seed-schemas produce jobs jobs-once test lint psql ui help

help:
	@echo "make up             Start Kafka, Schema Registry, MinIO, Postgres, observability, Airflow"
	@echo "make seed-schemas   Register Avro contracts (BACKWARD) and create topics"
	@echo "make produce        Run the chaos-capable event generator"
	@echo "make jobs           Start Spark bronze/silver/gold streaming jobs"
	@echo "make jobs-once      Run Spark jobs with availableNow (backfill / smoke)"
	@echo "make test           pytest + ruff"
	@echo "make down           Stop the stack"

up:
	$(COMPOSE) up -d kafka schema-registry kafka-ui minio minio-init postgres kafka-init prometheus grafana kafka-exporter airflow-init airflow
	$(COMPOSE) ps

down:
	$(COMPOSE) --profile jobs down -v

logs:
	$(COMPOSE) logs -f --tail=100

seed-schemas:
	$(COMPOSE) run --rm --build generator python -m scripts.register_schemas

produce:
	$(COMPOSE) run --rm --build generator

jobs:
	$(COMPOSE) --profile jobs up -d spark-bronze spark-silver spark-gold

jobs-once:
	$(COMPOSE) --profile jobs run --rm -e STREAMCART_ONCE=1 spark-bronze
	$(COMPOSE) --profile jobs run --rm -e STREAMCART_ONCE=1 spark-silver
	$(COMPOSE) --profile jobs run --rm -e STREAMCART_ONCE=1 spark-gold

test:
	python -m pytest -q
	python -m ruff check streamcart producers scripts tests

lint:
	python -m ruff check streamcart producers scripts tests

psql:
	$(COMPOSE) exec postgres psql -U streamcart -d streamcart

ui:
	@echo "Kafka UI     http://localhost:8082"
	@echo "Schema Reg   http://localhost:8081"
	@echo "MinIO        http://localhost:9001  (minioadmin/minioadmin)"
	@echo "Grafana      http://localhost:3000  (admin/admin)"
	@echo "Prometheus   http://localhost:9090"
	@echo "Airflow      http://localhost:8085  (admin/admin)"
	@echo "Postgres     localhost:5432"
