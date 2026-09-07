from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_user: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_password: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "lake")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user: str = os.getenv("POSTGRES_USER", "streamcart")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "streamcart")
    postgres_db: str = os.getenv("POSTGRES_DB", "streamcart")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


def get_settings() -> Settings:
    return Settings()


TOPICS = {
    "orders": "ecommerce.orders.v1",
    "payments": "ecommerce.payments.v1",
    "inventory": "ecommerce.inventory.v1",
    "clickstream": "ecommerce.clickstream.v1",
}

DLQ_TOPICS = {name: f"{topic}.dlq" for name, topic in TOPICS.items()}
