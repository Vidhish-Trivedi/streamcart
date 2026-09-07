from __future__ import annotations

import os
from glob import glob

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    jars = ",".join(sorted(glob("/opt/jars/*.jar")))
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if jars:
        builder = builder.config("spark.jars", jars)
    return builder.getOrCreate()


def lake_path(*parts: str) -> str:
    bucket = os.getenv("MINIO_BUCKET", "lake")
    return "s3a://" + bucket + "/" + "/".join(parts)


def kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")


def once_mode() -> bool:
    return os.getenv("STREAMCART_ONCE", "0") in {"1", "true", "True"}
