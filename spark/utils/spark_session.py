"""Spark session factory with S3/MinIO and Kafka configuration."""

import logging
from pyspark.sql import SparkSession

logger = logging.getLogger("spark_pipeline")


def create_spark_session(config: dict) -> SparkSession:
    """Build a SparkSession pre-configured for Kafka, MinIO (S3A), and Avro.

    Packages are provided via spark-submit --packages; do NOT set
    spark.jars.packages here to avoid double-resolution conflicts.

    Args:
        config: Full pipeline configuration dict.

    Returns:
        Configured SparkSession.
    """
    spark_cfg = config["spark"]
    minio_cfg = config["minio"]

    builder = (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        # ── S3A / MinIO ──
        .config("spark.hadoop.fs.s3a.endpoint", minio_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", minio_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Streaming ──
        .config("spark.sql.streaming.checkpointLocation",
                spark_cfg["checkpoint_location"])
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.schemaInference", "true")
        # ── Misc ──
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession created: %s", spark_cfg["app_name"])
    return spark
