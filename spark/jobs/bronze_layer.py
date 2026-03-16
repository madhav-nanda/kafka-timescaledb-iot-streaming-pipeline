"""
Bronze Layer — Raw Ingestion.

Reads JSON-encoded telemetry from Kafka and persists the raw events
as Parquet in MinIO, partitioned by date and turbine_id.

No transformations are applied; this layer is the immutable source of truth.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_date, current_timestamp

from spark.utils.schema import TELEMETRY_SCHEMA

logger = logging.getLogger("spark_pipeline.bronze")


def read_kafka_stream(spark: SparkSession, kafka_cfg: dict) -> DataFrame:
    """Create a streaming DataFrame from Kafka.

    Args:
        spark: Active SparkSession.
        kafka_cfg: Kafka section of pipeline config.

    Returns:
        Raw streaming DataFrame with key/value as binary columns.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg["bootstrap_servers"])
        .option("subscribe", kafka_cfg["topic"])
        .option("startingOffsets",
                kafka_cfg.get("consumer", {}).get("auto_offset_reset", "earliest"))
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )


def parse_telemetry(raw_df: DataFrame, schema_registry_url: str) -> DataFrame:
    """Deserialize Kafka values from JSON into structured columns.

    The producer sends JSON-encoded telemetry events. Schema validation
    is performed at the producer side using fastavro against the Avro
    schema registered with Schema Registry.

    Args:
        raw_df: Raw Kafka streaming DataFrame.
        schema_registry_url: URL of the Schema Registry (unused, kept for API compat).

    Returns:
        DataFrame with parsed telemetry columns.
    """
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value",
                     "topic", "partition", "offset", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_value"), TELEMETRY_SCHEMA).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
        )
        .select("data.*", "topic", "partition", "offset", "kafka_timestamp")
    )


def write_bronze(parsed_df: DataFrame, bronze_cfg: dict,
                 checkpoint_base: str):
    """Write parsed events to the Bronze data lake layer.

    Args:
        parsed_df: Parsed telemetry streaming DataFrame.
        bronze_cfg: Bronze layer config (output_path, partition_by).
        checkpoint_base: Base path for streaming checkpoints.

    Returns:
        The streaming query handle.
    """
    query = (
        parsed_df
        .withColumn("event_date", to_date(col("timestamp")))
        .withColumn("ingestion_timestamp", current_timestamp())
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", bronze_cfg["output_path"])
        .option("checkpointLocation", f"{checkpoint_base}/bronze")
        .partitionBy(*bronze_cfg.get("partition_by", ["event_date", "turbine_id"]))
        .trigger(processingTime=bronze_cfg.get("trigger_interval", "30 seconds"))
        .queryName("bronze_ingestion")
        .start()
    )
    logger.info("Bronze layer streaming query started: %s", query.id)
    return query
