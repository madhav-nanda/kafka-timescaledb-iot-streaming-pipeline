"""
Silver Layer — Cleaned & Enriched Data.

Reads from the Bronze layer, applies data quality filters, normalizes
timestamps, and computes derived metrics. Output is partitioned Parquet
in the Silver bucket.

Transformations:
  - Filter invalid / out-of-range sensor readings
  - Parse ISO timestamps to proper TimestampType
  - Compute derived metrics (capacity factor, delta temperatures)
  - Apply watermark for late-arriving data handling
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger("spark_pipeline.silver")

# ── Operational bounds for data quality filtering ───────────────────────────

QUALITY_BOUNDS = {
    "wind_speed": (0.0, 35.0),
    "power_output": (0.0, 3500.0),
    "generator_temperature": (-20.0, 150.0),
    "gearbox_temperature": (-20.0, 130.0),
    "ambient_temperature": (-40.0, 60.0),
    "rotor_rpm": (0.0, 50.0),
    "blade_pitch_angle": (-5.0, 95.0),
    "vibration_level": (0.0, 10.0),
}


def read_bronze_stream(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read a streaming DataFrame from the Bronze Parquet lake.

    Args:
        spark: Active SparkSession.
        bronze_path: S3A path to Bronze parquet data.

    Returns:
        Streaming DataFrame of raw telemetry.
    """
    return (
        spark.readStream
        .format("parquet")
        .option("maxFilesPerTrigger", 100)
        .schema(
            spark.read.parquet(bronze_path).schema
        )
        .load(bronze_path)
    )


def apply_quality_filters(df: DataFrame) -> DataFrame:
    """Remove rows with sensor values outside operational bounds.

    Args:
        df: Input telemetry DataFrame.

    Returns:
        Filtered DataFrame with only valid readings.
    """
    condition = F.lit(True)
    for col_name, (lo, hi) in QUALITY_BOUNDS.items():
        condition = condition & (
            F.col(col_name).between(lo, hi) & F.col(col_name).isNotNull()
        )

    filtered = df.filter(condition)
    logger.info("Quality filters applied for %d sensor columns",
                len(QUALITY_BOUNDS))
    return filtered


def enrich_telemetry(df: DataFrame) -> DataFrame:
    """Add derived metrics and normalize data types.

    Derived columns:
      - event_ts: parsed timestamp as TimestampType
      - capacity_factor: power_output / rated_power (3000 kW)
      - temp_delta_generator: generator_temperature - ambient_temperature
      - temp_delta_gearbox: gearbox_temperature - ambient_temperature
      - is_generating: boolean flag for power > 0
      - wind_speed_bin: categorical binning of wind speed

    Args:
        df: Quality-filtered telemetry DataFrame.

    Returns:
        Enriched DataFrame.
    """
    rated_power = 3000.0

    return (
        df
        .withColumn("event_ts",
                     F.to_timestamp(F.col("timestamp")))
        .withColumn("capacity_factor",
                     F.round(F.col("power_output") / F.lit(rated_power), 4))
        .withColumn("temp_delta_generator",
                     F.round(F.col("generator_temperature") -
                             F.col("ambient_temperature"), 2))
        .withColumn("temp_delta_gearbox",
                     F.round(F.col("gearbox_temperature") -
                             F.col("ambient_temperature"), 2))
        .withColumn("is_generating",
                     F.col("power_output") > 0)
        .withColumn("wind_speed_bin",
                     F.when(F.col("wind_speed") < 3, "calm")
                      .when(F.col("wind_speed") < 8, "low")
                      .when(F.col("wind_speed") < 14, "moderate")
                      .when(F.col("wind_speed") < 25, "high")
                      .otherwise("extreme"))
    )


def write_silver(enriched_df: DataFrame, silver_cfg: dict,
                 checkpoint_base: str):
    """Persist enriched data to the Silver data lake layer.

    Args:
        enriched_df: Enriched streaming DataFrame.
        silver_cfg: Silver layer config.
        checkpoint_base: Base checkpoint path.
    """
    query = (
        enriched_df
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_cfg["output_path"])
        .option("checkpointLocation", f"{checkpoint_base}/silver")
        .partitionBy(*silver_cfg.get("partition_by", ["event_date", "turbine_id"]))
        .trigger(processingTime=silver_cfg.get("trigger_interval", "30 seconds"))
        .queryName("silver_enrichment")
        .start()
    )
    logger.info("Silver layer streaming query started: %s", query.id)
    return query
