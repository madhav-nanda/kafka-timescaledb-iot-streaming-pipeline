"""
Gold Layer — Aggregated Analytics.

Reads cleaned data from the Silver layer and produces windowed aggregations
for downstream analytics and the TimescaleDB serving layer.

Outputs:
  - Hourly power generation per turbine
  - Rolling turbine efficiency metrics
  - Temperature anomaly detection flags
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger("spark_pipeline.gold")


def read_silver_stream(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read streaming DataFrame from the Silver Parquet lake."""
    return (
        spark.readStream
        .format("parquet")
        .option("maxFilesPerTrigger", 100)
        .schema(spark.read.parquet(silver_path).schema)
        .load(silver_path)
    )


def aggregate_hourly_power(df: DataFrame) -> DataFrame:
    """Compute hourly power generation per turbine.

    Metrics per window:
      - avg_power_kw, max_power_kw, min_power_kw
      - total_energy_kwh (avg_power * 1 hour)
      - avg_wind_speed, avg_efficiency
      - operating_minutes (count of generating readings)
      - record_count

    Args:
        df: Silver-layer streaming DataFrame with event_ts column.

    Returns:
        Aggregated DataFrame with hourly windows.
    """
    return (
        df
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "1 hour"),
            F.col("turbine_id"),
        )
        .agg(
            F.round(F.avg("power_output"), 2).alias("avg_power_kw"),
            F.round(F.max("power_output"), 2).alias("max_power_kw"),
            F.round(F.min("power_output"), 2).alias("min_power_kw"),
            F.round(F.avg("power_output"), 2).alias("total_energy_kwh"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
            F.round(F.avg("turbine_efficiency"), 2).alias("avg_efficiency"),
            F.round(F.avg("capacity_factor"), 4).alias("avg_capacity_factor"),
            F.round(F.avg("generator_temperature"), 2).alias("avg_generator_temp"),
            F.round(F.avg("gearbox_temperature"), 2).alias("avg_gearbox_temp"),
            F.round(F.avg("ambient_temperature"), 2).alias("avg_ambient_temp"),
            F.sum(F.when(F.col("is_generating"), 1).otherwise(0))
             .alias("generating_count"),
            F.count("*").alias("record_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "turbine_id",
            "avg_power_kw", "max_power_kw", "min_power_kw",
            "total_energy_kwh",
            "avg_wind_speed", "avg_efficiency", "avg_capacity_factor",
            "avg_generator_temp", "avg_gearbox_temp", "avg_ambient_temp",
            "generating_count", "record_count",
        )
    )


def detect_anomalies(df: DataFrame) -> DataFrame:
    """Flag temperature anomalies using static thresholds.

    An anomaly is flagged when:
      - Generator temp exceeds 85 C
      - Gearbox temp exceeds 75 C
      - Vibration level exceeds 1.5 mm/s RMS
      - Power output is 0 when wind speed is between cut-in and cut-out

    Args:
        df: Silver-layer streaming DataFrame.

    Returns:
        DataFrame with anomaly rows only.
    """
    return (
        df
        .withWatermark("event_ts", "5 minutes")
        .filter(
            (F.col("generator_temperature") > 85) |
            (F.col("gearbox_temperature") > 75) |
            (F.col("vibration_level") > 1.5) |
            (
                (F.col("wind_speed").between(3, 25)) &
                (F.col("power_output") == 0)
            )
        )
        .withColumn("anomaly_type",
                     F.when(F.col("generator_temperature") > 85,
                            "HIGH_GENERATOR_TEMP")
                      .when(F.col("gearbox_temperature") > 75,
                            "HIGH_GEARBOX_TEMP")
                      .when(F.col("vibration_level") > 1.5,
                            "HIGH_VIBRATION")
                      .otherwise("UNEXPECTED_ZERO_POWER"))
        .withColumn("severity",
                     F.when(F.col("generator_temperature") > 95, "CRITICAL")
                      .when(F.col("gearbox_temperature") > 85, "CRITICAL")
                      .when(F.col("vibration_level") > 2.0, "CRITICAL")
                      .otherwise("WARNING"))
        .select(
            "turbine_id", "event_ts", "anomaly_type", "severity",
            "wind_speed", "power_output",
            "generator_temperature", "gearbox_temperature",
            "vibration_level",
        )
    )


def write_gold_power(agg_df: DataFrame, gold_cfg: dict,
                     checkpoint_base: str):
    """Write hourly power aggregations to Gold layer."""
    query = (
        agg_df
        .withColumn("event_date", F.to_date(F.col("window_start")))
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{gold_cfg['output_path']}/hourly_power")
        .option("checkpointLocation", f"{checkpoint_base}/gold_power")
        .partitionBy("event_date")
        .trigger(processingTime=gold_cfg.get("trigger_interval", "1 minute"))
        .queryName("gold_hourly_power")
        .start()
    )
    logger.info("Gold hourly-power query started: %s", query.id)
    return query


def write_gold_anomalies(anomaly_df: DataFrame, gold_cfg: dict,
                         checkpoint_base: str):
    """Write anomaly events to Gold layer."""
    query = (
        anomaly_df
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{gold_cfg['output_path']}/anomalies")
        .option("checkpointLocation", f"{checkpoint_base}/gold_anomalies")
        .partitionBy("event_date")
        .trigger(processingTime=gold_cfg.get("trigger_interval", "1 minute"))
        .queryName("gold_anomalies")
        .start()
    )
    logger.info("Gold anomaly-detection query started: %s", query.id)
    return query
