"""
Wind Turbine Streaming Pipeline — Main Entry Point.

Orchestrates the Bronze -> Silver -> Gold pipeline using Spark Structured
Streaming. All four streaming queries read directly from Kafka — there is
NO file-source chaining (i.e. Silver does NOT read from Bronze Parquet).

Architecture (4 independent Kafka consumers):
  Stream 1 — Bronze:  Kafka -> parse -> Parquet on MinIO (raw, immutable)
  Stream 2 — Silver:  Kafka -> parse -> quality filter -> enrich -> Parquet on MinIO
  Stream 3 — Gold Power:   Kafka -> parse -> enrich -> 1h window agg -> Parquet + TimescaleDB
  Stream 4 — Gold Anomaly: Kafka -> parse -> enrich -> anomaly filter -> Parquet + TimescaleDB

Why no file chaining?
  The original design (Bronze Parquet -> Silver readStream -> ...) required each
  layer to wait for upstream data before starting.  If Bronze hadn't written any
  Parquet files yet, the downstream readStream would crash on an empty directory,
  terminating the entire pipeline.  By reading from Kafka for every layer, all
  four queries start immediately and process independently.  Each has its own
  checkpoint, so exactly-once guarantees are maintained.

Usage:
    spark-submit --packages <deps> spark/streaming_pipeline.py
"""

import logging
import sys
import os

# Add project root to path for config imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from configs import load_config
from spark.utils.spark_session import create_spark_session
from spark.jobs.bronze_layer import read_kafka_stream, parse_telemetry, write_bronze
from spark.jobs.silver_layer import (
    apply_quality_filters, enrich_telemetry, write_silver,
)
from spark.jobs.gold_layer import aggregate_hourly_power, detect_anomalies
from spark.jobs.timescaledb_sink import (
    HOURLY_POWER_COLUMNS, ANOMALY_COLUMNS,
    get_jdbc_url, get_jdbc_properties,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark_pipeline")


# ─── foreachBatch handlers for Gold + TimescaleDB ─────────────────────────

def _make_gold_power_handler(gold_path: str, db_cfg: dict):
    """Create a foreachBatch handler that writes hourly power aggregations
    to BOTH the Gold Parquet layer on MinIO AND to TimescaleDB.

    This avoids the need for a separate Parquet-reading TSDB stream.
    """
    jdbc_url = get_jdbc_url(db_cfg)
    jdbc_props = get_jdbc_properties(db_cfg)

    def handler(df, batch_id):
        if df.isEmpty():
            return
        row_count = df.count()

        # ── Write to Gold Parquet on MinIO ──
        (df
         .withColumn("event_date", F.to_date(F.col("window_start")))
         .write
         .mode("append")
         .partitionBy("event_date")
         .parquet(f"{gold_path}/hourly_power"))

        # ── Write to TimescaleDB (only columns that match the table) ──
        (df
         .select(*HOURLY_POWER_COLUMNS)
         .write
         .format("jdbc")
         .option("url", jdbc_url)
         .option("dbtable", "hourly_power_generation")
         .options(**jdbc_props)
         .mode("append")
         .save())

        logger.info("Gold power batch %d: wrote %d rows to Parquet + TimescaleDB",
                     batch_id, row_count)

    return handler


def _make_gold_anomaly_handler(gold_path: str, db_cfg: dict):
    """Create a foreachBatch handler that writes anomaly events
    to BOTH the Gold Parquet layer on MinIO AND to TimescaleDB.
    """
    jdbc_url = get_jdbc_url(db_cfg)
    jdbc_props = get_jdbc_properties(db_cfg)

    def handler(df, batch_id):
        if df.isEmpty():
            return
        row_count = df.count()

        # ── Write to Gold Parquet on MinIO ──
        (df
         .withColumn("event_date", F.to_date(F.col("event_ts")))
         .write
         .mode("append")
         .partitionBy("event_date")
         .parquet(f"{gold_path}/anomalies"))

        # ── Write to TimescaleDB (only columns that match the table) ──
        (df
         .select(*ANOMALY_COLUMNS)
         .write
         .format("jdbc")
         .option("url", jdbc_url)
         .option("dbtable", "turbine_anomalies")
         .options(**jdbc_props)
         .mode("append")
         .save())

        logger.info("Gold anomaly batch %d: wrote %d rows to Parquet + TimescaleDB",
                     batch_id, row_count)

    return handler


# ─── Main ────────────────────────────────────────────────────────────────

def main() -> None:
    config = load_config()
    spark_cfg = config["spark"]
    kafka_cfg = config["kafka"]
    db_cfg = config["timescaledb"]
    checkpoint_base = spark_cfg["checkpoint_location"]

    spark = create_spark_session(config)
    queries = []

    try:
        # ── Stream 1: Bronze Layer ──────────────────────────────────────
        # Reads from Kafka, parses JSON, writes raw Parquet to MinIO.
        logger.info("Starting Bronze layer ingestion from Kafka")
        raw_bronze = read_kafka_stream(spark, kafka_cfg)
        parsed_bronze = parse_telemetry(raw_bronze, kafka_cfg["schema_registry_url"])
        bronze_query = write_bronze(parsed_bronze, spark_cfg["bronze"], checkpoint_base)
        queries.append(bronze_query)
        logger.info("Bronze query started: %s", bronze_query.name)

        # ── Stream 2: Silver Layer ──────────────────────────────────────
        # Independent Kafka consumer → quality filter → enrich → Parquet
        logger.info("Starting Silver layer enrichment from Kafka")
        raw_silver = read_kafka_stream(spark, kafka_cfg)
        parsed_silver = parse_telemetry(raw_silver, kafka_cfg["schema_registry_url"])
        clean_silver = apply_quality_filters(parsed_silver)
        enriched_silver = enrich_telemetry(clean_silver)
        silver_query = write_silver(enriched_silver, spark_cfg["silver"], checkpoint_base)
        queries.append(silver_query)
        logger.info("Silver query started: %s", silver_query.name)

        # ── Stream 3: Gold Hourly Power + TimescaleDB ───────────────────
        # Independent Kafka consumer → enrich → 1h windowed aggregation
        # → foreachBatch → Parquet AND TimescaleDB in one batch.
        logger.info("Starting Gold hourly power aggregation from Kafka")
        raw_gold_power = read_kafka_stream(spark, kafka_cfg)
        parsed_gold_power = parse_telemetry(raw_gold_power, kafka_cfg["schema_registry_url"])
        clean_gold_power = apply_quality_filters(parsed_gold_power)
        enriched_gold_power = enrich_telemetry(clean_gold_power)
        hourly_power = aggregate_hourly_power(enriched_gold_power)

        gold_power_handler = _make_gold_power_handler(
            spark_cfg["gold"]["output_path"], db_cfg
        )
        gold_power_query = (
            hourly_power.writeStream
            .foreachBatch(gold_power_handler)
            .outputMode("append")
            .option("checkpointLocation", f"{checkpoint_base}/gold_power")
            .trigger(processingTime="1 minute")
            .queryName("gold_hourly_power")
            .start()
        )
        queries.append(gold_power_query)
        logger.info("Gold power query started: %s", gold_power_query.name)

        # ── Stream 4: Gold Anomaly Detection + TimescaleDB ──────────────
        # Independent Kafka consumer → enrich → anomaly threshold filter
        # → foreachBatch → Parquet AND TimescaleDB in one batch.
        logger.info("Starting Gold anomaly detection from Kafka")
        raw_gold_anomaly = read_kafka_stream(spark, kafka_cfg)
        parsed_gold_anomaly = parse_telemetry(raw_gold_anomaly, kafka_cfg["schema_registry_url"])
        clean_gold_anomaly = apply_quality_filters(parsed_gold_anomaly)
        enriched_gold_anomaly = enrich_telemetry(clean_gold_anomaly)
        anomalies = detect_anomalies(enriched_gold_anomaly)

        gold_anomaly_handler = _make_gold_anomaly_handler(
            spark_cfg["gold"]["output_path"], db_cfg
        )
        gold_anomaly_query = (
            anomalies.writeStream
            .foreachBatch(gold_anomaly_handler)
            .outputMode("append")
            .option("checkpointLocation", f"{checkpoint_base}/gold_anomalies")
            .trigger(processingTime="1 minute")
            .queryName("gold_anomalies")
            .start()
        )
        queries.append(gold_anomaly_query)
        logger.info("Gold anomaly query started: %s", gold_anomaly_query.name)

        # ── Await termination — resilient loop ──────────────────────────
        # If one query dies, keep running if others are still active.
        # This prevents a single query failure from killing the whole app.
        logger.info(
            "All %d streaming queries active — awaiting termination. "
            "Pipeline is now running continuously.",
            len(queries),
        )
        while True:
            try:
                spark.streams.awaitAnyTermination()
                # A query completed (possibly with error). Check survivors.
                active = spark.streams.active
                if not active:
                    logger.warning("All streaming queries have terminated.")
                    break
                logger.warning(
                    "A streaming query terminated. %d queries still active — continuing.",
                    len(active),
                )
                # Reset the terminated flag so awaitAnyTermination blocks again.
                spark.streams.resetTerminated()
            except Exception as e:
                logger.error("awaitAnyTermination raised: %s", e)
                if not spark.streams.active:
                    break
                spark.streams.resetTerminated()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received — stopping queries")
    except Exception:
        logger.exception("Pipeline encountered a fatal error during setup")
        sys.exit(1)
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        logger.info("Pipeline stopped cleanly.")


if __name__ == "__main__":
    main()
