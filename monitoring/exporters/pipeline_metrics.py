"""
Prometheus metrics exporter for the Wind Turbine streaming pipeline.

Exposes metrics on HTTP :8000/metrics covering:
  - Kafka producer throughput (events_produced_total)
  - Kafka delivery failures
  - Kafka consumer group lag (polled from broker via AdminClient)
  - Pipeline error counters
  - Data lake record counts (Bronze / Silver / Gold)
  - Spark batch duration placeholders
  - Anomaly detection counters
  - TimescaleDB write duration
  - End-to-end streaming latency

Run standalone:
    python -m monitoring.exporters.pipeline_metrics
"""

import logging
import os
import time
import threading

from prometheus_client import (
    start_http_server,
    Gauge, Counter, Histogram, Info,
)

logger = logging.getLogger("monitoring")

# ── Metric definitions ──────────────────────────────────────────────────────

PIPELINE_INFO = Info(
    "windturbine_pipeline",
    "Pipeline metadata",
)

EVENTS_PRODUCED = Counter(
    "windturbine_events_produced_total",
    "Total telemetry events produced to Kafka",
    ["turbine_id"],
)

EVENTS_CONSUMED = Counter(
    "windturbine_events_consumed_total",
    "Total events consumed by Spark streaming",
)

KAFKA_DELIVERY_FAILURES = Counter(
    "windturbine_kafka_delivery_failures_total",
    "Total Kafka delivery failures",
)

KAFKA_CONSUMER_LAG = Gauge(
    "windturbine_kafka_consumer_lag",
    "Kafka consumer group lag (messages behind)",
    ["topic", "partition"],
)

STREAMING_LATENCY = Histogram(
    "windturbine_streaming_latency_seconds",
    "End-to-end streaming latency from event generation to persistence",
    buckets=[0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60],
)

SPARK_BATCH_DURATION = Histogram(
    "windturbine_spark_batch_duration_seconds",
    "Duration of Spark streaming micro-batches",
    ["layer"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120],
)

PIPELINE_ERRORS = Counter(
    "windturbine_pipeline_errors_total",
    "Total pipeline errors by component",
    ["component", "error_type"],
)

BRONZE_RECORDS = Gauge(
    "windturbine_bronze_records_total",
    "Total records in the Bronze layer",
)

SILVER_RECORDS = Gauge(
    "windturbine_silver_records_total",
    "Total records in the Silver layer",
)

GOLD_RECORDS = Gauge(
    "windturbine_gold_records_total",
    "Total records in the Gold layer",
)

ANOMALIES_DETECTED = Counter(
    "windturbine_anomalies_detected_total",
    "Total anomalies detected",
    ["anomaly_type", "severity"],
)

DB_WRITE_DURATION = Histogram(
    "windturbine_db_write_duration_seconds",
    "Duration of TimescaleDB batch writes",
    ["table"],
    buckets=[0.1, 0.25, 0.5, 1, 2, 5, 10],
)


# ── Kafka consumer lag polling ──────────────────────────────────────────────

def _poll_kafka_lag():
    """Background thread that periodically polls Kafka consumer lag.

    Uses the AdminClient to compare committed offsets against log-end
    offsets for the spark-streaming-consumer group.
    """
    try:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import TopicPartition, KafkaException
    except ImportError:
        logger.warning("confluent_kafka not available — skipping lag polling")
        return

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.environ.get("KAFKA_TOPIC", "wind-turbine-telemetry")
    consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "spark-streaming-consumer")

    # Wait for Kafka to be available
    admin = None
    for attempt in range(30):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap})
            admin.list_topics(timeout=5)
            logger.info("Kafka lag poller connected to %s", bootstrap)
            break
        except Exception:
            time.sleep(5)

    if admin is None:
        logger.error("Kafka lag poller could not connect after retries")
        return

    while True:
        try:
            # Get the topic metadata to find partition count
            metadata = admin.list_topics(topic=topic, timeout=10)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                time.sleep(15)
                continue

            partition_count = len(topic_metadata.partitions)

            # Get committed offsets for the consumer group
            from confluent_kafka import Consumer
            tmp_consumer = Consumer({
                "bootstrap.servers": bootstrap,
                "group.id": consumer_group,
                "enable.auto.commit": False,
            })

            partitions = [TopicPartition(topic, p) for p in range(partition_count)]

            # Get committed offsets
            committed = tmp_consumer.committed(partitions, timeout=10)

            # Get high watermarks (log-end offsets)
            for tp in committed:
                try:
                    lo, hi = tmp_consumer.get_watermark_offsets(tp, timeout=10)
                    committed_offset = tp.offset if tp.offset >= 0 else 0
                    lag = max(0, hi - committed_offset)
                    KAFKA_CONSUMER_LAG.labels(
                        topic=topic,
                        partition=str(tp.partition),
                    ).set(lag)
                except Exception:
                    pass

            tmp_consumer.close()

        except Exception as e:
            logger.error("Failed to poll Kafka lag: %s", e)

        time.sleep(15)


# ── TimescaleDB record count polling ────────────────────────────────────────

def _poll_db_record_counts():
    """Background thread that periodically counts records in TimescaleDB."""
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 not available — skipping DB record polling")
        return

    db_host = os.environ.get("TIMESCALEDB_HOST", "timescaledb")
    db_port = os.environ.get("TIMESCALEDB_PORT", "5432")
    db_name = os.environ.get("TIMESCALEDB_DB", "windturbine")
    db_user = os.environ.get("TIMESCALEDB_USER", "windturbine")
    db_pass = os.environ.get("TIMESCALEDB_PASSWORD", "changeme_in_production")

    dsn = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}"

    # Wait for DB to be available
    conn = None
    for attempt in range(30):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = True
            logger.info("DB record count poller connected to %s:%s", db_host, db_port)
            break
        except Exception:
            time.sleep(5)

    if conn is None:
        logger.error("DB record count poller could not connect after retries")
        return

    while True:
        try:
            with conn.cursor() as cur:
                # Count hourly_power_generation rows (Gold layer in DB)
                cur.execute("SELECT COUNT(*) FROM hourly_power_generation")
                gold_count = cur.fetchone()[0]
                GOLD_RECORDS.set(gold_count)

                cur.execute("SELECT COUNT(*) FROM turbine_anomalies")
                anomaly_count = cur.fetchone()[0]
                # Use anomaly count as a proxy visible in metrics
                # (actual anomaly counters are incremented by the spark app)

        except Exception as e:
            logger.error("Failed to poll DB record counts: %s", e)
            # Reconnect
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = psycopg2.connect(dsn)
                conn.autocommit = True
            except Exception:
                pass

        time.sleep(30)


# ── Server entry point ──────────────────────────────────────────────────────

def start_metrics_server(port: int = 8000) -> None:
    """Start the Prometheus metrics HTTP server.

    Args:
        port: Port to expose /metrics on.
    """
    PIPELINE_INFO.info({
        "version": "1.0.0",
        "platform": "wind-turbine-streaming",
    })

    # Start Kafka lag poller in background
    lag_thread = threading.Thread(target=_poll_kafka_lag, daemon=True)
    lag_thread.start()

    # Start DB record count poller in background
    db_thread = threading.Thread(target=_poll_db_record_counts, daemon=True)
    db_thread.start()

    start_http_server(port)
    logger.info("Prometheus metrics server started on :%d", port)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )
    start_metrics_server()
    # Keep alive
    while True:
        time.sleep(3600)
