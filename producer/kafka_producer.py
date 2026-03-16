"""
Production-grade Kafka producer for wind turbine telemetry.

Features:
- JSON serialization with Avro schema validation via fastavro
- Schema Registry integration for governance (best-effort registration)
- Idempotent delivery with configurable acks
- Structured logging with delivery callbacks
- Connection retry logic for Kafka and Schema Registry
- Graceful shutdown with message flushing
"""

import json
import io
import logging
import time
from typing import Optional

import fastavro
from confluent_kafka import Producer, KafkaError

from schemas.registry import (
    load_avro_schema, get_schema_registry_client, register_schema,
)

logger = logging.getLogger("producer")


class TelemetryProducer:
    """Kafka producer that serializes turbine telemetry events as JSON.

    Events are validated against the Avro schema using fastavro before
    being sent as JSON to Kafka. The schema is also registered with
    Schema Registry for governance (best-effort, non-blocking).
    """

    def __init__(self, config: dict):
        kafka_cfg = config["kafka"]
        self._topic = kafka_cfg["topic"]

        # Load Avro schema for validation
        self._schema_dict = load_avro_schema("turbine_telemetry")
        self._parsed_schema = fastavro.parse_schema(self._schema_dict)

        # Best-effort Schema Registry registration (non-blocking)
        self._register_schema_with_retry(
            kafka_cfg.get("schema_registry_url", "http://localhost:8081"),
            max_retries=5,
            retry_delay=3,
        )

        # Producer configuration with retry logic
        producer_cfg = kafka_cfg.get("producer", {})
        self._producer = self._create_producer_with_retry(
            kafka_cfg["bootstrap_servers"],
            producer_cfg,
            max_retries=10,
            retry_delay=3,
        )

        # Counters
        self._delivered = 0
        self._failed = 0

        logger.info(
            "TelemetryProducer initialized",
            extra={
                "topic": self._topic,
                "bootstrap_servers": kafka_cfg["bootstrap_servers"],
            },
        )

    def _register_schema_with_retry(self, sr_url: str,
                                     max_retries: int = 5,
                                     retry_delay: float = 3) -> None:
        """Register the Avro schema with Schema Registry (best-effort)."""
        subject = f"{self._topic}-value"
        for attempt in range(1, max_retries + 1):
            try:
                sr_client = get_schema_registry_client(sr_url)
                schema_id = register_schema(sr_client, subject, self._schema_dict)
                logger.info(
                    "Schema registered with Schema Registry",
                    extra={"subject": subject, "schema_id": schema_id},
                )
                return
            except Exception as e:
                logger.warning(
                    "Schema Registry registration attempt %d/%d failed: %s",
                    attempt, max_retries, e,
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)

        logger.warning(
            "Schema Registry registration failed after %d attempts. "
            "Continuing without registration — schema validation still active.",
            max_retries,
        )

    def _create_producer_with_retry(self, bootstrap_servers: str,
                                     producer_cfg: dict,
                                     max_retries: int = 10,
                                     retry_delay: float = 3) -> Producer:
        """Create Kafka producer with retry logic for broker connectivity."""
        config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": str(producer_cfg.get("acks", "all")),
            "retries": producer_cfg.get("retries", 5),
            "retry.backoff.ms": producer_cfg.get("retry_backoff_ms", 500),
            "batch.size": producer_cfg.get("batch_size", 16384),
            "linger.ms": producer_cfg.get("linger_ms", 10),
            "compression.type": producer_cfg.get("compression_type", "snappy"),
            "max.in.flight.requests.per.connection":
                producer_cfg.get("max_in_flight_requests", 5),
            "enable.idempotence":
                producer_cfg.get("enable_idempotence", True),
            "socket.timeout.ms": 10000,
            "message.timeout.ms": 30000,
        }

        for attempt in range(1, max_retries + 1):
            try:
                producer = Producer(config)
                # Test connectivity by listing topics
                metadata = producer.list_topics(timeout=10)
                logger.info(
                    "Kafka producer connected (brokers: %d, topics: %d)",
                    len(metadata.brokers), len(metadata.topics),
                )
                return producer
            except Exception as e:
                logger.warning(
                    "Kafka connection attempt %d/%d failed: %s",
                    attempt, max_retries, e,
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)

        # Return producer anyway — it will retry internally
        logger.warning(
            "Could not verify Kafka connectivity after %d attempts. "
            "Producer created — will retry on first produce.",
            max_retries,
        )
        return Producer(config)

    # ── delivery callback ───────────────────────────────────────────────

    def _on_delivery(self, err: Optional[KafkaError], msg) -> None:
        if err is not None:
            self._failed += 1
            logger.error(
                "Delivery failed: %s (topic=%s, partition=%s)",
                err, msg.topic(), msg.partition(),
            )
        else:
            self._delivered += 1
            if self._delivered % 1000 == 0:
                logger.info(
                    "Delivery milestone: delivered=%d, failed=%d",
                    self._delivered, self._failed,
                )

    # ── validation ────────────────────────────────────────────────────

    def _validate_event(self, event: dict) -> bool:
        """Validate an event against the Avro schema using fastavro."""
        try:
            # Write to a throwaway buffer to validate
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, self._parsed_schema, event)
            return True
        except Exception as e:
            logger.error("Event validation failed: %s — event: %s", e, event)
            return False

    # ── public API ──────────────────────────────────────────────────────

    def produce(self, event: dict) -> None:
        """Validate and send a telemetry event as JSON to Kafka."""
        try:
            if not self._validate_event(event):
                self._failed += 1
                return

            value = json.dumps(event).encode("utf-8")
            self._producer.produce(
                topic=self._topic,
                key=event["turbine_id"].encode("utf-8"),
                value=value,
                on_delivery=self._on_delivery,
            )
            # Trigger delivery callbacks without blocking
            self._producer.poll(0)
        except BufferError:
            logger.warning("Producer queue full — flushing")
            self._producer.flush(timeout=5)
            # Retry once after flush
            value = json.dumps(event).encode("utf-8")
            self._producer.produce(
                topic=self._topic,
                key=event["turbine_id"].encode("utf-8"),
                value=value,
                on_delivery=self._on_delivery,
            )
        except Exception:
            self._failed += 1
            logger.exception("Failed to produce event")

    def flush(self, timeout: float = 30) -> int:
        """Flush any outstanding messages."""
        remaining = self._producer.flush(timeout)
        logger.info(
            "Producer flushed: remaining=%d, delivered=%d, failed=%d",
            remaining, self._delivered, self._failed,
        )
        return remaining

    @property
    def stats(self) -> dict:
        return {"delivered": self._delivered, "failed": self._failed}
