"""
Entry point for the Wind Turbine IoT Telemetry Simulator + Kafka Producer.

Usage:
    python -m simulator.main
"""

import logging
import signal
import sys
import time

import requests

from configs import load_config
from producer.kafka_producer import TelemetryProducer
from simulator.turbine_simulator import event_stream

logger = logging.getLogger("simulator")
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Shutdown signal received (signal=%d)", signum)
    _shutdown = True


def _wait_for_services(config: dict, max_wait: int = 120) -> bool:
    """Wait for Kafka and Schema Registry to be available."""
    kafka_cfg = config["kafka"]
    sr_url = kafka_cfg.get("schema_registry_url", "http://localhost:8081")

    logger.info("Waiting for Schema Registry at %s ...", sr_url)
    start = time.monotonic()
    while time.monotonic() - start < max_wait:
        try:
            resp = requests.get(sr_url, timeout=5)
            if resp.status_code == 200:
                logger.info("Schema Registry is ready")
                return True
        except Exception:
            pass
        time.sleep(3)

    logger.warning(
        "Schema Registry not reachable after %ds — proceeding anyway", max_wait
    )
    return False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )

    config = load_config()
    sim_cfg = config["simulator"]

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Wait for infrastructure services to be ready
    _wait_for_services(config, max_wait=90)

    producer = TelemetryProducer(config)

    logger.info(
        "Simulator starting — streaming events for %d turbines",
        sim_cfg["num_turbines"],
    )

    event_count = 0
    try:
        for event in event_stream(sim_cfg):
            if _shutdown:
                break
            producer.produce(event)
            event_count += 1
            if event_count % 500 == 0:
                logger.info(
                    "Events produced: %d (delivered=%d, failed=%d)",
                    event_count,
                    producer.stats["delivered"],
                    producer.stats["failed"],
                )
    except Exception:
        logger.exception("Simulator encountered a fatal error")
        sys.exit(1)
    finally:
        logger.info("Flushing producer — total events: %d", event_count)
        producer.flush()
        logger.info("Simulator stopped cleanly")


if __name__ == "__main__":
    main()
