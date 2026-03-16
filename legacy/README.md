# Legacy Scripts

Early prototypes used during initial development. These scripts demonstrate the iterative
evolution from standalone scripts to the current production-grade streaming architecture.

| Script | Purpose |
|--------|---------|
| `wind_turbine_sensorlog.py` | Initial sensor data generator (file-based output) |
| `kafka_producer_v1.py` | First Kafka producer (reads from log file) |
| `kafka_consumer_check.py` | Simple consumer for data verification |
| `kafka_consumer_to_timescaledb.py` | Direct Kafka-to-TimescaleDB consumer (pre-Spark) |

These have been superseded by the modular architecture in `simulator/`, `producer/`, and `spark/`.
