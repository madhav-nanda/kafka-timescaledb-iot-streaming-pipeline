# System Architecture

> Detailed architecture reference for the Wind Turbine Real-Time Streaming Platform.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     WIND TURBINE STREAMING PLATFORM                     │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION                                                          │
│                                                                          │
│  ┌─────────────┐    Avro     ┌──────────────────┐                       │
│  │ IoT Turbine │───────────▶ │  Kafka Producer  │                       │
│  │ Simulator   │  Serialized │  (Idempotent,    │                       │
│  │ (12 units)  │   Events    │   Snappy Comp.)  │                       │
│  └─────────────┘             └────────┬─────────┘                       │
│                                       │                                  │
│                              ┌────────▼─────────┐  ┌─────────────────┐  │
│                              │  Kafka Broker    │  │ Schema Registry │  │
│                              │  (KRaft Mode)    │◀─│ (Avro Schemas)  │  │
│                              │  No ZooKeeper    │  └─────────────────┘  │
│                              └────────┬─────────┘                       │
└───────────────────────────────────────┼──────────────────────────────────┘
                                        │
┌───────────────────────────────────────┼──────────────────────────────────┐
│  STREAM PROCESSING (PySpark Structured Streaming)                        │
│                                       │                                  │
│                              ┌────────▼─────────┐                       │
│                              │  Kafka Source     │                       │
│                              │  (4 consumers)    │                       │
│                              └────────┬─────────┘                       │
│                                       │                                  │
│  ┌────────────────────────────────────▼──────────────────────────────┐   │
│  │  BRONZE LAYER (Raw Ingestion)                                     │   │
│  │  • Deserialize Kafka JSON messages                                │   │
│  │  • Add ingestion timestamp + processing metadata                  │   │
│  │  • Partition by event_date + turbine_id                           │   │
│  │  • Write as Parquet → MinIO (immutable, replayable)               │   │
│  └────────────────────────────────────┬──────────────────────────────┘   │
│                                       │                                  │
│  ┌────────────────────────────────────▼──────────────────────────────┐   │
│  │  SILVER LAYER (Cleaned & Enriched)                                │   │
│  │  • Filter out-of-range sensor readings                            │   │
│  │  • Normalize timestamps to UTC                                    │   │
│  │  • Compute: capacity_factor, temp_deltas, wind_speed_bin          │   │
│  │  • Write as Parquet → MinIO                                       │   │
│  └────────────────────────────────────┬──────────────────────────────┘   │
│                                       │                                  │
│  ┌────────────────────────────────────▼──────────────────────────────┐   │
│  │  GOLD LAYER (Aggregated Analytics)                                │   │
│  │                                                                    │   │
│  │  ┌──────────────────────┐  ┌──────────────────────────────────┐   │   │
│  │  │ Hourly Power Agg     │  │ Anomaly Detection                │   │   │
│  │  │ • avg/max/min power  │  │ • Generator temp > 85°C          │   │   │
│  │  │ • total_energy_kwh   │  │ • Gearbox temp > 75°C            │   │   │
│  │  │ • avg efficiency     │  │ • Vibration > 1.5 mm/s           │   │   │
│  │  │ • capacity factor    │  │ • Zero power in wind range       │   │   │
│  │  └──────────┬───────────┘  └──────────┬───────────────────────┘   │   │
│  │             │                          │                           │   │
│  └─────────────┼──────────────────────────┼──────────────────────────┘   │
│                │                          │                              │
└────────────────┼──────────────────────────┼──────────────────────────────┘
                 │                          │
┌────────────────┼──────────────────────────┼──────────────────────────────┐
│  DATA STORAGE  │                          │                              │
│                │                          │                              │
│  ┌─────────────▼──────────────────────────▼──────────────────────────┐   │
│  │  MinIO (S3-Compatible Object Storage)                             │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐    │   │
│  │  │   Bronze      │  │   Silver     │  │       Gold           │    │   │
│  │  │   Bucket      │  │   Bucket     │  │      Bucket          │    │   │
│  │  │  (Raw Parquet) │  │ (Clean)     │  │ (Aggregated)         │    │   │
│  │  └──────────────┘  └──────────────┘  └──────────────────────┘    │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │  TimescaleDB (Serving Layer)                                      │   │
│  │  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐  │   │
│  │  │ turbine_metrics  │ │ hourly_power_    │ │ turbine_         │  │   │
│  │  │  (hypertable)    │ │  generation      │ │  anomalies       │  │   │
│  │  │  30-day retain   │ │  (hypertable)    │ │  (hypertable)    │  │   │
│  │  └──────────────────┘ └──────────────────┘ └──────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────────────────────┐ │   │
│  │  │ fleet_daily_summary (continuous aggregate, refreshed hourly) │ │   │
│  │  └──────────────────────────────────────────────────────────────┘ │   │
│  └───────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  OPERATIONS & OBSERVABILITY                                              │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────────────────────┐   │
│  │ Apache Airflow   │  │  Prometheus  │  │  Grafana Dashboards       │   │
│  │ • Health checks  │  │ • Kafka lag  │  │ • Event throughput        │   │
│  │ • Data freshness │  │ • Spark      │  │ • Consumer lag            │   │
│  │ • Record counts  │  │   batch time │  │ • Batch durations         │   │
│  │ • Auto-recovery  │  │ • Errors     │  │ • Anomaly rates           │   │
│  └─────────────────┘  └──────────────┘  └───────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### IoT Simulator (`simulator/`)

The simulator generates realistic wind turbine telemetry for **12 turbines** using physics-based models:

- **Cubic power curve**: Models the relationship between wind speed and power output with cut-in (3 m/s), rated (12 m/s), and cut-out (25 m/s) thresholds
- **Auto-correlated wind speed**: Realistic wind patterns using smoothing with configurable volatility
- **Correlated temperatures**: Generator and gearbox temperatures derived from load factor and ambient conditions
- **Event rate**: Configurable interval (default: 1 event/turbine/second)

Each event contains 12 telemetry fields defined in the Avro schema (`schemas/turbine_telemetry.avsc`).

### Kafka Producer (`producer/`)

Production-grade message producer with:

- **Avro serialization** via `fastavro` with Schema Registry integration
- **Idempotent delivery** (`enable.idempotence=true`) for exactly-once Kafka semantics
- **Snappy compression** for reduced network bandwidth
- **Key-based partitioning** by `turbine_id` for ordered per-turbine processing
- **Delivery callbacks** with structured logging
- **Graceful shutdown** with message flushing on SIGTERM/SIGINT

### Kafka Broker

- **KRaft mode** (no ZooKeeper) — simplified operations and reduced resource footprint
- **3 partitions** for the `wind-turbine-telemetry` topic
- **7-day retention** with delete cleanup policy
- **JMX metrics** exposed for Prometheus scraping

### Spark Structured Streaming (`spark/`)

The pipeline orchestrator runs **4 independent streaming queries**, each reading directly from Kafka:

1. **Bronze Query**: Raw ingestion → Parquet (append mode, 30s trigger)
2. **Silver Query**: Quality filter + enrichment → Parquet (append mode, 30s trigger)
3. **Gold Power Query**: 1-hour tumbling window aggregation → Parquet + TimescaleDB
4. **Gold Anomaly Query**: Rule-based anomaly detection → Parquet + TimescaleDB

Key configuration:
- `foreachBatch` sinks for dual-write (Parquet + JDBC)
- Watermarking for late data handling
- S3A filesystem with MinIO endpoint
- Checkpoint locations in MinIO for fault tolerance

### MinIO Data Lake

S3-compatible object storage organized as a **lakehouse**:

| Bucket | Content | Format | Partitioning |
|--------|---------|--------|-------------|
| `wind-turbine-bronze` | Raw events | Parquet | `event_date` / `turbine_id` |
| `wind-turbine-silver` | Cleaned + enriched | Parquet | `event_date` / `turbine_id` |
| `wind-turbine-gold` | Aggregations + anomalies | Parquet | By query type |
| `wind-turbine-checkpoints` | Spark streaming checkpoints | Internal | By query |

### TimescaleDB

PostgreSQL-based time-series database serving the analytics layer:

| Table | Type | Retention | Purpose |
|-------|------|-----------|---------|
| `turbine_metrics` | Hypertable | 30 days | Raw metric serving |
| `hourly_power_generation` | Hypertable | 1 year | Hourly aggregated power stats |
| `turbine_anomalies` | Hypertable | 90 days | Detected anomalies with severity |
| `fleet_daily_summary` | Continuous Aggregate | — | Auto-refreshed daily fleet overview |

### Observability Stack

**Prometheus** scrapes metrics from 5 targets every 10-15 seconds:
- `kafka-exporter` — consumer lag, partition offsets
- `timescaledb-exporter` — PostgreSQL and hypertable metrics
- `pipeline-exporter` — custom pipeline metrics (throughput, latency, anomaly counts)
- `minio` — object storage metrics
- Self-monitoring

**Grafana** dashboards are auto-provisioned with two datasources (Prometheus + TimescaleDB).

**Airflow** runs hourly health checks:
- Kafka broker and Schema Registry availability
- Consumer lag monitoring (alert if > 1000 messages behind)
- Bronze layer data freshness
- TimescaleDB record count validation
- Automatic Spark job restart on failure

## Port Map

| Port | Service | Access |
|------|---------|--------|
| 9092 | Kafka (host access) | Producers/consumers outside Docker |
| 8081 | Schema Registry | Schema management API |
| 8080 | Kafka UI | Web browser |
| 9000 | MinIO API | S3-compatible API |
| 9001 | MinIO Console | Web browser |
| 5432 | TimescaleDB | PostgreSQL clients |
| 4040 | Spark UI | Web browser |
| 9090 | Prometheus | Metrics queries |
| 3000 | Grafana | Web browser |
| 8082 | Airflow Webserver | Web browser |
| 8000 | Pipeline Exporter | Prometheus scrape target |
| 9308 | Kafka Exporter | Prometheus scrape target |
| 9187 | TimescaleDB Exporter | Prometheus scrape target |
