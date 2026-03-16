# System Design

> Design decisions, trade-offs, and rationale behind the Wind Turbine Streaming Platform.

## Design Principles

1. **Immutability first** — Raw events are never modified; Bronze layer serves as the replayable source of truth
2. **Separation of concerns** — Each pipeline layer has a single responsibility (ingest, clean, aggregate)
3. **Local-first** — Entire stack runs on a single machine via Docker Compose with no cloud dependency
4. **Observable by default** — Every component exports metrics; dashboards are pre-provisioned
5. **Fail gracefully** — Pipeline components handle downstream failures without data loss

## Key Design Decisions

### Why Kafka KRaft (No ZooKeeper)?

Kafka's KRaft mode eliminates the ZooKeeper dependency entirely:

- **Fewer moving parts** — one less stateful service to operate and monitor
- **Faster startup** — metadata quorum is built into the broker process
- **Simpler Docker setup** — no ZooKeeper container, no cross-service coordination
- **Production-ready** — KRaft has been GA since Kafka 3.3 and is the recommended deployment mode

### Why 4 Independent Kafka Consumers?

Each layer (Bronze, Silver, Gold Power, Gold Anomaly) maintains its own Kafka consumer rather than chaining file reads:

```
                    ┌── Consumer 1 ──▶ Bronze (Parquet)
Kafka Topic ────────┼── Consumer 2 ──▶ Silver (Parquet)
                    ├── Consumer 3 ──▶ Gold Power (Parquet + TSDB)
                    └── Consumer 4 ──▶ Gold Anomaly (Parquet + TSDB)
```

**Trade-offs**:
- (+) Each layer can be scaled, restarted, or backfilled independently
- (+) No coupling between layers — Bronze failure doesn't block Silver
- (+) Each consumer maintains its own checkpoint and offset position
- (-) 4x Kafka read bandwidth (acceptable for single-partition development workloads)
- (-) Slightly higher memory footprint for 4 streaming queries

### Why Avro + Schema Registry?

- **Schema evolution** — fields can be added/removed with forward/backward compatibility
- **Compact binary format** — smaller payloads than JSON over the wire
- **Contract enforcement** — producers and consumers agree on the schema at compile time
- **Schema Registry** — centralized schema management with version history

### Why MinIO Over Cloud S3?

- **Zero cloud cost** — no AWS account or billing required for development
- **Identical API** — uses the same `s3a://` filesystem and AWS SDK under the hood
- **Local development** — data stays on your machine, no network latency
- **Easy migration** — switching to real S3 requires only changing the endpoint and credentials

### Why Parquet?

- **Columnar storage** — analytical queries read only the columns they need
- **Efficient compression** — Snappy/ZSTD compression reduces storage by 5-10x vs. JSON
- **Schema enforcement** — embedded schema prevents type drift
- **Ecosystem support** — native read support in Spark, Pandas, DuckDB, Presto, and most query engines

### Why TimescaleDB Over InfluxDB?

- **PostgreSQL compatibility** — standard SQL, no new query language to learn
- **Hypertables** — automatic time-based partitioning with transparent query routing
- **Continuous aggregates** — materialized views that refresh incrementally (used for `fleet_daily_summary`)
- **Retention policies** — automatic data lifecycle management (30 days raw, 90 days anomalies, 1 year aggregates)
- **Rich ecosystem** — works with any PostgreSQL client, Grafana, and JDBC drivers

### Why `foreachBatch` Sinks?

Spark Structured Streaming's `foreachBatch` enables:

- **Dual-write** — a single micro-batch writes to both Parquet (MinIO) and JDBC (TimescaleDB)
- **Transaction control** — each batch can be written atomically to the database
- **Custom logic** — column selection, renaming, and type casting before JDBC write
- **Retry semantics** — failed batches are retried with checkpoint-based idempotency

## Data Modeling

### Lakehouse Layer Strategy

```
Bronze (Raw)          Silver (Clean)         Gold (Analytics)
─────────────         ──────────────         ────────────────
• Append-only         • Quality-filtered     • Windowed aggregations
• No transforms       • Derived metrics      • Anomaly detection
• Source of truth     • UTC-normalized       • Dual-write to TSDB
• For replay          • For ad-hoc queries   • For dashboards
```

### TimescaleDB Schema

**Hypertable partitioning**: All three tables use `timestamp` as the time dimension with automatic chunk intervals managed by TimescaleDB.

**Retention policies**:
| Table | Retention | Rationale |
|-------|-----------|-----------|
| `turbine_metrics` | 30 days | High-frequency raw data; long-term stored in MinIO |
| `turbine_anomalies` | 90 days | Anomalies need longer investigation windows |
| `hourly_power_generation` | 1 year | Aggregated data is small; useful for trend analysis |

**Continuous aggregate** (`fleet_daily_summary`):
- Refreshes every hour
- Pre-computes daily fleet-wide averages from `hourly_power_generation`
- Enables sub-second dashboard queries over the full retention window

### Indexing Strategy

- Primary indexes on `(turbine_id, timestamp)` for time-range + turbine-scoped queries
- TimescaleDB automatically creates chunk-level indexes
- No additional secondary indexes (query patterns are predictable)

## Monitoring Design

### Three-Tier Observability

```
Tier 1: Infrastructure    Tier 2: Pipeline           Tier 3: Business
─────────────────────     ──────────────────         ──────────────────
• Kafka broker health     • Consumer lag             • Anomalies/hour
• Container status        • Batch duration           • Power generation
• Disk/memory usage       • Events/second            • Fleet efficiency
• Network connectivity    • Error rates              • Turbine downtime
```

### Custom Pipeline Exporter

The `pipeline_metrics.py` exporter bridges infrastructure and business metrics:

| Metric Type | Examples |
|-------------|----------|
| **Counters** | `events_produced_total`, `delivery_failures_total`, `anomalies_detected_total` |
| **Gauges** | `kafka_consumer_lag`, `bronze_record_count`, `silver_record_count`, `gold_record_count` |
| **Histograms** | `streaming_latency_seconds`, `spark_batch_duration_seconds`, `db_write_duration_seconds` |

### Airflow Health Checks

The DAG runs every hour and validates:

1. **Kafka availability** — broker responds to API version request
2. **Schema Registry** — HTTP health endpoint returns 200
3. **Consumer lag** — lag < 1000 messages (configurable threshold)
4. **Data freshness** — Bronze layer has data within the last 2 hours
5. **Record counts** — TimescaleDB tables are being populated
6. **Auto-recovery** — restarts Spark container if health checks fail

## Scalability Considerations

### Current Design (Single-Node Docker Compose)

This deployment is optimized for **development, demonstration, and portfolio presentation**. All 15 services run on a single machine.

### Path to Production

| Component | Current | Production |
|-----------|---------|------------|
| Kafka | Single broker, KRaft | Multi-broker cluster with rack awareness |
| Spark | `local[*]` mode | YARN/K8s cluster with dedicated executors |
| MinIO | Single instance | Distributed mode (4+ nodes) or migrate to S3 |
| TimescaleDB | Single instance | Multi-node with replication |
| Airflow | LocalExecutor | CeleryExecutor or KubernetesExecutor |
| Monitoring | Single Prometheus | Thanos or Cortex for long-term storage |

### Estimated Throughput

With the current single-node setup:
- **Simulator**: 12 events/second (12 turbines x 1 event/second)
- **Kafka**: Handles 100K+ events/second (well under capacity)
- **Spark**: Processes micro-batches every 30 seconds
- **TimescaleDB**: Handles 10K+ inserts/second with hypertables

The architecture is designed to scale horizontally — adding Kafka partitions, Spark executors, and TimescaleDB nodes requires configuration changes, not code changes.
