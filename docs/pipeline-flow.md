# Data Pipeline Flow

> Step-by-step walkthrough of how data flows from simulated IoT sensors to analytics dashboards.

## End-to-End Event Lifecycle

```
Turbine Sensor → Simulator → Avro Serialize → Kafka Topic → Spark Consumers
                                                                    │
                                              ┌─────────────────────┼─────────────────────┐
                                              │                     │                     │
                                         Bronze Layer          Silver Layer          Gold Layer
                                              │                     │                     │
                                         Raw Parquet          Clean Parquet     ┌─────────┴─────────┐
                                          (MinIO)              (MinIO)       Hourly Agg       Anomalies
                                                                                │                 │
                                                                           Parquet +         Parquet +
                                                                           TimescaleDB       TimescaleDB
                                                                                │                 │
                                                                                └────────┬────────┘
                                                                                         │
                                                                                      Grafana
```

## Stage 1: Event Generation

The simulator (`simulator/turbine_simulator.py`) generates telemetry for **12 wind turbines** using physics-based models.

**Event schema** (12 fields):

| Field | Type | Description |
|-------|------|-------------|
| `turbine_id` | string | Unique turbine identifier (e.g., `WT-001`) |
| `timestamp` | string | ISO 8601 UTC timestamp |
| `wind_speed` | double | Wind speed in m/s |
| `power_output` | double | Power output in kW |
| `generator_temperature` | double | Generator temp in °C |
| `gearbox_temperature` | double | Gearbox temp in °C |
| `ambient_temperature` | double | Ambient air temp in °C |
| `rotor_rpm` | double | Rotor speed in RPM |
| `blade_pitch_angle` | double | Blade pitch in degrees |
| `nacelle_direction` | double | Nacelle heading in degrees |
| `vibration_level` | double | Vibration in mm/s |
| `turbine_efficiency` | double | Operating efficiency (0–1) |

**Physics models applied**:
- Cubic power curve with cut-in (3 m/s), rated (12 m/s), and cut-out (25 m/s) wind speeds
- Auto-correlated wind speed with smoothing factor for realistic variation
- Generator/gearbox temperatures correlated with load factor

## Stage 2: Kafka Ingestion

The producer (`producer/kafka_producer.py`) serializes events and publishes to Kafka.

**Producer configuration**:
- Topic: `wind-turbine-telemetry` (3 partitions, 7-day retention)
- Serialization: JSON with Avro schema validation via `fastavro`
- Delivery: Idempotent (`enable.idempotence=true`)
- Compression: Snappy
- Partitioning: Keyed by `turbine_id` for ordered per-turbine delivery
- Schema Registry: Avro schema registered on startup

**Delivery guarantees**:
- `acks=all` — waits for all in-sync replicas to acknowledge
- Idempotent producer — prevents duplicate messages on retry
- Delivery callbacks log success/failure per message

## Stage 3: Bronze Layer (Raw Ingestion)

**Source**: `spark/jobs/bronze_layer.py`

```
Kafka → JSON Deserialize → Add Metadata → Parquet (MinIO)
```

- Reads from Kafka with `startingOffsets=earliest`
- Parses JSON message values into structured columns using `TELEMETRY_SCHEMA`
- Adds `ingestion_timestamp` for lineage tracking
- Writes as **append-only Parquet** to `s3a://wind-turbine-bronze/`
- Partitioned by `event_date` and `turbine_id`
- **No transformations** — serves as the immutable source of truth for replay

**Trigger**: Every 30 seconds (micro-batch)

## Stage 4: Silver Layer (Cleaned & Enriched)

**Source**: `spark/jobs/silver_layer.py`

```
Kafka → JSON Deserialize → Quality Filter → Enrichment → Parquet (MinIO)
```

**Quality filters** (out-of-range values removed):

| Sensor | Valid Range |
|--------|-------------|
| Wind Speed | 0–35 m/s |
| Power Output | 0–3500 kW |
| Generator Temp | -20–150 °C |
| Gearbox Temp | -20–120 °C |
| Ambient Temp | -40–60 °C |
| Rotor RPM | 0–25 |
| Vibration Level | 0–10 mm/s |

**Derived metrics computed**:

| Metric | Formula | Purpose |
|--------|---------|---------|
| `capacity_factor` | `power_output / 2500.0` | Actual vs. rated power ratio |
| `temp_delta_generator` | `generator_temp - ambient_temp` | Overheating indicator |
| `temp_delta_gearbox` | `gearbox_temp - ambient_temp` | Overheating indicator |
| `is_generating` | `power_output > 0` | Boolean generation flag |
| `wind_speed_bin` | Categorical binning | calm / light / moderate / strong / extreme |

**Trigger**: Every 30 seconds (micro-batch)

## Stage 5: Gold Layer (Aggregated Analytics)

**Source**: `spark/jobs/gold_layer.py`

### Hourly Power Aggregation

```
Kafka → Enrich → 1-Hour Tumbling Window → Aggregation → Parquet + TimescaleDB
```

Computed per turbine per hour:
- `avg_power`, `max_power`, `min_power`
- `total_energy_kwh` (sum of power output)
- `avg_efficiency`, `avg_capacity_factor`
- `record_count` (events in window)

Written to both MinIO Parquet and `hourly_power_generation` hypertable via JDBC.

### Anomaly Detection

```
Kafka → Enrich → Rule-Based Filter → Severity Classification → Parquet + TimescaleDB
```

**Detection rules**:

| Anomaly Type | Condition | Severity |
|-------------|-----------|----------|
| Generator Overheating | `generator_temp > 85°C` | CRITICAL |
| Gearbox Overheating | `gearbox_temp > 75°C` | WARNING |
| High Vibration | `vibration_level > 1.5 mm/s` | WARNING |
| Unexpected Zero Power | `power_output = 0` AND `wind_speed` in generation range | CRITICAL |

Written to both MinIO Parquet and `turbine_anomalies` hypertable via JDBC.

## Stage 6: Serving & Visualization

### TimescaleDB

Three hypertables serve analytical queries with automatic time-based partitioning:

- `turbine_metrics` — raw metrics (30-day retention)
- `hourly_power_generation` — hourly aggregates (1-year retention)
- `turbine_anomalies` — anomaly events (90-day retention)

A **continuous aggregate** (`fleet_daily_summary`) refreshes hourly, pre-computing daily fleet-wide statistics for fast dashboard queries.

### Grafana

Auto-provisioned dashboards visualize:
- Real-time event throughput and Kafka consumer lag
- Power generation trends per turbine
- Anomaly frequency and severity distribution
- Data lake layer record counts
- Spark batch processing duration

Datasources: Prometheus (metrics) and TimescaleDB (time-series SQL queries).

## Fault Tolerance

| Failure Scenario | Recovery Mechanism |
|-----------------|-------------------|
| Kafka broker restart | Spark consumer resumes from checkpointed offsets |
| Spark job crash | Checkpoint-based recovery, Airflow auto-restart |
| MinIO unavailable | Spark retries with backoff; no data loss due to Kafka retention |
| TimescaleDB down | JDBC write fails gracefully; Gold Parquet still written |
| Late-arriving data | Watermarking handles events within configurable delay window |
