-- =============================================================================
-- Wind Turbine Streaming Platform — TimescaleDB Schema
--
-- This migration creates the core tables and hypertables for the serving layer.
-- Executed automatically on container startup via docker-entrypoint-initdb.d.
-- =============================================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────── Raw Turbine Metrics ─────────────────────────────────
-- Individual telemetry readings (written by Silver/Gold layer for real-time queries)

CREATE TABLE IF NOT EXISTS turbine_metrics (
    time                    TIMESTAMPTZ     NOT NULL,
    turbine_id              VARCHAR(20)     NOT NULL,
    wind_speed              DOUBLE PRECISION,
    power_output            DOUBLE PRECISION,
    generator_temperature   DOUBLE PRECISION,
    gearbox_temperature     DOUBLE PRECISION,
    ambient_temperature     DOUBLE PRECISION,
    rotor_rpm               DOUBLE PRECISION,
    blade_pitch_angle       DOUBLE PRECISION,
    nacelle_direction       DOUBLE PRECISION,
    vibration_level         DOUBLE PRECISION,
    turbine_efficiency      DOUBLE PRECISION,
    capacity_factor         DOUBLE PRECISION
);

SELECT create_hypertable('turbine_metrics', 'time',
       if_not_exists => TRUE,
       chunk_time_interval => INTERVAL '1 day');

CREATE INDEX IF NOT EXISTS idx_turbine_metrics_turbine_id
    ON turbine_metrics (turbine_id, time DESC);


-- ─────────────────────── Hourly Power Generation ─────────────────────────────
-- Gold-layer aggregation: one row per turbine per hour.

CREATE TABLE IF NOT EXISTS hourly_power_generation (
    window_start            TIMESTAMPTZ     NOT NULL,
    window_end              TIMESTAMPTZ     NOT NULL,
    turbine_id              VARCHAR(20)     NOT NULL,
    avg_power_kw            DOUBLE PRECISION,
    max_power_kw            DOUBLE PRECISION,
    min_power_kw            DOUBLE PRECISION,
    total_energy_kwh        DOUBLE PRECISION,
    avg_wind_speed          DOUBLE PRECISION,
    avg_efficiency          DOUBLE PRECISION,
    avg_capacity_factor     DOUBLE PRECISION,
    avg_generator_temp      DOUBLE PRECISION,
    avg_gearbox_temp        DOUBLE PRECISION,
    avg_ambient_temp        DOUBLE PRECISION,
    generating_count        INTEGER,
    record_count            INTEGER
);

SELECT create_hypertable('hourly_power_generation', 'window_start',
       if_not_exists => TRUE,
       chunk_time_interval => INTERVAL '1 day');

CREATE INDEX IF NOT EXISTS idx_hourly_power_turbine
    ON hourly_power_generation (turbine_id, window_start DESC);


-- ─────────────────────── Turbine Anomalies ───────────────────────────────────
-- Events flagged by the Gold-layer anomaly detector.

CREATE TABLE IF NOT EXISTS turbine_anomalies (
    event_ts                TIMESTAMPTZ     NOT NULL,
    turbine_id              VARCHAR(20)     NOT NULL,
    anomaly_type            VARCHAR(50)     NOT NULL,
    severity                VARCHAR(20)     NOT NULL,
    wind_speed              DOUBLE PRECISION,
    power_output            DOUBLE PRECISION,
    generator_temperature   DOUBLE PRECISION,
    gearbox_temperature     DOUBLE PRECISION,
    vibration_level         DOUBLE PRECISION
);

SELECT create_hypertable('turbine_anomalies', 'event_ts',
       if_not_exists => TRUE,
       chunk_time_interval => INTERVAL '1 day');

CREATE INDEX IF NOT EXISTS idx_anomalies_turbine
    ON turbine_anomalies (turbine_id, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_severity
    ON turbine_anomalies (severity, event_ts DESC);


-- ─────────────────────── Continuous Aggregates ───────────────────────────────
-- Materialized view for fast dashboard queries across all turbines.

CREATE MATERIALIZED VIEW IF NOT EXISTS fleet_daily_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', window_start) AS day,
    turbine_id,
    AVG(avg_power_kw)               AS daily_avg_power_kw,
    MAX(max_power_kw)               AS daily_peak_power_kw,
    SUM(total_energy_kwh)           AS daily_energy_kwh,
    AVG(avg_wind_speed)             AS daily_avg_wind_speed,
    AVG(avg_efficiency)             AS daily_avg_efficiency,
    AVG(avg_capacity_factor)        AS daily_avg_capacity_factor,
    SUM(record_count)               AS daily_record_count
FROM hourly_power_generation
GROUP BY time_bucket('1 day', window_start), turbine_id
WITH NO DATA;

-- Refresh policy: update daily summary every hour
SELECT add_continuous_aggregate_policy('fleet_daily_summary',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);

-- ─────────────────────── Data Retention ──────────────────────────────────────
-- Keep raw metrics for 30 days, aggregates for 1 year.

SELECT add_retention_policy('turbine_metrics',
       drop_after => INTERVAL '30 days',
       if_not_exists => TRUE);

SELECT add_retention_policy('turbine_anomalies',
       drop_after => INTERVAL '90 days',
       if_not_exists => TRUE);
