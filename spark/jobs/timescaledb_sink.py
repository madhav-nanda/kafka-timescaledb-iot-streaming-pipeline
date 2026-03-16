"""
TimescaleDB Sink — JDBC helpers and column definitions.

Provides the column lists matching TimescaleDB table schemas and
public JDBC helper functions used by the streaming pipeline's
foreachBatch handlers.

The actual write logic lives in streaming_pipeline.py's foreachBatch
handlers, which write to both Gold Parquet AND TimescaleDB in one batch.
"""

import logging

logger = logging.getLogger("spark_pipeline.timescaledb_sink")

# ── Column lists matching the TimescaleDB table schemas exactly ──────────
# Any extra columns in the DataFrame (e.g. event_date from Parquet
# partitioning) are excluded to prevent JDBC "column does not exist" errors.

HOURLY_POWER_COLUMNS = [
    "window_start", "window_end", "turbine_id",
    "avg_power_kw", "max_power_kw", "min_power_kw",
    "total_energy_kwh", "avg_wind_speed", "avg_efficiency",
    "avg_capacity_factor", "avg_generator_temp", "avg_gearbox_temp",
    "avg_ambient_temp", "generating_count", "record_count",
]

ANOMALY_COLUMNS = [
    "event_ts", "turbine_id", "anomaly_type", "severity",
    "wind_speed", "power_output",
    "generator_temperature", "gearbox_temperature", "vibration_level",
]


def get_jdbc_url(db_cfg: dict) -> str:
    """Build a JDBC PostgreSQL URL from the database config section.

    Args:
        db_cfg: TimescaleDB configuration dict (host, port, database).

    Returns:
        JDBC connection URL string.
    """
    return (
        f"jdbc:postgresql://{db_cfg['host']}:{db_cfg['port']}"
        f"/{db_cfg['database']}"
    )


def get_jdbc_properties(db_cfg: dict) -> dict:
    """Build JDBC connection properties from the database config section.

    Args:
        db_cfg: TimescaleDB configuration dict (user, password).

    Returns:
        Dict with user, password, and driver class name.
    """
    return {
        "user": db_cfg["user"],
        "password": str(db_cfg["password"]),
        "driver": "org.postgresql.Driver",
    }
