"""Spark SQL schema definitions for turbine telemetry."""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

TELEMETRY_SCHEMA = StructType([
    StructField("turbine_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("wind_speed", DoubleType(), False),
    StructField("power_output", DoubleType(), False),
    StructField("generator_temperature", DoubleType(), False),
    StructField("gearbox_temperature", DoubleType(), False),
    StructField("ambient_temperature", DoubleType(), False),
    StructField("rotor_rpm", DoubleType(), False),
    StructField("blade_pitch_angle", DoubleType(), False),
    StructField("nacelle_direction", DoubleType(), False),
    StructField("vibration_level", DoubleType(), False),
    StructField("turbine_efficiency", DoubleType(), False),
])
