"""Unit tests for Avro schema loading."""

import json
from pathlib import Path

import pytest


def _load_schema(name: str = "turbine_telemetry") -> dict:
    """Load schema directly from file to avoid confluent_kafka dependency."""
    schema_path = Path(__file__).parent.parent.parent / "schemas" / f"{name}.avsc"
    with open(schema_path, "r") as f:
        return json.load(f)


class TestAvroSchema:
    def test_loads_turbine_telemetry(self):
        schema = _load_schema()
        assert schema["type"] == "record"
        assert schema["name"] == "TurbineTelemetry"

    def test_has_required_fields(self):
        schema = _load_schema()
        field_names = {f["name"] for f in schema["fields"]}
        required = {
            "turbine_id", "timestamp", "wind_speed", "power_output",
            "generator_temperature", "gearbox_temperature",
            "ambient_temperature",
        }
        assert required.issubset(field_names)

    def test_field_types(self):
        schema = _load_schema()
        field_types = {f["name"]: f["type"] for f in schema["fields"]}
        assert field_types["turbine_id"] == "string"
        assert field_types["wind_speed"] == "double"
        assert field_types["power_output"] == "double"

    def test_schema_is_valid_json(self):
        schema = _load_schema()
        # Should round-trip through JSON without issue
        json_str = json.dumps(schema)
        reloaded = json.loads(json_str)
        assert reloaded == schema
