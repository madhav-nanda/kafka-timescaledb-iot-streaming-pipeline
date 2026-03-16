"""Avro schema loading and Schema Registry helpers."""

import json
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

_SCHEMA_DIR = Path(__file__).parent


def load_avro_schema(schema_name: str = "turbine_telemetry") -> dict:
    """Load an Avro schema from the schemas directory.

    Args:
        schema_name: Base name of the .avsc file (without extension).

    Returns:
        Parsed schema as a Python dict.
    """
    schema_path = _SCHEMA_DIR / f"{schema_name}.avsc"
    with open(schema_path, "r") as f:
        return json.load(f)


def get_schema_registry_client(url: str) -> SchemaRegistryClient:
    """Create a Schema Registry client.

    Args:
        url: Schema Registry URL (e.g. http://localhost:8081).

    Returns:
        Configured SchemaRegistryClient.
    """
    return SchemaRegistryClient({
        "url": url,
        "timeout": 10,
    })


def register_schema(client: SchemaRegistryClient, subject: str,
                     schema_dict: dict) -> int:
    """Register an Avro schema with the Schema Registry.

    Args:
        client: SchemaRegistryClient instance.
        subject: Subject name (e.g. wind-turbine-telemetry-value).
        schema_dict: Avro schema as a dict.

    Returns:
        Schema ID assigned by the registry.
    """
    schema = Schema(json.dumps(schema_dict), schema_type="AVRO")
    schema_id = client.register_schema(subject, schema)
    return schema_id
