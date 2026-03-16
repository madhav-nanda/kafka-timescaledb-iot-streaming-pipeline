"""Unit tests for configuration loading."""

import os
import pytest
from configs import load_config, _resolve_env_vars, _walk_and_resolve


class TestResolveEnvVars:
    def test_with_default(self):
        result = _resolve_env_vars("${NONEXISTENT_VAR:-fallback}")
        assert result == "fallback"

    def test_with_env_var_set(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR_12345", "hello")
        result = _resolve_env_vars("${TEST_VAR_12345:-default}")
        assert result == "hello"

    def test_no_default(self):
        result = _resolve_env_vars("${NONEXISTENT_VAR}")
        assert result == ""

    def test_embedded_in_string(self):
        result = _resolve_env_vars("http://${HOST:-localhost}:9092")
        assert result == "http://localhost:9092"


class TestWalkAndResolve:
    def test_resolves_nested_dict(self):
        obj = {"a": {"b": "${MISSING:-val}"}}
        resolved = _walk_and_resolve(obj)
        assert resolved == {"a": {"b": "val"}}

    def test_resolves_list(self):
        obj = ["${MISSING:-x}", "${MISSING:-y}"]
        resolved = _walk_and_resolve(obj)
        assert resolved == ["x", "y"]

    def test_numeric_coercion(self):
        obj = {"port": "${MISSING:-5432}"}
        resolved = _walk_and_resolve(obj)
        assert resolved == {"port": 5432}

    def test_preserves_non_string(self):
        obj = {"flag": True, "count": 42}
        resolved = _walk_and_resolve(obj)
        assert resolved == {"flag": True, "count": 42}


class TestLoadConfig:
    def test_loads_successfully(self):
        config = load_config()
        assert "kafka" in config
        assert "simulator" in config
        assert "spark" in config
        assert "minio" in config
        assert "timescaledb" in config

    def test_kafka_has_required_keys(self):
        config = load_config()
        kafka = config["kafka"]
        assert "bootstrap_servers" in kafka
        assert "topic" in kafka
        assert "schema_registry_url" in kafka

    def test_simulator_has_turbine_count(self):
        config = load_config()
        assert config["simulator"]["num_turbines"] == 12
