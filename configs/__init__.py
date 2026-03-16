"""Configuration loading utilities for the Wind Turbine Streaming Platform."""

import os
import re
from pathlib import Path
from typing import Any

import yaml


_CONFIG_DIR = Path(__file__).parent
_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::-(.*?))?\}")


def _resolve_env_vars(value: str) -> str:
    """Replace ${VAR:-default} patterns with environment variable values."""

    def _replacer(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2) if match.group(2) is not None else ""
        return os.environ.get(var_name, default)

    return _ENV_VAR_PATTERN.sub(_replacer, value)


def _walk_and_resolve(obj: Any) -> Any:
    """Recursively resolve environment variables in a config dict."""
    if isinstance(obj, str):
        resolved = _resolve_env_vars(obj)
        # Attempt numeric coercion for simple values
        if resolved.isdigit():
            return int(resolved)
        try:
            return float(resolved)
        except (ValueError, TypeError):
            pass
        return resolved
    elif isinstance(obj, dict):
        return {k: _walk_and_resolve(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_walk_and_resolve(item) for item in obj]
    return obj


def load_config(config_file: str = "pipeline.yaml") -> dict:
    """Load and resolve the pipeline configuration.

    Args:
        config_file: Name of the YAML config file inside the configs/ directory.

    Returns:
        Fully resolved configuration dictionary.
    """
    config_path = _CONFIG_DIR / config_file
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return _walk_and_resolve(raw)
