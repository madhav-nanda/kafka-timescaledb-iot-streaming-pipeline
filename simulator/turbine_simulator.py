"""
Wind Turbine IoT Telemetry Simulator.

Generates realistic telemetry events for a fleet of wind turbines using
physics-based models. Events are yielded as dictionaries, suitable for
direct ingestion by a Kafka producer.

Turbine power curve follows a standard cubic relationship between cut-in
and rated wind speed, with flat output at rated power until cut-out.
"""

import math
import random
import time
import logging
from datetime import datetime, timezone
from typing import Generator

logger = logging.getLogger("simulator")

# ── Turbine state persistence across ticks ──────────────────────────────────
_turbine_state: dict[str, dict] = {}


def _power_curve(wind_speed: float, rated_kw: float,
                 cut_in: float, rated_ws: float, cut_out: float) -> float:
    """Calculate power output from the standard cubic power curve.

    Below cut-in or above cut-out wind speeds, output is zero.
    Between cut-in and rated, output follows a cubic curve.
    Between rated and cut-out, output is clamped at rated power.
    """
    if wind_speed < cut_in or wind_speed > cut_out:
        return 0.0
    if wind_speed >= rated_ws:
        return rated_kw
    # Cubic interpolation between cut-in and rated
    fraction = ((wind_speed - cut_in) / (rated_ws - cut_in)) ** 3
    return round(rated_kw * fraction, 2)


def _efficiency(power_kw: float, wind_speed: float,
                rated_kw: float) -> float:
    """Compute turbine efficiency as actual vs. theoretical max output."""
    if wind_speed <= 0 or rated_kw <= 0:
        return 0.0
    theoretical = rated_kw * min(1.0, (wind_speed / 12.0) ** 3)
    if theoretical <= 0:
        return 0.0
    return round(min(power_kw / theoretical, 1.0) * 100, 2)


def _correlated_temperature(base: float, variance: float,
                            power_kw: float, rated_kw: float) -> float:
    """Temperature correlated with load — higher power means higher temp."""
    load_factor = power_kw / rated_kw if rated_kw > 0 else 0
    temp = base + (variance * load_factor) + random.gauss(0, 1.5)
    return round(temp, 2)


def _smooth_wind(prev_wind: float, cfg: dict) -> float:
    """Generate next wind speed with realistic auto-correlation."""
    delta = random.gauss(0, 0.8)
    new_wind = prev_wind + delta
    return round(max(cfg["min"], min(cfg["max"], new_wind)), 2)


def generate_event(turbine_id: str, cfg: dict) -> dict:
    """Generate a single telemetry event for the given turbine.

    Maintains per-turbine state for smooth, realistic time-series data.

    Args:
        turbine_id: Unique identifier for the turbine (e.g. 'WT-001').
        cfg: Simulator configuration dict from pipeline.yaml.

    Returns:
        Dictionary containing all telemetry fields.
    """
    ws_cfg = cfg["wind_speed"]
    pwr_cfg = cfg["power_output"]
    temp_cfg = cfg["temperature"]

    # Initialize or retrieve turbine state
    if turbine_id not in _turbine_state:
        _turbine_state[turbine_id] = {
            "wind_speed": random.uniform(ws_cfg["cut_in"], ws_cfg["rated"]),
            "ambient_temp": random.uniform(temp_cfg["ambient_min"],
                                           temp_cfg["ambient_max"]),
        }

    state = _turbine_state[turbine_id]

    # Evolve wind speed with auto-correlation
    wind_speed = _smooth_wind(state["wind_speed"], ws_cfg)
    state["wind_speed"] = wind_speed

    # Drift ambient temperature slowly
    state["ambient_temp"] += random.gauss(0, 0.1)
    state["ambient_temp"] = round(
        max(temp_cfg["ambient_min"],
            min(temp_cfg["ambient_max"], state["ambient_temp"])), 2
    )

    rated_kw = pwr_cfg["rated_kw"]
    power = _power_curve(wind_speed, rated_kw,
                         ws_cfg["cut_in"], ws_cfg["rated"], ws_cfg["cut_out"])

    # Add minor noise to power output (inverter fluctuation)
    if power > 0:
        power = round(power * random.uniform(0.97, 1.03), 2)
        power = min(power, rated_kw)

    generator_temp = _correlated_temperature(
        temp_cfg["generator_base"], temp_cfg["generator_variance"],
        power, rated_kw,
    )
    gearbox_temp = _correlated_temperature(
        temp_cfg["gearbox_base"], temp_cfg["gearbox_variance"],
        power, rated_kw,
    )

    # Rotor metrics
    rotor_rpm = round(wind_speed * 1.2 + random.gauss(0, 0.5), 2) if power > 0 else 0.0
    blade_pitch = round(
        max(0, min(90, (wind_speed - ws_cfg["cut_in"]) * 3 + random.gauss(0, 1))), 2
    )
    nacelle_direction = round(random.uniform(0, 360), 2)
    vibration = round(0.2 + (power / rated_kw) * 0.8 + random.gauss(0, 0.05), 4)

    eff = _efficiency(power, wind_speed, rated_kw)

    return {
        "turbine_id": turbine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "wind_speed": wind_speed,
        "power_output": power,
        "generator_temperature": generator_temp,
        "gearbox_temperature": gearbox_temp,
        "ambient_temperature": state["ambient_temp"],
        "rotor_rpm": rotor_rpm,
        "blade_pitch_angle": blade_pitch,
        "nacelle_direction": nacelle_direction,
        "vibration_level": vibration,
        "turbine_efficiency": eff,
    }


def event_stream(cfg: dict) -> Generator[dict, None, None]:
    """Infinite generator that yields telemetry events for all turbines.

    Cycles through all turbines, yielding one event per turbine per tick,
    then sleeps for the configured interval.

    Args:
        cfg: Full simulator configuration section from pipeline.yaml.
    """
    num_turbines = cfg.get("num_turbines", 12)
    prefix = cfg.get("turbine_id_prefix", "WT")
    interval = cfg.get("event_interval_seconds", 1.0)

    turbine_ids = [f"{prefix}-{str(i).zfill(3)}" for i in range(1, num_turbines + 1)]

    logger.info("Starting telemetry stream for %d turbines", num_turbines)

    while True:
        tick_start = time.monotonic()
        for tid in turbine_ids:
            yield generate_event(tid, cfg)
        elapsed = time.monotonic() - tick_start
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)
