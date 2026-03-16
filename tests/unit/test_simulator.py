"""Unit tests for the wind turbine telemetry simulator."""

import pytest
from simulator.turbine_simulator import (
    generate_event, _power_curve, _efficiency, _smooth_wind, event_stream,
)

# ── Test configuration (mirrors pipeline.yaml defaults) ──────────────────────

SIM_CFG = {
    "num_turbines": 3,
    "turbine_id_prefix": "WT",
    "event_interval_seconds": 0.01,
    "wind_speed": {
        "min": 0.0,
        "max": 30.0,
        "cut_in": 3.0,
        "rated": 12.0,
        "cut_out": 25.0,
    },
    "power_output": {
        "rated_kw": 3000.0,
    },
    "temperature": {
        "ambient_min": -10.0,
        "ambient_max": 45.0,
        "generator_base": 60.0,
        "generator_variance": 25.0,
        "gearbox_base": 55.0,
        "gearbox_variance": 20.0,
    },
}


class TestPowerCurve:
    def test_below_cut_in_returns_zero(self):
        assert _power_curve(2.0, 3000, 3.0, 12.0, 25.0) == 0.0

    def test_above_cut_out_returns_zero(self):
        assert _power_curve(26.0, 3000, 3.0, 12.0, 25.0) == 0.0

    def test_at_rated_returns_rated(self):
        assert _power_curve(12.0, 3000, 3.0, 12.0, 25.0) == 3000.0

    def test_between_rated_and_cutout_returns_rated(self):
        assert _power_curve(20.0, 3000, 3.0, 12.0, 25.0) == 3000.0

    def test_mid_range_returns_positive(self):
        power = _power_curve(7.5, 3000, 3.0, 12.0, 25.0)
        assert 0 < power < 3000

    def test_cubic_relationship(self):
        # Power at mid-range should be less than linear interpolation
        power = _power_curve(7.5, 3000, 3.0, 12.0, 25.0)
        linear = 3000 * (7.5 - 3.0) / (12.0 - 3.0)
        assert power < linear


class TestEfficiency:
    def test_zero_wind_returns_zero(self):
        assert _efficiency(100, 0, 3000) == 0.0

    def test_full_power_high_efficiency(self):
        eff = _efficiency(3000, 12.0, 3000)
        assert eff > 90

    def test_capped_at_100(self):
        eff = _efficiency(5000, 5.0, 3000)
        assert eff <= 100.0


class TestSmoothWind:
    def test_stays_in_bounds(self):
        ws_cfg = SIM_CFG["wind_speed"]
        for _ in range(100):
            wind = _smooth_wind(15.0, ws_cfg)
            assert ws_cfg["min"] <= wind <= ws_cfg["max"]

    def test_autocorrelation(self):
        """Sequential values should be close to each other."""
        ws_cfg = SIM_CFG["wind_speed"]
        prev = 10.0
        deltas = []
        for _ in range(100):
            new = _smooth_wind(prev, ws_cfg)
            deltas.append(abs(new - prev))
            prev = new
        avg_delta = sum(deltas) / len(deltas)
        assert avg_delta < 3.0  # Should change gradually


class TestGenerateEvent:
    def test_returns_all_fields(self):
        event = generate_event("WT-001", SIM_CFG)
        expected_fields = {
            "turbine_id", "timestamp", "wind_speed", "power_output",
            "generator_temperature", "gearbox_temperature",
            "ambient_temperature", "rotor_rpm", "blade_pitch_angle",
            "nacelle_direction", "vibration_level", "turbine_efficiency",
        }
        assert set(event.keys()) == expected_fields

    def test_turbine_id_preserved(self):
        event = generate_event("WT-042", SIM_CFG)
        assert event["turbine_id"] == "WT-042"

    def test_values_in_range(self):
        event = generate_event("WT-001", SIM_CFG)
        assert 0 <= event["wind_speed"] <= 30
        assert 0 <= event["power_output"] <= 3500
        assert 0 <= event["nacelle_direction"] <= 360

    def test_timestamp_is_iso_format(self):
        event = generate_event("WT-001", SIM_CFG)
        from datetime import datetime
        # Should not raise
        datetime.fromisoformat(event["timestamp"])


class TestEventStream:
    def test_yields_events(self):
        stream = event_stream(SIM_CFG)
        events = [next(stream) for _ in range(6)]
        assert len(events) == 6

    def test_cycles_through_turbines(self):
        cfg = {**SIM_CFG, "num_turbines": 3}
        stream = event_stream(cfg)
        ids = [next(stream)["turbine_id"] for _ in range(6)]
        assert ids == ["WT-001", "WT-002", "WT-003",
                       "WT-001", "WT-002", "WT-003"]
