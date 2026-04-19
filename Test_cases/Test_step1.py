"""
Step 1 smoke test.

Asserts:
  - All pinned deps import cleanly.
  - config loads and summary() returns a dict with expected keys.
  - EIA key detection honors both env var names.

Run: python -m pytest test_step1.py -v
"""

import os
import importlib
import pytest


def test_all_deps_importable():
    # If any of these fail, run `pip install -r requirements.txt` first.
    for mod in ["flask", "flask_cors", "requests", "jwt", "bcrypt",
                "apscheduler", "pytest"]:
        importlib.import_module(mod)


def test_config_loads():
    import config
    s = config.summary()
    for key in ["db_path", "has_eia_key", "base_cap_tokens",
                "initial_grant_fraction", "cost_multiplier_range",
                "intensity_refresh_seconds"]:
        assert key in s, f"missing config key: {key}"
    # Defaults sanity
    assert s["base_cap_tokens"] == 1000
    assert s["cost_multiplier_range"] == [0.33, 3.0]


def test_eia_key_primary_name(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "primary_key_value")
    monkeypatch.delenv("EIA_KEY", raising=False)
    import importlib, config
    importlib.reload(config)
    assert config.EIA_API_KEY == "primary_key_value"


def test_eia_key_fallback_name(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    monkeypatch.setenv("EIA_KEY", "fallback_key_value")
    import importlib, config
    importlib.reload(config)
    assert config.EIA_API_KEY == "fallback_key_value"


def test_eia_key_missing_is_none(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    monkeypatch.delenv("EIA_KEY", raising=False)
    import importlib, config
    importlib.reload(config)
    assert config.EIA_API_KEY is None
    assert config.summary()["has_eia_key"] is False
