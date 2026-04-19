"""
Step 5 tests for eia_client.

Two groups:
  - Unit tests: mocked HTTP, test parser logic deterministically
  - Integration test: hits real EIA API, skipped if no key set

Run unit tests only:
    python -m pytest test_step5.py -v -k "not live"
Run everything including live:
    EIA_API_KEY=... python -m pytest test_step5.py -v
"""

import os
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest

import eia_client
from eia_client import EIAClient, EIAError, EMISSION_FACTORS


# ---------------------------------------------------------------------------
# Fixture: synthetic fuel-mix payload matching EIA's real shape
# ---------------------------------------------------------------------------

def _payload(rows):
    return {"response": {"data": rows}}


def _row(period, ba, fueltype, value):
    return {
        "period": period,
        "respondent": ba,
        "respondent-name": "Test BA",
        "fueltype": fueltype,
        "type-name": "Test",
        "value": str(value),
        "value-units": "megawatthours",
    }


# ---------------------------------------------------------------------------
# Emission factor math
# ---------------------------------------------------------------------------

def test_emission_factors_present():
    for fuel in ["COL", "NG", "NUC", "SUN", "WND", "WAT"]:
        assert fuel in EMISSION_FACTORS
    assert EMISSION_FACTORS["COL"] > EMISSION_FACTORS["NG"] > EMISSION_FACTORS["WAT"]
    assert EMISSION_FACTORS["WND"] < 50
    assert EMISSION_FACTORS["COL"] > 500


# ---------------------------------------------------------------------------
# Query B (latest hour) — parser correctness
# ---------------------------------------------------------------------------

def test_fetch_latest_picks_newest_complete_hour():
    c = EIAClient(api_key="test")
    rows = [
        _row("2026-04-18T14", "CISO", "SUN", 8000),
        _row("2026-04-18T14", "CISO", "NG", 2000),
        _row("2026-04-18T14", "CISO", "NUC", 1000),
        _row("2026-04-18T13", "CISO", "SUN", 7500),
        _row("2026-04-18T13", "CISO", "NG", 2500),
    ]
    fake = MagicMock(status_code=200)
    fake.json.return_value = _payload(rows)
    with patch.object(c.session, "get", return_value=fake):
        result = c.fetch_fuel_mix_latest("CISO")

    assert result["ba"] == "CISO"
    assert result["period"] == "2026-04-18T14"  # newest wins
    assert result["total_mwh"] == 11000.0
    # intensity = (8000*48 + 2000*490 + 1000*12) / 11000
    expected = (8000 * 48 + 2000 * 490 + 1000 * 12) / 11000
    assert abs(result["intensity_gco2_per_kwh"] - round(expected, 1)) < 0.2


def test_fetch_latest_skips_single_fuel_hours():
    """An hour with only 1 fuel type is suspicious — skip to previous hour."""
    c = EIAClient(api_key="test")
    rows = [
        _row("2026-04-18T15", "CISO", "SUN", 9000),  # single fuel, skip
        _row("2026-04-18T14", "CISO", "SUN", 8000),
        _row("2026-04-18T14", "CISO", "NG", 2000),
    ]
    fake = MagicMock(status_code=200)
    fake.json.return_value = _payload(rows)
    with patch.object(c.session, "get", return_value=fake):
        result = c.fetch_fuel_mix_latest("CISO")
    assert result["period"] == "2026-04-18T14"


def test_fetch_latest_clamps_negative_values():
    """Battery discharge sometimes produces negative MWh; treat as 0."""
    c = EIAClient(api_key="test")
    rows = [
        _row("2026-04-18T14", "CISO", "NG", 2000),
        _row("2026-04-18T14", "CISO", "BAT", -500),  # negative (battery charging)
        _row("2026-04-18T14", "CISO", "SUN", 8000),
    ]
    fake = MagicMock(status_code=200)
    fake.json.return_value = _payload(rows)
    with patch.object(c.session, "get", return_value=fake):
        result = c.fetch_fuel_mix_latest("CISO")
    # BAT treated as 0; doesn't affect intensity beyond being dropped
    assert result["mix_mwh"].get("BAT", 0) == 0
    assert result["total_mwh"] == 10000


def test_fetch_latest_raises_on_empty():
    c = EIAClient(api_key="test")
    fake = MagicMock(status_code=200)
    fake.json.return_value = _payload([])
    with patch.object(c.session, "get", return_value=fake):
        with pytest.raises(EIAError):
            c.fetch_fuel_mix_latest("CISO")


def test_no_key_raises(monkeypatch):
    # Clear env AND force-reset the module-level key in eia_client
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    monkeypatch.delenv("EIA_KEY", raising=False)
    c = EIAClient(api_key=None)
    c.api_key = None  # override whatever config.EIA_API_KEY had at import
    with pytest.raises(EIAError, match="EIA_API_KEY"):
        c.fetch_fuel_mix_latest("CISO")


def test_http_error_becomes_eia_error():
    c = EIAClient(api_key="test")
    fake = MagicMock(status_code=403)
    fake.text = "Forbidden"
    with patch.object(c.session, "get", return_value=fake):
        with pytest.raises(EIAError, match="HTTP 403"):
            c.fetch_fuel_mix_latest("CISO")


# ---------------------------------------------------------------------------
# Query A (24h window) — parser correctness
# ---------------------------------------------------------------------------

def test_fetch_window_computes_24h_average():
    c = EIAClient(api_key="test")
    # Two hours of very clean grid, two hours of very dirty grid
    # Avg should be exactly between clean and dirty intensities
    rows = [
        _row("2026-04-17T00", "CISO", "WND", 1000),  # pure wind: 11
        _row("2026-04-17T01", "CISO", "WND", 1000),  # pure wind: 11
        _row("2026-04-17T02", "CISO", "COL", 1000),  # pure coal: 820
        _row("2026-04-17T03", "CISO", "COL", 1000),  # pure coal: 820
    ]
    fake = MagicMock(status_code=200)
    fake.json.return_value = _payload(rows)
    with patch.object(c.session, "get", return_value=fake):
        start = datetime(2026, 4, 17, 0, tzinfo=timezone.utc)
        end = datetime(2026, 4, 17, 4, tzinfo=timezone.utc)
        result = c.fetch_fuel_mix_window("CISO", start, end)

    # Note: single-fuel hours ARE included in window query (unlike latest).
    # Each hour has total > 0 so it counts.
    assert result["hours_returned"] == 4
    expected_avg = (11 + 11 + 820 + 820) / 4
    assert abs(result["intensity_avg_gco2_per_kwh"] - expected_avg) < 0.5


def test_fetch_window_raises_on_empty():
    c = EIAClient(api_key="test")
    fake = MagicMock(status_code=200)
    fake.json.return_value = _payload([])
    with patch.object(c.session, "get", return_value=fake):
        with pytest.raises(EIAError):
            c.fetch_fuel_mix_window(
                "CISO",
                datetime(2026, 4, 17, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 18, 0, tzinfo=timezone.utc),
            )


# ---------------------------------------------------------------------------
# State → BA mapping sanity
# ---------------------------------------------------------------------------

def test_state_to_ba_mapping():
    assert eia_client.STATE_TO_BA["ca"] == "CISO"
    assert eia_client.STATE_TO_BA["tx"] == "ERCO"
    assert eia_client.STATE_TO_BA["wa"] == "BPAT"
    # All keys lowercase
    for s in eia_client.STATE_TO_BA:
        assert s == s.lower()
    # All BA codes uppercase (EIA convention)
    for ba in eia_client.STATE_TO_BA.values():
        assert ba == ba.upper()


# ---------------------------------------------------------------------------
# Live integration (skipped without key)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not (os.environ.get("EIA_API_KEY") or os.environ.get("EIA_KEY")),
    reason="no EIA key set"
)
def test_live_ciso_latest_hour():
    """Actually hit EIA. Verifies the client works end-to-end."""
    c = EIAClient()
    result = c.fetch_fuel_mix_latest("CISO")
    assert result["ba"] == "CISO"
    assert result["total_mwh"] > 0
    # CISO intensity realistic range: 50-600 gCO2/kWh depending on time of day
    assert 30 < result["intensity_gco2_per_kwh"] < 800
    assert len(result["mix_pct"]) >= 2
