"""Step 6 tests: Oracle."""

import os
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import pytest

import ledger_schema
import oracle
from eia_client import EIAError


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "oracle_test.db")
    ledger_schema.init_db(p)
    return p


def test_compute_cap_clean_grid():
    assert oracle.compute_cap(100) == 2000   # clean → max cap
    assert oracle.compute_cap(150) == 2000


def test_compute_cap_dirty_grid():
    assert oracle.compute_cap(800) == 500
    assert oracle.compute_cap(1500) == 300   # clamped floor


def test_compute_cap_reference():
    assert oracle.compute_cap(400) == 1000   # exactly reference


def test_sync_state_writes_budget_row(db):
    client = MagicMock()
    client.fetch_fuel_mix_window.return_value = {
        "intensity_avg_gco2_per_kwh": 250.0,
        "hours_returned": 24,
    }
    result = oracle.sync_state("ca", client, "2026-04-18", db_path=db)
    assert result["cap_tokens"] == 1600
    assert result["source"] == "eia"

    with ledger_schema.connect(db) as conn:
        row = conn.execute(
            "SELECT * FROM daily_budgets WHERE state='ca' AND utc_date='2026-04-18'"
        ).fetchone()
    assert row["cap_tokens"] == 1600
    assert row["intensity_avg"] == 250.0
    assert row["ba_code"] == "CISO"
    assert row["source"] == "eia"


def test_sync_state_falls_back_to_yesterday_when_eia_fails(db):
    # Seed yesterday's budget
    with ledger_schema.connect(db) as conn:
        conn.execute(
            "INSERT INTO daily_budgets "
            "(state, utc_date, cap_tokens, intensity_avg, ba_code, source, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ca", "2026-04-17", 1600, 250.0, "CISO", "eia", "2026-04-17T00:05:00Z")
        )
        conn.commit()

    client = MagicMock()
    client.fetch_fuel_mix_window.side_effect = EIAError("down")

    result = oracle.sync_state("ca", client, "2026-04-18", db_path=db)
    assert result["source"] == "stale"
    assert result["cap_tokens"] == 1600
    assert result["intensity_avg"] == 250.0


def test_sync_state_falls_back_to_hardcoded_when_no_yesterday(db):
    client = MagicMock()
    client.fetch_fuel_mix_window.side_effect = EIAError("down")

    result = oracle.sync_state("wv", client, "2026-04-18", db_path=db)
    assert result["source"] == "fallback"
    assert result["intensity_avg"] == 780.0
    assert result["cap_tokens"] == oracle.compute_cap(780)


def test_sync_state_replaces_existing_same_day(db):
    client = MagicMock()
    client.fetch_fuel_mix_window.return_value = {
        "intensity_avg_gco2_per_kwh": 250.0, "hours_returned": 24,
    }
    oracle.sync_state("ca", client, "2026-04-18", db_path=db)

    client.fetch_fuel_mix_window.return_value = {
        "intensity_avg_gco2_per_kwh": 300.0, "hours_returned": 24,
    }
    oracle.sync_state("ca", client, "2026-04-18", db_path=db)

    with ledger_schema.connect(db) as conn:
        rows = conn.execute(
            "SELECT * FROM daily_budgets WHERE state='ca'"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["intensity_avg"] == 300.0


def test_sync_all_covers_every_state(db):
    client = MagicMock()
    client.fetch_fuel_mix_window.return_value = {
        "intensity_avg_gco2_per_kwh": 400.0, "hours_returned": 24,
    }
    results = oracle.sync_all("2026-04-18", db_path=db, client=client)
    assert len(results) == 10
    states = {r["state"] for r in results}
    assert states == {"ca", "tx", "wa", "wv", "ny", "pa", "fl", "oh", "il", "ga"}


@pytest.mark.skipif(
    not (os.environ.get("EIA_API_KEY") or os.environ.get("EIA_KEY")),
    reason="no EIA key"
)
def test_live_sync_ca(db):
    from eia_client import EIAClient
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    result = oracle.sync_state("ca", EIAClient(), today, db_path=db)
    assert result["source"] == "eia"
    assert 30 < result["intensity_avg"] < 800
    assert 300 <= result["cap_tokens"] <= 2000
