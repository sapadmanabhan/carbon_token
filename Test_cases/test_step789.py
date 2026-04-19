"""Tests for Allocator (Step 7), Intensity Cache (Step 8), Identity (Step 9)."""

import os
import time
from unittest.mock import MagicMock, patch
import pytest

import ledger_schema
import ledger
import allocator
import identity
import intensity_cache
from eia_client import EIAError


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "s789.db")
    ledger_schema.init_db(p)
    return p


# --- Step 7: Allocator -----------------------------------------------------

def _seed_budget(db, state, date, cap):
    with ledger_schema.connect(db) as conn:
        conn.execute(
            "INSERT INTO daily_budgets "
            "(state, utc_date, cap_tokens, intensity_avg, ba_code, source, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (state, date, cap, 250.0, "CISO", "eia", "2026-04-18T00:05:00Z")
        )
        conn.commit()


def _seed_users(db, state, count):
    ids = []
    with ledger_schema.connect(db) as conn:
        for i in range(count):
            cur = conn.execute(
                "INSERT INTO users (username, password_hash, state, created_at) "
                "VALUES (?, ?, ?, ?)",
                (f"u{state}_{i}", "x", state, "2026-04-18T00:00:00Z")
            )
            ids.append(cur.lastrowid)
        conn.commit()
    return ids


def test_allocator_splits_60_40(db):
    _seed_budget(db, "ca", "2026-04-18", 1000)
    user_ids = _seed_users(db, "ca", 5)

    result = allocator.allocate_state("ca", "2026-04-18", db_path=db)
    assert result["cap"] == 1000
    assert result["user_count"] == 5
    assert result["per_user_grant"] == 120  # floor(600/5)
    assert result["reserve_seeded"] == 400  # 1000 - 600

    for uid in user_ids:
        acct = ledger.balance(uid, "2026-04-18", db_path=db)
        assert acct["balance_tokens"] == 120
    assert ledger.reserve_balance("ca", "2026-04-18", db_path=db) == 400


def test_allocator_no_users_puts_all_in_reserve(db):
    _seed_budget(db, "wv", "2026-04-18", 500)
    result = allocator.allocate_state("wv", "2026-04-18", db_path=db)
    assert result["user_count"] == 0
    assert result["per_user_grant"] == 0
    assert result["reserve_seeded"] == 500
    assert ledger.reserve_balance("wv", "2026-04-18", db_path=db) == 500


def test_allocator_no_budget_returns_error(db):
    result = allocator.allocate_state("ca", "2026-04-18", db_path=db)
    assert result.get("error") == "no_budget"


def test_allocator_handles_rounding(db):
    _seed_budget(db, "ca", "2026-04-18", 1000)
    _seed_users(db, "ca", 7)  # 600 / 7 = 85.7, floor=85, so 7*85=595

    result = allocator.allocate_state("ca", "2026-04-18", db_path=db)
    assert result["per_user_grant"] == 85
    # Reserve picks up the rounding remainder: 1000 - (85*7) = 405
    assert result["reserve_seeded"] == 405


# --- Step 8: Intensity Cache ----------------------------------------------

def test_cache_refresh_stores_data():
    client = MagicMock()
    client.has_key = True
    client.fetch_fuel_mix_latest.return_value = {
        "intensity_gco2_per_kwh": 234.5,
        "mix_pct": {"SUN": 0.7, "NG": 0.3},
        "period": "2026-04-18T14",
        "fetched_at": time.time(),
    }
    cache = intensity_cache.IntensityCache(client=client)
    cache.refresh_all()
    data = cache.snapshot()
    assert "ca" in data
    assert data["ca"]["intensity"] == 234.5
    assert data["ca"]["source"] == "eia"


def test_cache_get_returns_live_when_fresh():
    client = MagicMock()
    client.has_key = True
    client.fetch_fuel_mix_latest.return_value = {
        "intensity_gco2_per_kwh": 180.0, "mix_pct": {},
        "period": "2026-04-18T14", "fetched_at": time.time(),
    }
    cache = intensity_cache.IntensityCache(client=client)
    cache.refresh_all()
    result = cache.get("ca")
    assert result["intensity"] == 180.0
    assert result["source"] == "eia"


def test_cache_get_falls_back_to_daily_budget(db):
    _seed_budget(db, "ca", __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc).strftime("%Y-%m-%d"), 1600)
    client = MagicMock()
    client.has_key = False  # simulate no key
    cache = intensity_cache.IntensityCache(client=client)
    result = cache.get("ca", db_path=db)
    assert result["source"] == "daily_avg_fallback"
    assert result["intensity"] == 250.0


def test_cache_get_hardcoded_fallback_with_no_data(db):
    client = MagicMock()
    client.has_key = False
    cache = intensity_cache.IntensityCache(client=client)
    result = cache.get("ca", db_path=db)
    assert result["source"] == "hardcoded_fallback"
    assert result["intensity"] == 500.0


# --- Step 9: Identity ------------------------------------------------------

def test_register_creates_user(db):
    uid = identity.register("alice", "pw123", "ca", db_path=db)
    assert uid > 0
    with ledger_schema.connect(db) as conn:
        row = conn.execute(
            "SELECT username, state FROM users WHERE id = ?", (uid,)
        ).fetchone()
    assert row["username"] == "alice"
    assert row["state"] == "ca"


def test_register_rejects_duplicate(db):
    identity.register("alice", "pw", "ca", db_path=db)
    with pytest.raises(identity.IdentityError, match="already taken"):
        identity.register("alice", "pw", "ca", db_path=db)


def test_register_rejects_bad_state(db):
    with pytest.raises(identity.IdentityError, match="unsupported state"):
        identity.register("bob", "pw", "xx", db_path=db)


def test_register_hashes_password(db):
    identity.register("alice", "hunter2", "ca", db_path=db)
    with ledger_schema.connect(db) as conn:
        row = conn.execute(
            "SELECT password_hash FROM users WHERE username='alice'"
        ).fetchone()
    assert row["password_hash"] != "hunter2"
    assert row["password_hash"].startswith("$2")  # bcrypt prefix


def test_login_returns_token(db):
    identity.register("alice", "pw123", "ca", db_path=db)
    token = identity.login("alice", "pw123", db_path=db)
    assert isinstance(token, str) and len(token) > 20


def test_login_wrong_password_rejected(db):
    identity.register("alice", "pw123", "ca", db_path=db)
    with pytest.raises(identity.IdentityError, match="invalid credentials"):
        identity.login("alice", "wrong", db_path=db)


def test_login_unknown_user_rejected(db):
    with pytest.raises(identity.IdentityError, match="invalid credentials"):
        identity.login("nobody", "pw", db_path=db)


def test_verify_token_roundtrip(db):
    uid = identity.register("alice", "pw", "ca", db_path=db)
    token = identity.login("alice", "pw", db_path=db)
    payload = identity.verify_token(token)
    assert payload["user_id"] == uid
    assert payload["state"] == "ca"
    assert payload["allocation_class"] == "standard"


def test_verify_token_rejects_garbage():
    with pytest.raises(identity.IdentityError):
        identity.verify_token("not.a.real.token")


def test_verify_token_rejects_wrong_signature():
    token = identity.issue_token(1, "ca", "standard")
    # Tamper with last char
    bad = token[:-1] + ("x" if token[-1] != "x" else "y")
    with pytest.raises(identity.IdentityError):
        identity.verify_token(bad)
