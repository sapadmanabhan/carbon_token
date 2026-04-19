"""Tests for Cost (10), Server (11), Rebalancer (12)."""

import os
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest

import ledger_schema
import ledger
import identity
import cost
import rebalancer


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = str(tmp_path / "s101112.db")
    ledger_schema.init_db(p)
    monkeypatch.setattr("config.DB_PATH", p)
    return p


# --- Step 10: Cost ---------------------------------------------------------

def test_cost_at_reference_intensity():
    r = cost.compute_cost("/api/v1/training", 400)
    assert r["base"] == 10
    assert abs(r["multiplier"] - 1.0) < 0.01
    assert r["cost"] == 10


def test_cost_cheap_during_clean_grid():
    r = cost.compute_cost("/api/v1/training", 100)
    # 100/400 = 0.25, clamped to 0.33
    assert r["multiplier"] == 0.33
    assert r["cost"] == 4  # ceil(10 * 0.33)


def test_cost_expensive_during_dirty_grid():
    r = cost.compute_cost("/api/v1/training", 1500)
    # 1500/400 = 3.75, clamped to 3.0
    assert r["multiplier"] == 3.0
    assert r["cost"] == 30


def test_cost_unknown_route():
    r = cost.compute_cost("/nope", 400)
    assert r["cost"] is None


def test_cost_inference_cheaper_than_training():
    inf = cost.compute_cost("/api/v1/inference", 400)
    tr = cost.compute_cost("/api/v1/training", 400)
    assert inf["cost"] < tr["cost"]


# --- Step 11: Server -------------------------------------------------------

@pytest.fixture
def client(db, monkeypatch):
    # Patch intensity cache to return predictable values
    from intensity_cache import IntensityCache
    fake = IntensityCache(client=MagicMock(has_key=False))
    fake._data = {
        "ca": {"intensity": 200.0, "mix_pct": {}, "period": "2026-04-18T14",
               "fetched_at": time.time(), "source": "eia", "ba": "CISO"},
        "wv": {"intensity": 780.0, "mix_pct": {}, "period": "2026-04-18T14",
               "fetched_at": time.time(), "source": "eia", "ba": "PJM"},
    }
    monkeypatch.setattr("intensity_cache._instance", fake)

    import importlib, server
    importlib.reload(server)
    server.app.testing = True
    return server.app.test_client()


def test_register_and_login(client):
    r = client.post("/register", json={
        "username": "alice", "password": "pw", "state": "ca"
    })
    assert r.status_code == 201

    r = client.post("/login", json={"username": "alice", "password": "pw"})
    assert r.status_code == 200
    assert "token" in r.get_json()


def test_register_bad_state(client):
    r = client.post("/register", json={
        "username": "x", "password": "pw", "state": "zz"
    })
    assert r.status_code == 400


def test_protected_endpoint_requires_token(client):
    r = client.post("/api/v1/inference")
    assert r.status_code == 401


def test_inference_allowed_with_balance(client):
    client.post("/register", json={"username": "a", "password": "p", "state": "ca"})
    token = client.post("/login", json={"username": "a", "password": "p"}).get_json()["token"]
    # Grant tokens
    import identity
    payload = identity.verify_token(token)
    ledger.grant(payload["user_id"], "ca", 50, "test")

    r = client.post("/api/v1/inference",
                    headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    body = r.get_json()
    assert body["decision"] == "ALLOW"
    assert body["cost"] > 0
    assert body["balance_after"] < 50


def test_inference_blocked_no_balance(client):
    client.post("/register", json={"username": "a", "password": "p", "state": "ca"})
    token = client.post("/login", json={"username": "a", "password": "p"}).get_json()["token"]
    # No grant — account doesn't even exist yet, try_spend returns no_account
    r = client.post("/api/v1/inference",
                    headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 429
    body = r.get_json()
    assert body["decision"] == "BLOCK"
    assert "suggestions" in body


def test_block_includes_cleaner_alternatives(client):
    # User is in WV (dirty). On block, suggestions should list CA as cleaner.
    client.post("/register", json={"username": "b", "password": "p", "state": "wv"})
    token = client.post("/login", json={"username": "b", "password": "p"}).get_json()["token"]
    r = client.post("/api/v1/training",
                    headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 429
    alternatives = r.get_json()["suggestions"]["cleaner_states_now"]
    assert len(alternatives) > 0
    # CA (200) should be cleaner than WV's 780
    ca_alt = next(a for a in alternatives if a["state"] == "ca")
    assert ca_alt["intensity"] == 200.0


def test_me_endpoint(client):
    client.post("/register", json={"username": "c", "password": "p", "state": "ca"})
    token = client.post("/login", json={"username": "c", "password": "p"}).get_json()["token"]
    ledger.grant(identity.verify_token(token)["user_id"], "ca", 30, "test")

    r = client.get("/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    body = r.get_json()
    assert body["balance"] == 30
    assert body["cost_now"]["inference"] >= 1
    assert body["cost_now"]["training"] >= 1


def test_cleanest_endpoint(client):
    r = client.get("/api/carbon/cleanest")
    assert r.status_code == 200
    regions = r.get_json()["regions"]
    assert len(regions) == 10
    intensities = [r["intensity"] for r in regions]
    assert intensities == sorted(intensities)


def test_health_endpoint(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert "config" in r.get_json()


# --- Step 12: Rebalancer ---------------------------------------------------

def _setup_users_and_accounts(db):
    with ledger_schema.connect(db) as conn:
        for i in range(3):
            conn.execute(
                "INSERT INTO users (username, password_hash, state, created_at) "
                "VALUES (?, ?, ?, ?)",
                (f"u{i}", "x", "ca", "2026-04-18T00:00:00Z")
            )
        conn.commit()
    return [1, 2, 3]


def test_rebalancer_reclaims_from_dormant(db):
    uids = _setup_users_and_accounts(db)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for uid in uids:
        ledger.grant(uid, "ca", 20, "initial", utc_date=today, db_path=db)

    # User 1 has spent recently: not dormant
    ledger.try_spend(1, 1, "recent", utc_date=today, db_path=db)
    # Users 2 and 3 have never spent → dormant (last_spend_at is NULL)

    result = rebalancer.sweep(utc_date=today, db_path=db)
    assert result["accounts_swept"] == 2  # users 2 and 3
    assert result["total_reclaimed"] == 20  # 10 each
    # User 1 untouched
    assert ledger.balance(1, today, db_path=db)["balance_tokens"] == 19


def test_rebalancer_skips_recently_active(db):
    _setup_users_and_accounts(db)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for uid in [1, 2, 3]:
        ledger.grant(uid, "ca", 20, "initial", utc_date=today, db_path=db)
        ledger.try_spend(uid, 1, "recent", utc_date=today, db_path=db)

    result = rebalancer.sweep(utc_date=today, db_path=db)
    assert result["accounts_swept"] == 0


def test_rebalancer_skips_low_balance(db):
    _setup_users_and_accounts(db)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for uid in [1, 2, 3]:
        ledger.grant(uid, "ca", 20, "initial", utc_date=today, db_path=db)

    # Drain user 1 below half
    for _ in range(15):
        ledger.try_spend(1, 1, "drain", utc_date=today, db_path=db)

    # Push user 1's last_spend_at to very old
    import sqlite3
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with ledger_schema.connect(db) as conn:
        conn.execute(
            "UPDATE accounts SET last_spend_at = ? WHERE user_id = 1", (old,)
        )
        conn.commit()

    result = rebalancer.sweep(utc_date=today, db_path=db)
    # User 1 is dormant but under half of granted; skip.
    # Users 2 & 3 are dormant with full balance; reclaim 10 each.
    assert result["accounts_swept"] == 2
    assert result["total_reclaimed"] == 20
    # User 1 balance intact
    assert ledger.balance(1, today, db_path=db)["balance_tokens"] == 5
