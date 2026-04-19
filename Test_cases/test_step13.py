"""Tests for compliance endpoints (Step 13)."""

import time
from unittest.mock import MagicMock
import pytest

import ledger_schema
import ledger
import identity


@pytest.fixture
def client(tmp_path, monkeypatch):
    p = str(tmp_path / "c.db")
    ledger_schema.init_db(p)
    monkeypatch.setattr("config.DB_PATH", p)

    from intensity_cache import IntensityCache
    fake = IntensityCache(client=MagicMock(has_key=False))
    fake._data = {
        "ca": {"intensity": 200.0, "mix_pct": {}, "period": "2026-04-18T14",
               "fetched_at": time.time(), "source": "eia", "ba": "CISO"},
    }
    monkeypatch.setattr("intensity_cache._instance", fake)

    import importlib, server
    importlib.reload(server)
    server.app.testing = True
    return server.app.test_client()


def _auth(client):
    client.post("/register", json={"username": "a", "password": "p", "state": "ca"})
    tok = client.post("/login", json={"username": "a", "password": "p"}).get_json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def test_compliance_report_requires_auth(client):
    r = client.get("/api/compliance/report")
    assert r.status_code == 401


def test_compliance_report_returns_aggregates(client):
    h = _auth(client)
    uid = identity.verify_token(h["Authorization"][7:])["user_id"]
    ledger.grant(uid, "ca", 50, "test")
    ledger.try_spend(uid, 5, "test_spend")

    r = client.get("/api/compliance/report", headers=h)
    assert r.status_code == 200
    body = r.get_json()
    assert "totals_by_type" in body
    assert "by_state" in body
    assert "daily_budgets" in body
    # Should see at least one grant and one spend in totals
    types = {t["tx_type"] for t in body["totals_by_type"]}
    assert "grant" in types
    assert "spend" in types


def test_compliance_transactions_user_scoped(client):
    h = _auth(client)
    uid = identity.verify_token(h["Authorization"][7:])["user_id"]
    ledger.grant(uid, "ca", 20, "t1")
    ledger.try_spend(uid, 3, "t2")

    r = client.get("/api/compliance/transactions?limit=10", headers=h)
    body = r.get_json()
    assert body["count"] == 2
    for tx in body["transactions"]:
        assert tx["user_id"] == uid


def test_my_history(client):
    h = _auth(client)
    uid = identity.verify_token(h["Authorization"][7:])["user_id"]
    ledger.grant(uid, "ca", 20, "t")
    ledger.try_spend(uid, 5, "t")

    r = client.get("/api/compliance/my_history", headers=h)
    body = r.get_json()
    assert body["user_id"] == uid
    assert len(body["days"]) >= 1
    assert body["days"][0]["granted"] == 20
    assert body["days"][0]["spent"] == 5
