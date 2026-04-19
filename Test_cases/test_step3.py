"""
Step 3 tests: Ledger operations + concurrency.

Critical test: test_concurrent_spend_never_overspends() spawns many threads
all trying to spend from the same account. If any ever succeeds when funds
are exhausted, the system's economic correctness is broken.

Run: python -m pytest test_step3.py -v
"""

import os
import sqlite3
import threading
from datetime import datetime, timezone
import pytest

import ledger_schema
import ledger


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Fresh DB with a few users pre-inserted."""
    p = str(tmp_path / "ledger_test.db")
    ledger_schema.init_db(p)
    with ledger_schema.connect(p) as conn:
        for i, (u, st) in enumerate([("alice", "ca"), ("bob", "wv"),
                                      ("carol", "ca"), ("dave", "wv")], start=1):
            conn.execute(
                "INSERT INTO users (id, username, password_hash, state, "
                "created_at) VALUES (?, ?, ?, ?, ?)",
                (i, u, "x", st, "2026-04-18T00:00:00+00:00")
            )
        conn.commit()
    return p


# ---------------------------------------------------------------------------
# Basic operations
# ---------------------------------------------------------------------------

def test_grant_creates_account(db):
    new_balance = ledger.grant(1, "ca", 21, "initial", "2026-04-18", db_path=db)
    assert new_balance == 21
    acct = ledger.balance(1, "2026-04-18", db_path=db)
    assert acct["balance_tokens"] == 21
    assert acct["granted_tokens"] == 21
    assert acct["state"] == "ca"


def test_grant_appends_transaction(db):
    ledger.grant(1, "ca", 21, "initial", "2026-04-18", db_path=db)
    with ledger_schema.connect(db) as conn:
        txs = conn.execute(
            "SELECT * FROM transactions WHERE user_id = 1"
        ).fetchall()
    assert len(txs) == 1
    assert txs[0]["tx_type"] == "grant"
    assert txs[0]["delta"] == 21
    assert txs[0]["balance_after"] == 21


def test_grant_twice_stacks(db):
    ledger.grant(1, "ca", 10, "initial", "2026-04-18", db_path=db)
    final = ledger.grant(1, "ca", 5, "bonus", "2026-04-18", db_path=db)
    assert final == 15
    acct = ledger.balance(1, "2026-04-18", db_path=db)
    assert acct["granted_tokens"] == 15


# ---------------------------------------------------------------------------
# try_spend basics
# ---------------------------------------------------------------------------

def test_spend_success(db):
    ledger.grant(1, "ca", 21, "initial", "2026-04-18", db_path=db)
    result = ledger.try_spend(1, 5, "inference", utc_date="2026-04-18", db_path=db)
    assert result.ok is True
    assert result.new_balance == 16
    assert result.reason == "spent"
    assert result.drew_from_reserve == 0


def test_spend_insufficient_no_reserve(db):
    ledger.grant(1, "ca", 3, "initial", "2026-04-18", db_path=db)
    result = ledger.try_spend(1, 5, "training", utc_date="2026-04-18", db_path=db)
    assert result.ok is False
    assert result.new_balance == 3  # unchanged
    assert result.reason == "insufficient"
    acct = ledger.balance(1, "2026-04-18", db_path=db)
    assert acct["balance_tokens"] == 3  # never touched


def test_spend_no_account(db):
    result = ledger.try_spend(1, 5, "inference", utc_date="2026-04-18", db_path=db)
    assert result.ok is False
    assert result.reason == "no_account"


def test_spend_drips_from_reserve(db):
    """Reserve-and-drip: when user has less than cost but reserve covers gap."""
    ledger.grant(1, "ca", 3, "initial", "2026-04-18", db_path=db)
    ledger.seed_reserve("ca", 100, "2026-04-18", db_path=db)

    result = ledger.try_spend(1, 10, "training",
                              utc_date="2026-04-18", db_path=db)
    assert result.ok is True
    assert result.drew_from_reserve == 7  # gap = 10 - 3
    assert result.new_balance == 0  # all spent after drip
    assert ledger.reserve_balance("ca", "2026-04-18", db_path=db) == 93


def test_spend_insufficient_even_with_reserve(db):
    """Account low AND reserve too small = reject, no partial spend."""
    ledger.grant(1, "ca", 3, "initial", "2026-04-18", db_path=db)
    ledger.seed_reserve("ca", 2, "2026-04-18", db_path=db)

    result = ledger.try_spend(1, 10, "training",
                              utc_date="2026-04-18", db_path=db)
    assert result.ok is False
    assert result.reason == "insufficient"
    # Nothing moved
    assert ledger.balance(1, "2026-04-18", db_path=db)["balance_tokens"] == 3
    assert ledger.reserve_balance("ca", "2026-04-18", db_path=db) == 2


# ---------------------------------------------------------------------------
# reclaim
# ---------------------------------------------------------------------------

def test_reclaim_moves_tokens_to_reserve(db):
    ledger.grant(1, "ca", 20, "initial", "2026-04-18", db_path=db)
    ledger.seed_reserve("ca", 50, "2026-04-18", db_path=db)

    taken = ledger.reclaim(1, 10, "dormant", "2026-04-18", db_path=db)
    assert taken == 10
    assert ledger.balance(1, "2026-04-18", db_path=db)["balance_tokens"] == 10
    assert ledger.reserve_balance("ca", "2026-04-18", db_path=db) == 60


def test_reclaim_bounded_by_balance(db):
    ledger.grant(1, "ca", 5, "initial", "2026-04-18", db_path=db)
    taken = ledger.reclaim(1, 999, "dormant", "2026-04-18", db_path=db)
    assert taken == 5  # can't take more than they have
    assert ledger.balance(1, "2026-04-18", db_path=db)["balance_tokens"] == 0


# ---------------------------------------------------------------------------
# Transaction log integrity
# ---------------------------------------------------------------------------

def test_spend_logs_balance_after(db):
    ledger.grant(1, "ca", 20, "initial", "2026-04-18", db_path=db)
    ledger.try_spend(1, 5, "r1", request_id="req-1",
                     utc_date="2026-04-18", db_path=db)
    ledger.try_spend(1, 3, "r2", request_id="req-2",
                     utc_date="2026-04-18", db_path=db)

    with ledger_schema.connect(db) as conn:
        rows = conn.execute(
            "SELECT tx_type, delta, balance_after, request_id "
            "FROM transactions WHERE user_id = 1 ORDER BY id"
        ).fetchall()

    assert [r["tx_type"] for r in rows] == ["grant", "spend", "spend"]
    assert [r["balance_after"] for r in rows] == [20, 15, 12]
    assert rows[1]["request_id"] == "req-1"
    assert rows[2]["request_id"] == "req-2"


def test_drip_logs_refill_and_spend(db):
    ledger.grant(1, "ca", 2, "initial", "2026-04-18", db_path=db)
    ledger.seed_reserve("ca", 50, "2026-04-18", db_path=db)
    ledger.try_spend(1, 5, "training", request_id="req-9",
                     utc_date="2026-04-18", db_path=db)

    with ledger_schema.connect(db) as conn:
        rows = conn.execute(
            "SELECT tx_type, delta, user_id, reason FROM transactions "
            "WHERE state = 'ca' AND utc_date = '2026-04-18' ORDER BY id"
        ).fetchall()

    types = [r["tx_type"] for r in rows]
    # Expected sequence: initial grant, seed reserve, drip out, drip in, spend
    assert types == ["grant", "refill", "refill", "refill", "spend"]


# ---------------------------------------------------------------------------
# THE critical concurrency test
# ---------------------------------------------------------------------------

def test_concurrent_spend_never_overspends(db):
    """
    50 threads each try to spend 1 token from an account with only 20 tokens.
    Exactly 20 must succeed, 30 must fail. If atomicity breaks, >20 succeed
    and the account goes negative or the sum violates conservation.
    """
    ledger.grant(1, "ca", 20, "initial", "2026-04-18", db_path=db)

    successes = []
    failures = []
    lock = threading.Lock()
    barrier = threading.Barrier(50)

    def worker(idx):
        barrier.wait()  # release all at once
        r = ledger.try_spend(1, 1, f"req-{idx}",
                             request_id=f"rid-{idx}",
                             utc_date="2026-04-18", db_path=db)
        with lock:
            (successes if r.ok else failures).append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(successes) == 20, f"expected 20 successes, got {len(successes)}"
    assert len(failures) == 30, f"expected 30 failures, got {len(failures)}"

    # Final balance must be exactly 0
    final = ledger.balance(1, "2026-04-18", db_path=db)
    assert final["balance_tokens"] == 0

    # Transaction log must show exactly 20 spends + 1 grant = 21 rows
    with ledger_schema.connect(db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE user_id = 1"
        ).fetchone()[0]
    assert count == 21


def test_concurrent_spend_with_reserve_drip(db):
    """
    Account has 5 tokens, reserve has 20. 20 threads each spend 1 token.
    First 5 hit account directly; next 15 should drip from reserve.
    All 20 should succeed, reserve should end at 5, account at 0.
    """
    ledger.grant(1, "ca", 5, "initial", "2026-04-18", db_path=db)
    ledger.seed_reserve("ca", 20, "2026-04-18", db_path=db)

    successes = []
    lock = threading.Lock()
    barrier = threading.Barrier(20)

    def worker(idx):
        barrier.wait()
        r = ledger.try_spend(1, 1, f"req-{idx}",
                             utc_date="2026-04-18", db_path=db)
        with lock:
            if r.ok:
                successes.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(successes) == 20
    assert ledger.balance(1, "2026-04-18", db_path=db)["balance_tokens"] == 0
    assert ledger.reserve_balance("ca", "2026-04-18", db_path=db) == 5


def test_concurrent_spend_conservation_of_tokens(db):
    """
    Hammer with mixed-size spends. Verify account + reserve + total spent
    always equals total granted. If anything is double-spent or lost, this fails.
    """
    GRANT = 100
    RESERVE = 200
    TOTAL = GRANT + RESERVE

    ledger.grant(1, "ca", GRANT, "initial", "2026-04-18", db_path=db)
    ledger.seed_reserve("ca", RESERVE, "2026-04-18", db_path=db)

    spent = []
    lock = threading.Lock()
    barrier = threading.Barrier(40)

    def worker(idx):
        barrier.wait()
        amt = (idx % 7) + 1  # spend 1..7 tokens
        r = ledger.try_spend(1, amt, f"req-{idx}",
                             utc_date="2026-04-18", db_path=db)
        if r.ok:
            with lock:
                spent.append(amt)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for t in threads: t.start()
    for t in threads: t.join()

    final_balance = ledger.balance(1, "2026-04-18", db_path=db)["balance_tokens"]
    final_reserve = ledger.reserve_balance("ca", "2026-04-18", db_path=db)
    total_spent = sum(spent)

    # Conservation law: nothing disappears, nothing appears from nowhere
    assert final_balance + final_reserve + total_spent == TOTAL, (
        f"conservation violated: balance={final_balance} reserve={final_reserve} "
        f"spent={total_spent} total={TOTAL}"
    )
