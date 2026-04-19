"""
Step 2 tests — schema only. No ops yet.

Run: python -m pytest test_step2.py -v
"""

import os
import sqlite3
import pytest

import ledger_schema


@pytest.fixture
def db(tmp_path):
    """Fresh DB per test, isolated in tmp_path. Returns the path."""
    p = str(tmp_path / "test_carbon.db")
    ledger_schema.init_db(p)
    return p


def _tables(db):
    with ledger_schema.connect(db) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    return {r["name"] for r in rows}


def _indexes(db):
    with ledger_schema.connect(db) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    return {r["name"] for r in rows}


def _columns(db, table):
    with ledger_schema.connect(db) as conn:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"]: r["type"] for r in rows}


# -----------------------------------------------------------------------

def test_init_creates_db_file(tmp_path):
    p = str(tmp_path / "fresh.db")
    assert not os.path.exists(p)
    ledger_schema.init_db(p)
    assert os.path.exists(p)


def test_all_tables_present(db):
    tables = _tables(db)
    assert tables == {"users", "daily_budgets", "accounts",
                      "reserves", "transactions"}


def test_users_columns(db):
    cols = _columns(db, "users")
    assert set(cols.keys()) == {
        "id", "username", "password_hash", "state",
        "allocation_class", "created_at"
    }
    assert cols["id"] == "INTEGER"
    assert cols["username"] == "TEXT"


def test_transactions_columns(db):
    cols = _columns(db, "transactions")
    assert set(cols.keys()) == {
        "id", "ts", "user_id", "state", "utc_date",
        "tx_type", "delta", "balance_after", "reason", "request_id"
    }


def test_indexes_present(db):
    idx = _indexes(db)
    for name in ["idx_users_state", "idx_accounts_state_date",
                 "idx_tx_user_ts", "idx_tx_state_date"]:
        assert name in idx, f"missing index: {name}"


def test_wal_mode_enabled(db):
    with ledger_schema.connect(db) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_foreign_keys_enforced(db):
    """Inserting an account with a non-existent user_id must fail."""
    with ledger_schema.connect(db) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO accounts "
                "(user_id, utc_date, state, granted_tokens, balance_tokens) "
                "VALUES (?, ?, ?, ?, ?)",
                (99999, "2026-04-18", "ca", 21, 21)
            )
            conn.commit()


def test_init_is_idempotent(db):
    """Running init on an existing DB preserves data."""
    # Insert a row
    with ledger_schema.connect(db) as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, state, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("alice", "hash", "ca", "2026-04-18T00:00:00Z")
        )
        conn.commit()
    # Re-init
    ledger_schema.init_db(db)
    # Row still there
    with ledger_schema.connect(db) as conn:
        row = conn.execute(
            "SELECT username, state FROM users WHERE username='alice'"
        ).fetchone()
    assert row is not None
    assert row["username"] == "alice"
    assert row["state"] == "ca"


def test_reset_db_wipes_data(db):
    """reset_db must actually remove data."""
    with ledger_schema.connect(db) as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, state, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("bob", "hash", "wv", "2026-04-18T00:00:00Z")
        )
        conn.commit()
    ledger_schema.reset_db(db)
    with ledger_schema.connect(db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    assert count == 0


def test_primary_key_rejects_duplicate_budget(db):
    """daily_budgets PK on (state, utc_date) must prevent duplicates."""
    with ledger_schema.connect(db) as conn:
        conn.execute(
            "INSERT INTO daily_budgets "
            "(state, utc_date, cap_tokens, intensity_avg, ba_code, "
            "source, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ca", "2026-04-18", 1600, 250.0, "CISO", "eia",
             "2026-04-18T00:05:00Z")
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO daily_budgets "
                "(state, utc_date, cap_tokens, intensity_avg, ba_code, "
                "source, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("ca", "2026-04-18", 1200, 300.0, "CISO", "eia",
                 "2026-04-18T00:06:00Z")
            )
            conn.commit()
