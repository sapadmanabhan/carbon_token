"""
Ledger schema: table definitions, pragmas, init.

No operations (grant/spend/reclaim) in here — those live in ledger.py (Step 3).
Keeping schema separate makes the storage layer independently testable and
gives us one clean place to port to Postgres later.

Pragmas are connection-scoped for foreign_keys, so `connect()` always sets them.
journal_mode = WAL is a DB-level property, set once at init.
"""

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

import config


SCHEMA_STATEMENTS = [
    # --- users -----------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS users (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        username          TEXT    NOT NULL UNIQUE,
        password_hash     TEXT    NOT NULL,
        state             TEXT    NOT NULL,
        allocation_class  TEXT    NOT NULL DEFAULT 'standard',
        created_at        TEXT    NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_users_state ON users(state)",

    # --- daily_budgets ---------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS daily_budgets (
        state          TEXT    NOT NULL,
        utc_date       TEXT    NOT NULL,
        cap_tokens     INTEGER NOT NULL,
        intensity_avg  REAL    NOT NULL,
        ba_code        TEXT    NOT NULL,
        source         TEXT    NOT NULL,
        computed_at    TEXT    NOT NULL,
        PRIMARY KEY (state, utc_date)
    )
    """,

    # --- accounts --------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS accounts (
        user_id         INTEGER NOT NULL,
        utc_date        TEXT    NOT NULL,
        state           TEXT    NOT NULL,
        granted_tokens  INTEGER NOT NULL,
        balance_tokens  INTEGER NOT NULL,
        last_spend_at   TEXT,
        PRIMARY KEY (user_id, utc_date),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_accounts_state_date ON accounts(state, utc_date)",

    # --- reserves --------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS reserves (
        state        TEXT    NOT NULL,
        utc_date     TEXT    NOT NULL,
        pool_tokens  INTEGER NOT NULL,
        PRIMARY KEY (state, utc_date)
    )
    """,

    # --- transactions (append-only audit log) ----------------------------
    """
    CREATE TABLE IF NOT EXISTS transactions (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        ts             TEXT    NOT NULL,
        user_id        INTEGER,
        state          TEXT    NOT NULL,
        utc_date       TEXT    NOT NULL,
        tx_type        TEXT    NOT NULL,
        delta          INTEGER NOT NULL,
        balance_after  INTEGER NOT NULL,
        reason         TEXT    NOT NULL,
        request_id     TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tx_user_ts ON transactions(user_id, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_tx_state_date ON transactions(state, utc_date)",
]


def _apply_connection_pragmas(conn: sqlite3.Connection) -> None:
    """Per-connection pragmas. Must be called on every new connection."""
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")


def _apply_db_pragmas(conn: sqlite3.Connection) -> None:
    """DB-level pragmas. Persistent. Safe to re-apply (idempotent)."""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")


def init_db(db_path: str | None = None) -> str:
    """
    Create tables and indexes if missing. Idempotent — existing data survives.
    Returns the resolved DB path.
    """
    path = db_path or config.DB_PATH
    # Touch-create the file so sqlite3 doesn't need write perms on cwd's parent.
    conn = sqlite3.connect(path)
    try:
        _apply_db_pragmas(conn)
        _apply_connection_pragmas(conn)
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(stmt)
        conn.commit()
    finally:
        conn.close()
    return path


def reset_db(db_path: str | None = None) -> str:
    """
    Nuke and recreate. For tests and dev. DO NOT call in production paths.
    """
    path = db_path or config.DB_PATH
    # Remove the DB and WAL sidecar files if present.
    for suffix in ("", "-wal", "-shm"):
        p = path + suffix
        if os.path.exists(p):
            os.remove(p)
    return init_db(path)


@contextmanager
def connect(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """
    Context-managed connection with pragmas applied and Row row_factory.
    Usage:
        with connect() as conn:
            row = conn.execute("SELECT ...").fetchone()
    """
    path = db_path or config.DB_PATH
    conn = sqlite3.connect(path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    _apply_connection_pragmas(conn)
    try:
        yield conn
    finally:
        conn.close()
