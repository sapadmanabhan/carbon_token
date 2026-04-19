"""
Ledger operations. All writes go through BEGIN IMMEDIATE transactions.
All balance changes append a row to transactions. No in-Python check-then-write
races — the database is the source of truth.

Four public operations:
    grant(user_id, amount, reason, ...)    # Allocator calls this
    try_spend(user_id, amount, ...)        # Enforcement calls this
    reclaim(user_id, amount, reason, ...)  # Rebalancer calls this
    balance(user_id, utc_date)             # read-only dashboard

Plus a few helpers (seed_reserve, ensure_account, stats) that keep the
Allocator and Rebalancer simple.
"""

import sqlite3
from datetime import datetime, timezone
from typing import NamedTuple, Optional

import ledger_schema


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------

class SpendResult(NamedTuple):
    ok: bool
    new_balance: int
    attempted_cost: int
    reason: str                  # 'spent' | 'insufficient' | 'no_account'
    drew_from_reserve: int = 0   # tokens refilled from reserve mid-spend


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _append_tx(conn, *, user_id, state, utc_date, tx_type, delta,
               balance_after, reason, request_id=None):
    """Append one row to transactions. Caller provides the transaction context."""
    conn.execute(
        "INSERT INTO transactions "
        "(ts, user_id, state, utc_date, tx_type, delta, "
        "balance_after, reason, request_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (_now_iso(), user_id, state, utc_date, tx_type, delta,
         balance_after, reason, request_id)
    )


# ---------------------------------------------------------------------------
# grant — Allocator and on-demand account creation
# ---------------------------------------------------------------------------

def grant(user_id: int, state: str, amount: int, reason: str,
          utc_date: Optional[str] = None, db_path: Optional[str] = None) -> int:
    """
    Create or top-up an account row and credit tokens.
    Returns new balance. Atomic.
    """
    assert amount >= 0, "grant amount must be non-negative"
    date = utc_date or _today_utc()

    with ledger_schema.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT balance_tokens, granted_tokens FROM accounts "
                "WHERE user_id = ? AND utc_date = ?",
                (user_id, date)
            ).fetchone()

            if existing is None:
                new_balance = amount
                conn.execute(
                    "INSERT INTO accounts "
                    "(user_id, utc_date, state, granted_tokens, balance_tokens) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (user_id, date, state, amount, amount)
                )
            else:
                new_balance = existing["balance_tokens"] + amount
                new_granted = existing["granted_tokens"] + amount
                conn.execute(
                    "UPDATE accounts SET balance_tokens = ?, granted_tokens = ? "
                    "WHERE user_id = ? AND utc_date = ?",
                    (new_balance, new_granted, user_id, date)
                )

            _append_tx(conn,
                       user_id=user_id, state=state, utc_date=date,
                       tx_type="grant", delta=amount,
                       balance_after=new_balance, reason=reason)
            conn.commit()
            return new_balance
        except Exception:
            conn.rollback()
            raise


# ---------------------------------------------------------------------------
# try_spend — the hot path. ATOMIC.
# ---------------------------------------------------------------------------

def try_spend(user_id: int, amount: int, reason: str,
              request_id: Optional[str] = None,
              utc_date: Optional[str] = None,
              db_path: Optional[str] = None) -> SpendResult:
    """
    Atomic compare-and-decrement. Two concurrent calls cannot both succeed
    if only one has enough funds.

    If account lacks funds but the state's reserve has enough, auto-pulls
    the gap from reserve into the account (the 'drip' half of reserve-and-drip),
    records both a refill and a spend, then succeeds.

    Returns SpendResult. Never raises on insufficient funds; raises only on
    DB errors or missing account (caller must have registered first).
    """
    assert amount >= 0, "spend amount must be non-negative"
    date = utc_date or _today_utc()

    with ledger_schema.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            acct = conn.execute(
                "SELECT state, balance_tokens FROM accounts "
                "WHERE user_id = ? AND utc_date = ?",
                (user_id, date)
            ).fetchone()

            if acct is None:
                conn.rollback()
                return SpendResult(
                    ok=False, new_balance=0, attempted_cost=amount,
                    reason="no_account"
                )

            state = acct["state"]
            balance = acct["balance_tokens"]
            drew_from_reserve = 0

            # Fast path: account has enough
            if balance >= amount:
                new_balance = balance - amount
                conn.execute(
                    "UPDATE accounts SET balance_tokens = ?, last_spend_at = ? "
                    "WHERE user_id = ? AND utc_date = ?",
                    (new_balance, _now_iso(), user_id, date)
                )
                _append_tx(conn,
                           user_id=user_id, state=state, utc_date=date,
                           tx_type="spend", delta=-amount,
                           balance_after=new_balance, reason=reason,
                           request_id=request_id)
                conn.commit()
                return SpendResult(
                    ok=True, new_balance=new_balance,
                    attempted_cost=amount, reason="spent"
                )

            # Slow path: try to drip from reserve — but capped by fair share
            gap = amount - balance

            # Import here to avoid circular
            import fair_share as fs
            drip_available = fs.drip_cap_for_user(user_id, state, date, db_path)
            if gap > drip_available:
                # Over the max-min fair borrow limit
                conn.rollback()
                return SpendResult(
                    ok=False, new_balance=balance,
                    attempted_cost=amount, reason="insufficient"
                )

            reserve = conn.execute(
                "SELECT pool_tokens FROM reserves "
                "WHERE state = ? AND utc_date = ?",
                (state, date)
            ).fetchone()

            if reserve is None or reserve["pool_tokens"] < gap:
                # Neither account nor reserve can cover. Reject.
                conn.rollback()
                return SpendResult(
                    ok=False, new_balance=balance,
                    attempted_cost=amount, reason="insufficient"
                )

            # Drip: move `gap` from reserve → account, then spend the full `amount`
            new_reserve = reserve["pool_tokens"] - gap
            conn.execute(
                "UPDATE reserves SET pool_tokens = ? "
                "WHERE state = ? AND utc_date = ?",
                (new_reserve, state, date)
            )
            _append_tx(conn,
                       user_id=None, state=state, utc_date=date,
                       tx_type="refill", delta=-gap,
                       balance_after=new_reserve,
                       reason=f"drip_to_user={user_id}")

            topped_up = balance + gap
            _append_tx(conn,
                       user_id=user_id, state=state, utc_date=date,
                       tx_type="refill", delta=gap,
                       balance_after=topped_up,
                       reason=f"drip_from_reserve")

            new_balance = topped_up - amount
            conn.execute(
                "UPDATE accounts SET balance_tokens = ?, last_spend_at = ? "
                "WHERE user_id = ? AND utc_date = ?",
                (new_balance, _now_iso(), user_id, date)
            )
            _append_tx(conn,
                       user_id=user_id, state=state, utc_date=date,
                       tx_type="spend", delta=-amount,
                       balance_after=new_balance, reason=reason,
                       request_id=request_id)

            drew_from_reserve = gap
            conn.commit()
            return SpendResult(
                ok=True, new_balance=new_balance,
                attempted_cost=amount, reason="spent",
                drew_from_reserve=drew_from_reserve
            )
        except Exception:
            conn.rollback()
            raise


# ---------------------------------------------------------------------------
# reclaim — Rebalancer
# ---------------------------------------------------------------------------

def reclaim(user_id: int, amount: int, reason: str,
            utc_date: Optional[str] = None,
            db_path: Optional[str] = None) -> int:
    """
    Move tokens from a user's account back to the state reserve.
    Bounded by current balance — never takes more than what's there.
    Returns the amount actually reclaimed. Atomic.
    """
    assert amount >= 0
    date = utc_date or _today_utc()

    with ledger_schema.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            acct = conn.execute(
                "SELECT state, balance_tokens FROM accounts "
                "WHERE user_id = ? AND utc_date = ?",
                (user_id, date)
            ).fetchone()
            if acct is None:
                conn.rollback()
                return 0

            take = min(amount, acct["balance_tokens"])
            if take == 0:
                conn.rollback()
                return 0

            state = acct["state"]
            new_balance = acct["balance_tokens"] - take
            conn.execute(
                "UPDATE accounts SET balance_tokens = ? "
                "WHERE user_id = ? AND utc_date = ?",
                (new_balance, user_id, date)
            )
            _append_tx(conn,
                       user_id=user_id, state=state, utc_date=date,
                       tx_type="reclaim", delta=-take,
                       balance_after=new_balance, reason=reason)

            reserve = conn.execute(
                "SELECT pool_tokens FROM reserves "
                "WHERE state = ? AND utc_date = ?",
                (state, date)
            ).fetchone()
            new_reserve = (reserve["pool_tokens"] if reserve else 0) + take
            if reserve is None:
                conn.execute(
                    "INSERT INTO reserves (state, utc_date, pool_tokens) "
                    "VALUES (?, ?, ?)",
                    (state, date, new_reserve)
                )
            else:
                conn.execute(
                    "UPDATE reserves SET pool_tokens = ? "
                    "WHERE state = ? AND utc_date = ?",
                    (new_reserve, state, date)
                )
            _append_tx(conn,
                       user_id=None, state=state, utc_date=date,
                       tx_type="refill", delta=take,
                       balance_after=new_reserve,
                       reason=f"reclaim_from_user={user_id}")
            conn.commit()
            return take
        except Exception:
            conn.rollback()
            raise


# ---------------------------------------------------------------------------
# balance — read-only
# ---------------------------------------------------------------------------

def balance(user_id: int, utc_date: Optional[str] = None,
            db_path: Optional[str] = None) -> Optional[dict]:
    """Returns the account row as a dict, or None if no account today."""
    date = utc_date or _today_utc()
    with ledger_schema.connect(db_path) as conn:
        row = conn.execute(
            "SELECT user_id, utc_date, state, granted_tokens, "
            "balance_tokens, last_spend_at "
            "FROM accounts WHERE user_id = ? AND utc_date = ?",
            (user_id, date)
        ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Reserve seed — used by Allocator to initialize the daily pool
# ---------------------------------------------------------------------------

def seed_reserve(state: str, amount: int, utc_date: Optional[str] = None,
                 db_path: Optional[str] = None) -> int:
    """Set the state's reserve for today (REPLACES existing, not adds)."""
    assert amount >= 0
    date = utc_date or _today_utc()

    with ledger_schema.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO reserves (state, utc_date, pool_tokens) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(state, utc_date) DO UPDATE SET pool_tokens = excluded.pool_tokens",
                (state, date, amount)
            )
            _append_tx(conn,
                       user_id=None, state=state, utc_date=date,
                       tx_type="refill", delta=amount,
                       balance_after=amount, reason="reserve_seed")
            conn.commit()
            return amount
        except Exception:
            conn.rollback()
            raise


def reserve_balance(state: str, utc_date: Optional[str] = None,
                    db_path: Optional[str] = None) -> int:
    """Read-only reserve peek."""
    date = utc_date or _today_utc()
    with ledger_schema.connect(db_path) as conn:
        row = conn.execute(
            "SELECT pool_tokens FROM reserves "
            "WHERE state = ? AND utc_date = ?",
            (state, date)
        ).fetchone()
    return row["pool_tokens"] if row else 0
