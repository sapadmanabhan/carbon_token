"""
Max-Min Fairness allocation (Bertsekas & Gallager, 1987).

Every user in a state is entitled to their 'fair share' = cap / N,
where N is the number of registered users in that state.

On registration:
    user gets fair_share from reserve immediately (no batch job needed).

On drip (mid-request auto-topup):
    user can borrow up to fair_share additional from reserve,
    capping their maximum daily spend at 2× fair share.

This is the same algorithm TCP uses to share bandwidth across flows.
"""

from typing import Optional
import ledger_schema
import ledger


def fair_share(state: str, utc_date: str,
               db_path: Optional[str] = None) -> int:
    """
    Max-min fair share = cap / N_users for today.
    Returns 0 if no budget exists yet, or no users in state.
    """
    with ledger_schema.connect(db_path) as conn:
        budget = conn.execute(
            "SELECT cap_tokens FROM daily_budgets "
            "WHERE state = ? AND utc_date = ?", (state, utc_date)
        ).fetchone()
        if not budget:
            return 0
        users = conn.execute(
            "SELECT COUNT(*) AS n FROM users WHERE state = ?", (state,)
        ).fetchone()["n"]
    if users == 0:
        return 0
    return budget["cap_tokens"] // users


def grant_fair_share_from_reserve(user_id: int, state: str, utc_date: str,
                                   db_path: Optional[str] = None) -> dict:
    """
    Pull this user's fair share from the reserve and grant to them.
    Used on registration. Returns dict with granted amount + reserve remaining.
    Fails gracefully if reserve can't cover (still grants whatever's available).
    """
    share = fair_share(state, utc_date, db_path)
    if share <= 0:
        return {"granted": 0, "reason": "no_budget_or_no_users"}

    reserve = ledger.reserve_balance(state, utc_date, db_path)
    take = min(share, reserve)
    if take <= 0:
        return {"granted": 0, "reason": "reserve_empty"}

    # Move `take` from reserve to user account
    with ledger_schema.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            new_reserve = reserve - take
            conn.execute(
                "UPDATE reserves SET pool_tokens = ? "
                "WHERE state = ? AND utc_date = ?",
                (new_reserve, state, utc_date)
            )
            conn.execute(
                "INSERT INTO transactions "
                "(ts, user_id, state, utc_date, tx_type, delta, balance_after, reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (_now(), None, state, utc_date, "refill", -take, new_reserve,
                 f"fair_share_grant_to_user={user_id}")
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # Now grant to the user
    new_balance = ledger.grant(user_id, state, take, "fair_share_on_register",
                                utc_date=utc_date, db_path=db_path)
    return {"granted": take, "new_balance": new_balance,
            "reserve_remaining": new_reserve, "fair_share": share}


def drip_cap_for_user(user_id: int, state: str, utc_date: str,
                      db_path: Optional[str] = None) -> int:
    """
    Max additional tokens this user can pull from reserve today beyond
    their granted amount. Capped at fair_share (so total daily spend
    cannot exceed 2× fair share).
    """
    share = fair_share(state, utc_date, db_path)
    with ledger_schema.connect(db_path) as conn:
        # How much have they already drip-borrowed today?
        row = conn.execute(
            "SELECT COALESCE(SUM(delta), 0) AS borrowed FROM transactions "
            "WHERE user_id = ? AND utc_date = ? "
            "AND tx_type = 'refill' AND reason = 'drip_from_reserve'",
            (user_id, utc_date)
        ).fetchone()
    already_borrowed = row["borrowed"] if row else 0
    remaining = share - already_borrowed
    return max(0, remaining)


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
