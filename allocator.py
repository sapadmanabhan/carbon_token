"""Allocator: distributes daily cap as 60% per-user grants + 40% reserve."""

import logging
from datetime import datetime, timezone
from typing import Optional

import config
import ledger
import ledger_schema

log = logging.getLogger("allocator")


def allocate_state(state: str, utc_date: str,
                   db_path: Optional[str] = None) -> dict:
    """
    Max-min fairness mode: seed the reserve with the FULL daily cap.
    Users pull their fair share (cap/N) on registration or first spend.
    """
    with ledger_schema.connect(db_path) as conn:
        budget = conn.execute(
            "SELECT cap_tokens FROM daily_budgets "
            "WHERE state = ? AND utc_date = ?", (state, utc_date)
        ).fetchone()
        if not budget:
            return {"state": state, "error": "no_budget"}
        cap = budget["cap_tokens"]
        users = conn.execute(
            "SELECT id FROM users WHERE state = ?", (state,)
        ).fetchall()

    user_ids = [u["id"] for u in users]
    n = len(user_ids)

    # Full cap goes into reserve. Users pull fair share on demand.
    ledger.seed_reserve(state, cap, utc_date=utc_date, db_path=db_path)

    # For existing users (pre-dawn registration), grant their fair share now.
    fair = cap // n if n > 0 else 0
    granted_users = 0
    if fair > 0:
        import fair_share as fs
        for uid in user_ids:
            result = fs.grant_fair_share_from_reserve(uid, state, utc_date, db_path)
            if result.get("granted", 0) > 0:
                granted_users += 1

    return {
        "state": state, "cap": cap, "user_count": n,
        "fair_share": fair, "granted_users": granted_users,
        "reserve_remaining": ledger.reserve_balance(state, utc_date, db_path),
    }


def allocate_all(utc_date: Optional[str] = None,
                 db_path: Optional[str] = None) -> list:
    date = utc_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with ledger_schema.connect(db_path) as conn:
        states = conn.execute(
            "SELECT DISTINCT state FROM daily_budgets WHERE utc_date = ?", (date,)
        ).fetchall()
    return [allocate_state(s["state"], date, db_path) for s in states]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ledger_schema.init_db()
    results = allocate_all()
    for r in results:
        print(r)
