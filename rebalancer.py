"""Rebalancer: moves tokens from dormant accounts back to state reserves."""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import config
import ledger
import ledger_schema

log = logging.getLogger("rebalancer")


def sweep(utc_date: Optional[str] = None,
          db_path: Optional[str] = None) -> dict:
    """
    Dormant = last_spend_at older than DORMANT_MINUTES (or never spent,
    and granted > 0). Reclaim half of balance from each.
    """
    date = utc_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cutoff = (datetime.now(timezone.utc)
              - timedelta(minutes=config.DORMANT_MINUTES)).isoformat()

    with ledger_schema.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT user_id, state, balance_tokens, granted_tokens, last_spend_at "
            "FROM accounts WHERE utc_date = ? AND balance_tokens > 0",
            (date,)
        ).fetchall()

    total_reclaimed = 0
    count = 0
    for r in rows:
        last = r["last_spend_at"]
        # Dormant if never spent, OR last spend is older than cutoff.
        if last is not None and last > cutoff:
            continue
        # Only reclaim if they still hold > half of initial grant
        # (prevents taking from users who are just low on tokens).
        if r["balance_tokens"] <= r["granted_tokens"] // 2:
            continue
        take = r["balance_tokens"] // 2
        if take <= 0:
            continue
        actually_taken = ledger.reclaim(
            r["user_id"], take, "rebalancer_dormant_sweep",
            utc_date=date, db_path=db_path
        )
        if actually_taken > 0:
            total_reclaimed += actually_taken
            count += 1

    return {
        "utc_date": date,
        "accounts_swept": count,
        "total_reclaimed": total_reclaimed,
    }
