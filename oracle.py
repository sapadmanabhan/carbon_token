"""
Oracle: daily sync. Computes each state's cap from 24h EIA fuel mix.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import config
import ledger_schema
from eia_client import EIAClient, EIAError, STATE_TO_BA

log = logging.getLogger("oracle")

BASE_CAP = config.BASE_CAP_TOKENS
REFERENCE = 400.0
CLAMP_MIN = 0.3
CLAMP_MAX = 2.0

FALLBACK_INTENSITY = {
    "ca": 250, "tx": 520, "wa": 180, "wv": 780, "ny": 310,
    "pa": 450, "fl": 490, "oh": 650, "il": 420, "ga": 510,
}


def compute_cap(intensity: float) -> int:
    ratio = REFERENCE / max(intensity, 1.0)
    return int(BASE_CAP * max(CLAMP_MIN, min(CLAMP_MAX, ratio)))


def _write_budget(conn, state, utc_date, cap, intensity, ba, source):
    conn.execute(
        "INSERT OR REPLACE INTO daily_budgets "
        "(state, utc_date, cap_tokens, intensity_avg, ba_code, source, computed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (state, utc_date, cap, intensity, ba, source,
         datetime.now(timezone.utc).isoformat())
    )


def _yesterday_budget(conn, state, utc_date):
    y = (datetime.strptime(utc_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return conn.execute(
        "SELECT cap_tokens, intensity_avg, ba_code FROM daily_budgets "
        "WHERE state = ? AND utc_date = ?", (state, y)
    ).fetchone()


def sync_state(state: str, client: EIAClient, utc_date: str,
               db_path: Optional[str] = None) -> dict:
    ba = STATE_TO_BA[state]
    end = datetime.strptime(utc_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = end - timedelta(hours=24)

    try:
        result = client.fetch_fuel_mix_window(ba, start, end)
        intensity = result["intensity_avg_gco2_per_kwh"]
        cap = compute_cap(intensity)
        source = "eia"
    except EIAError as e:
        log.warning("EIA fail for %s: %s", state, e)
        with ledger_schema.connect(db_path) as conn:
            prev = _yesterday_budget(conn, state, utc_date)
        if prev:
            intensity = prev["intensity_avg"]
            cap = prev["cap_tokens"]
            source = "stale"
        else:
            intensity = float(FALLBACK_INTENSITY.get(state, 500))
            cap = compute_cap(intensity)
            source = "fallback"

    with ledger_schema.connect(db_path) as conn:
        _write_budget(conn, state, utc_date, cap, intensity, ba, source)
        conn.commit()

    return {"state": state, "utc_date": utc_date, "cap_tokens": cap,
            "intensity_avg": intensity, "ba_code": ba, "source": source}


def sync_all(utc_date: Optional[str] = None,
             db_path: Optional[str] = None,
             client: Optional[EIAClient] = None) -> list:
    date = utc_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    c = client or EIAClient()
    return [sync_state(s, c, date, db_path) for s in STATE_TO_BA]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ledger_schema.init_db()
    results = sync_all()
    print(f"\n{'state':<6} {'cap':>6} {'intensity':>10} {'source':<10} ba")
    print("-" * 50)
    for r in sorted(results, key=lambda x: x["intensity_avg"]):
        print(f"{r['state']:<6} {r['cap_tokens']:>6} "
              f"{r['intensity_avg']:>10.1f} {r['source']:<10} {r['ba_code']}")
