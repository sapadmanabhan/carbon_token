"""Intensity cache: in-memory, refreshed periodically, used on hot path."""

import threading
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import config
import ledger_schema
from eia_client import EIAClient, EIAError, STATE_TO_BA

log = logging.getLogger("intensity_cache")


class IntensityCache:
    def __init__(self, client: Optional[EIAClient] = None):
        self.client = client or EIAClient()
        self._lock = threading.RLock()
        self._data: dict = {}  # state -> {intensity, mix_pct, period, fetched_at, source}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _loop(self):
        self.refresh_all()
        while not self._stop.wait(config.INTENSITY_REFRESH_SECONDS):
            try:
                self.refresh_all()
            except Exception as e:
                log.exception("refresh failed: %s", e)

    def refresh_all(self):
        if not self.client.has_key:
            return
        for state, ba in STATE_TO_BA.items():
            self._refresh_one(state, ba)

    def _refresh_one(self, state: str, ba: str):
        try:
            r = self.client.fetch_fuel_mix_latest(ba)
        except EIAError as e:
            log.warning("refresh %s failed: %s", state, e)
            return
        with self._lock:
            self._data[state] = {
                "intensity": r["intensity_gco2_per_kwh"],
                "mix_pct": r["mix_pct"],
                "period": r["period"],
                "ba": ba,
                "fetched_at": r["fetched_at"],
                "source": "eia",
            }

    def get(self, state: str, db_path: Optional[str] = None) -> dict:
        """
        Returns {intensity, source, mix_pct, period}.
        Falls back to daily_budgets.intensity_avg if cache is empty/stale.
        """
        state = state.lower()
        with self._lock:
            entry = self._data.get(state)
        if entry and (time.time() - entry["fetched_at"]) < 3600:
            return entry

        # Stale or missing: fall back to today's budget avg
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with ledger_schema.connect(db_path) as conn:
            row = conn.execute(
                "SELECT intensity_avg FROM daily_budgets "
                "WHERE state = ? AND utc_date = ?", (state, today)
            ).fetchone()
        if row:
            return {
                "intensity": row["intensity_avg"],
                "source": "daily_avg_fallback",
                "mix_pct": {}, "period": None, "ba": STATE_TO_BA.get(state),
                "fetched_at": time.time(),
            }
        return {
            "intensity": 500.0, "source": "hardcoded_fallback",
            "mix_pct": {}, "period": None, "ba": None,
            "fetched_at": time.time(),
        }

    def snapshot(self) -> dict:
        with self._lock:
            return {k: dict(v) for k, v in self._data.items()}


_instance: Optional[IntensityCache] = None
_lock = threading.Lock()


def get_cache() -> IntensityCache:
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = IntensityCache()
    return _instance
