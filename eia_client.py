"""
EIA API v2 client.

Talks to api.eia.gov and nothing else. Returns normalized Python dicts.
No caching, no policy, no Flask — those live in other modules.

Two public methods:
    fetch_fuel_mix_window(ba, start, end) -> dict  # Query A: 24h window
    fetch_fuel_mix_latest(ba)             -> dict  # Query B: latest hour

Both return a dict with intensity_gco2_per_kwh, mix_mwh, mix_pct, period,
and total_mwh. Caller decides what to do with it.
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional
import requests

import config

log = logging.getLogger("eia_client")

EIA_BASE = "https://api.eia.gov/v2"

# IPCC AR5 lifecycle emission factors, gCO2-eq/kWh, median values.
EMISSION_FACTORS = {
    "COL": 820, "NG": 490, "OIL": 650, "NUC": 12,
    "SUN": 48,  "WND": 11, "WAT": 24, "GEO": 38,
    "BIO": 230, "OTH": 300,
}

# Default factor for fueltypes not in the table (conservative middling value).
DEFAULT_FACTOR = 300

# State → primary balancing authority (EIA respondent code).
# Simplification: several states span multiple BAs; this picks the dominant one.
STATE_TO_BA = {
    "ca": "CISO", "tx": "ERCO", "wa": "BPAT", "wv": "PJM",
    "ny": "NYIS", "pa": "PJM",  "fl": "FPC",  "oh": "PJM",
    "il": "MISO", "ga": "SOCO",
}


class EIAError(Exception):
    """Raised on any EIA API failure or parsing error."""


class EIAClient:
    def __init__(self, api_key: Optional[str] = None, timeout: float = 10.0):
        self.api_key = api_key or config.EIA_API_KEY
        self.timeout = timeout
        self.session = requests.Session()

    @property
    def has_key(self) -> bool:
        return bool(self.api_key)

    def _get(self, path: str, params: dict) -> dict:
        if not self.api_key:
            raise EIAError("EIA_API_KEY not set")
        params = dict(params)
        params["api_key"] = self.api_key
        url = f"{EIA_BASE}/{path.lstrip('/')}"
        try:
            r = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as e:
            raise EIAError(f"network error: {e}") from e
        if r.status_code != 200:
            raise EIAError(f"HTTP {r.status_code}: {r.text[:200]}")
        try:
            payload = r.json()
        except ValueError as e:
            raise EIAError(f"invalid JSON: {e}") from e
        if "response" not in payload:
            raise EIAError(f"unexpected payload keys: {list(payload)}")
        return payload["response"]

    # --- Query A: 24-hour window (Oracle's daily sync) --------------------

    def fetch_fuel_mix_window(self, ba_code: str, start: datetime,
                              end: datetime) -> dict:
        """
        Fetch fuel mix for a BA over [start, end). Both UTC.
        Returns dict with hourly breakdown AND 24h average intensity.
        """
        fmt = lambda d: d.strftime("%Y-%m-%dT%H")
        resp = self._get(
            "electricity/rto/fuel-type-data/data/",
            {
                "frequency": "hourly",
                "data[0]": "value",
                "facets[respondent][]": ba_code,
                "start": fmt(start),
                "end": fmt(end),
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "length": 500,
            },
        )
        rows = resp.get("data") or []
        if not rows:
            raise EIAError(f"no data for {ba_code} in window")

        by_period: dict = {}
        for row in rows:
            p, ft, v = row.get("period"), row.get("fueltype"), row.get("value")
            if not p or not ft or v is None:
                continue
            try:
                v = max(float(v), 0.0)
            except (TypeError, ValueError):
                continue
            by_period.setdefault(p, {})[ft] = v

        hourly = []
        for period in sorted(by_period.keys()):
            mix = by_period[period]
            total = sum(mix.values())
            if total <= 0:
                continue
            weighted = sum(EMISSION_FACTORS.get(f, DEFAULT_FACTOR) * m
                           for f, m in mix.items())
            hourly.append({
                "period": period,
                "total_mwh": total,
                "intensity": weighted / total,
                "mix_mwh": mix,
            })
        if not hourly:
            raise EIAError(f"no complete hours for {ba_code}")

        avg = sum(h["intensity"] for h in hourly) / len(hourly)
        return {
            "ba": ba_code,
            "start": fmt(start),
            "end": fmt(end),
            "hourly": hourly,
            "hours_returned": len(hourly),
            "intensity_avg_gco2_per_kwh": round(avg, 1),
            "fetched_at": time.time(),
        }

    # --- Query B: latest hour (Intensity Cache) ---------------------------

    def fetch_fuel_mix_latest(self, ba_code: str) -> dict:
        """
        Latest complete hour of generation for a BA. Used by Intensity Cache.
        """
        resp = self._get(
            "electricity/rto/fuel-type-data/data/",
            {
                "frequency": "hourly",
                "data[0]": "value",
                "facets[respondent][]": ba_code,
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 50,
            },
        )
        rows = resp.get("data") or []
        if not rows:
            raise EIAError(f"no fuel-mix rows for {ba_code}")

        by_period: dict = {}
        for row in rows:
            p, ft, v = row.get("period"), row.get("fueltype"), row.get("value")
            if not p or not ft or v is None:
                continue
            try:
                v = max(float(v), 0.0)
            except (TypeError, ValueError):
                continue
            by_period.setdefault(p, {})[ft] = v

        latest_period = None
        for p in sorted(by_period.keys(), reverse=True):
            if len(by_period[p]) >= 2:
                latest_period = p
                break
        if latest_period is None:
            raise EIAError(f"no complete hour of fuel-mix for {ba_code}")

        mix = by_period[latest_period]
        total = sum(mix.values())
        if total <= 0:
            raise EIAError(f"zero generation for {ba_code} at {latest_period}")

        weighted = sum(EMISSION_FACTORS.get(f, DEFAULT_FACTOR) * m
                       for f, m in mix.items())
        intensity = weighted / total

        return {
            "ba": ba_code,
            "period": latest_period,
            "mix_mwh": mix,
            "total_mwh": total,
            "mix_pct": {k: round(v / total, 4) for k, v in mix.items()},
            "intensity_gco2_per_kwh": round(intensity, 1),
            "fetched_at": time.time(),
        }
