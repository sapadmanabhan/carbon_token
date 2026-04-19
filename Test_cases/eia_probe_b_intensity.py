"""
EIA Probe B — Intensity Cache query (latest hour, no window).

This is what the Intensity Cache (Step 8) will call every 5 minutes to
get the current grid carbon intensity, which drives per-request cost.

Asks EIA for the 50 most recent rows, groups by period, picks the newest
complete hour. This is the pattern that survives EIA's inconsistent
publication lag across BAs.

Usage:
    python eia_probe_b_intensity.py
    python eia_probe_b_intensity.py PJM
    python eia_probe_b_intensity.py BPAT     # Bonneville Power — hydro-heavy
"""

import os
import sys
from datetime import datetime, timezone
import requests

EMISSION_FACTORS = {
    "COL": 820, "NG": 490, "OIL": 650, "NUC": 12,
    "SUN": 48,  "WND": 11, "WAT": 24, "GEO": 38,
    "BIO": 230, "OTH": 300,
}


def main():
    key = os.environ.get("EIA_API_KEY") or os.environ.get("EIA_KEY")
    if not key:
        sys.exit("ERROR: set EIA_API_KEY in your shell first.")

    ba = sys.argv[1].upper() if len(sys.argv) > 1 else "CISO"

    url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    params = {
        "api_key": key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": ba,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",   # newest first — no start/end window
        "length": 50,
    }

    print(f"Probe B: {ba}, latest available hour")
    print(f"URL: {url}")
    print()

    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        sys.exit(f"HTTP {r.status_code}: {r.text[:300]}")

    rows = r.json().get("response", {}).get("data") or []
    print(f"Got {len(rows)} rows.")
    if not rows:
        sys.exit("Empty — try PJM, ERCO, NYIS.")

    # Group by period, keep newest period with ≥2 fuel types.
    by_period = {}
    for row in rows:
        p = row["period"]
        by_period.setdefault(p, {})[row["fueltype"]] = max(float(row["value"] or 0), 0.0)

    latest = None
    for p in sorted(by_period.keys(), reverse=True):
        if len(by_period[p]) >= 2:
            latest = p
            break
    if not latest:
        sys.exit("No complete hour found in results.")

    mix = by_period[latest]
    total = sum(mix.values())
    weighted = sum(EMISSION_FACTORS.get(f, 300) * mwh for f, mwh in mix.items())
    intensity = weighted / total

    age_min = (
        datetime.now(timezone.utc)
        - datetime.strptime(latest, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    ).total_seconds() / 60

    print(f"\nLatest complete hour: {latest} UTC  (≈ {age_min:.0f} min ago)")
    print(f"Total generation: {int(total):,} MWh")
    print(f"\nFuel mix:")
    for fuel, mwh in sorted(mix.items(), key=lambda x: -x[1]):
        pct = mwh / total * 100
        factor = EMISSION_FACTORS.get(fuel, 300)
        bar = "█" * int(pct / 2)
        print(f"  {fuel:<4} {int(mwh):>7} MWh  {pct:>5.1f}%  "
              f"({factor:>4} gCO2/kWh factor)  {bar}")

    print(f"\nCURRENT INTENSITY: {intensity:.1f} gCO2/kWh")

    # What this would mean for a request right now
    base_cost_inference = 2
    base_cost_training = 10
    multiplier = max(0.33, min(3.0, intensity / 400))
    import math
    inf_cost = math.ceil(base_cost_inference * multiplier)
    tr_cost = math.ceil(base_cost_training * multiplier)
    print(f"\nCost multiplier right now: {multiplier:.2f}x (intensity / 400)")
    print(f"  /api/v1/inference  would cost: {inf_cost} tokens  "
          f"(base {base_cost_inference} × {multiplier:.2f})")
    print(f"  /api/v1/training   would cost: {tr_cost} tokens  "
          f"(base {base_cost_training} × {multiplier:.2f})")


if __name__ == "__main__":
    main()
