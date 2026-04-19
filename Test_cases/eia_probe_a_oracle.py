"""
EIA Probe A — Oracle query (24-hour window).

This is what the Oracle (Step 6) will call once per day to compute the
24-hour average carbon intensity that sets each state's token cap.

Tests one state — California (BA = CISO) — over the last 24 UTC hours.
Prints the hourly fuel mix, computed hourly intensity, and the 24h average.

Usage:
    python eia_probe_a_oracle.py
    python eia_probe_a_oracle.py PJM        # try a different BA
    python eia_probe_a_oracle.py ERCO       # Texas
"""

import os
import sys
from datetime import datetime, timedelta, timezone
import requests

# IPCC AR5 lifecycle emission factors, gCO2-eq per kWh, median values.
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

    # 24-hour window ending at the most recent completed UTC hour.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end = now
    start = end - timedelta(hours=24)
    fmt = lambda d: d.strftime("%Y-%m-%dT%H")

    url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    params = {
        "api_key": key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": ba,
        "start": fmt(start),
        "end": fmt(end),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 500,
    }

    print(f"Probe A: {ba}, {fmt(start)} → {fmt(end)} UTC")
    print(f"URL: {url}")
    print()

    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        sys.exit(f"HTTP {r.status_code}: {r.text[:300]}")

    payload = r.json()
    rows = payload.get("response", {}).get("data") or []
    print(f"Got {len(rows)} rows from EIA.")
    if not rows:
        sys.exit("No data — try a different BA like PJM, ERCO, NYIS.")

    # Group by period → {fueltype: mwh}
    by_period = {}
    for row in rows:
        p = row["period"]
        ft = row["fueltype"]
        v = max(float(row["value"] or 0), 0.0)
        by_period.setdefault(p, {})[ft] = v

    # Compute hourly intensity
    hourly = []
    print(f"\n{'hour':<15} {'MWh total':>10} {'gCO2/kWh':>10}  mix")
    print("-" * 80)
    for period in sorted(by_period.keys()):
        mix = by_period[period]
        total = sum(mix.values())
        if total <= 0:
            continue
        weighted = sum(EMISSION_FACTORS.get(f, 300) * mwh for f, mwh in mix.items())
        intensity = weighted / total
        hourly.append(intensity)
        top = sorted(mix.items(), key=lambda x: -x[1])[:3]
        mix_str = ", ".join(f"{f}={int(m)}" for f, m in top)
        print(f"{period:<15} {int(total):>10} {intensity:>10.1f}  {mix_str}")

    if not hourly:
        sys.exit("\nAll hours had zero generation — weird result, investigate.")

    avg = sum(hourly) / len(hourly)
    print("-" * 80)
    print(f"\n24h AVERAGE INTENSITY: {avg:.1f} gCO2/kWh")
    print(f"(This is the number Oracle would write to daily_budgets.intensity_avg)")

    # Show what cap this would produce
    base_cap = 1000
    ratio = 400 / avg
    clamped = max(0.3, min(2.0, ratio))
    cap = int(base_cap * clamped)
    print(f"\nImplied token cap for {ba}: {cap} tokens")
    print(f"  (base 1000 × clamp(400/{avg:.1f}, 0.3, 2.0) = {cap})")


if __name__ == "__main__":
    main()
