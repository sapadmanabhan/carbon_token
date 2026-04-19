"""
EIA Probe C — Multi-state comparison.

Hits the same dataset for all tracked states and shows them side by side.
Useful to confirm every state's balancing authority actually reports data.
Not a system query per se — this is the kind of thing the dashboard
(`GET /api/carbon/cleanest`) would return at the end of the project.

Usage:
    python eia_probe_c_allstates.py
"""

import os
import sys
import requests

EMISSION_FACTORS = {
    "COL": 820, "NG": 490, "OIL": 650, "NUC": 12,
    "SUN": 48,  "WND": 11, "WAT": 24, "GEO": 38,
    "BIO": 230, "OTH": 300,
}

# State → primary balancing authority
STATE_TO_BA = {
    "ca": "CISO", "tx": "ERCO", "wa": "BPAT", "wv": "PJM",
    "ny": "NYIS", "pa": "PJM",  "fl": "FPC",  "oh": "PJM",
    "il": "MISO", "ga": "SOCO",
}


def latest_intensity(key, ba):
    url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    params = {
        "api_key": key, "frequency": "hourly", "data[0]": "value",
        "facets[respondent][]": ba,
        "sort[0][column]": "period", "sort[0][direction]": "desc",
        "length": 50,
    }
    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    rows = r.json().get("response", {}).get("data") or []
    by_period = {}
    for row in rows:
        by_period.setdefault(row["period"], {})[row["fueltype"]] = max(
            float(row["value"] or 0), 0.0)
    for p in sorted(by_period.keys(), reverse=True):
        if len(by_period[p]) >= 2:
            mix = by_period[p]
            total = sum(mix.values())
            if total <= 0:
                continue
            weighted = sum(EMISSION_FACTORS.get(f, 300) * m for f, m in mix.items())
            return (p, weighted / total, mix), None
    return None, "no complete hour"


def main():
    key = os.environ.get("EIA_API_KEY") or os.environ.get("EIA_KEY")
    if not key:
        sys.exit("ERROR: set EIA_API_KEY in your shell first.")

    print(f"{'state':<6} {'BA':<6} {'period (UTC)':<16} {'gCO2/kWh':>10}  top fuels")
    print("-" * 80)
    results = []
    for state, ba in STATE_TO_BA.items():
        data, err = latest_intensity(key, ba)
        if err:
            print(f"{state:<6} {ba:<6} {'—':<16} {'—':>10}  ({err})")
            continue
        period, intensity, mix = data
        top = sorted(mix.items(), key=lambda x: -x[1])[:3]
        mix_str = ", ".join(f"{f}={int(m/sum(mix.values())*100)}%" for f, m in top)
        print(f"{state:<6} {ba:<6} {period:<16} {intensity:>10.1f}  {mix_str}")
        results.append((state, ba, intensity))

    if results:
        results.sort(key=lambda x: x[2])
        print()
        print("Cleanest → dirtiest RIGHT NOW:")
        for i, (state, ba, i_val) in enumerate(results, 1):
            print(f"  {i}. {state.upper():<4} ({ba}): {i_val:.1f} gCO2/kWh")


if __name__ == "__main__":
    main()
