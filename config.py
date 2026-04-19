"""
Config — every tunable in one place.

All env vars are optional and have sane defaults so the thing boots
even if nothing is set. Production: override via env. AWS: would come
from Secrets Manager or Parameter Store.
"""

import os

# --- Storage ----------------------------------------------------------------
DB_PATH = os.environ.get("CARBON_DB_PATH", "carbon_ledger.db")

# --- EIA --------------------------------------------------------------------
# Accepts EIA_API_KEY (primary) or EIA_KEY (fallback). Same convention as
# the earlier eia_client.py.
EIA_API_KEY = os.environ.get("EIA_API_KEY") or os.environ.get("EIA_KEY")

# --- Identity ---------------------------------------------------------------
# Dev default is obviously insecure; production replaces via env / secrets.
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me-in-production-this-must-be-at-least-32-bytes-long")
JWT_TTL_SECONDS = int(os.environ.get("JWT_TTL_SECONDS", str(24 * 3600)))

# --- Token economics --------------------------------------------------------
BASE_CAP_TOKENS = int(os.environ.get("BASE_CAP_TOKENS", "1000"))

# Reserve-and-drip: fraction granted upfront, rest held in shared pool.
INITIAL_GRANT_FRACTION = float(os.environ.get("INITIAL_GRANT_FRACTION", "0.6"))

# Intensity multiplier clamp for per-request cost.
# 0.33 - 3.0 = dramatic demo default. Tune via env for tighter/looser swings.
COST_MULT_MIN = float(os.environ.get("COST_MULT_MIN", "0.33"))
COST_MULT_MAX = float(os.environ.get("COST_MULT_MAX", "3.0"))

# Reference intensity (gCO2/kWh) = multiplier 1.0. 400 is roughly the
# US average, chosen so clean grids discount and dirty grids surcharge.
COST_REFERENCE_INTENSITY = float(os.environ.get("COST_REFERENCE_INTENSITY", "400"))

# --- Refresh cadences -------------------------------------------------------
INTENSITY_REFRESH_SECONDS = int(os.environ.get("INTENSITY_REFRESH_SECONDS", "300"))
REBALANCE_INTERVAL_MINUTES = int(os.environ.get("REBALANCE_INTERVAL_MINUTES", "15"))
DORMANT_MINUTES = int(os.environ.get("DORMANT_MINUTES", "30"))


def summary() -> dict:
    """Redacted view of config — safe to expose in /health."""
    return {
        "db_path": DB_PATH,
        "has_eia_key": bool(EIA_API_KEY),
        "jwt_secret_is_default": JWT_SECRET == "dev-secret-change-me",
        "base_cap_tokens": BASE_CAP_TOKENS,
        "initial_grant_fraction": INITIAL_GRANT_FRACTION,
        "cost_multiplier_range": [COST_MULT_MIN, COST_MULT_MAX],
        "cost_reference_intensity": COST_REFERENCE_INTENSITY,
        "intensity_refresh_seconds": INTENSITY_REFRESH_SECONDS,
        "rebalance_interval_minutes": REBALANCE_INTERVAL_MINUTES,
        "dormant_minutes": DORMANT_MINUTES,
    }
