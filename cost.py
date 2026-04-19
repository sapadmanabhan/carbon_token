"""Cost function: base_cost × clamp(intensity/400, MIN, MAX)."""

import math
import config

# Base token cost per route (tokens at reference intensity = 400 gCO2/kWh).
ROUTE_BASE_COSTS = {
    "/api/v1/inference": 2,
    "/api/v1/training": 10,
}


def base_cost(route: str):
    return ROUTE_BASE_COSTS.get(route)


def intensity_multiplier(intensity: float) -> float:
    raw = intensity / config.COST_REFERENCE_INTENSITY
    return max(config.COST_MULT_MIN, min(config.COST_MULT_MAX, raw))


def compute_cost(route: str, intensity: float) -> dict:
    base = base_cost(route)
    if base is None:
        return {"cost": None, "base": None, "multiplier": None,
                "reason": "route_not_priced"}
    mult = intensity_multiplier(intensity)
    return {
        "cost": math.ceil(base * mult),
        "base": base,
        "multiplier": round(mult, 3),
        "intensity": intensity,
    }
