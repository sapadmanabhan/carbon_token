"""
Carbon Trust Engine — HTTP surface.

Endpoints:
  POST /register              {username, password, state}
  POST /login                 {username, password} -> {token}
  GET  /health
  GET  /me                    (JWT) current balance, cost to run now
  POST /api/v1/inference      (JWT) gated request (2 base cost)
  POST /api/v1/training       (JWT) gated request (10 base cost)
  GET  /api/carbon/current    (JWT) current intensity for my state
  GET  /api/carbon/cleanest   rank states by current intensity
  GET  /api/state/<st>/budget today's cap, reserve, user count
"""

import logging
import uuid
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS

import config
import ledger_schema
import ledger
import identity
import cost
from intensity_cache import get_cache
from eia_client import STATE_TO_BA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("server")

app = Flask(__name__)
CORS(app)

ledger_schema.init_db()
cache = get_cache()
cache.start()


def _register_blueprints():
    """Lazy-imported to avoid circular import with compliance.py."""
    try:
        from compliance import bp as compliance_bp
        app.register_blueprint(compliance_bp)
    except ImportError:
        pass

_register_blueprints()


# --- auth decorator --------------------------------------------------------

def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "missing bearer token"}), 401
        token = auth[7:]
        try:
            payload = identity.verify_token(token)
        except identity.IdentityError as e:
            return jsonify({"error": str(e)}), 401
        request.user = payload
        return f(*args, **kwargs)
    return wrapper


# --- auth endpoints --------------------------------------------------------

@app.route("/register", methods=["POST"])
def register():
    body = request.get_json(silent=True) or {}
    try:
        uid = identity.register(
            body.get("username", ""), body.get("password", ""),
            body.get("state", ""),
            body.get("allocation_class", "standard"),
        )
    except identity.IdentityError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"user_id": uid, "state": body.get("state", "").lower()}), 201


@app.route("/login", methods=["POST"])
def do_login():
    body = request.get_json(silent=True) or {}
    try:
        token = identity.login(body.get("username", ""), body.get("password", ""))
    except identity.IdentityError as e:
        return jsonify({"error": str(e)}), 401
    return jsonify({"token": token}), 200


# --- info endpoints --------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    snap = cache.snapshot()
    return jsonify({
        "status": "ok",
        "has_eia_key": config.summary()["has_eia_key"],
        "intensity_cache_states": list(snap.keys()),
        "config": config.summary(),
    }), 200


@app.route("/me", methods=["GET"])
@require_auth
def me():
    u = request.user
    state = u["state"]
    acct = ledger.balance(u["user_id"]) or {}
    ci = cache.get(state)
    inf = cost.compute_cost("/api/v1/inference", ci["intensity"])
    tr = cost.compute_cost("/api/v1/training", ci["intensity"])
    return jsonify({
        "user_id": u["user_id"], "state": state,
        "balance": acct.get("balance_tokens", 0),
        "granted_today": acct.get("granted_tokens", 0),
        "current_intensity": ci["intensity"],
        "intensity_source": ci["source"],
        "cost_now": {"inference": inf["cost"], "training": tr["cost"]},
    }), 200


# --- gated endpoints -------------------------------------------------------

def _run_gated(route: str):
    u = request.user
    state = u["state"]
    ci = cache.get(state)
    intensity = ci["intensity"]
    priced = cost.compute_cost(route, intensity)
    if priced["cost"] is None:
        return jsonify({"error": "route not priced"}), 500

    req_id = str(uuid.uuid4())
    result = ledger.try_spend(
        u["user_id"], priced["cost"],
        reason=f"{route}@intensity={intensity:.1f}",
        request_id=req_id,
    )

    body = {
        "route": route, "state": state, "request_id": req_id,
        "cost": priced["cost"],
        "cost_breakdown": {
            "base": priced["base"], "multiplier": priced["multiplier"],
            "intensity": intensity, "intensity_source": ci["source"],
        },
        "balance_after": result.new_balance,
        "drew_from_reserve": result.drew_from_reserve,
    }

    if result.ok:
        body["decision"] = "ALLOW"
        return jsonify(body), 200

    # 429: suggest alternatives
    alternatives = []
    for s in STATE_TO_BA:
        if s == state:
            continue
        other = cache.get(s)
        alternatives.append({
            "state": s, "intensity": other["intensity"],
            "multiplier": cost.intensity_multiplier(other["intensity"]),
        })
    alternatives.sort(key=lambda x: x["intensity"])
    body["decision"] = "BLOCK"
    body["reason"] = result.reason
    body["suggestions"] = {
        "cleaner_states_now": alternatives[:3],
        "message": "Your balance + state reserve can't cover this cost.",
    }
    return jsonify(body), 429


@app.route("/api/v1/inference", methods=["POST"])
@require_auth
def inference():
    return _run_gated("/api/v1/inference")


@app.route("/api/v1/training", methods=["POST"])
@require_auth
def training():
    return _run_gated("/api/v1/training")


# --- read-only info --------------------------------------------------------

@app.route("/api/carbon/current", methods=["GET"])
@require_auth
def carbon_current():
    state = request.user["state"]
    ci = cache.get(state)
    return jsonify({"state": state, **ci}), 200


@app.route("/api/carbon/cleanest", methods=["GET"])
def carbon_cleanest():
    rows = []
    for s in STATE_TO_BA:
        ci = cache.get(s)
        rows.append({
            "state": s, "intensity": ci["intensity"],
            "source": ci["source"],
        })
    rows.sort(key=lambda x: x["intensity"])
    return jsonify({"regions": rows}), 200


@app.route("/api/state/<state>/budget", methods=["GET"])
def state_budget(state):
    from datetime import datetime, timezone
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with ledger_schema.connect() as conn:
        bud = conn.execute(
            "SELECT * FROM daily_budgets WHERE state=? AND utc_date=?",
            (state.lower(), date)
        ).fetchone()
        user_count = conn.execute(
            "SELECT COUNT(*) AS c FROM users WHERE state=?", (state.lower(),)
        ).fetchone()["c"]
    reserve = ledger.reserve_balance(state.lower(), date)
    return jsonify({
        "state": state.lower(), "utc_date": date,
        "budget": dict(bud) if bud else None,
        "reserve_tokens": reserve,
        "registered_users": user_count,
    }), 200


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", "5050"))
    host = os.environ.get("HOST", "127.0.0.1")  # localhost only by default
    app.run(host=host, port=port, debug=False)
