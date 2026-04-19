"""Compliance endpoints: audit log, report, state budget queries."""

from datetime import datetime, timezone
from flask import Blueprint, request, jsonify

import ledger_schema
from server import require_auth

bp = Blueprint("compliance", __name__)


@bp.route("/api/compliance/report", methods=["GET"])
@require_auth
def report():
    """Aggregate stats — available to all authenticated users."""
    date = request.args.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with ledger_schema.connect() as conn:
        totals = conn.execute(
            "SELECT tx_type, COUNT(*) AS n, SUM(delta) AS total "
            "FROM transactions WHERE utc_date = ? GROUP BY tx_type",
            (date,)
        ).fetchall()
        by_state = conn.execute(
            "SELECT state, tx_type, COUNT(*) AS n, SUM(delta) AS total "
            "FROM transactions WHERE utc_date = ? GROUP BY state, tx_type",
            (date,)
        ).fetchall()
        budgets = conn.execute(
            "SELECT * FROM daily_budgets WHERE utc_date = ?", (date,)
        ).fetchall()

    return jsonify({
        "utc_date": date,
        "totals_by_type": [dict(r) for r in totals],
        "by_state": [dict(r) for r in by_state],
        "daily_budgets": [dict(r) for r in budgets],
    }), 200


@bp.route("/api/compliance/transactions", methods=["GET"])
@require_auth
def transactions():
    """
    Recent transactions. Regular users see only their own.
    (Admin role would see all — not implemented for MVP.)
    """
    limit = min(int(request.args.get("limit", "50")), 500)
    user = request.user
    with ledger_schema.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM transactions WHERE user_id = ? "
            "ORDER BY id DESC LIMIT ?",
            (user["user_id"], limit)
        ).fetchall()
    return jsonify({
        "user_id": user["user_id"],
        "count": len(rows),
        "transactions": [dict(r) for r in rows],
    }), 200


@bp.route("/api/compliance/my_history", methods=["GET"])
@require_auth
def my_history():
    """Per-user daily summary — how many tokens spent over time."""
    user = request.user
    with ledger_schema.connect() as conn:
        rows = conn.execute(
            "SELECT utc_date, "
            "  SUM(CASE WHEN tx_type='spend' THEN -delta ELSE 0 END) AS spent, "
            "  SUM(CASE WHEN tx_type='grant' THEN delta ELSE 0 END) AS granted "
            "FROM transactions WHERE user_id = ? "
            "GROUP BY utc_date ORDER BY utc_date DESC",
            (user["user_id"],)
        ).fetchall()
    return jsonify({
        "user_id": user["user_id"],
        "days": [dict(r) for r in rows],
    }), 200
