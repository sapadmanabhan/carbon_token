"""Identity: registration, login, JWT verification."""

from datetime import datetime, timezone, timedelta
from typing import Optional
import bcrypt
import jwt

import config
import ledger_schema
from eia_client import STATE_TO_BA


class IdentityError(Exception):
    pass


def register(username: str, password: str, state: str,
             allocation_class: str = "standard",
             db_path: Optional[str] = None) -> int:
    state = state.lower()
    if state not in STATE_TO_BA:
        raise IdentityError(f"unsupported state: {state}")
    if not username or not password:
        raise IdentityError("username and password required")
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        with ledger_schema.connect(db_path) as conn:
            cur = conn.execute(
                "INSERT INTO users "
                "(username, password_hash, state, allocation_class, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (username, pw_hash, state, allocation_class,
                 datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            user_id = cur.lastrowid
    except Exception as e:
        if "UNIQUE" in str(e):
            raise IdentityError("username already taken") from e
        raise

    # Auto-grant fair share from reserve so new users immediately have tokens.
    # If no budget/reserve exists yet, this silently does nothing — user will
    # get tokens on next allocator run or day rollover.
    try:
        import fair_share
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        fair_share.grant_fair_share_from_reserve(user_id, state, today, db_path)
    except Exception:
        pass  # non-fatal: registration still succeeds

    return user_id


def login(username: str, password: str,
          db_path: Optional[str] = None) -> str:
    with ledger_schema.connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, password_hash, state, allocation_class "
            "FROM users WHERE username = ?", (username,)
        ).fetchone()
    if not row:
        raise IdentityError("invalid credentials")
    if not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        raise IdentityError("invalid credentials")
    return issue_token(row["id"], row["state"], row["allocation_class"])


def issue_token(user_id: int, state: str, allocation_class: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "user_id": user_id,
        "state": state,
        "allocation_class": allocation_class,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=config.JWT_TTL_SECONDS)).timestamp()),
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")


def verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise IdentityError("token expired")
    except jwt.InvalidTokenError as e:
        raise IdentityError(f"invalid token: {e}")
