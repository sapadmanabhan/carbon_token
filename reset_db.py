"""
Development helper — WIPE and recreate the ledger DB.

Run: python reset_db.py

DO NOT run in production. Does not prompt for confirmation because
that would break automation; instead relies on you knowing what you typed.
"""

import sys
import os
import ledger_schema
import config


def main() -> int:
    path = config.DB_PATH
    existed = os.path.exists(path)
    ledger_schema.reset_db(path)
    verb = "RESET" if existed else "CREATED"
    print(f"[reset_db] {verb} {path}")
    print("[reset_db] all tables empty, ready for Step 3 operations.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
