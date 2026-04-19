"""
Load generator / demo simulator.

Usage:
    python simulate_users.py --state ca --users 20 --duration 120
    python simulate_users.py --state wv --users 50 --duration 60 --intensity-bias dirty

Creates N fake users in a state, runs the allocator, then spawns one thread
per user that randomly sends inference/training/agent/embedding requests
for --duration seconds. Prints a live table every 2 seconds.

Leaves a set of user credentials behind so you can log in as "demo_ca_3"
in the UI and watch that user's balance live.
"""

import argparse
import random
import threading
import time
import sys
import requests
from datetime import datetime, timezone
from collections import defaultdict

BASE_URL = "http://localhost:5050"
ROUTES = ["/api/v1/inference", "/api/v1/training"]
# (route, weight) — inference happens more than training
ROUTE_WEIGHTS = [("/api/v1/inference", 0.75), ("/api/v1/training", 0.25)]


class UserSim:
    def __init__(self, username, password, state):
        self.username = username
        self.password = password
        self.state = state
        self.token = None
        self.balance = None
        self.spent = 0
        self.allowed = 0
        self.blocked = 0
        self.last_response = None

    def register_and_login(self):
        try:
            r = requests.post(f"{BASE_URL}/register",
                              json={"username": self.username,
                                    "password": self.password,
                                    "state": self.state},
                              timeout=5)
            # ignore "already taken" - just log in
        except Exception as e:
            print(f"register err {self.username}: {e}", file=sys.stderr)
            return False
        try:
            r = requests.post(f"{BASE_URL}/login",
                              json={"username": self.username,
                                    "password": self.password}, timeout=5)
            if r.status_code != 200:
                return False
            self.token = r.json()["token"]
            return True
        except Exception as e:
            print(f"login err {self.username}: {e}", file=sys.stderr)
            return False

    def spend_once(self):
        route = random.choices([r for r, _ in ROUTE_WEIGHTS],
                               weights=[w for _, w in ROUTE_WEIGHTS])[0]
        try:
            r = requests.post(f"{BASE_URL}{route}",
                              headers={"Authorization": f"Bearer {self.token}"},
                              timeout=5)
            body = r.json()
            if r.status_code == 200:
                self.allowed += 1
                self.spent += body.get("cost", 0)
                self.balance = body.get("balance_after", self.balance)
            elif r.status_code == 429:
                self.blocked += 1
                self.balance = body.get("balance_after", self.balance)
            self.last_response = (r.status_code, body.get("cost"))
        except Exception:
            pass


def spend_loop(user: UserSim, stop_event: threading.Event, speed: float):
    """Each user sends a request every 'speed' seconds, jittered."""
    while not stop_event.is_set():
        user.spend_once()
        # jitter so all users don't hit at once
        time.sleep(max(0.2, random.gauss(speed, speed * 0.3)))


def print_dashboard(users, start_time, duration):
    elapsed = time.time() - start_time
    remaining = max(0, duration - elapsed)
    total_allowed = sum(u.allowed for u in users)
    total_blocked = sum(u.blocked for u in users)
    total_spent = sum(u.spent for u in users)

    # clear screen
    print("\033[2J\033[H", end="")
    print(f"SIMULATOR  state={users[0].state.upper()}  "
          f"users={len(users)}  "
          f"elapsed={elapsed:.0f}s/{duration}s  "
          f"remaining={remaining:.0f}s")
    print(f"TOTALS  allowed={total_allowed}  blocked={total_blocked}  "
          f"tokens_spent={total_spent}")
    print("-" * 80)
    print(f"{'user':<18} {'balance':>8} {'spent':>8} {'allowed':>8} "
          f"{'blocked':>8} {'status':>8}")
    print("-" * 80)
    # show first 15 + last 5 for crowded screens
    show = users[:15]
    if len(users) > 15:
        show = users[:12] + users[-3:]
    for u in show:
        bal = u.balance if u.balance is not None else "?"
        status = "OK" if u.last_response and u.last_response[0] == 200 else \
                 "BLOCKED" if u.last_response and u.last_response[0] == 429 else \
                 "init"
        print(f"{u.username:<18} {str(bal):>8} {u.spent:>8} "
              f"{u.allowed:>8} {u.blocked:>8} {status:>8}")
    if len(users) > 15:
        print(f"  ... ({len(users) - 15} more) ...")
    print("-" * 80)
    print("CTRL+C to stop early.  Open UI at http://localhost:5173 and log in "
          f"as '{users[0].username}' / 'pw' to watch live.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--state", default="ca",
                   choices=["ca", "tx", "wa", "wv", "ny", "pa", "fl", "oh", "il", "ga"])
    p.add_argument("--users", type=int, default=10)
    p.add_argument("--duration", type=int, default=60, help="seconds")
    p.add_argument("--speed", type=float, default=3.0,
                   help="seconds between each user's requests (avg)")
    p.add_argument("--run-allocator", action="store_true", default=True,
                   help="run allocator after registration")
    args = p.parse_args()

    print(f"Creating {args.users} users in {args.state.upper()}...")
    users = []
    for i in range(args.users):
        u = UserSim(f"demo_{args.state}_{i}", "pw", args.state)
        if u.register_and_login():
            users.append(u)
        else:
            print(f"  failed: {u.username}")
    print(f"  registered + logged in: {len(users)}/{args.users}")

    if args.run_allocator:
        print("Running allocator to distribute today's tokens...")
        import subprocess
        subprocess.run([sys.executable, "allocator.py"], check=False)

    # Seed each user's initial balance via /me
    for u in users:
        try:
            r = requests.get(f"{BASE_URL}/me",
                             headers={"Authorization": f"Bearer {u.token}"}, timeout=5)
            if r.status_code == 200:
                u.balance = r.json().get("balance")
        except Exception:
            pass

    print(f"Starting traffic: {args.users} users, {args.duration}s, "
          f"~{args.speed}s between requests each\n")
    time.sleep(1)

    stop = threading.Event()
    threads = [threading.Thread(target=spend_loop, args=(u, stop, args.speed),
                                 daemon=True) for u in users]
    for t in threads: t.start()

    start = time.time()
    try:
        while time.time() - start < args.duration:
            print_dashboard(users, start, args.duration)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nstopping early...")
    finally:
        stop.set()
        time.sleep(1)

    print_dashboard(users, start, args.duration)
    print("\nDone. Users persist in DB — log in as any of them in the UI.")
    print(f"  Example: username='{users[0].username}' password='pw'")


if __name__ == "__main__":
    main()
