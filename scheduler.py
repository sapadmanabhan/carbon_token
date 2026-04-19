"""
Scheduler: runs Oracle, Allocator, Rebalancer, Intensity refresh automatically.

Use:
    python scheduler.py

Runs indefinitely. Ctrl+C to stop.
Oracle 00:05 UTC daily, Allocator 00:10, Rebalancer every 15 min,
Intensity refresh every 5 min.
"""

import logging
import time
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler

import config
import oracle
import allocator
import rebalancer
import ledger_schema
from intensity_cache import get_cache

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("scheduler")


def run_daily_sync():
    log.info("daily sync: oracle")
    results = oracle.sync_all()
    log.info("oracle done: %d states", len(results))
    log.info("daily sync: allocator")
    results = allocator.allocate_all()
    log.info("allocator done: %d states", len(results))


def run_rebalancer():
    log.info("rebalancer sweep")
    result = rebalancer.sweep()
    log.info("rebalancer: swept=%d reclaimed=%d",
             result["accounts_swept"], result["total_reclaimed"])


def run_intensity_refresh():
    cache = get_cache()
    cache.refresh_all()
    snap = cache.snapshot()
    log.info("intensity refresh: %d states", len(snap))


def main():
    ledger_schema.init_db()
    log.info("scheduler starting")

    # Eager boot: run everything once so we don't wait until tomorrow.
    run_intensity_refresh()
    run_daily_sync()

    sched = BlockingScheduler(timezone="UTC")
    sched.add_job(run_daily_sync, "cron", hour=0, minute=5, id="oracle_and_allocator")
    sched.add_job(run_rebalancer, "interval",
                  minutes=config.REBALANCE_INTERVAL_MINUTES, id="rebalancer")
    sched.add_job(run_intensity_refresh, "interval",
                  seconds=config.INTENSITY_REFRESH_SECONDS, id="intensity")

    log.info("scheduled: oracle+allocator daily 00:05 UTC, rebalancer every %d min, "
             "intensity every %d sec",
             config.REBALANCE_INTERVAL_MINUTES, config.INTENSITY_REFRESH_SECONDS)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("scheduler stopping")


if __name__ == "__main__":
    main()
