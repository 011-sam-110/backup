"""
Standalone targeted scraper — separate process from scheduler.py.

Loops every 60 seconds and calls scrape_targeted() for each configured
interval group. Uses the same proven scrape_targeted() function that was
already working inside scheduler.py, just isolated into its own process
so the full scrape can never block it.
"""
import asyncio
import logging
import os
import time

from dotenv import load_dotenv

import db
from log_setup import setup_logging
from scraper import scrape_targeted

load_dotenv()
setup_logging(log_file="logs/targeted_scraper.log")
log = logging.getLogger("evanti.targeted")

CYCLE_INTERVAL_S = int(os.getenv("TARGETED_CYCLE_S", 60))
SCRAPE_TIMEOUT_S = 90


async def main():
    cycle = 0
    log.info("Targeted scraper starting — cycle %ds, DB: %s", CYCLE_INTERVAL_S, db.DB_PATH)

    while True:
        cycle_start = time.monotonic()
        cycle += 1

        if db.get_setting("targeted_scraping_enabled", "1") != "1":
            log.debug("Targeted scraping disabled — skipping cycle %d", cycle)
            await asyncio.sleep(CYCLE_INTERVAL_S)
            continue

        for interval_min in range(1, 6):
            if cycle % interval_min != 0:
                continue
            uuids = db.get_hubs_for_scrape_interval(interval_min)
            if not uuids:
                continue
            log.info("Cycle %d | %dm: scraping %d hub(s)", cycle, interval_min, len(uuids))
            t0 = time.monotonic()
            try:
                count = await asyncio.wait_for(scrape_targeted(uuids), timeout=SCRAPE_TIMEOUT_S)
                log.info("Cycle %d | %dm: %d snapshots in %.1fs",
                         cycle, interval_min, count, time.monotonic() - t0)
            except asyncio.TimeoutError:
                log.error("Cycle %d | %dm: timed out after %ds", cycle, interval_min, SCRAPE_TIMEOUT_S)
            except Exception as exc:
                log.error("Cycle %d | %dm: failed — %s", cycle, interval_min, exc)

        elapsed = time.monotonic() - cycle_start
        sleep_s = max(1.0, CYCLE_INTERVAL_S - elapsed)
        log.debug("Cycle %d done in %.1fs — sleeping %.1fs", cycle, elapsed, sleep_s)
        await asyncio.sleep(sleep_s)


if __name__ == "__main__":
    asyncio.run(main())
