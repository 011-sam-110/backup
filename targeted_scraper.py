"""
Standalone targeted scraper — separate process from scheduler.py.

Bearer token strategy:
  - On startup (and every ~55 min), navigate to zapmap.com and use
    page.expect_request() to wait however long it takes for zapmap's own
    JS to fire an API call — captures the bearer from that request.
  - Injects the token directly into scraper._last_bearer so that all
    subsequent scrape_targeted() calls skip the page load entirely and
    just hit the API.
  - Caches the token to disk so a scheduler restart can also pick it up.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright
from dotenv import load_dotenv

import db
import scraper as scraper_mod
from log_setup import setup_logging
from scraper import scrape_targeted

load_dotenv()
setup_logging(log_file="logs/targeted_scraper.log")
log = logging.getLogger("evanti.targeted")

CYCLE_INTERVAL_S  = int(os.getenv("TARGETED_CYCLE_S", 60))
SCRAPE_TIMEOUT_S  = 90
BEARER_MAX_AGE_S  = 55 * 60
BEARER_CACHE_FILE = Path("bearer_token.cache")

_STEALTH_JS = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
    Object.defineProperty(window, 'chrome', {
        writable: true, enumerable: true, configurable: false,
        value: {runtime: {}}
    });
    const _origQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : _origQuery(p);
"""


async def _acquire_bearer() -> str | None:
    """
    Load zapmap.com in a fresh browser and wait (up to 60 s) for zapmap's
    own JavaScript to fire an authenticated API call. Capture the bearer
    token from that request's Authorization header.

    Uses page.expect_request() so we never rely on a fixed sleep — we wait
    exactly as long as the page needs, no more.
    """
    log.info("Acquiring bearer token from zapmap.com...")
    bearer = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--enable-unsafe-swiftshader",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        await page.add_init_script(_STEALTH_JS)

        try:
            async with page.expect_request(
                lambda r: (
                    "api.zap-map.io" in r.url
                    and r.headers.get("authorization", "").startswith("Bearer ")
                ),
                timeout=60_000,
            ) as req_info:
                await page.goto(
                    "https://www.zapmap.com/live/",
                    wait_until="domcontentloaded",
                    timeout=90_000,
                )

            req = await req_info.value
            bearer = req.headers.get("authorization")
            log.info("Bearer token captured successfully")

        except Exception as exc:
            log.warning("Bearer acquisition failed: %s", exc)
        finally:
            await browser.close()

    return bearer


def _bearer_still_valid() -> bool:
    """Check whether scraper_mod's in-memory bearer is still within its TTL."""
    if not scraper_mod._last_bearer:
        return False
    age = time.monotonic() - scraper_mod._bearer_cached_at
    return age < BEARER_MAX_AGE_S


def _write_cache(token: str) -> None:
    try:
        BEARER_CACHE_FILE.write_text(json.dumps({"token": token, "ts": time.time()}))
    except Exception as e:
        log.warning("Failed to write bearer cache: %s", e)


async def ensure_bearer() -> bool:
    """
    Guarantee scraper_mod._last_bearer is populated and fresh.
    Returns True if a valid bearer is available, False if acquisition failed.
    """
    if _bearer_still_valid():
        return True

    token = await _acquire_bearer()
    if not token:
        return False

    # Inject into scraper module so scrape_targeted() skips page load
    scraper_mod._last_bearer = token
    scraper_mod._bearer_cached_at = time.monotonic()
    _write_cache(token)
    return True


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

        # Ensure we have a bearer before attempting any scrape
        if not await ensure_bearer():
            log.error("Could not obtain bearer token — skipping cycle %d", cycle)
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
