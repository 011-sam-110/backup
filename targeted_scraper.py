"""
Standalone targeted scraper — separate process from scheduler.py.

Bearer token strategy:
  - On startup (and every ~55 min), navigate to zapmap.com using the exact
    same pattern as scrape() in scraper.py: page.on("request") listener +
    12-second wait after domcontentloaded. This is proven to capture the token.
  - Injects the token into scraper._last_bearer so subsequent scrape_targeted()
    calls skip the page load entirely and just hit the API directly.
  - Caches the token to disk so a restart can pick it up immediately.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

from playwright.async_api import async_playwright
from dotenv import load_dotenv

import db
import scraper as scraper_mod
from log_setup import setup_logging
from scraper import scrape_targeted, _pick_proxy

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
    Navigate to zapmap.com and capture the bearer token using the identical
    pattern to scrape() in scraper.py: page.on("request") + 12s wait.
    That is the only approach proven to work on this server.
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
            proxy=_pick_proxy(),
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

        def on_request(request):
            nonlocal bearer
            if bearer or "api.zap-map.io" not in request.url:
                return
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                bearer = auth

        page.on("request", on_request)

        try:
            await page.goto(
                "https://www.zapmap.com/live/",
                wait_until="domcontentloaded",
                timeout=90_000,
            )
            await page.wait_for_timeout(12_000)  # same as scrape() line 406
        except Exception as exc:
            log.warning("Bearer page load failed: %s", exc)
        finally:
            await browser.close()

    if bearer:
        log.info("Bearer token captured successfully")
    else:
        log.warning("Bearer token not captured after 12s wait")
    return bearer


def _write_cache(token: str) -> None:
    try:
        BEARER_CACHE_FILE.write_text(json.dumps({"token": token, "ts": time.time()}))
    except Exception as e:
        log.warning("Failed to write bearer cache: %s", e)


def _bearer_still_valid() -> bool:
    if not scraper_mod._last_bearer:
        return False
    return (time.monotonic() - scraper_mod._bearer_cached_at) < BEARER_MAX_AGE_S


async def ensure_bearer() -> bool:
    """Guarantee scraper_mod._last_bearer is fresh. Returns False if acquisition fails."""
    if _bearer_still_valid():
        return True

    token = await _acquire_bearer()
    if not token:
        return False

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
