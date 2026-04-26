"""
Standalone targeted scraper — persistent Chromium browser, never closed between cycles.

Runs as a separate process / Docker service alongside scheduler.py.
Loops every 60 seconds. Hubs assigned to 1-min groups are scraped every cycle;
hubs in 2-min groups every 2nd cycle; 3-min every 3rd, etc.

Bearer token is refreshed by navigating to zapmap.com only when it expires (~55 min).
All other cycles skip the page load entirely and just call the API via page.evaluate().
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
from log_setup import setup_logging
from scraper import _parse_status

load_dotenv()
setup_logging(log_file="logs/targeted_scraper.log")
log = logging.getLogger("evanti.targeted")

CYCLE_INTERVAL_S  = int(os.getenv("TARGETED_CYCLE_S", 60))
BEARER_MAX_AGE_S  = 55 * 60
BEARER_CACHE_FILE = Path("bearer_token.cache")
BASE_API          = "https://api.zap-map.io/v4"

_BLOCK_TYPES = {"image", "font", "media", "stylesheet"}
_BLOCK_DOMAINS = (
    "google-analytics.com", "googletagmanager.com", "googlesyndication.com",
    "doubleclick.net", "hotjar.com", "pagead",
    "fonts.googleapis.com", "fonts.gstatic.com",
    "tiles.mapbox.com", "events.mapbox.com", "api.mapbox.com/events",
)
_STEALTH_JS = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en']});
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


async def _block_junk(route):
    req = route.request
    if req.resource_type in _BLOCK_TYPES or any(d in req.url for d in _BLOCK_DOMAINS):
        await route.abort()
    else:
        await route.continue_()


def _read_bearer_cache() -> tuple[str | None, float]:
    """Return (token, age_in_seconds) from disk cache, or (None, inf)."""
    try:
        data = json.loads(BEARER_CACHE_FILE.read_text())
        age = time.time() - data["ts"]
        if age < BEARER_MAX_AGE_S:
            return data["token"], age
    except Exception:
        pass
    return None, float("inf")


def _write_bearer_cache(token: str) -> None:
    try:
        BEARER_CACHE_FILE.write_text(json.dumps({"token": token, "ts": time.time()}))
    except Exception as e:
        log.warning("Failed to write bearer cache: %s", e)


async def _launch(pw):
    """Launch a fresh persistent browser + page."""
    browser = await pw.chromium.launch(
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
    log.info("Browser launched")
    return browser, page


async def _navigate_zapmap(page, capture_bearer: bool = False) -> str | None:
    """Navigate to zapmap.com/live/ to establish origin context for API fetches.

    The Zapmap API only accepts CORS requests from https://www.zapmap.com —
    fetch() from about:blank is rejected. This must be called once after every
    browser launch. When capture_bearer=True, also sniffs the bearer token from
    outgoing requests (used on first boot and every ~55 min when token expires).
    """
    bearer = None

    if capture_bearer:
        def on_request(request):
            nonlocal bearer
            if bearer or "api.zap-map.io" not in request.url:
                return
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                bearer = auth
        page.on("request", on_request)

    try:
        await page.goto("https://www.zapmap.com/live/",
                        wait_until="domcontentloaded", timeout=90_000)
        # Wait for JS to fire its API calls so we can capture the bearer
        await page.wait_for_timeout(5_000 if capture_bearer else 2_000)
    except Exception as exc:
        log.warning("_navigate_zapmap: page.goto failed: %s", exc)
    finally:
        if capture_bearer:
            page.remove_listener("request", on_request)

    return bearer


async def _api_fetch(page, url: str, bearer: str) -> dict | None:
    """Call a Zapmap API endpoint using the browser's JS fetch (real Chrome TLS)."""
    try:
        result = await page.evaluate(
            """async ([url, auth]) => {
                const r = await fetch(url, { headers: { Authorization: auth } });
                if (!r.ok) return { _err: r.status };
                return await r.json();
            }""",
            [url, bearer],
        )
        if result and "_err" in result:
            log.warning("_api_fetch: HTTP %s for %s", result["_err"], url)
            return None
        return result
    except Exception as exc:
        log.warning("_api_fetch exception: %s", exc)
        return None


async def _scrape_hubs(page, bearer: str, uuids: list[str]) -> int:
    """Fetch status for uuids, parse, and write to DB. Returns snapshot count saved."""
    status_map: dict = {}
    for i in range(0, len(uuids), 50):
        chunk = uuids[i:i + 50]
        url = f"{BASE_API}/transient/status?uuids={','.join(chunk)}"
        data = await _api_fetch(page, url, bearer)
        if data:
            for item in data.get("data", []):
                status_map[item["uuid"]] = item

    scraped_at = datetime.now(timezone.utc).isoformat()
    records = []
    for uuid in uuids:
        s = status_map.get(uuid)
        if not s:
            continue
        parsed = _parse_status(s)
        records.append({
            "uuid":               uuid,
            "scraped_at":         scraped_at,
            "available_count":    parsed["available"],
            "charging_count":     parsed["charging"],
            "inoperative_count":  parsed["inoperative"],
            "out_of_order_count": parsed["out_of_order"],
            "unknown_count":      parsed["unknown"],
            "devices":            parsed["devices_out"],
        })

    if records:
        db.process_evse_events(records)
        db.insert_snapshots(records, source="targeted")
    return len(records)


async def main():
    pw = await async_playwright().start()
    browser = None
    page    = None
    bearer  = None
    bearer_age_s = float("inf")
    cycle = 0

    # Warm up from disk cache so first cycle doesn't need a page load
    cached_token, cached_age = _read_bearer_cache()
    if cached_token:
        bearer = cached_token
        bearer_age_s = cached_age
        log.info("Loaded bearer token from cache (age %.0fs)", cached_age)

    while True:
        cycle_start = time.monotonic()
        cycle += 1

        try:
            # (Re)launch browser if it died
            if browser is None or not browser.is_connected():
                log.info("(Re)launching browser...")
                if browser:
                    try: await browser.close()
                    except Exception: pass
                browser, page = await _launch(pw)
                # Always navigate to zapmap.com after launch — fetch() from
                # about:blank is CORS-blocked by the Zapmap API; the page must
                # be on https://www.zapmap.com for API calls to succeed.
                need_bearer = not bearer or bearer_age_s >= BEARER_MAX_AGE_S
                log.info("Navigating to zapmap.com (capture_bearer=%s)...", need_bearer)
                fresh = await _navigate_zapmap(page, capture_bearer=need_bearer)
                if fresh:
                    bearer = fresh
                    bearer_age_s = 0.0
                    _write_bearer_cache(bearer)
                    log.info("Bearer token captured from navigation")
                elif need_bearer:
                    # Navigation didn't yield a bearer — try disk cache as fallback
                    cached_token, cached_age = _read_bearer_cache()
                    if cached_token:
                        bearer = cached_token
                        bearer_age_s = cached_age
                        log.info("Bearer reloaded from disk cache (age %.0fs)", cached_age)
                    else:
                        log.warning("No bearer token available — skipping cycle %d", cycle)
                        await asyncio.sleep(CYCLE_INTERVAL_S)
                        continue

            # Refresh bearer token when it expires (~55 min)
            if bearer_age_s >= BEARER_MAX_AGE_S:
                log.info("Bearer token expired — refreshing...")
                fresh = await _navigate_zapmap(page, capture_bearer=True)
                if fresh:
                    bearer = fresh
                    bearer_age_s = 0.0
                    _write_bearer_cache(bearer)
                    log.info("Bearer token refreshed")
                else:
                    log.warning("Bearer refresh failed — will retry next cycle")
                    await asyncio.sleep(CYCLE_INTERVAL_S)
                    continue

            # Skip if targeted scraping has been disabled via the settings UI
            if db.get_setting("targeted_scraping_enabled", "1") != "1":
                log.debug("Targeted scraping disabled — skipping cycle %d", cycle)
                await asyncio.sleep(CYCLE_INTERVAL_S)
                continue

            # Scrape each interval that is due this cycle
            for interval_min in range(1, 6):
                if cycle % interval_min != 0:
                    continue
                uuids = db.get_hubs_for_scrape_interval(interval_min)
                if not uuids:
                    log.info("Cycle %d | %dm: no hubs configured for this interval", cycle, interval_min)
                    continue
                log.info("Cycle %d | %dm: scraping %d hub(s)", cycle, interval_min, len(uuids))
                t0 = time.monotonic()
                count = await _scrape_hubs(page, bearer, uuids)
                log.info("Cycle %d | %dm: %d snapshots in %.1fs",
                         cycle, interval_min, count, time.monotonic() - t0)

        except Exception as exc:
            log.error("Cycle %d error — forcing browser restart: %s", cycle, exc, exc_info=True)
            try:
                if browser: await browser.close()
            except Exception: pass
            browser = None
            page    = None
            bearer  = None

        # Update bearer age counter and sleep for remainder of the minute
        elapsed = time.monotonic() - cycle_start
        bearer_age_s += elapsed
        sleep_s = max(1.0, CYCLE_INTERVAL_S - elapsed)
        log.debug("Cycle %d done in %.1fs — sleeping %.1fs", cycle, elapsed, sleep_s)
        await asyncio.sleep(sleep_s)


if __name__ == "__main__":
    log.info("Targeted scraper starting — cycle %ds, DB: %s", CYCLE_INTERVAL_S, db.DB_PATH)
    asyncio.run(main())
