"""
Standalone targeted scraper — separate process from scheduler.py.

Bearer token strategy:
  - On startup: read bearer_token.cache written by the scheduler's full scrape.
    If valid (<55 min old) skip zapmap.com entirely and go straight to API calls.
  - If cache is cold/expired: navigate to zapmap.com with the same stealth +
    interaction pattern as scrape() (cookie consent, scroll) to pass Cloudflare.
  - If direct acquisition also fails: poll the cache file for up to 2 min while
    the scheduler's next run writes a fresh token.
  - Caches every acquired token to disk for the next restart.
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
from scraper import scrape_targeted, _pick_proxy, _block_junk

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
    """Navigate to zapmap.com and capture the bearer token.

    Mirrors scrape() interaction (route blocking, cookie consent, scroll) to
    pass Cloudflare bot detection. Logs page URL/title and api request count
    so it's immediately clear whether Cloudflare served a challenge page.
    """
    log.info("Acquiring bearer token from zapmap.com...")
    bearer = None
    api_requests: list[str] = []

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
        await page.route("**/*", _block_junk)
        await page.add_init_script(_STEALTH_JS)

        def on_request(request):
            nonlocal bearer
            if "api.zap-map.io" not in request.url:
                return
            api_requests.append(request.url[:80])
            if bearer:
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
            log.info("Page loaded: url=%s title=%r", page.url, (await page.title())[:80])
            await page.wait_for_timeout(12_000)

            # Interaction mirrors scrape() — helps pass Cloudflare bot check
            for sel in [
                "#onetrust-accept-btn-handler",
                "button:has-text('Allow all cookies')",
                "button:has-text('Accept All')",
                "button:has-text('Accept')",
            ]:
                try:
                    await page.click(sel, timeout=3_000)
                    log.info("Cookie consent dismissed")
                    break
                except Exception:
                    pass

            for _ in range(3):
                await page.mouse.wheel(0, 400)
                await page.wait_for_timeout(500)
            await page.wait_for_timeout(8_000)

        except Exception as exc:
            log.warning("Bearer page load failed: %s", exc)
        finally:
            log.info(
                "Bearer acquisition done: api_requests=%d bearer=%s",
                len(api_requests), "YES" if bearer else "NO",
            )
            if api_requests:
                log.debug("api.zap-map.io requests seen: %s", api_requests[:5])
            await browser.close()

    if not bearer:
        log.warning("Bearer token not captured (api_requests=%d)", len(api_requests))
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


def _load_bearer_from_cache() -> bool:
    """Read bearer_token.cache (written by scheduler's full scrape) into memory.

    Returns True if a valid, non-expired token was loaded.
    Logs the specific reason for any failure so the next restart is diagnosable.
    """
    try:
        if not BEARER_CACHE_FILE.exists():
            log.info("Cache file not found: %s", BEARER_CACHE_FILE.resolve())
            return False
        raw = BEARER_CACHE_FILE.read_text().strip()
        if not raw:
            log.warning("Cache file is empty")
            return False
        data = json.loads(raw)
        token = data.get("token", "")
        ts    = float(data.get("ts", 0))
        age_s = time.time() - ts
        if not token:
            log.warning("Cache file has no token field")
            return False
        if age_s >= BEARER_MAX_AGE_S:
            log.warning("Cache token expired (age %.0fs / max %.0fs)", age_s, BEARER_MAX_AGE_S)
            return False
        scraper_mod._last_bearer = token
        # Translate wall-clock age to monotonic so _bearer_still_valid() works correctly.
        scraper_mod._bearer_cached_at = time.monotonic() - age_s
        log.info("Bearer loaded from cache (age %.0fs)", age_s)
        return True
    except json.JSONDecodeError as exc:
        log.warning("Cache file malformed: %s", exc)
    except Exception as exc:
        log.warning("Failed to read cache: %s", exc)
    return False


async def ensure_bearer() -> bool:
    """Guarantee scraper_mod._last_bearer is fresh. Returns False if acquisition fails."""
    # 1. In-memory cache still valid (fast path — most cycles take this branch)
    if _bearer_still_valid():
        return True

    # 2. Disk cache written by scheduler's full scrape (runs every ~15 min)
    if _load_bearer_from_cache():
        return True

    # 3. Direct acquisition — loads zapmap.com with full interaction
    log.info("Cache absent/expired — attempting direct acquisition...")
    token = await _acquire_bearer()
    if token:
        scraper_mod._last_bearer = token
        scraper_mod._bearer_cached_at = time.monotonic()
        _write_cache(token)
        return True

    # 4. Direct acquisition failed (likely Cloudflare block) — poll the cache
    #    file every 15s for up to 2 min while the scheduler's next run writes it
    log.warning(
        "Direct acquisition failed — polling cache for up to 120s "
        "(scheduler writes every ~15 min)..."
    )
    for i in range(8):
        await asyncio.sleep(15)
        if _load_bearer_from_cache():
            log.info("Cache populated after %ds wait", (i + 1) * 15)
            return True
        log.debug("Cache poll %d/8 — still empty/expired", i + 1)

    log.error("Bearer token unavailable after 120s polling")
    return False


async def main():
    cycle = 0
    log.info("Targeted scraper starting — cycle %ds, DB: %s", CYCLE_INTERVAL_S, db.DB_PATH)
    log.info("Bearer cache path: %s", BEARER_CACHE_FILE.resolve())
    _load_bearer_from_cache()  # pre-warm from scheduler's cache before first cycle

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
