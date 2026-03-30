"""
discover.py — Fetch full details for new hubs found by parse_har.py and upsert into DB.

Run after parse_har.py has created pending_uuids.json:
  python discover.py
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright

import db
from log_setup import setup_logging
from scraper import BASE_API, fetch_via_browser, fetch_location_details, is_great_britain, HEADLESS, _pick_proxy

setup_logging(log_file="logs/discover.log")
log = logging.getLogger("evanti.discover")

PENDING_PATH = Path("pending_uuids.json")
CONCURRENCY = 20


def build_hub_record_from_detail(uuid: str, detail: dict, scraped_at: str) -> dict:
    """
    Build a hub record suitable for db.upsert_hubs() using only the /location/{uuid}
    detail response. Used when we have UUIDs but no bounding-box loc dict.
    """
    operator_obj = (detail.get("operator") or {})
    operator = operator_obj.get("name") or operator_obj.get("trading_name") or ""

    pricing_set, pm_set, connector_types, power_vals = set(), set(), set(), []
    for dev in detail.get("devices", []):
        pd_ = dev.get("payment_details") or {}
        if pd_.get("pricing"):
            pricing_set.add(pd_["pricing"])
        for m in (pd_.get("payment_methods") or []):
            pm_set.add(m)
        for evse in dev.get("evses", []):
            for conn in evse.get("connectors", []):
                if conn.get("standard"):
                    connector_types.add(conn["standard"])
                if conn.get("max_electric_power"):
                    power_vals.append(conn["max_electric_power"])

    coords = detail.get("coordinates") or {}
    max_power_kw = round(max(power_vals) / 1000, 1) if power_vals else 0.0
    total_evses = sum(len(dev.get("evses", [])) for dev in detail.get("devices", []))

    return {
        "uuid": uuid,
        "latitude": coords.get("latitude", 0.0),
        "longitude": coords.get("longitude", 0.0),
        "max_power_kw": max_power_kw,
        "total_evses": total_evses,
        "total_devices": len(detail.get("devices", [])),
        "connector_types": sorted(connector_types),
        "available_count": 0,
        "charging_count": 0,
        "inoperative_count": 0,
        "out_of_order_count": 0,
        "unknown_count": 0,
        "devices_raw_loc": detail.get("devices", []),
        "hub_name": detail.get("name") or None,
        "operator": operator or None,
        "user_rating": detail.get("user_rating"),
        "user_rating_count": detail.get("user_rating_count"),
        "address": detail.get("address") or None,
        "city": detail.get("city") or None,
        "postal_code": detail.get("postal_code") or None,
        "is_24_7": 1 if (detail.get("opening_times") or {}).get("twentyfourseven") else 0,
        "pricing": sorted(pricing_set),
        "payment_methods": sorted(pm_set),
        "scraped_at": scraped_at,
    }


async def discover():
    if not PENDING_PATH.exists():
        log.error("%s not found — run parse_har.py first to generate it.", PENDING_PATH)
        return

    uuids = json.loads(PENDING_PATH.read_text())
    if not uuids:
        log.info("No pending UUIDs.")
        PENDING_PATH.unlink()
        return

    t_start = time.monotonic()
    log.info("=== discover started: %d UUIDs to fetch ===", len(uuids))

    bearer_token = None

    log.info("Phase 1/3: launching browser, capturing bearer token...")
    async with async_playwright() as p:
        proxy_url = _pick_proxy()
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
            proxy=proxy_url,
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

        def on_request(request):
            nonlocal bearer_token
            if bearer_token or "api.zap-map.io" not in request.url:
                return
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                bearer_token = auth

        page.on("request", on_request)

        log.info("Loading zapmap.com to capture bearer token...")
        await page.goto(
            "https://www.zapmap.com/live/",
            wait_until="domcontentloaded",
            timeout=90_000,
        )

        # Dismiss cookie consent so the map initialises and fires authenticated API calls
        for selector in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Allow all cookies')",
            "button:has-text('Accept All')",
            "button:has-text('Accept all cookies')",
            "button:has-text('Accept')",
        ]:
            try:
                await page.click(selector, timeout=5_000)
                log.info("Cookie consent dismissed.")
                break
            except Exception:
                pass

        # Scroll page down to bring map into view
        for _ in range(5):
            await page.mouse.wheel(0, 400)
            await page.wait_for_timeout(500)

        # Ctrl+scroll to zoom the map — this fires authenticated bounding-box requests
        # which is the reliable way to get a bearer token (same approach as scraper.py)
        log.info("Zooming map to trigger authenticated API calls...")
        viewport = page.viewport_size or {"width": 1280, "height": 720}
        cx, cy = viewport["width"] // 2, viewport["height"] // 2
        await page.mouse.move(cx, cy)
        await page.keyboard.down("Control")
        for _ in range(8):
            await page.mouse.wheel(0, 300)
            await page.wait_for_timeout(300)
        await page.keyboard.up("Control")

        # Wait up to 30s for a bearer token (should arrive quickly after zoom)
        for _ in range(30):
            if bearer_token:
                break
            await page.wait_for_timeout(1_000)

        if not bearer_token:
            log.warning("No bearer token captured — location details may fail.")
        else:
            log.info("Bearer token captured.")

        log.info("Phase 2/3: fetching location details (concurrency=%d, batches=%d)...",
                 CONCURRENCY, (len(uuids) + CONCURRENCY - 1) // CONCURRENCY)
        loc_detail_map = await fetch_location_details(
            page, uuids, auth=bearer_token, concurrency=CONCURRENCY
        )

        await browser.close()

    fetched = len(loc_detail_map)
    missed = len(uuids) - fetched
    log.info("Got details for %d/%d hub(s).%s", fetched, len(uuids),
             f" ({missed} failed)" if missed else "")

    scraped_at = datetime.now(timezone.utc).isoformat()

    records = []
    skipped = 0
    for uuid in uuids:
        if uuid not in loc_detail_map:
            continue
        detail = loc_detail_map[uuid]
        coords = detail.get("coordinates") or {}
        if not is_great_britain({"coordinates": coords}):
            skipped += 1
            continue
        rec = build_hub_record_from_detail(uuid, detail, scraped_at)
        if rec["max_power_kw"] < 100:
            skipped += 1
            continue
        records.append(rec)

    if skipped:
        log.info("Skipped %d hub(s) (outside Great Britain or below 100 kW).", skipped)

    log.info("Phase 3/3: upserting %d hub(s) into DB...", len(records))
    db.init_db()
    db.upsert_hubs(records)

    PENDING_PATH.unlink()

    elapsed = time.monotonic() - t_start
    log.info("=== discover complete: %d upserted, %d skipped, elapsed %.0fs ===",
             len(records), skipped, elapsed)


if __name__ == "__main__":
    asyncio.run(discover())
