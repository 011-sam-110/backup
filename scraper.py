import asyncio
import json
import logging
import os
import random
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

import db

log = logging.getLogger("evanti.scraper")

load_dotenv()
BASE_API = "https://api.zap-map.io/locations/v1"
MIN_POWER_W = 100_000
GB_LAT = (49.9, 61.0)   # Scilly Isles → Shetland
GB_LNG = (-8.7, 1.8)    # Outer Hebrides / W Scotland → East Anglia

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
USE_PROXY = os.getenv("USE_PROXY", "false").lower() == "true"


def _pick_proxy() -> str | None:
    """Return a random proxy URL from proxies.txt, or None if disabled/empty."""
    if not USE_PROXY:
        return None
    proxies_path = Path("proxies.txt")
    if not proxies_path.exists():
        return None
    lines = [l.strip() for l in proxies_path.read_text().splitlines()
             if l.strip() and not l.startswith("#")]
    return random.choice(lines) if lines else None


def max_power_w(location: dict) -> int:
    return max(location.get("power", [0]))


def is_great_britain(loc: dict) -> bool:
    lat = loc["coordinates"]["latitude"]
    lng = loc["coordinates"]["longitude"]
    return GB_LAT[0] <= lat <= GB_LAT[1] and GB_LNG[0] <= lng <= GB_LNG[1]


def _parse_status(status: dict) -> dict:
    """Extract per-EVSE counts and device list from a raw status response."""
    available = charging = inoperative = out_of_order = unknown = 0
    devices_out = []
    connector_types: set = set()
    for device in (status or {}).get("devices", []):
        evses_out = []
        for evse in device.get("evses", []):
            net = (evse.get("status") or {}).get("network") or {}
            usr = (evse.get("status") or {}).get("user") or {}
            net_status = net.get("status", "UNKNOWN")
            connectors = evse.get("connectors", [])
            connector_types.update(connectors)
            s = net_status.upper()
            if s == "AVAILABLE":    available += 1
            elif s == "CHARGING":   charging += 1
            elif s == "INOPERATIVE": inoperative += 1
            elif s == "OUTOFORDER": out_of_order += 1
            else:                   unknown += 1
            evses_out.append({
                "evse_uuid": evse.get("uuid"),
                "connectors": connectors,
                "network_status": net_status,
                "network_updated_at": net.get("updated_at"),
                "user_status": usr.get("status"),
                "user_updated_at": usr.get("updated_at"),
            })
        devices_out.append({"device_uuid": device.get("uuid"), "evses": evses_out})
    return {
        "available": available, "charging": charging,
        "inoperative": inoperative, "out_of_order": out_of_order, "unknown": unknown,
        "devices_out": devices_out, "connector_types": sorted(connector_types),
    }


def build_record(loc: dict, status: dict | None, scraped_at: str, loc_detail: dict | None = None) -> dict:
    parsed = _parse_status(status)

    ld = loc_detail or {}
    operator_obj = ld.get("operator") or {}
    operator = operator_obj.get("name") or operator_obj.get("trading_name") or ""
    hub_name = ld.get("name") or ""
    user_rating       = ld.get("user_rating")
    user_rating_count = ld.get("user_rating_count")
    address     = ld.get("address") or ""
    city        = ld.get("city") or ""
    postal_code = ld.get("postal_code") or ""
    is_24_7 = 1 if (ld.get("opening_times") or {}).get("twentyfourseven") else 0

    pricing_set, pm_set = set(), set()
    for dev in ld.get("devices", []):
        pd_ = dev.get("payment_details") or {}
        if pd_.get("pricing"):
            pricing_set.add(pd_["pricing"])
        for m in (pd_.get("payment_methods") or []):
            pm_set.add(m)

    return {
        "uuid": loc["uuid"],
        "latitude": loc["coordinates"]["latitude"],
        "longitude": loc["coordinates"]["longitude"],
        "max_power_kw": round(max_power_w(loc) / 1000, 1),
        "total_devices": len(parsed["devices_out"]),
        "total_evses": sum(len(d["evses"]) for d in parsed["devices_out"]),
        "connector_types": parsed["connector_types"],
        "available_count":    parsed["available"],
        "charging_count":     parsed["charging"],
        "inoperative_count":  parsed["inoperative"],
        "out_of_order_count": parsed["out_of_order"],
        "unknown_count":      parsed["unknown"],
        "devices": parsed["devices_out"],
        "hub_name":          hub_name or None,
        "operator":          operator or None,
        "user_rating":       user_rating,
        "user_rating_count": user_rating_count,
        "address":           address or None,
        "city":              city or None,
        "postal_code":       postal_code or None,
        "is_24_7":           is_24_7,
        "pricing":           sorted(pricing_set),
        "payment_methods":   sorted(pm_set),
        "scraped_at": scraped_at,
    }


async def fetch_via_browser(page, url: str, auth: str | None = None) -> dict | None:
    """Call an API endpoint using the browser's own fetch — real Chrome TLS, no 422."""
    try:
        return await page.evaluate(
            """async ([url, auth]) => {
                const headers = auth ? { Authorization: auth } : {};
                const r = await fetch(url, { headers });
                if (!r.ok) return null;
                return await r.json();
            }""",
            [url, auth],
        )
    except Exception:
        return None


async def fetch_location_details(
    page, uuids: list[str], auth: str | None, concurrency: int = 20
) -> dict:
    """Call /location/{uuid} for each uuid in concurrent batches. Returns {uuid: data_dict}."""
    results = {}
    for i in range(0, len(uuids), concurrency):
        chunk = uuids[i : i + concurrency]
        responses = await asyncio.gather(*[
            fetch_via_browser(page, f"{BASE_API}/location/{uid}", auth=auth)
            for uid in chunk
        ])
        for uid, resp in zip(chunk, responses):
            if resp and isinstance(resp.get("data"), dict):
                results[uid] = resp["data"]
    return results


async def scrape():
    locations: dict = {}
    bearer_token = None
    _bbox_base_url = None
    _bbox_last_page = None

    async with async_playwright() as p:
        proxy_url = _pick_proxy()
        if proxy_url:
            print(f"  Using proxy: {proxy_url.split('@')[-1]}")
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
            proxy={"server": proxy_url} if proxy_url else None,
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        # ── Capture Bearer token from outgoing requests ────────────────────────
        def on_request(request):
            nonlocal bearer_token
            if bearer_token or "api.zap-map.io" not in request.url:
                return
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                bearer_token = auth

        page.on("request", on_request)
        

        # ── Intercept all bounding-box responses ───────────────────────────────
        async def handle_response(response):
            nonlocal _bbox_base_url, _bbox_last_page
            if "bounding-box" in response.url:
                try:
                    body = await response.json()
                    before = len(locations)
                    for loc in body.get("data", []):
                        locations[loc["uuid"]] = loc
                    meta = body.get("meta", {})
                    added = len(locations) - before
                    print(
                        f"  bounding-box page {meta.get('current_page')}/{meta.get('last_page')}"
                        f" — {added} new locations (total: {len(locations)})"
                    )
                    _bbox_base_url = re.sub(r"[?&]page=\d+", "", response.url)
                    _bbox_last_page = meta.get("last_page", 1)
                except Exception:
                    pass
                return

        page.on("response", handle_response)

        # ── Load zapmap.com/live/ ──────────────────────────────────────────────
        print("Loading zapmap.com...")
        await page.goto("https://www.zapmap.com/live/", wait_until="domcontentloaded", timeout=90_000)

        # Dismiss cookie consent
        for selector in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Allow all cookies')",
            "button:has-text('Accept All')",
            "button:has-text('Accept all cookies')",
            "button:has-text('Accept')",
        ]:
            try:
                await page.click(selector, timeout=5_000)
                print("  Cookie consent dismissed.")
                break
            except Exception:
                pass

        # Scroll down to bring the map into view
        print("  Scrolling to map...")
        for _ in range(5):
            await page.mouse.wheel(0, 400)
            await page.wait_for_timeout(500)

        # Zoom out with Ctrl+scroll to get a UK-wide view (fires bounding-box requests)
        print("  Zooming out to UK view...")
        viewport = page.viewport_size or {"width": 1280, "height": 800}
        cx = viewport["width"] // 2
        cy = viewport["height"] // 2
        await page.mouse.move(cx, cy)
        await page.keyboard.down("Control")
        for _ in range(10):
            await page.mouse.wheel(0, 300)
            await page.wait_for_timeout(300)
        await page.keyboard.up("Control")

        # Wait for bounding-box requests triggered by the zoom to complete
        print("  Waiting for map data to load...")
        await page.wait_for_timeout(10_000)
        print(f"\nDiscovered {len(locations)} unique locations.")
        print(f"  Bbox base URL: {_bbox_base_url}")
        print(f"  Last page: {_bbox_last_page}")

        # ── Fetch any additional bounding-box pages not captured by intercept ──
        if _bbox_last_page is not None and _bbox_last_page > 1:
            sep = "&" if "?" in _bbox_base_url else "?"
            for page_num in range(2, _bbox_last_page + 1):
                print(f"  Fetching page {page_num}/{_bbox_last_page} via direct API...")
                url = f"{_bbox_base_url}{sep}page={page_num}"
                data = await fetch_via_browser(page, url, auth=bearer_token)
                if data:
                    for loc in data.get("data", []):
                        locations[loc["uuid"]] = loc

        if not bearer_token:
            log.warning("No Bearer token captured — snapshots will have zero counts")
            print("WARNING: No Bearer token captured — status data will be missing. Snapshots will have zero counts.")

        # ── Power filter + England filter ──────────────────────────────────────
        qualifying = [
            loc for loc in locations.values()
            if is_great_britain(loc) and max_power_w(loc) >= MIN_POWER_W
        ]
        log.info("Bounding-box: %d total locations, %d GB 100kW+ qualifying",
                 len(locations), len(qualifying))
        print(f"Filtered to {len(qualifying)} GB 100kW+ locations.")

        if not qualifying:
            log.warning("No qualifying chargers found — aborting scrape")
            print("No qualifying chargers found. Exiting.")
            await browser.close()
            return

        # ── Status via browser fetch — covers ALL tracked hubs ────────────────
        qualifying_uuids = {loc["uuid"] for loc in qualifying}
        db_hub_uuids = {h["uuid"] for h in db.get_all_hubs_for_scrape()}
        all_status_uuids = list(qualifying_uuids | db_hub_uuids)
        status_map: dict = {}
        chunks = [all_status_uuids[i:i + 50] for i in range(0, len(all_status_uuids), 50)]
        print(f"Fetching status for {len(all_status_uuids)} locations ({len(chunks)} chunks) "
              f"[{len(qualifying_uuids)} bounding-box + {len(db_hub_uuids - qualifying_uuids)} DB-only]...")

        if bearer_token:
            failed_chunks = 0
            for i, chunk in enumerate(chunks):
                url = f"{BASE_API}/transient/status?uuids={','.join(chunk)}"
                data = await fetch_via_browser(page, url, auth=bearer_token)
                if data:
                    for item in data.get("data", []):
                        status_map[item["uuid"]] = item
                else:
                    failed_chunks += 1
                    print(f"  WARNING: Status fetch failed for chunk {i+1}/{len(chunks)} ({len(chunk)} hubs)")

            if failed_chunks:
                log.warning("Status fetch: %d/%d chunks failed", failed_chunks, len(chunks))
                print(f"WARNING: {failed_chunks}/{len(chunks)} status chunks failed — those hubs have zero counts.")
            else:
                log.info("Status fetch: all %d chunks OK, %d hubs with status", len(chunks), len(status_map))
        else:
            log.warning("Skipping status fetch — no Bearer token")
            print("Skipping status fetch — no Bearer token.")

        print(f"Fetching location details for {len(qualifying)} hubs...")
        loc_detail_map = await fetch_location_details(
            page, [loc["uuid"] for loc in qualifying], auth=bearer_token
        )
        log.info("Location details fetched: %d/%d", len(loc_detail_map), len(qualifying))

        scraped_at = datetime.now(timezone.utc).isoformat()

        await browser.close()

    # ── Build records ──────────────────────────────────────────────────────────
    # Full records for bounding-box hubs (upsert static data + snapshot)
    all_records = [
        build_record(loc, status_map.get(loc["uuid"]), scraped_at,
                     loc_detail=loc_detail_map.get(loc["uuid"]))
        for loc in qualifying
    ]

    # Snapshot-only records for DB hubs not found in current bounding-box sweep
    db_only_records = []
    for uuid in db_hub_uuids - qualifying_uuids:
        s = status_map.get(uuid)
        if not s:
            continue
        parsed = _parse_status(s)
        db_only_records.append({
            "uuid": uuid,
            "scraped_at": scraped_at,
            "available_count":    parsed["available"],
            "charging_count":     parsed["charging"],
            "inoperative_count":  parsed["inoperative"],
            "out_of_order_count": parsed["out_of_order"],
            "unknown_count":      parsed["unknown"],
            "devices":            parsed["devices_out"],
        })

    total_records = len(all_records) + len(db_only_records)
    log.info("Records built: %d bounding-box + %d db-only = %d total",
             len(all_records), len(db_only_records), total_records)
    print(f"Collected {len(all_records)} bounding-box + {len(db_only_records)} DB-only hub records.")

    # Persist to SQLite
    db.init_db()
    db.upsert_hubs(all_records)
    db.insert_snapshots(all_records + db_only_records)
    db.update_latest_devices_status(db_only_records)
    log.info("DB write complete — %d hub snapshots saved", total_records)
    print(f"Saved {total_records} hub snapshots to database.")


if __name__ == "__main__":
    asyncio.run(scrape())
