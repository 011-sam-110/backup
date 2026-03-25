import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

import db

load_dotenv()

OUTPUT_DIR = Path("output")
BASE_API = "https://api.zap-map.io/locations/v1"
MIN_POWER_W = 100_000
ENGLAND_LAT = (49.9, 55.8)
ENGLAND_LNG = (-5.7, 1.8)


def max_power_w(location: dict) -> int:
    return max(location.get("power", [0]))


def is_england(loc: dict) -> bool:
    lat = loc["coordinates"]["latitude"]
    lng = loc["coordinates"]["longitude"]
    return ENGLAND_LAT[0] <= lat <= ENGLAND_LAT[1] and ENGLAND_LNG[0] <= lng <= ENGLAND_LNG[1]


def build_record(loc: dict, status: dict | None, scraped_at: str, loc_detail: dict | None = None) -> dict:
    devices_out = []
    available = charging = inoperative = out_of_order = unknown = 0
    connector_types: set = set()

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
    pricing = sorted(pricing_set)
    payment_methods = sorted(pm_set)
    devices_raw_loc = ld.get("devices", [])

    for device in (status or {}).get("devices", []):
        evses_out = []
        for evse in device.get("evses", []):
            net = (evse.get("status") or {}).get("network") or {}
            usr = (evse.get("status") or {}).get("user") or {}
            net_status = net.get("status", "UNKNOWN")
            connectors = evse.get("connectors", [])
            connector_types.update(connectors)

            s = net_status.upper()
            if s == "AVAILABLE":
                available += 1
            elif s == "CHARGING":
                charging += 1
            elif s == "INOPERATIVE":
                inoperative += 1
            elif s == "OUTOFORDER":
                out_of_order += 1
            else:
                unknown += 1

            evses_out.append({
                "evse_uuid": evse.get("uuid"),
                "connectors": connectors,
                "network_status": net_status,
                "network_updated_at": net.get("updated_at"),
                "user_status": usr.get("status"),
                "user_updated_at": usr.get("updated_at"),
            })
        devices_out.append({
            "device_uuid": device.get("uuid"),
            "evses": evses_out,
        })

    return {
        "uuid": loc["uuid"],
        "latitude": loc["coordinates"]["latitude"],
        "longitude": loc["coordinates"]["longitude"],
        "max_power_kw": round(max_power_w(loc) / 1000, 1),
        "location_raw": loc,
        "total_devices": len(devices_out),
        "total_evses": sum(len(d["evses"]) for d in devices_out),
        "connector_types": sorted(connector_types),
        "available_count": available,
        "charging_count": charging,
        "inoperative_count": inoperative,
        "out_of_order_count": out_of_order,
        "unknown_count": unknown,
        "devices": devices_out,
        "hub_name":          hub_name or None,
        "operator":          operator or None,
        "user_rating":       user_rating,
        "user_rating_count": user_rating_count,
        "address":           address or None,
        "city":              city or None,
        "postal_code":       postal_code or None,
        "is_24_7":           is_24_7,
        "pricing":           pricing,
        "payment_methods":   payment_methods,
        "devices_raw_loc":   devices_raw_loc,
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
    OUTPUT_DIR.mkdir(exist_ok=True)
    locations: dict = {}
    bearer_token = None
    _bbox_base_url = None
    _bbox_last_page = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
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
            print("WARNING: No Bearer token captured — status data will be missing. Snapshots will have zero counts.")

        # ── Power filter + England filter ──────────────────────────────────────
        qualifying = [
            loc for loc in locations.values()
            if is_england(loc) and max_power_w(loc) >= MIN_POWER_W
        ]
        print(f"Filtered to {len(qualifying)} England 100kW+ locations.")

        if not qualifying:
            print("No qualifying chargers found. Exiting.")
            await browser.close()
            return

        # ── Status via browser fetch (with auth header) ────────────────────────
        uuids = [loc["uuid"] for loc in qualifying]
        status_map: dict = {}
        chunks = [uuids[i:i + 50] for i in range(0, len(uuids), 50)]
        print(f"Fetching status for {len(uuids)} locations ({len(chunks)} chunks)...")

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
                print(f"WARNING: {failed_chunks}/{len(chunks)} status chunks failed — those hubs have zero counts.")
        else:
            print("Skipping status fetch — no Bearer token.")

        print(f"Fetching location details for {len(qualifying)} hubs...")
        loc_detail_map = await fetch_location_details(
            page, [loc["uuid"] for loc in qualifying], auth=bearer_token
        )

        scraped_at = datetime.now(timezone.utc).isoformat()

        await browser.close()

    # ── Merge + filter by EVSE count + write ───────────────────────────────────
    all_records = [
        build_record(loc, status_map.get(loc["uuid"]), scraped_at,
                     loc_detail=loc_detail_map.get(loc["uuid"]))
        for loc in qualifying
    ]

    results = all_records
    print(f"Collected {len(results)} hubs.")

    # Persist to SQLite
    db.init_db()
    db.upsert_hubs(results)
    db.insert_snapshots(results)
    print(f"Saved {len(results)} hubs to database.")

    # Also write JSON snapshot
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = OUTPUT_DIR / f"chargers_{timestamp}.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"Wrote {len(results)} hubs → {out_path}")


if __name__ == "__main__":
    asyncio.run(scrape())
