import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from playwright.async_api import async_playwright

import db

log = logging.getLogger("evanti.scraper")

load_dotenv()
BASE_API = "https://api.zap-map.io/locations/v1"
MIN_POWER_W = 100_000
EXCLUDED_CONNECTORS = {"CHADEMO"}  # connector type strings excluded from tracking entirely
MIN_EVSES = 6                       # hub must have this many qualifying EVSEs to be tracked
MIN_SHARED_POWER_W = 150_000        # 150kW minimum per EVSE — tracks all rapid/ultra-rapid CCS2 units


def _filter_raw_devices(devices: list) -> list:
    """Return only qualifying EVSEs from a raw /location/{uuid} device list.

    An EVSE qualifies when:
      - After stripping EXCLUDED_CONNECTORS, at least one connector remains
      - Its max connector power (after stripping) >= MIN_SHARED_POWER_W (150kW minimum per EVSE)

    Excluded connectors (e.g. CHAdeMO) are stripped from each EVSE's connector list rather than
    dropping the whole EVSE — this preserves CCS connectors on dual-standard EVSEs that have both
    CCS and CHAdeMO on the same unit.

    Connectors in this format are objects with 'standard' and 'max_electric_power' fields.
    Devices with no remaining EVSEs after filtering are also dropped.
    """
    out = []
    for dev in devices:
        kept = []
        for evse in dev.get("evses", []):
            conns = evse.get("connectors", [])
            # Strip excluded connector types; only skip the EVSE if nothing remains
            conns = [c for c in conns if c.get("standard") not in EXCLUDED_CONNECTORS]
            if not conns:
                continue
            if max((c.get("max_electric_power") or 0 for c in conns), default=0) < MIN_SHARED_POWER_W:
                continue
            kept.append({**evse, "connectors": conns})
        if kept:
            out.append({**dev, "evses": kept})
    return out
GB_LAT = (49.9, 61.0)    # Scilly Isles → Shetland
GB_LNG = (-5.85, 1.75)  # SW Scotland / Land's End → East Anglia (excludes Ireland, French coast)

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
USE_PROXY = os.getenv("USE_PROXY", "false").lower() == "true"
SCRAPE_RESPONSE_TIMEOUT_MS = int(os.getenv("SCRAPE_RESPONSE_TIMEOUT_MS", 45_000))

# ── Bearer token cache (shared between full and targeted scrapes) ─────────────
_last_bearer: str | None = None
_bearer_cached_at: float = 0.0
BEARER_MAX_AGE_S: float = 55 * 60  # 55 minutes
BEARER_CACHE_FILE = Path("bearer_token.cache")


def _write_bearer_cache(token: str) -> None:
    """Persist bearer token to disk so discover.py (subprocess) can reuse it."""
    try:
        BEARER_CACHE_FILE.write_text(json.dumps({"token": token, "ts": time.time()}))
    except Exception as e:
        log.warning("Failed to write bearer token cache: %s", e)

# ── Bandwidth reduction — block resources that are never needed ───────────────
_BLOCK_TYPES = {"image", "font", "media", "stylesheet"}
_BLOCK_DOMAINS = (
    "google-analytics.com", "googletagmanager.com", "googlesyndication.com",
    "doubleclick.net", "hotjar.com", "pagead",
    "fonts.googleapis.com", "fonts.gstatic.com",
    "tiles.mapbox.com", "events.mapbox.com", "api.mapbox.com/events",
)

async def _block_junk(route):
    req = route.request
    if req.resource_type in _BLOCK_TYPES or any(d in req.url for d in _BLOCK_DOMAINS):
        await route.abort()
    else:
        await route.continue_()


def _pick_proxy() -> dict | None:
    """Return a Playwright proxy dict from proxies.txt, or None if disabled/empty."""
    if not USE_PROXY:
        return None
    proxies_path = Path("proxies.txt")
    if not proxies_path.exists():
        return None
    lines = [l.strip() for l in proxies_path.read_text().splitlines()
             if l.strip() and not l.startswith("#")]
    if not lines:
        return None
    parsed = urlparse(random.choice(lines))
    proxy: dict = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    return proxy


def max_power_w(location: dict) -> int:
    return max(location.get("power", [0]))


_IOM_LAT = (53.9, 54.5)
_IOM_LNG = (-4.9, -4.0)


def is_great_britain(loc: dict) -> bool:
    lat = loc["coordinates"]["latitude"]
    lng = loc["coordinates"]["longitude"]
    if not (GB_LAT[0] <= lat <= GB_LAT[1] and GB_LNG[0] <= lng <= GB_LNG[1]):
        return False
    # Exclude Isle of Man
    if _IOM_LAT[0] <= lat <= _IOM_LAT[1] and _IOM_LNG[0] <= lng <= _IOM_LNG[1]:
        return False
    return True


def _parse_status(status: dict, allowed_evse_uuids: set | None = None) -> dict:
    """Extract per-EVSE counts and device list from a raw status response.

    allowed_evse_uuids — when provided, only process EVSEs whose UUID is in this set.
    Use this to restrict counts to EVSEs that passed the _filter_raw_devices() checks
    (connector-type exclusion + power threshold).  Pass None to apply only the
    connector-type check (used when no location-detail data is available).
    """
    available = charging = inoperative = out_of_order = unknown = 0
    devices_out = []
    connector_types: set = set()
    for device in (status or {}).get("devices", []):
        evses_out = []
        for evse in device.get("evses", []):
            evse_uuid = evse.get("uuid")
            # UUID allow-list check (power + connector filter from location-detail data)
            if allowed_evse_uuids is not None and evse_uuid not in allowed_evse_uuids:
                continue
            connectors = evse.get("connectors", [])
            # Safety-net: when no UUID allow-list is available, fall back to connector-type check.
            # When an allow-list IS present, _filter_raw_devices already handled exclusions —
            # dual-standard EVSEs (e.g. CCS2 + CHAdeMO) are in the list and must not be dropped.
            if allowed_evse_uuids is None and set(connectors) & EXCLUDED_CONNECTORS:
                continue
            # Strip excluded connector types from counts/storage even on dual-standard EVSEs
            connectors = [c for c in connectors if c not in EXCLUDED_CONNECTORS]
            net = (evse.get("status") or {}).get("network") or {}
            usr = (evse.get("status") or {}).get("user") or {}
            net_status = net.get("status", "UNKNOWN")
            connector_types.update(connectors)
            s = net_status.upper()
            if s == "AVAILABLE":    available += 1
            elif s == "CHARGING":   charging += 1
            elif s == "INOPERATIVE": inoperative += 1
            elif s == "OUTOFORDER": out_of_order += 1
            else:                   unknown += 1
            evses_out.append({
                "evse_uuid": evse_uuid,
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
    ld = loc_detail or {}

    # Filter the raw device list first: strips excluded connectors + sub-threshold power EVSEs.
    # This becomes devices_raw_loc (displayed in the modal) and determines which EVSE UUIDs
    # are passed to _parse_status so counts are consistent with what is displayed.
    filtered_raw = _filter_raw_devices(ld.get("devices", []))
    qualifying_evse_uuids: set | None = None
    if ld:
        qualifying_evse_uuids = {
            evse.get("uuid")
            for dev in filtered_raw
            for evse in dev.get("evses", [])
        }

    parsed = _parse_status(status, allowed_evse_uuids=qualifying_evse_uuids)

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
        "devices_raw_loc":   filtered_raw,
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
    total = len(uuids)
    total_batches = (total + concurrency - 1) // concurrency
    for batch_num, i in enumerate(range(0, total, concurrency), 1):
        chunk = uuids[i : i + concurrency]
        responses = await asyncio.gather(*[
            fetch_via_browser(page, f"{BASE_API}/location/{uid}", auth=auth)
            for uid in chunk
        ])
        batch_ok = 0
        for uid, resp in zip(chunk, responses):
            if resp and isinstance(resp.get("data"), dict):
                results[uid] = resp["data"]
                batch_ok += 1
            elif batch_num == 1 and batch_ok == 0:
                # Log first failed response to diagnose format/auth issues
                log.warning("fetch_location_details sample failure — uid=%s resp=%s", uid, str(resp)[:300])
        fetched_so_far = min(i + concurrency, total)
        log.info("fetch_location_details: %d/%d (%.0f%%) — batch %d/%d (%d ok)",
                 fetched_so_far, total, 100 * fetched_so_far / total,
                 batch_num, total_batches, batch_ok)
    return results


async def scrape():
    locations: dict = {}
    bearer_token = None
    _bbox_base_url = None
    _bbox_last_page = None

    async with async_playwright() as p:
        MAX_PROXY_ATTEMPTS = 3
        browser = page = None

        for proxy_attempt in range(1, MAX_PROXY_ATTEMPTS + 1):
            proxy_cfg = _pick_proxy()
            if proxy_cfg:
                log.info("Proxy attempt %d/%d: %s (user: %s)",
                         proxy_attempt, MAX_PROXY_ATTEMPTS,
                         proxy_cfg['server'], proxy_cfg.get('username', 'none'))
                print(f"  Using proxy: {proxy_cfg['server']}")

            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--enable-unsafe-swiftshader",
                    "--no-zygote",
                ],
                proxy=proxy_cfg,
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
            await page.route("**/*", _block_junk)

            # ── Capture Bearer token from outgoing requests ────────────────────
            def on_request(request):
                nonlocal bearer_token
                if bearer_token or "api.zap-map.io" not in request.url:
                    return
                auth = request.headers.get("authorization", "")
                if auth.startswith("Bearer "):
                    bearer_token = auth

            page.on("request", on_request)
            page.on("console", lambda msg: log.debug("BROWSER %s: %s", msg.type, msg.text)
                    if msg.type == "error" else None)
            page.on("requestfailed", lambda req: log.debug(
                    "REQUEST FAILED [%s]: %s", req.failure, req.url[:120]))

            # ── Intercept all bounding-box responses ──────────────────────────
            async def handle_response(response):
                nonlocal _bbox_base_url, _bbox_last_page
                if "api.zap-map.io" in response.url:
                    log.debug("API response [%d]: %s", response.status, response.url[:120])
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

            # ── Apply stealth patches to bypass Cloudflare bot detection ──────
            await page.add_init_script("""
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
            """)

            # ── Load zapmap.com/live/ ──────────────────────────────────────────
            print("Loading zapmap.com...")
            try:
                await page.goto("https://www.zapmap.com/live/",
                                wait_until="domcontentloaded", timeout=90_000)
                break  # page loaded — proceed with scrape
            except Exception as exc:
                log.warning(
                    "Proxy attempt %d/%d failed [%s]: %s — %s",
                    proxy_attempt, MAX_PROXY_ATTEMPTS,
                    proxy_cfg['server'] if proxy_cfg else 'direct',
                    type(exc).__name__, str(exc)[:80],
                )
                await browser.close()
                if proxy_attempt == MAX_PROXY_ATTEMPTS:
                    raise

        # Give JS time to initialise before any interaction (longer on headless Linux)
        await page.wait_for_timeout(12_000)

        webgl_ok = await page.evaluate(
            "() => !!document.createElement('canvas').getContext('webgl')"
        )
        log.info("WebGL available: %s", webgl_ok)
        if not webgl_ok:
            log.warning("WebGL unavailable — Mapbox will not render, expect 0 locations")

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
                log.info("Cookie consent dismissed")
                break
            except Exception:
                pass

        # Scroll down to bring the map into view
        print("  Scrolling to map...")
        for _ in range(5):
            await page.mouse.wheel(0, 400)
            await page.wait_for_timeout(500)

        # Let the page settle after scroll
        await page.wait_for_timeout(3_000)

        viewport = page.viewport_size or {"width": 1280, "height": 800}
        cx = viewport["width"] // 2
        cy = viewport["height"] // 2

        # Click the map area to ensure it has focus before keyboard events
        await page.mouse.click(cx, cy)
        await page.wait_for_timeout(1_000)

        # Zoom out with Ctrl+scroll — up to 3 rounds until we capture locations
        for attempt in range(3):
            if locations:
                break
            print(f"  Zooming out to UK view (attempt {attempt + 1}/3)...")
            log.info("Zoom attempt %d/3 — locations so far: %d", attempt + 1, len(locations))
            await page.mouse.move(cx, cy)
            # Set up response waiter BEFORE scrolling so we don't miss a fast response
            async with page.expect_response(
                lambda r: "bounding-box" in r.url, timeout=SCRAPE_RESPONSE_TIMEOUT_MS
            ) as resp_info:
                await page.keyboard.down("Control")
                for _ in range(10):
                    await page.mouse.wheel(0, 300)
                    await page.wait_for_timeout(300)
                await page.keyboard.up("Control")
            try:
                await resp_info.value
                await page.wait_for_timeout(2_000)  # let subsequent pages trickle in
                break
            except Exception as exc:
                log.warning("No bounding-box response (attempt %d/3): %s", attempt + 1, exc)

        print(f"\nDiscovered {len(locations)} unique locations.")
        log.info("Page interaction complete — %d locations captured, bearer token: %s",
                 len(locations), "YES" if bearer_token else "NO")
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

        # ── Multi-centre sweep: tile GB with additional bounding-box queries ──
        # The Zapmap API returns at most ~300 results per centre point. A single
        # centre at lat=52/lng=-1 (central England) misses Scotland, Wales, and
        # the far south-west. These extra centres cover the remainder.
        EXTRA_BBOX_CENTRES = [
            (54.5, -1.5),   # Northern England (Newcastle/Durham)
            (53.5, -2.5),   # NW England (Manchester/Liverpool)
            (51.5, -3.5),   # Wales / Bristol
            (51.2,  0.5),   # SE England (Kent/Surrey)
            (56.5, -4.0),   # Central Scotland (Glasgow/Edinburgh)
            (57.5, -4.0),   # Highland Scotland (Inverness)
        ]
        if _bbox_base_url:
            for clat, clng in EXTRA_BBOX_CENTRES:
                centre_url = re.sub(
                    r"(latitude=)[^&]+(&longitude=)[^&]+",
                    rf"\g<1>{clat}\g<2>{clng}",
                    _bbox_base_url,
                )
                before = len(locations)
                sep = "&" if "?" in centre_url else "?"
                first = await fetch_via_browser(page, centre_url, auth=bearer_token)
                if not first:
                    log.warning("Multi-centre bbox failed for (%.1f, %.1f)", clat, clng)
                    continue
                last_page = (first.get("meta") or {}).get("last_page", 1)
                for loc in first.get("data", []):
                    locations[loc["uuid"]] = loc
                for page_num in range(2, last_page + 1):
                    data = await fetch_via_browser(
                        page, f"{centre_url}{sep}page={page_num}", auth=bearer_token
                    )
                    if data:
                        for loc in data.get("data", []):
                            locations[loc["uuid"]] = loc
                added = len(locations) - before
                print(f"  Centre ({clat}, {clng}): +{added} new locations (total: {len(locations)})")
                log.info("Multi-centre bbox (%.1f, %.1f): %d pages, +%d locations",
                         clat, clng, last_page, added)

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

        # Cache bearer token for targeted fast scrapes
        global _last_bearer, _bearer_cached_at
        if bearer_token:
            _last_bearer = bearer_token
            _bearer_cached_at = time.monotonic()
            _write_bearer_cache(bearer_token)

        await browser.close()

    # ── Build records ──────────────────────────────────────────────────────────
    # Full records for ALL bounding-box hubs (CHAdeMO already stripped from devices).
    # We upsert every record so the DB device list / total_evses / connector_types
    # are updated to the post-exclusion values even for hubs that fall below threshold.
    all_records = [
        build_record(loc, status_map.get(loc["uuid"]), scraped_at,
                     loc_detail=loc_detail_map.get(loc["uuid"]))
        for loc in qualifying
    ]
    # Separate set: only hubs that meet the minimum non-excluded EVSE count get snapshots.
    snapshot_records = [r for r in all_records if r["total_evses"] >= MIN_EVSES]
    log.info("After CHAdeMO/EVSE filter: %d/%d bounding-box hubs meet >= %d non-excluded EVSEs",
             len(snapshot_records), len(all_records), MIN_EVSES)

    # Snapshot-only records for DB hubs not found in current bounding-box sweep.
    # Load stored devices_raw_loc from DB so we can derive qualifying EVSE UUIDs
    # (power + connector filter) without re-fetching location details.
    db_only_uuids = list(db_hub_uuids - qualifying_uuids)
    db_stored_raw = db.get_devices_raw_for_hubs(db_only_uuids) if db_only_uuids else {}

    db_only_records = []
    for uuid in db_only_uuids:
        s = status_map.get(uuid)
        if not s:
            continue
        filtered = _filter_raw_devices(db_stored_raw.get(uuid, []))
        allowed = {e.get("uuid") for dev in filtered for e in dev.get("evses", [])}
        # If no stored device data fall back to connector-only filter (None = no UUID restriction)
        parsed = _parse_status(s, allowed_evse_uuids=allowed if allowed else None)
        non_qual_evses = sum(len(d["evses"]) for d in parsed["devices_out"])
        if non_qual_evses < MIN_EVSES:
            continue  # hub falls below threshold after exclusions
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

    total_snapshots = len(snapshot_records) + len(db_only_records)
    log.info("Records built: %d bounding-box snapshots + %d db-only = %d total",
             len(snapshot_records), len(db_only_records), total_snapshots)
    print(f"Collected {len(snapshot_records)} bounding-box + {len(db_only_records)} DB-only hub records.")

    # Persist to SQLite
    db.init_db()

    # Exclude hubs covered by targeted scraping — they get snapshots + event detection
    # from targeted_scraper.py, so writing full-scrape snapshots for them would double
    # visit counts and inflate utilisation history.
    targeted_uuids = db.get_all_targeted_hub_uuids()
    if targeted_uuids:
        snapshot_records = [r for r in snapshot_records if r["uuid"] not in targeted_uuids]
        db_only_records  = [r for r in db_only_records  if r["uuid"] not in targeted_uuids]
        log.info("Full scrape: excluded %d targeted hub(s) from snapshot write", len(targeted_uuids))

    # EVSE-level change detection must run before upsert_hubs overwrites latest_devices_status
    db.process_evse_events(snapshot_records + db_only_records)

    # Upsert ALL bounding-box records (not just snapshot_records) so that
    # total_evses / connector_types / latest_devices_status are corrected in the DB
    # for hubs that lost CHAdeMO bays — the API layer filters by total_evses >= MIN_EVSES.
    db.upsert_hubs(all_records)
    db.insert_snapshots(snapshot_records + db_only_records)
    db.update_latest_devices_status(db_only_records)
    log.info("DB write complete — %d hub snapshots saved", total_snapshots)
    print(f"Saved {total_snapshots} hub snapshots to database.")


async def scrape_targeted(uuids: list[str]) -> int:
    """Fetch status snapshots for a specific set of hub UUIDs (high-frequency mode).

    Launches a lightweight browser session — no bounding-box capture, no zooming.
    When a valid cached bearer token exists, skips loading zapmap.com entirely
    (new page starts at about:blank) to keep targeted runs fast and on-schedule.
    Falls back to loading zapmap.com to obtain a fresh token only when the cache
    is empty or expired.

    Returns the number of snapshots saved.
    """
    global _last_bearer, _bearer_cached_at

    if not uuids:
        return 0

    # Fast path: skip zapmap.com navigation when the cached token is still valid.
    cached_age = time.monotonic() - _bearer_cached_at
    skip_page_load = bool(_last_bearer and cached_age < BEARER_MAX_AGE_S)
    bearer_token: str | None = _last_bearer if skip_page_load else None

    if skip_page_load:
        log.info("scrape_targeted: cached bearer token valid (age %.0fs) — skipping page load", cached_age)
    else:
        log.info("scrape_targeted: no valid cached token — navigating to zapmap.com/live/")

    async with async_playwright() as p:
        proxy_cfg = _pick_proxy()
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--enable-unsafe-swiftshader",
                "--no-zygote",
            ],
            proxy=proxy_cfg,
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

        if not skip_page_load:
            def on_request(request):
                nonlocal bearer_token
                if bearer_token or "api.zap-map.io" not in request.url:
                    return
                auth = request.headers.get("authorization", "")
                if auth.startswith("Bearer "):
                    bearer_token = auth

            page.on("request", on_request)

            await page.add_init_script("""
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
            """)

            await page.goto("https://www.zapmap.com/live/",
                            wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(4_000)

            if not bearer_token:
                log.warning("scrape_targeted: no bearer token available — skipping")
                await browser.close()
                return 0

        # Refresh the in-memory cache so the next run can skip page load too
        _last_bearer = bearer_token
        _bearer_cached_at = time.monotonic()
        _write_bearer_cache(bearer_token)

        # Fetch status in chunks of 50
        status_map: dict = {}
        chunks = [uuids[i:i + 50] for i in range(0, len(uuids), 50)]
        log.info("scrape_targeted: fetching status for %d hubs (%d chunks)", len(uuids), len(chunks))
        for chunk in chunks:
            url = f"{BASE_API}/transient/status?uuids={','.join(chunk)}"
            data = await fetch_via_browser(page, url, auth=bearer_token)
            if data:
                for item in data.get("data", []):
                    status_map[item["uuid"]] = item

        await browser.close()

    scraped_at = datetime.now(timezone.utc).isoformat()
    records = []
    for uuid in uuids:
        s = status_map.get(uuid)
        if not s:
            continue
        parsed = _parse_status(s)
        records.append({
            "uuid": uuid,
            "scraped_at": scraped_at,
            "available_count":    parsed["available"],
            "charging_count":     parsed["charging"],
            "inoperative_count":  parsed["inoperative"],
            "out_of_order_count": parsed["out_of_order"],
            "unknown_count":      parsed["unknown"],
            "devices":            parsed["devices_out"],
        })

    if records:
        db.process_evse_events(records)
        db.insert_snapshots(records, source='targeted')
        log.info("scrape_targeted: saved %d snapshots", len(records))

    return len(records)


if __name__ == "__main__":
    asyncio.run(scrape())
