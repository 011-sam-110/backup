"""
parse_har.py — Extract hub UUIDs from a Chrome/Edge HAR recording of zapmap.com/live/

UUIDs are pulled from transient/status request URLs (not response bodies, which Chrome
doesn't always capture). Any UUID that the map requested a status update for is a real
hub Zapmap knows about.

Usage:
  1. Open Chrome/Edge, press F12 → Network tab (red record circle must be on)
  2. Navigate to https://www.zapmap.com/live/
  3. Browse freely — scroll to zoom, drag to pan, cover all of England
  4. Right-click in the Network tab → "Save all as HAR with content"
  5. Save as 'discovery.har' in this folder
  6. Run: python parse_har.py
  7. Then run: python discover.py
"""

import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import db

HAR_PATH = Path("discovery.har")
PENDING_PATH = Path("pending_uuids.json")


def extract_uuids(path: Path) -> set[str]:
    print(f"Reading {path} ...")
    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data["log"]["entries"]

    uuids: set[str] = set()
    status_calls = 0

    for entry in entries:
        url = entry["request"]["url"]
        if "transient/status" not in url:
            continue
        status_calls += 1
        qs = parse_qs(urlparse(url).query)
        for uuid in qs.get("uuids", [""])[0].split(","):
            uuid = uuid.strip()
            if uuid:
                uuids.add(uuid)

    print(f"{status_calls} status request(s) found, {len(uuids)} unique UUIDs extracted.")
    return uuids


def main():
    if not HAR_PATH.exists():
        print(f"ERROR: {HAR_PATH} not found in {Path('.').resolve()}")
        print()
        print("How to create it:")
        print("  1. Open Chrome/Edge and press F12")
        print("  2. Click the Network tab — make sure the red record circle is on")
        print("  3. Navigate to https://www.zapmap.com/live/")
        print("  4. Browse freely: scroll to zoom in/out, drag to pan — cover all England")
        print("  5. Right-click anywhere in the Network request list")
        print("  6. Click 'Save all as HAR with content'")
        print(f"  7. Save the file as 'discovery.har' here: {Path('.').resolve()}")
        print("  8. Run this script again: python parse_har.py")
        sys.exit(1)

    all_uuids = extract_uuids(HAR_PATH)

    if not all_uuids:
        print("\nNo status request URLs found in the HAR file.")
        print("Make sure you browsed zapmap.com/live/ while the Network tab was recording.")
        sys.exit(1)

    # Compare with DB
    db.init_db()
    known_uuids = {h["uuid"] for h in db.get_all_hubs_for_scrape()}
    new_uuids = sorted(all_uuids - known_uuids)
    already = len(all_uuids) - len(new_uuids)

    print(f"\nAlready tracked in DB: {already}")
    print(f"New (not yet in DB):   {len(new_uuids)}")

    if not new_uuids:
        print("\nNothing new to add — DB is up to date.")
        return

    PENDING_PATH.write_text(json.dumps(new_uuids, indent=2))
    print(f"\nSaved {len(new_uuids)} new UUID(s) to {PENDING_PATH}")
    print("Run next:  python discover.py")


if __name__ == "__main__":
    main()
