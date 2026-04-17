"""
remediate_push.py — One-time CHAdeMO remediation: re-discover all hubs from HAR files.

Reads all discovery*.har files, extracts every UUID seen across all sessions, and pushes
them ALL to the server's /api/admin/rediscover endpoint — bypassing the "already known"
filter so that every hub (including those already in the DB) gets re-fetched and re-upserted
with the corrected CHAdeMO-per-connector filtering logic.

Why this is needed:
  Old logic dropped an entire EVSE when it contained any CHAdeMO connector. This took
  CCS connectors with it on dual-standard units (one physical bay with both CCS + CHAdeMO
  ports). The code was fixed in commit 963bf91 but the DB still holds the wrong counts.

  This script forces discover.py on the server to re-run for every UUID so:
    - EVSE counts are corrected (CCS preserved, CHAdeMO stripped)
    - Hubs previously excluded because their count fell below the threshold get added back
    - MIN_EVSES=6, MIN_SHARED_POWER_W=150kW are respected by discover.py automatically

Usage:
  1. Ensure SERVER_URL and DASHBOARD_PASSWORD are set in .env
  2. Run: python remediate_push.py
  3. On server: docker compose logs -f api  (watch discover.py work through the list)
"""

import json
import os
import ssl
import sys
import urllib.request
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
load_dotenv()


def extract_uuids_from_har(path: Path) -> set[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    uuids: set[str] = set()
    status_calls = 0
    for entry in data["log"]["entries"]:
        url = entry["request"]["url"]
        if "transient/status" not in url:
            continue
        status_calls += 1
        qs = parse_qs(urlparse(url).query)
        for uuid in qs.get("uuids", [""])[0].split(","):
            uuid = uuid.strip()
            if uuid:
                uuids.add(uuid)
    return uuids


def main():
    har_files = sorted(Path(".").glob("discovery*.har"))
    if not har_files:
        print("ERROR: no discovery*.har files found in current directory.")
        sys.exit(1)

    print(f"Found {len(har_files)} HAR file(s):")
    all_uuids: set[str] = set()
    for har_path in har_files:
        uuids = extract_uuids_from_har(har_path)
        print(f"  {har_path.name}: {len(uuids)} UUIDs")
        all_uuids |= uuids

    print(f"\nTotal unique UUIDs across all HAR files: {len(all_uuids)}")

    server_url = os.getenv("SERVER_URL", "").rstrip("/")
    password = os.getenv("DASHBOARD_PASSWORD", "")

    if not server_url or not password:
        print("\nERROR: SERVER_URL and DASHBOARD_PASSWORD must be set in .env")
        sys.exit(1)

    endpoint = f"{server_url}/api/admin/rediscover"
    print(f"\nPushing {len(all_uuids)} UUIDs to {endpoint} ...")

    payload = json.dumps({"uuids": sorted(all_uuids)}).encode()
    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {password}",
        },
        method="POST",
    )

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            result = json.loads(resp.read())
        print(f"Server response: {result}")
        print(f"\nQueued {result.get('queued', '?')} UUIDs for re-discovery.")
        print("Discovery is running in the background on the server (~5-15 min depending on count).")
        print("Monitor with: docker compose logs -f api")
    except Exception as e:
        print(f"ERROR: Push to server failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
