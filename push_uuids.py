"""
push_uuids.py — extract hub UUIDs from a local chargers.db and push them
to the live server's /api/admin/discover endpoint.

The server will deduplicate and run discover.py to re-fetch full location
details for each UUID from the Zapmap API.

Usage:
    python push_uuids.py
    python push_uuids.py --db path/to/chargers.db
    python push_uuids.py --dry-run        # print UUIDs, don't push
    python push_uuids.py --batch 50       # override batch size (default 100)

Environment / .env:
    SERVER_URL        Base URL of the live server, e.g. https://yourdomain.com
    DASHBOARD_PASSWORD  Bearer token for the API

NOTE: If your local DB is reasonably up to date, copying it directly to the
server is much better than this script — it preserves all snapshots, visits,
and evse_events history:

    scp chargers.db root@<server-ip>:~/backup/chargers.db
    # then on server: docker compose up -d
"""

import argparse
import os
import sqlite3
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB   = "chargers.db"
DEFAULT_BATCH = 100


def get_uuids(db_path: str) -> list[str]:
    if not os.path.exists(db_path):
        sys.exit(f"Database not found: {db_path}")
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT uuid FROM hubs ORDER BY first_seen_at").fetchall()
    con.close()
    return [r[0] for r in rows]


def push_batch(server_url: str, token: str, uuids: list[str]) -> dict:
    url = f"{server_url.rstrip('/')}/api/admin/discover"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.post(url, json={"uuids": uuids}, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Push hub UUIDs from local DB to live server")
    parser.add_argument("--db",      default=DEFAULT_DB,    help="Path to local chargers.db")
    parser.add_argument("--batch",   default=DEFAULT_BATCH, type=int, help="UUIDs per request")
    parser.add_argument("--dry-run", action="store_true",   help="Print UUIDs without pushing")
    args = parser.parse_args()

    uuids = get_uuids(args.db)
    print(f"Found {len(uuids)} UUIDs in {args.db}")

    if args.dry_run:
        for u in uuids:
            print(u)
        return

    server_url = os.getenv("SERVER_URL", "").strip()
    token      = os.getenv("DASHBOARD_PASSWORD", "").strip()

    if not server_url:
        sys.exit("Set SERVER_URL in your .env or environment, e.g. https://yourdomain.com")

    total_new = 0
    total_dup = 0
    batches   = [uuids[i:i + args.batch] for i in range(0, len(uuids), args.batch)]

    for i, batch in enumerate(batches, 1):
        print(f"Pushing batch {i}/{len(batches)} ({len(batch)} UUIDs)...", end=" ", flush=True)
        try:
            result = push_batch(server_url, token, batch)
            new = result.get("new_count", 0)
            dup = result.get("duplicate_count", 0)
            total_new += new
            total_dup += dup
            print(f"OK — {new} new, {dup} already known")
        except requests.HTTPError as e:
            print(f"FAILED — {e.response.status_code} {e.response.text[:120]}")
        except Exception as e:
            print(f"FAILED — {e}")
        if i < len(batches):
            time.sleep(1)  # avoid hammering discover.py

    print(f"\nDone. {total_new} new UUIDs queued for discovery, {total_dup} duplicates skipped.")
    if total_new:
        print("The server is running discover.py in the background — check logs in a few minutes.")


if __name__ == "__main__":
    main()
