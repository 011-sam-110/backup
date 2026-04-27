#!/usr/bin/env python3
"""One-time cleanup: remove duplicate visits created by the targeted scraper bug.

Bug: scrape_targeted() called detect_evse_changes() but never updated
latest_devices_status, so every targeted poll re-detected the same
AVAILABLE->CHARGING transition and opened a new visit row each minute.

The duplicates have a specific fingerprint on targeted-hub EVSEs:
  - dwell_min IS NULL  (set by close_stale_visits after 12h — never properly closed)
  - ended_at IS NOT NULL  (already stale-closed)

Properly-closed visits (dwell_min IS NOT NULL) are left untouched.
Non-targeted hubs are not touched.

Run with --dry-run to preview without making changes.
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("chargers.db")


def main(dry_run: bool = False) -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # Only act on EVSEs belonging to targeted hubs (scrape_interval IS NOT NULL)
    targeted_uuids = [
        r["hub_uuid"] for r in con.execute("""
            SELECT DISTINCT gh.hub_uuid
            FROM group_hubs gh
            JOIN groups g ON g.id = gh.group_id
            WHERE g.scrape_interval IS NOT NULL
        """).fetchall()
    ]

    if not targeted_uuids:
        print("No targeted hubs found — nothing to do.")
        con.close()
        return

    print(f"Targeted hubs: {len(targeted_uuids)}")

    ph = ",".join("?" * len(targeted_uuids))

    # Count what we're about to delete
    stale_count = con.execute(f"""
        SELECT COUNT(*) FROM visits
        WHERE hub_uuid IN ({ph})
          AND ended_at IS NOT NULL
          AND dwell_min IS NULL
    """, targeted_uuids).fetchone()[0]

    # Count what we're keeping
    real_count = con.execute(f"""
        SELECT COUNT(*) FROM visits
        WHERE hub_uuid IN ({ph})
          AND dwell_min IS NOT NULL
    """, targeted_uuids).fetchone()[0]

    open_count = con.execute(f"""
        SELECT COUNT(*) FROM visits
        WHERE hub_uuid IN ({ph})
          AND ended_at IS NULL
    """, targeted_uuids).fetchone()[0]

    print(f"\nVisits for targeted hubs:")
    print(f"  Stale-closed duplicates (dwell_min IS NULL): {stale_count}  ← will DELETE")
    print(f"  Properly closed (dwell_min IS NOT NULL):     {real_count}   ← kept")
    print(f"  Still open (ended_at IS NULL):               {open_count}   ← kept")

    if stale_count == 0:
        print("\nNothing to delete.")
        con.close()
        return

    if dry_run:
        print(f"\n[DRY RUN] Would delete {stale_count} stale-closed visits.")
        con.close()
        return

    con.execute(f"""
        DELETE FROM visits
        WHERE hub_uuid IN ({ph})
          AND ended_at IS NOT NULL
          AND dwell_min IS NULL
    """, targeted_uuids)
    con.commit()
    con.close()
    print(f"\nDeleted {stale_count} stale-closed duplicate visits.")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
