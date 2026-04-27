#!/usr/bin/env python3
"""One-time cleanup: delete all visits for targeted hubs and start fresh.

The old targeted scraper wrote duplicate visits to the main visits table.
The data is too mixed to unpick cleanly, so we clear it and let the fixed
code rebuild accurate visits from this point forward.

Run with --dry-run to preview without making changes.
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("chargers.db")


def main(dry_run: bool = False) -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

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

    ph = ",".join("?" * len(targeted_uuids))
    count = con.execute(
        f"SELECT COUNT(*) FROM visits WHERE hub_uuid IN ({ph})", targeted_uuids
    ).fetchone()[0]

    print(f"Targeted hubs: {len(targeted_uuids)}")
    print(f"Visits to delete: {count}")

    if count == 0:
        print("Nothing to delete.")
        con.close()
        return

    if dry_run:
        print("[DRY RUN] No changes made.")
        con.close()
        return

    con.execute(f"DELETE FROM visits WHERE hub_uuid IN ({ph})", targeted_uuids)
    con.commit()
    con.close()
    print(f"Deleted {count} visits. Accurate counts will rebuild from the next scrape.")


if __name__ == "__main__":
    main("--dry-run" in sys.argv)
