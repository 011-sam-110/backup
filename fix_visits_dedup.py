#!/usr/bin/env python3
"""Cleanup: for each EVSE on targeted hubs, keep only the oldest open visit.

An EVSE can only have one active charging session at a time, so any EVSE with
multiple open visits has duplicates. We keep the oldest (the real session start)
and delete the rest. Visits that are already closed are not touched.

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

    # Find EVSEs with more than one open visit
    duped_evses = con.execute(f"""
        SELECT evse_uuid, COUNT(*) AS open_count
        FROM visits
        WHERE hub_uuid IN ({ph})
          AND ended_at IS NULL
        GROUP BY evse_uuid
        HAVING COUNT(*) > 1
    """, targeted_uuids).fetchall()

    if not duped_evses:
        print("No EVSEs with multiple open visits — nothing to do.")
        con.close()
        return

    total_to_delete = 0
    ids_to_delete = []

    for row in duped_evses:
        evse_uuid = row["evse_uuid"]
        # Get all open visits for this EVSE, oldest first
        open_visits = con.execute("""
            SELECT id, started_at FROM visits
            WHERE evse_uuid = ? AND ended_at IS NULL
            ORDER BY started_at ASC
        """, (evse_uuid,)).fetchall()

        # Keep the oldest (index 0), delete the rest
        duplicates = [v["id"] for v in open_visits[1:]]
        ids_to_delete.extend(duplicates)
        total_to_delete += len(duplicates)
        print(f"  EVSE {evse_uuid}: keep oldest ({open_visits[0]['started_at'][:19]}), "
              f"delete {len(duplicates)} duplicate(s)")

    print(f"\nTotal duplicates to delete: {total_to_delete}")

    if dry_run:
        print("[DRY RUN] No changes made.")
        con.close()
        return

    ph2 = ",".join("?" * len(ids_to_delete))
    con.execute(f"DELETE FROM visits WHERE id IN ({ph2})", ids_to_delete)
    con.commit()
    con.close()
    print(f"Done. Deleted {total_to_delete} duplicate open visits.")


if __name__ == "__main__":
    main("--dry-run" in sys.argv)
