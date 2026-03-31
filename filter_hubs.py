"""
filter_hubs.py — Delete hubs that don't meet minimum spec and all their associated data.

Criteria for removal:
  - max_power_kw < 100  (below 100 kW)
  - OR total_evses < 6  (fewer than 6 EVSE bays)

Run from the project root: python filter_hubs.py
On the server:            docker compose exec api python3 filter_hubs.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "chargers.db"


def main():
    if not DB_PATH.exists():
        print(f"ERROR: database not found at {DB_PATH}")
        return

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")

    rows = con.execute(
        """
        SELECT uuid, hub_name, max_power_kw, total_evses
        FROM hubs
        WHERE max_power_kw < 100 OR total_evses < 6
        ORDER BY max_power_kw, total_evses
        """
    ).fetchall()

    if not rows:
        print("No hubs found matching the filter criteria.")
        con.close()
        return

    print(f"Found {len(rows)} hub(s) to delete:\n")
    print(f"  {'UUID':<12}  {'kW':>6}  {'EVSEs':>5}  Name")
    print(f"  {'-'*12}  {'-'*6}  {'-'*5}  {'-'*30}")
    for uuid, name, kw, evses in rows:
        kw_str = f"{kw:.0f}" if kw is not None else "?"
        evse_str = str(evses) if evses is not None else "?"
        print(f"  {uuid:<12}  {kw_str:>6}  {evse_str:>5}  {name or '(no name)'}")

    confirm = input(f"\nDelete these {len(rows)} hub(s) and all their data? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        con.close()
        return

    uuids = [row[0] for row in rows]
    ph = ",".join("?" * len(uuids))

    evse_count  = con.execute(f"DELETE FROM evse_events WHERE hub_uuid IN ({ph})", uuids).rowcount
    snap_count  = con.execute(f"DELETE FROM snapshots   WHERE hub_uuid IN ({ph})", uuids).rowcount
    visit_count = con.execute(f"DELETE FROM visits      WHERE hub_uuid IN ({ph})", uuids).rowcount
    gh_count    = con.execute(f"DELETE FROM group_hubs  WHERE hub_uuid IN ({ph})", uuids).rowcount
    hub_count   = con.execute(f"DELETE FROM hubs        WHERE uuid     IN ({ph})", uuids).rowcount

    con.commit()
    con.close()

    print(f"\nDone.")
    print(f"  Hubs deleted:        {hub_count}")
    print(f"  Snapshots deleted:   {snap_count}")
    print(f"  EVSE events deleted: {evse_count}")
    print(f"  Visits deleted:      {visit_count}")
    print(f"  Group links removed: {gh_count}")


if __name__ == "__main__":
    main()
