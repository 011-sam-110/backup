"""
One-off script: delete all legacy delta-detected visits (evse_uuid IS NULL).

These were created by the old _detect_visits() function which inferred visits
from aggregate charging_count deltas. They are systematically inaccurate:
  - Undercounted due to net-delta cancellation
  - Dwell times inflated by scraper downtime gaps (e.g. 1000+ min sessions)

Safe to delete — the new per-EVSE detect_evse_changes() system has replaced
this approach and will build accurate visit records going forward.
"""

import sqlite3
from pathlib import Path
import os

DB_PATH = Path(os.getenv("DATABASE_PATH", "chargers.db"))


def main():
    con = sqlite3.connect(DB_PATH)

    total = con.execute("SELECT COUNT(*) FROM visits").fetchone()[0]
    legacy = con.execute("SELECT COUNT(*) FROM visits WHERE evse_uuid IS NULL").fetchone()[0]
    evse = total - legacy

    print(f"visits table: {total} total ({legacy} legacy, {evse} per-EVSE)")

    if legacy == 0:
        print("Nothing to delete.")
        con.close()
        return

    confirm = input(f"\nDelete {legacy} legacy visits? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        con.close()
        return

    con.execute("DELETE FROM visits WHERE evse_uuid IS NULL")
    con.commit()

    remaining = con.execute("SELECT COUNT(*) FROM visits").fetchone()[0]
    print(f"Done. {legacy} legacy visits deleted. {remaining} per-EVSE visits remain.")

    con.execute("VACUUM")
    con.commit()
    print("Database vacuumed.")
    con.close()


if __name__ == "__main__":
    main()
