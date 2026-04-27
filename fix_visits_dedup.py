#!/usr/bin/env python3
"""One-time cleanup: remove duplicate visits created by the targeted scraper bug.

Bug: scrape_targeted() called detect_evse_changes() but never updated
latest_devices_status, so every targeted poll re-detected the same
AVAILABLE->CHARGING transition and opened a new visit row each minute.

This script collapses each cluster of visits for the same EVSE (started_at
within SESSION_GAP_MIN of each other) into a single visit with:
  - started_at = earliest in cluster (true session start)
  - ended_at   = from the properly-closed visit in the cluster (if any)
  - dwell_min  = recalculated from earliest started_at to ended_at

Legacy visits with evse_uuid IS NULL (from the old _detect_visits path) are
left untouched.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path("chargers.db")
SESSION_GAP_MIN = 20  # gap larger than this = distinct new session


def isoparse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    duped = con.execute("""
        SELECT evse_uuid FROM visits
        WHERE evse_uuid IS NOT NULL
        GROUP BY evse_uuid HAVING COUNT(*) > 1
    """).fetchall()

    if not duped:
        print("No duplicate visits found — nothing to do.")
        con.close()
        return

    print(f"Found {len(duped)} EVSEs with multiple visits. Processing...")

    total_deleted = 0
    total_updated = 0

    for row in duped:
        evse_uuid = row["evse_uuid"]
        visits = con.execute("""
            SELECT id, hub_uuid, evse_uuid, started_at, ended_at, dwell_min
            FROM visits WHERE evse_uuid = ?
            ORDER BY started_at ASC
        """, (evse_uuid,)).fetchall()

        # Group into sessions by gap threshold
        sessions: list[list[dict]] = []
        current: list[dict] = [dict(visits[0])]
        for v in visits[1:]:
            gap = (isoparse(v["started_at"]) - isoparse(current[-1]["started_at"])).total_seconds() / 60
            if gap > SESSION_GAP_MIN:
                sessions.append(current)
                current = [dict(v)]
            else:
                current.append(dict(v))
        sessions.append(current)

        for session in sessions:
            if len(session) == 1:
                continue

            keeper = session[0]
            to_delete = [v["id"] for v in session[1:]]

            # Prefer the last properly-closed visit for the real ended_at
            properly_closed = [v for v in session if v["dwell_min"] is not None]
            if properly_closed:
                best = properly_closed[-1]
                new_ended_at = best["ended_at"]
                try:
                    new_dwell_min = round(
                        (isoparse(new_ended_at) - isoparse(keeper["started_at"])).total_seconds() / 60
                    )
                except Exception:
                    new_dwell_min = best["dwell_min"]
            else:
                new_ended_at = keeper["ended_at"]
                new_dwell_min = keeper["dwell_min"]

            con.execute(
                "UPDATE visits SET ended_at = ?, dwell_min = ? WHERE id = ?",
                (new_ended_at, new_dwell_min, keeper["id"]),
            )
            total_updated += 1

            ph = ",".join("?" * len(to_delete))
            con.execute(f"DELETE FROM visits WHERE id IN ({ph})", to_delete)
            total_deleted += len(to_delete)

    con.commit()
    con.close()
    print(f"Done. Deleted {total_deleted} duplicate visit rows, updated {total_updated} keepers.")


if __name__ == "__main__":
    main()
