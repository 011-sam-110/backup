#!/usr/bin/env python3
"""Diagnostic: show open visits for targeted hubs, grouped by EVSE.

If any EVSE has more than 1 open visit, those are duplicates —
a real session can only have one open visit per EVSE at a time.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("chargers.db")


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    targeted = con.execute("""
        SELECT DISTINCT gh.hub_uuid, h.hub_name
        FROM group_hubs gh
        JOIN groups g ON g.id = gh.group_id
        JOIN hubs h ON h.uuid = gh.hub_uuid
        WHERE g.scrape_interval IS NOT NULL
    """).fetchall()

    if not targeted:
        print("No targeted hubs found.")
        con.close()
        return

    for hub in targeted:
        hub_uuid = hub["hub_uuid"]
        hub_name = hub["hub_name"] or hub_uuid

        total_open = con.execute(
            "SELECT COUNT(*) FROM visits WHERE hub_uuid = ? AND ended_at IS NULL",
            (hub_uuid,)
        ).fetchone()[0]

        print(f"\n{'='*60}")
        print(f"Hub: {hub_name} ({hub_uuid})")
        print(f"Total open visits: {total_open}")
        print(f"{'='*60}")

        if total_open == 0:
            print("  No open visits.")
            continue

        rows = con.execute("""
            SELECT
                evse_uuid,
                COUNT(*)        AS open_count,
                MIN(started_at) AS oldest,
                MAX(started_at) AS newest
            FROM visits
            WHERE hub_uuid = ? AND ended_at IS NULL
            GROUP BY evse_uuid
            ORDER BY open_count DESC, newest DESC
        """, (hub_uuid,)).fetchall()

        duped   = [r for r in rows if r["open_count"] > 1]
        singles = [r for r in rows if r["open_count"] == 1]

        print(f"\n  EVSEs with >1 open visit (duplicates): {len(duped)}")
        for r in duped[:20]:
            print(f"    {r['evse_uuid']}  count={r['open_count']}  "
                  f"oldest={r['oldest'][:19]}  newest={r['newest'][:19]}")
        if len(duped) > 20:
            print(f"    ... and {len(duped) - 20} more")

        print(f"\n  EVSEs with exactly 1 open visit (possibly real): {len(singles)}")
        for r in singles[:20]:
            print(f"    {r['evse_uuid']}  started={r['oldest'][:19]}")
        if len(singles) > 20:
            print(f"    ... and {len(singles) - 20} more")

    con.close()


if __name__ == "__main__":
    main()
