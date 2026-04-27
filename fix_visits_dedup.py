#!/usr/bin/env python3
"""Cleanup duplicate completed visits on targeted hubs.

The targeted scraper bug opened one visit per minute while an EVSE was
charging. When the EVSE went AVAILABLE, subsequent polls each closed one
duplicate (most-recent-first), giving every duplicate a plausible dwell_min.

Detection: two completed visits for the same EVSE overlap when
  B.started_at < A.ended_at  (B started before A had finished)
This is impossible for real back-to-back sessions.

For each overlapping cluster we keep ONE visit:
  started_at = earliest in cluster  (best session-start estimate)
  ended_at   = earliest ended_at    (first close = most accurate session end)
  dwell_min  = recalculated from above

Open visits (ended_at IS NULL) are not touched — handled separately.
Run with --dry-run to preview without making changes.
"""
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("chargers.db")


def isoparse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


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

    # All completed visits for targeted hubs, per EVSE ordered by started_at
    evse_uuids = [
        r["evse_uuid"] for r in con.execute(f"""
            SELECT DISTINCT evse_uuid FROM visits
            WHERE hub_uuid IN ({ph})
              AND evse_uuid IS NOT NULL
              AND ended_at IS NOT NULL
        """, targeted_uuids).fetchall()
    ]

    total_deleted = 0
    total_updated = 0

    for evse_uuid in evse_uuids:
        visits = con.execute("""
            SELECT id, started_at, ended_at, dwell_min
            FROM visits
            WHERE evse_uuid = ? AND ended_at IS NOT NULL
            ORDER BY started_at ASC
        """, (evse_uuid,)).fetchall()

        if len(visits) < 2:
            continue

        # Build overlapping clusters
        clusters: list[list[dict]] = []
        current = [dict(visits[0])]
        cluster_max_end = isoparse(visits[0]["ended_at"])

        for v in visits[1:]:
            v_start = isoparse(v["started_at"])
            if v_start < cluster_max_end:
                # Overlaps — same session
                current.append(dict(v))
                v_end = isoparse(v["ended_at"])
                if v_end > cluster_max_end:
                    cluster_max_end = v_end
            else:
                clusters.append(current)
                current = [dict(v)]
                cluster_max_end = isoparse(v["ended_at"])
        clusters.append(current)

        for cluster in clusters:
            if len(cluster) == 1:
                continue

            # Best started_at: earliest (real session start)
            # Best ended_at:   earliest (first close = most accurate real end)
            keeper = min(cluster, key=lambda v: v["started_at"])
            earliest_end = min(cluster, key=lambda v: v["ended_at"])

            new_ended_at = earliest_end["ended_at"]
            new_dwell_min = round(
                (isoparse(new_ended_at) - isoparse(keeper["started_at"])).total_seconds() / 60
            )

            to_delete = [v["id"] for v in cluster if v["id"] != keeper["id"]]
            total_deleted += len(to_delete)

            print(f"  EVSE {evse_uuid[:8]}…  cluster={len(cluster)}  "
                  f"keep started={keeper['started_at'][:19]}  "
                  f"ended={new_ended_at[:19]}  dwell={new_dwell_min}m  "
                  f"delete={len(to_delete)}")

            if not dry_run:
                con.execute(
                    "UPDATE visits SET ended_at = ?, dwell_min = ? WHERE id = ?",
                    (new_ended_at, new_dwell_min, keeper["id"]),
                )
                del_ph = ",".join("?" * len(to_delete))
                con.execute(f"DELETE FROM visits WHERE id IN ({del_ph})", to_delete)
                total_updated += 1

    if not dry_run:
        con.commit()

    con.close()
    tag = "[DRY RUN] " if dry_run else ""
    print(f"\n{tag}Total: {total_deleted} duplicate visits removed, "
          f"{total_updated} keepers updated.")


if __name__ == "__main__":
    main("--dry-run" in sys.argv)
