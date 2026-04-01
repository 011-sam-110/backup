"""
recover.py — Extract all readable data from a corrupt chargers.db into a clean new DB.

Usage (on the server):
    python3 recover.py [source] [destination]

Defaults:
    source      = chargers.db.bak
    destination = chargers_recovered.db
"""

import sqlite3
import sys
from pathlib import Path

SRC_PATH = sys.argv[1] if len(sys.argv) > 1 else "chargers.db.bak"
DST_PATH = sys.argv[2] if len(sys.argv) > 2 else "chargers_recovered.db"

print(f"Source : {SRC_PATH}")
print(f"Dest   : {DST_PATH}")

if Path(DST_PATH).exists():
    print(f"ERROR: {DST_PATH} already exists — delete it first to avoid merging corrupt data.")
    sys.exit(1)

src = sqlite3.connect(SRC_PATH)
src.row_factory = sqlite3.Row
dst = sqlite3.connect(DST_PATH)

# ── Step 1: recreate schema from source ───────────────────────────────────────
print("\n[1/3] Recreating schema...")
schema = src.execute(
    "SELECT name, sql FROM sqlite_master "
    "WHERE type IN ('table', 'index') AND sql IS NOT NULL "
    "ORDER BY type DESC"  # tables before indexes
).fetchall()

for row in schema:
    try:
        dst.execute(row["sql"])
        print(f"  + {row['name']}")
    except Exception as e:
        print(f"  ! skip {row['name']}: {e}")

dst.commit()

# ── Step 2: copy data table by table ──────────────────────────────────────────
# FK-safe order: parents before children
TABLES = ["groups", "hubs", "visits", "group_hubs", "snapshots", "evse_events"]

print("\n[2/3] Copying data...")
for table in TABLES:
    try:
        cur = src.execute(f"SELECT * FROM {table}")
        cols = len(cur.description)
        ph = ",".join(["?"] * cols)
        count = 0
        batch = []
        errors = 0
        for row in cur:
            try:
                batch.append(tuple(row))
            except Exception:
                errors += 1
                continue
            if len(batch) >= 10000:
                try:
                    dst.executemany(f"INSERT OR IGNORE INTO {table} VALUES ({ph})", batch)
                    dst.commit()
                except Exception as e:
                    print(f"  ! batch write error in {table}: {e}")
                count += len(batch)
                batch = []
        if batch:
            try:
                dst.executemany(f"INSERT OR IGNORE INTO {table} VALUES ({ph})", batch)
                dst.commit()
            except Exception as e:
                print(f"  ! final batch error in {table}: {e}")
            count += len(batch)
        msg = f"  {table}: {count:,} rows"
        if errors:
            msg += f" ({errors} row errors skipped)"
        print(msg)
    except Exception as e:
        print(f"  ERROR reading {table}: {e}")

# ── Step 3: VACUUM and integrity check ────────────────────────────────────────
print("\n[3/3] VACUUM + integrity check...")
dst.execute("VACUUM")
dst.commit()

result = dst.execute("PRAGMA integrity_check").fetchone()[0]
print(f"  integrity_check: {result}")

hubs = dst.execute("SELECT COUNT(*) FROM hubs").fetchone()[0]
snaps = dst.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
print(f"  hubs      : {hubs:,}")
print(f"  snapshots : {snaps:,}")

print(f"\nRecovered DB written to: {DST_PATH}")
print("Next: cp chargers_recovered.db chargers.db && docker compose up -d")
