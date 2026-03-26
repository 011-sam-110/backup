"""
purge_hubs.py — Delete specific hub UUIDs and all their associated data from chargers.db.
Run from the project root: python purge_hubs.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "chargers.db"

UUIDS_TO_DELETE = [
    "137MALY", "14HZ49R", "1H58E4T", "1XUOWK4", "296MSPJ", "2L70F99",
    "2UUG8E1", "2W79SS4", "39480H7", "3IY5PRU", "3JU1IL8", "3OWK8FE",
    "3RF21OL", "45RKD98", "4AOXAZP", "4EU84A3", "4J1QX4C", "500UBXS",
    "55DVLZJ", "5657134", "568W6ZZ", "5C7T6K3", "5LE7QQE", "5R4SITI",
    "5WIHXRC", "5YK8YWV", "63GHCGA", "6867JN0", "6AY31J6", "6HU5Y0F",
    "6OLRNHB", "72MTK6O", "7492GME", "7DYK3XE", "7I4E2M5", "7L7UGOZ",
    "7OTYRUF", "7P8JEBH", "7R841OB", "7TYOE2B", "7V7910P", "7W0EFOL",
    "7W5MWQE", "7WRLN58", "849MFTS", "8P82DKZ", "8PLU485", "93ZJA6N",
    "949UKC8", "95NJO3L", "9B9V67L", "9DOYAJW", "A038VXD", "A7VB2XN",
    "A8PZVZB", "AD5PWNM", "AUCDXRU", "AXMGZVO", "AZ55VW2", "B4J5WO6",
    "BHZJR19", "BM2URVQ", "BP0TFRE", "BPLRJS1", "BRR49AJ", "BTPLIH6",
    "BUDJEA0", "BVC2CQ8", "BXA760V", "CA41H3V", "CG4CDSS", "CHIYJQ9",
    "CRKX8HY", "D3NTA3F", "D640CCY", "DZ4VEFX", "EC9GI3W", "EGITLND",
    "EJEZCHF", "EP5X8WC", "ERSVBCB", "EYN7JOZ", "JYAY6J0", "KKWFQ2T",
    "N6MBLQH", "PH82I47",
    # Batch 2
    "21U3AB5", "2ECAF4Y", "2WOJCTH", "4FY04U6", "4NG5WAW", "4UNTUZQ",
    "4XA7GS0", "4XGYB2G", "5GBBFN1", "6KY9BXW", "8EKY5V2", "8LD8HUG",
    "9YH6W3R", "AZ118J4", "EBTK7RP",
]

def main():
    if not DB_PATH.exists():
        print(f"ERROR: database not found at {DB_PATH}")
        return

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")

    placeholders = ",".join("?" * len(UUIDS_TO_DELETE))

    # Verify which UUIDs actually exist before deleting
    existing = con.execute(
        f"SELECT uuid, hub_name FROM hubs WHERE uuid IN ({placeholders})",
        UUIDS_TO_DELETE,
    ).fetchall()

    if not existing:
        print("None of the specified UUIDs were found in the database.")
        con.close()
        return

    print(f"Found {len(existing)} hubs to delete:")
    for uuid, name in existing:
        print(f"  {uuid}  {name or '(no name)'}")

    confirm = input(f"\nDelete these {len(existing)} hubs and all their snapshots/visits? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        con.close()
        return

    existing_uuids = [row[0] for row in existing]
    ph = ",".join("?" * len(existing_uuids))

    # Delete child rows explicitly (in case foreign_keys pragma isn't respected)
    snap_count = con.execute(f"DELETE FROM snapshots  WHERE hub_uuid IN ({ph})", existing_uuids).rowcount
    visit_count = con.execute(f"DELETE FROM visits     WHERE hub_uuid IN ({ph})", existing_uuids).rowcount
    gh_count    = con.execute(f"DELETE FROM group_hubs WHERE hub_uuid IN ({ph})", existing_uuids).rowcount
    hub_count   = con.execute(f"DELETE FROM hubs       WHERE uuid     IN ({ph})", existing_uuids).rowcount

    con.commit()
    con.close()

    print(f"\nDone.")
    print(f"  Hubs deleted:      {hub_count}")
    print(f"  Snapshots deleted: {snap_count}")
    print(f"  Visits deleted:    {visit_count}")
    print(f"  Group links removed: {gh_count}")

if __name__ == "__main__":
    main()
