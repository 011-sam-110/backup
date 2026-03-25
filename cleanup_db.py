"""
cleanup_db.py — Remove hubs (and their snapshots) outside Great Britain from the DB.

Great Britain bbox: lat 49.9–61.0, lng -8.7–1.8
Run once: python cleanup_db.py
"""
import sqlite3

GB_LAT = (49.9, 61.0)
GB_LNG = (-8.7, 1.8)

con = sqlite3.connect("chargers.db")

outside = con.execute("""
    SELECT COUNT(*) FROM hubs
    WHERE latitude  < ? OR latitude  > ?
       OR longitude < ? OR longitude > ?
""", (GB_LAT[0], GB_LAT[1], GB_LNG[0], GB_LNG[1])).fetchone()[0]

print(f"Hubs outside GB to delete: {outside}")
if outside == 0:
    print("Nothing to do.")
    con.close()
    exit()

snap_del = con.execute("""
    DELETE FROM snapshots WHERE hub_uuid IN (
        SELECT uuid FROM hubs
        WHERE latitude  < ? OR latitude  > ?
           OR longitude < ? OR longitude > ?
    )
""", (GB_LAT[0], GB_LAT[1], GB_LNG[0], GB_LNG[1])).rowcount

hub_del = con.execute("""
    DELETE FROM hubs
    WHERE latitude  < ? OR latitude  > ?
       OR longitude < ? OR longitude > ?
""", (GB_LAT[0], GB_LAT[1], GB_LNG[0], GB_LNG[1])).rowcount

con.commit()
con.close()

print(f"Deleted {hub_del} hub(s) and {snap_del} snapshot(s).")
remaining = sqlite3.connect("chargers.db").execute("SELECT COUNT(*) FROM hubs").fetchone()[0]
print(f"Remaining hubs in DB: {remaining}")
