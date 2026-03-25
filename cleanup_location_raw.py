"""
cleanup_location_raw.py — NULL out the location_raw column and VACUUM the DB.

The location_raw column stored the full raw Zapmap API response per hub.
It was never used by the website and inflated the DB unnecessarily.
Run once after deploying the updated scraper/api code.
"""
import sqlite3

con = sqlite3.connect("chargers.db")
rows = con.execute("SELECT COUNT(*) FROM hubs WHERE location_raw IS NOT NULL").fetchone()[0]
print(f"Hubs with location_raw data: {rows}")
if rows > 0:
    con.execute("UPDATE hubs SET location_raw = NULL")
    con.commit()
    print("Cleared location_raw column.")
con.close()

print("Running VACUUM to reclaim space...")
con = sqlite3.connect("chargers.db")
con.execute("VACUUM")
con.close()
print("Done.")
