import db

con = db._connect()
print('Count  :', con.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0])
print('Latest :', con.execute('SELECT MAX(scraped_at) FROM snapshots').fetchone()[0])
print('Columns:', [r[1] for r in con.execute('PRAGMA table_info(snapshots)').fetchall()])
print('WAL    :', con.execute('PRAGMA journal_mode').fetchone()[0])
