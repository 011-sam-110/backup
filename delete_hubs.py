"""
One-off script to permanently delete specific hubs and all their data.
Run on the server with: docker compose exec api python3 delete_hubs.py
"""
import db

REMOVE = ['700BPQF', '4Q4QSUI', '4THKKKQ']

con = db._connect()
for table, col in [
    ('snapshots',  'hub_uuid'),
    ('visits',     'hub_uuid'),
    ('group_hubs', 'hub_uuid'),
    ('hubs',       'uuid'),
]:
    placeholders = ','.join('?' * len(REMOVE))
    cur = con.execute(f'DELETE FROM {table} WHERE {col} IN ({placeholders})', REMOVE)
    print(f'{table}: {cur.rowcount} rows deleted')
con.commit()
con.close()
print('Done.')
