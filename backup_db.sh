#!/bin/bash
# 15-minute DB snapshot → R2. Keeps last KEEP_HOURS hours of snapshots.
# Crontab: */15 * * * * /root/jamie_dad/backup_db.sh >> /var/log/jamie_dad_backup.log 2>&1

set -euo pipefail

DB_PATH="${DB_PATH:-/root/jamie_dad/chargers.db}"
BUCKET="${R2_BUCKET:-ev-scraper}"
KEEP_HOURS="${KEEP_HOURS:-9}"
TIMESTAMP=$(date -u +%FT%H%MZ)
LOG_TAG="[$(date -u +%FT%TZ)] [backup_db]"

DB_SNAPSHOT="/tmp/chargers_${TIMESTAMP}.db"
echo "$LOG_TAG Snapshotting DB..."
sqlite3 "${DB_PATH}" ".backup ${DB_SNAPSHOT}"

IC=$(sqlite3 "${DB_SNAPSHOT}" "PRAGMA integrity_check;" 2>&1 | head -1)
if [[ "${IC}" != "ok" ]]; then
    echo "$LOG_TAG ERROR: integrity_check='${IC}' — skipping upload"
    rm -f "${DB_SNAPSHOT}"
    exit 1
fi

echo "$LOG_TAG Uploading → r2:${BUCKET}/db-backups/chargers_${TIMESTAMP}.db"
rclone copyto "${DB_SNAPSHOT}" "r2:${BUCKET}/db-backups/chargers_${TIMESTAMP}.db"
rm -f "${DB_SNAPSHOT}"

echo "$LOG_TAG Pruning snapshots older than ${KEEP_HOURS}h..."
rclone delete "r2:${BUCKET}/db-backups/" --min-age "${KEEP_HOURS}h" --include "chargers_*.db"
echo "$LOG_TAG Done."
