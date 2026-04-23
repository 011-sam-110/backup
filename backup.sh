#!/bin/bash
# Daily SQLite backup to Cloudflare R2. Keeps 90 days of backups.
#
# One-time setup on the Ubuntu server:
#   1. apt install rclone sqlite3
#   2. rclone config  →  add remote named "r2" (type: s3, provider: Cloudflare)
#      Access key ID + Secret from Cloudflare dashboard → R2 → Manage API tokens
#   3. Create bucket "jamie-dad-backups" in Cloudflare R2 dashboard
#   4. chmod +x /opt/jamie_dad/backup.sh
#   5. crontab -e  →  add:
#      0 2 * * * DB_PATH=/opt/jamie_dad/chargers.db /opt/jamie_dad/backup.sh >> /var/log/jamie_dad_backup.log 2>&1

set -euo pipefail

DB_PATH="${DB_PATH:-/opt/jamie_dad/chargers.db}"
BUCKET="${R2_BUCKET:-ev-scraper}"
DATE=$(date -u +%Y-%m-%d)
TMP="/tmp/chargers_${DATE}.db"

echo "[$(date -u +%FT%TZ)] Starting backup of ${DB_PATH}..."

sqlite3 "$DB_PATH" ".backup '${TMP}'"
rclone copy "${TMP}" "r2:${BUCKET}/"
rm "${TMP}"

# Delete backups older than 91 days
rclone delete "r2:${BUCKET}/" --min-age 91d

echo "[$(date -u +%FT%TZ)] Backup complete: chargers_${DATE}.db uploaded to r2:${BUCKET}/"
