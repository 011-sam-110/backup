#!/bin/bash
# Daily backup script — uploads Excel exports + SQLite DB snapshot to Cloudflare R2.
# DB backups: keeps last 2 days (today + yesterday). Excel exports: keeps 90 days.
#
# One-time setup on the Ubuntu server:
#   1. apt install rclone
#   2. rclone config  →  add remote named "r2" (type: s3, provider: Cloudflare)
#      Access Key ID + Secret from Cloudflare dashboard → R2 → Manage R2 API Tokens
#   3. rclone config update r2 no_check_bucket true
#   4. chmod +x /root/backup/backup.sh
#   5. crontab -e  →  add:
#      0 3 * * * /root/backup/backup.sh >> /var/log/jamie_dad_backup.log 2>&1

set -euo pipefail

EXPORTS_DIR="${EXPORTS_DIR:-/root/backup/exports}"
DB_PATH="${DB_PATH:-/root/backup/chargers.db}"
BUCKET="${R2_BUCKET:-ev-scraper}"
TODAY=$(date -u +%F)
YESTERDAY=$(date -u -d yesterday +%F)

echo "[$(date -u +%FT%TZ)] === Daily backup start ==="

# --- DB backup ---
DB_SNAPSHOT="/tmp/chargers_${TODAY}.db"
echo "[$(date -u +%FT%TZ)] Creating DB snapshot via sqlite3 .backup..."
sqlite3 "${DB_PATH}" ".backup ${DB_SNAPSHOT}"
echo "[$(date -u +%FT%TZ)] Uploading DB snapshot to r2:${BUCKET}/db-backups/..."
rclone copyto "${DB_SNAPSHOT}" "r2:${BUCKET}/db-backups/chargers_${TODAY}.db"
rm -f "${DB_SNAPSHOT}"

# Delete DB backups older than yesterday (keep today + yesterday only)
echo "[$(date -u +%FT%TZ)] Pruning old DB backups (keeping ${TODAY} and ${YESTERDAY})..."
rclone lsf "r2:${BUCKET}/db-backups/" | grep '^chargers_' | while read -r fname; do
    date_part="${fname#chargers_}"
    date_part="${date_part%.db}"
    if [[ "${date_part}" != "${TODAY}" && "${date_part}" != "${YESTERDAY}" ]]; then
        echo "[$(date -u +%FT%TZ)] Deleting old backup: ${fname}"
        rclone deletefile "r2:${BUCKET}/db-backups/${fname}"
    fi
done

# --- Excel exports ---
echo "[$(date -u +%FT%TZ)] Uploading Excel exports to r2:${BUCKET}/..."
rclone copy "${EXPORTS_DIR}/" "r2:${BUCKET}/" --include "*.xlsx"
rclone delete "r2:${BUCKET}/" --min-age 91d --include "*.xlsx"

echo "[$(date -u +%FT%TZ)] === Backup complete ==="
