#!/bin/bash
# Daily upload of Excel exports to Cloudflare R2. Keeps 90 days of files.
#
# One-time setup on the Ubuntu server:
#   1. apt install rclone
#   2. rclone config  →  add remote named "r2" (type: s3, provider: Cloudflare)
#      Access Key ID + Secret from Cloudflare dashboard → R2 → Manage R2 API Tokens
#   3. rclone config update r2 no_check_bucket true
#   4. chmod +x /root/backup/backup.sh
#   5. crontab -e  →  add:
#      0 7 * * * /root/backup/backup.sh >> /var/log/jamie_dad_backup.log 2>&1

set -euo pipefail

EXPORTS_DIR="${EXPORTS_DIR:-/root/backup/exports}"
BUCKET="${R2_BUCKET:-ev-scraper}"

echo "[$(date -u +%FT%TZ)] Uploading Excel exports from ${EXPORTS_DIR} to r2:${BUCKET}/..."

rclone copy "${EXPORTS_DIR}/" "r2:${BUCKET}/" --include "*.xlsx"

# Delete exports older than 91 days from R2
rclone delete "r2:${BUCKET}/" --min-age 91d --include "*.xlsx"

echo "[$(date -u +%FT%TZ)] Upload complete."
