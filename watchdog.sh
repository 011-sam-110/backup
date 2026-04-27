#!/bin/bash
# Watchdog — runs every minute via cron.
# If the scheduler or api container is not running, backs up the current DB,
# wipes it, and does a fresh docker compose up.
#
# Crontab entry:
#   * * * * * /root/jamie_dad/watchdog.sh >> /var/log/jamie_dad_watchdog.log 2>&1

set -uo pipefail

PROJECT_DIR="/root/jamie_dad"
DB_PATH="${PROJECT_DIR}/chargers.db"
BACKUP_DIR="${PROJECT_DIR}/watchdog_backups"
LOG_TAG="[$(date -u +%FT%TZ)] [watchdog]"

cd "$PROJECT_DIR"

# Check which services are healthy
RUNNING=$(docker compose ps --services --filter status=running 2>/dev/null || true)
SCHEDULER_UP=$(echo "$RUNNING" | grep -c "^scheduler$" || true)
API_UP=$(echo "$RUNNING"       | grep -c "^api$"       || true)

if [[ "$SCHEDULER_UP" -gt 0 && "$API_UP" -gt 0 ]]; then
    exit 0  # everything fine — silent exit, no log spam
fi

echo "$LOG_TAG ALERT: container(s) down (scheduler=$SCHEDULER_UP api=$API_UP) — initiating recovery"

# --- Backup current DB before wiping ---
mkdir -p "$BACKUP_DIR"
STAMP=$(date -u +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/chargers_crash_${STAMP}.db"

if [[ -f "$DB_PATH" && -s "$DB_PATH" ]]; then
    echo "$LOG_TAG Backing up DB → ${BACKUP_FILE}"
    if sqlite3 "$DB_PATH" ".backup ${BACKUP_FILE}"; then
        IC=$(sqlite3 "$BACKUP_FILE" "PRAGMA integrity_check;" 2>&1 | head -1)
        echo "$LOG_TAG Integrity check: ${IC}"
    else
        echo "$LOG_TAG WARNING: sqlite3 backup failed — copying raw file instead"
        cp "$DB_PATH" "$BACKUP_FILE" || true
    fi
else
    echo "$LOG_TAG DB missing or empty — skipping backup"
fi

# Keep only the last 10 watchdog backups locally
ls -t "${BACKUP_DIR}"/chargers_crash_*.db 2>/dev/null | tail -n +11 | xargs -r rm -f

# --- Wipe DB and fresh restart ---
echo "$LOG_TAG Wiping chargers.db for fresh start"
rm -f "$DB_PATH"
touch "$DB_PATH"

echo "$LOG_TAG Restarting containers..."
docker compose up -d --force-recreate 2>&1
echo "$LOG_TAG Recovery complete"
