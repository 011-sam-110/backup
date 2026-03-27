#!/usr/bin/env bash
# evdashboard server maintenance — run weekly via cron
#
# Cron setup (run once on the server):
#   chmod +x /root/backup/maintenance.sh
#   (crontab -l 2>/dev/null; echo "0 3 * * 0 /root/backup/maintenance.sh && sleep 10 && reboot") | crontab -
#
# Logs to: /var/log/evdashboard-maintenance.log

set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
MAINT_LOG="/var/log/evdashboard-maintenance.log"
exec >> "$MAINT_LOG" 2>&1

echo ""
echo "=== evdashboard maintenance: $(date -u '+%Y-%m-%d %H:%M:%S UTC') ==="

# ── 1. Vacuum SQLite DB ────────────────────────────────────────────────────────
echo "[1/4] Vacuuming database..."
cd "$APP_DIR"
# Load DATABASE_PATH from .env if present
if [ -f .env ]; then
    export $(grep -E '^DATABASE_PATH=' .env | xargs) 2>/dev/null || true
fi
python3 - <<'EOF'
import os, sqlite3
from pathlib import Path
db = Path(os.getenv("DATABASE_PATH", "chargers.db"))
if db.exists():
    con = sqlite3.connect(db)
    size_before = db.stat().st_size // 1024
    con.execute("VACUUM")
    con.close()
    size_after = db.stat().st_size // 1024
    print(f"  DB: {size_before} KB → {size_after} KB (saved {size_before - size_after} KB)")
else:
    print("  DB not found, skipping.")
EOF

# ── 2. Rotate scheduler log ────────────────────────────────────────────────────
echo "[2/4] Checking log size..."
SCHED_LOG="$APP_DIR/logs/scheduler.log"
if [ -f "$SCHED_LOG" ]; then
    SIZE=$(du -m "$SCHED_LOG" | cut -f1)
    if [ "$SIZE" -gt 50 ]; then
        mv "$SCHED_LOG" "${SCHED_LOG}.1"
        echo "  Rotated scheduler.log (was ${SIZE} MB)"
    else
        echo "  scheduler.log is ${SIZE} MB — no rotation needed"
    fi
else
    echo "  scheduler.log not found, skipping."
fi

# ── 3. Docker prune ────────────────────────────────────────────────────────────
echo "[3/4] Pruning Docker (images/containers older than 24h)..."
docker system prune --force --filter "until=24h" 2>&1 | tail -5

# ── 4. Container health check ──────────────────────────────────────────────────
echo "[4/4] Container health..."
ALL_OK=true
for name in backup-api-1 backup-scheduler-1; do
    STATUS=$(docker inspect --format='{{.State.Status}}' "$name" 2>/dev/null || echo "not found")
    echo "  $name: $STATUS"
    if [ "$STATUS" != "running" ]; then
        ALL_OK=false
        echo "  WARNING: $name is not running — attempting restart..."
        cd "$APP_DIR" && docker compose up -d 2>&1 | tail -3
    fi
done

if $ALL_OK; then
    echo "  All containers healthy."
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo "Disk: $(df -h / | awk 'NR==2 {print $3 " used of " $2 " (" $5 ")"}')"
echo "=== done ==="
