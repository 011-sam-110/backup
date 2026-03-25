import asyncio
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import colorama
from colorama import Fore, Style
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from scraper import scrape
import db

load_dotenv()
INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", 15))
OUTPUT_DIR = Path("output")
DB_PATH = Path("chargers.db")

colorama.init()

W = 55
history: deque[str] = deque(maxlen=5)
start_time = datetime.now(timezone.utc)
next_run_at: datetime | None = None
run_count = 0
scraping = False          # suppresses auto-refresh while live scraper output is printing


# ── Formatting helpers ────────────────────────────────────────────────────────

def fmt_uptime(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


def fmt_bytes(n: int) -> str:
    if n >= 1_073_741_824:
        return f"{n / 1_073_741_824:.1f} GB"
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MB"
    if n >= 1_024:
        return f"{n / 1_024:.1f} KB"
    return f"{n} B"


def dir_size(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    files = list(path.glob("*.json"))
    return sum(f.stat().st_size for f in files), len(files)


def util_colour(pct: float) -> str:
    if pct <= 25:
        return Fore.GREEN
    if pct <= 40:
        return Fore.YELLOW
    return Fore.RED


def border(char="━") -> str:
    return Fore.CYAN + char * W + Style.RESET_ALL


def lbl(text: str, width: int = 14) -> str:
    return Style.DIM + text.ljust(width) + Style.RESET_ALL


def val(text: str) -> str:
    return Style.BRIGHT + str(text) + Style.RESET_ALL


def section(text: str) -> str:
    return Fore.YELLOW + Style.BRIGHT + text + Style.RESET_ALL


# ── Display ───────────────────────────────────────────────────────────────────

def print_scrape_header(n: int) -> None:
    now = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M:%S UTC")
    print(border())
    print(Fore.CYAN + f"  ↻  Scrape #{n}  —  {now}" + Style.RESET_ALL)
    print(border())


def build_status_card(n: int, duration: float) -> str:
    """Builds a static card string for a completed scrape run."""
    stats = db.get_stats()
    db_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    json_bytes, json_count = dir_size(OUTPUT_DIR)

    uptime_secs = (datetime.now(timezone.utc) - start_time).total_seconds()
    dur_str = fmt_uptime(duration)

    util = stats["avg_utilisation_pct"] or 0.0
    charging = stats["total_charging_evses"] or 0
    total_evses = stats["total_evses"] or 0
    hubs = stats["total_hubs"] or 0
    snapshots = stats["total_snapshots"] or 0
    uc = util_colour(util)

    lines = [
        border(),
        Fore.GREEN + Style.BRIGHT + f"  ✓  Scrape #{n} done  ·  {dur_str}" + Style.RESET_ALL,
        "",
        f"  {lbl('UPTIME')}{val(fmt_uptime(uptime_secs))}  "
            + Style.DIM + f"(since {start_time.strftime('%d %b %H:%M UTC')})" + Style.RESET_ALL,
        f"  {lbl('RUNS')}{val(str(n))}  "
            + Style.DIM + f"every {INTERVAL_MINUTES} min" + Style.RESET_ALL,
        "",
        f"  {section('NETWORK')}",
        f"    {lbl('Hubs')}{val(f'{hubs:,}')}",
        f"    {lbl('Snapshots')}{val(f'{snapshots:,}')}",
        f"    {lbl('Utilisation')}{uc}{Style.BRIGHT}{util:.1f}%{Style.RESET_ALL}  "
            + Style.DIM + f"{charging:,} charging  /  {total_evses:,} EVSEs" + Style.RESET_ALL,
        "",
        f"  {section('STORAGE')}",
        f"    {lbl('chargers.db')}{val(fmt_bytes(db_bytes))}",
        f"    {lbl(f'output/ ({json_count})')}{val(fmt_bytes(json_bytes))}",
        f"    {Style.DIM}{'─' * 32}{Style.RESET_ALL}",
        f"    {lbl('Total')}{val(fmt_bytes(db_bytes + json_bytes))}",
    ]
    return "\n".join(lines)


def render_screen() -> None:
    """Clears the terminal and redraws banner + history cards + live countdown footer."""
    os.system("cls" if os.name == "nt" else "clear")

    # ── banner (uptime ticks each second) ────────────────────────────────────
    uptime_secs = (datetime.now(timezone.utc) - start_time).total_seconds()
    started = start_time.strftime("%d %b %Y %H:%M UTC")
    print(border())
    print(Fore.CYAN + Style.BRIGHT + "EVANTI EV Charger Monitor" + Style.RESET_ALL)
    print(Style.DIM
          + f"  Interval: {INTERVAL_MINUTES} min  ·  Started: {started}"
          + f"  ·  Uptime: {fmt_uptime(uptime_secs)}"
          + Style.RESET_ALL)
    print(border())
    print()

    # ── history cards ─────────────────────────────────────────────────────────
    for card in history:
        print(card)
        print()

    # ── countdown footer (ticks each second) ─────────────────────────────────
    if next_run_at:
        secs_until = max(0, (next_run_at - datetime.now(timezone.utc)).total_seconds())
        countdown = f"in {fmt_uptime(secs_until)}  ({next_run_at.strftime('%H:%M:%S UTC')})"
    else:
        countdown = "starting soon…"

    print(border())
    print(f"  {section('NEXT SCRAPE')}  {Fore.CYAN}{countdown}{Style.RESET_ALL}")
    print(border())


def _refresh_loop() -> None:
    """Background thread: redraws the screen every second when not scraping."""
    while True:
        if not scraping:
            render_screen()
        time.sleep(1)


# ── Job ───────────────────────────────────────────────────────────────────────

def job():
    global run_count, next_run_at, scraping
    run_count += 1
    scraping = True
    print_scrape_header(run_count)
    t0 = time.monotonic()
    asyncio.run(scrape())
    duration = time.monotonic() - t0
    next_run_at = datetime.now(timezone.utc) + timedelta(minutes=INTERVAL_MINUTES)
    card = build_status_card(run_count, duration)
    history.append(card)
    scraping = False          # re-enables auto-refresh


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(job, "interval", minutes=INTERVAL_MINUTES)
    threading.Thread(target=_refresh_loop, daemon=True).start()
    job()
    scheduler.start()
