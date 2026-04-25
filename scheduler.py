import asyncio
import logging
import os
import sys
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import colorama
from colorama import Fore, Style
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from log_setup import setup_logging
from scraper import scrape, scrape_targeted
import db
from export import export_reports, export_interval_comparison

load_dotenv()
setup_logging()
log = logging.getLogger("evanti.scheduler")
INTERVAL_MINUTES    = int(os.getenv("SCRAPE_INTERVAL_MINUTES", 15))
MAX_RETRIES         = 3    # attempts per scrape run
RETRY_DELAY_S       = 30   # seconds between retry attempts
SCRAPE_TIMEOUT_S    = int(os.getenv("SCRAPE_TIMEOUT_S", 300))  # hard timeout per scrape attempt (kills hung Playwright)
DB_PATH = Path("chargers.db")

colorama.init()

W = 55
history: deque[str] = deque(maxlen=5)
start_time = datetime.now(timezone.utc)
next_run_at: datetime | None = None
run_count = 0
scraping = False          # suppresses auto-refresh while live scraper output is printing

_IS_TTY            = sys.stdout.isatty()  # True in interactive terminal, False in Docker/pipe
_scrape_lock       = threading.Lock()     # prevents job() and fast_job() writing DB concurrently
_last_render_lines: int   = 0             # lines printed in last render_screen() call
_cached_stats: dict | None = None         # last db.get_stats() result
_stats_cached_at: float    = 0.0          # time.monotonic() when cache was filled
STATS_CACHE_TTL = 30.0                    # seconds between DB queries in live display


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


def _get_cached_stats() -> dict:
    """Returns db stats, refreshing from DB at most every STATS_CACHE_TTL seconds."""
    global _cached_stats, _stats_cached_at
    if _cached_stats is None or (time.monotonic() - _stats_cached_at) > STATS_CACHE_TTL:
        try:
            _cached_stats = db.get_stats()
        except Exception:
            _cached_stats = _cached_stats or {}
        _stats_cached_at = time.monotonic()
    return _cached_stats


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
    ]
    return "\n".join(lines)


def render_screen() -> None:
    """Redraws the live display. Uses ANSI cursor-up in TTY mode (flicker-free, preserves
    scroll buffer). Falls back to os.system clear in non-TTY (Docker/pipe)."""
    global _last_render_lines

    uptime_secs = (datetime.now(timezone.utc) - start_time).total_seconds()
    started = start_time.strftime("%d %b %Y %H:%M UTC")

    # ── live stats (cached) ───────────────────────────────────────────────────
    s = _get_cached_stats()
    util      = s.get("avg_utilisation_pct") or 0.0
    charging  = s.get("total_charging_evses") or 0
    tot_evses = s.get("total_evses") or 0
    hubs      = s.get("total_hubs") or 0
    snapshots = s.get("total_snapshots") or 0
    db_bytes  = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    uc = util_colour(util)

    # ── assemble output ───────────────────────────────────────────────────────
    parts = [
        border(),
        Fore.CYAN + Style.BRIGHT + "EV Charger Monitor" + Style.RESET_ALL,
        (Style.DIM
         + f"  Interval: {INTERVAL_MINUTES} min  ·  Started: {started}"
         + f"  ·  Uptime: {fmt_uptime(uptime_secs)}"
         + Style.RESET_ALL),
        border(),
        "",
        f"  {section('LIVE NETWORK')}",
        (f"    {lbl('Hubs')}{val(f'{hubs:,}')}  "
         + Style.DIM + f"·  {snapshots:,} snapshots  ·  {fmt_bytes(db_bytes)}" + Style.RESET_ALL),
        (f"    {lbl('Utilisation')}{uc}{Style.BRIGHT}{util:.1f}%{Style.RESET_ALL}  "
         + Style.DIM + f"{charging:,} charging  /  {tot_evses:,} EVSEs" + Style.RESET_ALL),
        "",
    ]

    for card in history:
        parts.append(card)
        parts.append("")

    # ── countdown footer ──────────────────────────────────────────────────────
    if next_run_at:
        secs_until = max(0, (next_run_at - datetime.now(timezone.utc)).total_seconds())
        countdown = f"in {fmt_uptime(secs_until)}  ({next_run_at.strftime('%H:%M:%S UTC')})"
    else:
        countdown = "starting soon…"

    parts += [
        border(),
        f"  {section('NEXT SCRAPE')}  {Fore.CYAN}{countdown}{Style.RESET_ALL}",
        border(),
    ]

    output = "\n".join(parts)
    line_count = output.count("\n") + 1

    if _IS_TTY:
        if _last_render_lines > 0:
            sys.stdout.write(f"\033[{_last_render_lines}A\033[J")
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
        _last_render_lines = line_count
    else:
        os.system("cls" if os.name == "nt" else "clear")
        print(output)


def _refresh_loop() -> None:
    """Background thread: redraws the screen every second when not scraping."""
    while True:
        if not scraping:
            render_screen()
        time.sleep(1)


# ── Retry helpers ─────────────────────────────────────────────────────────────

def _print_retry_warning(n: int, attempt: int, max_retries: int, delay: int, exc: Exception) -> None:
    print(
        Fore.YELLOW + Style.BRIGHT
        + f"\n  ⚠  Scrape #{n} — attempt {attempt}/{max_retries} failed  ·  retrying in {delay}s"
        + Style.RESET_ALL
    )
    print(Style.DIM + f"     {type(exc).__name__}: {exc}" + Style.RESET_ALL + "\n")


def _print_failure_box(n: int, max_retries: int, duration: float, exc: Exception) -> None:
    red_border = Fore.RED + Style.BRIGHT + "━" * W + Style.RESET_ALL
    dur_str = fmt_uptime(duration)
    print(f"\n{red_border}")
    print(Fore.RED + Style.BRIGHT + f"  ✗  SCRAPE #{n} FAILED — all {max_retries} attempts exhausted  ({dur_str} total)" + Style.RESET_ALL)
    print(red_border)
    print(Style.DIM + f"     Last error : {type(exc).__name__}: {exc}" + Style.RESET_ALL)
    print(Style.DIM + f"     Next scrape: in ~{INTERVAL_MINUTES} min" + Style.RESET_ALL)
    print(f"{red_border}\n")


def _build_failure_card(n: int, max_retries: int, duration: float, exc: Exception) -> str:
    red_border = Fore.RED + "━" * W + Style.RESET_ALL
    dur_str = fmt_uptime(duration)
    err_msg = f"{type(exc).__name__}: {exc}"
    if len(err_msg) > W - 5:
        err_msg = err_msg[:W - 8] + "…"
    lines = [
        red_border,
        Fore.RED + Style.BRIGHT + f"  ✗  Scrape #{n}  ·  FAILED  ·  all {max_retries} attempts  ·  {dur_str}" + Style.RESET_ALL,
        Style.DIM + f"     {err_msg}" + Style.RESET_ALL,
        red_border,
    ]
    return "\n".join(lines)


# ── Job ───────────────────────────────────────────────────────────────────────

def job():
    global run_count, next_run_at, scraping, _last_render_lines, _stats_cached_at
    _scrape_lock.acquire()
    try:
        run_count += 1
        scraping = True
        _last_render_lines = 0   # don't cursor-up into scraper output on next render
        print_scrape_header(run_count)
        log.info("Scrape #%d started", run_count)
        t0 = time.monotonic()

        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                asyncio.run(asyncio.wait_for(scrape(), timeout=SCRAPE_TIMEOUT_S))
                last_exc = None
                break                           # success — exit retry loop
            except Exception as exc:
                last_exc = exc
                elapsed = time.monotonic() - t0
                if attempt < MAX_RETRIES:
                    log.warning(
                        "Scrape #%d attempt %d/%d failed after %.0fs — retrying in %ds | %s",
                        run_count, attempt, MAX_RETRIES, elapsed, RETRY_DELAY_S, exc,
                    )
                    _print_retry_warning(run_count, attempt, MAX_RETRIES, RETRY_DELAY_S, exc)
                    time.sleep(RETRY_DELAY_S)
                else:
                    log.error(
                        "Scrape #%d FAILED all %d attempts after %.0fs — %s\n%s",
                        run_count, MAX_RETRIES, elapsed, exc, traceback.format_exc(),
                    )

        duration = time.monotonic() - t0
        next_run_at = datetime.now(timezone.utc) + timedelta(minutes=INTERVAL_MINUTES)

        if last_exc is None:
            log.info("Scrape #%d done in %.0fs", run_count, duration)
            try:
                card = build_status_card(run_count, duration)
                history.append(card)
            except Exception as exc:
                log.error("build_status_card failed: %s", exc)
        else:
            _print_failure_box(run_count, MAX_RETRIES, duration, last_exc)
            try:
                card = _build_failure_card(run_count, MAX_RETRIES, duration, last_exc)
                history.append(card)
            except Exception as exc:
                log.error("_build_failure_card failed: %s", exc)

        _stats_cached_at = 0.0   # force live stats to refresh immediately after scrape
        scraping = False          # re-enables auto-refresh
    finally:
        _scrape_lock.release()


def targeted_job(minutes: int):
    """Targeted scrape for all groups configured at the given interval (1–5 min)."""
    if db.get_setting("targeted_scraping_enabled", "1") != "1":
        log.debug("Targeted scrape (%d min) skipped — targeted scraping disabled", minutes)
        return
    if not _scrape_lock.acquire(blocking=False):
        log.debug("Targeted scrape (%d min) skipped — full scrape in progress", minutes)
        return
    try:
        uuids = db.get_hubs_for_scrape_interval(minutes)
        if not uuids:
            return
        log.info("Targeted scrape (%d min): %d hubs", minutes, len(uuids))
        try:
            count = asyncio.run(asyncio.wait_for(scrape_targeted(uuids), timeout=SCRAPE_TIMEOUT_S))
            log.info("Targeted scrape (%d min): %d snapshots saved", minutes, count)
        except Exception as exc:
            log.error("Targeted scrape (%d min) failed: %s", minutes, exc)
    finally:
        _scrape_lock.release()


def export_job():
    """Daily Excel report export — all sites + per-group + interval comparison."""
    try:
        paths = export_reports()
        paths += export_interval_comparison()
        log.info("Daily export complete: %d file(s)", len(paths))
        for p in paths:
            log.info("  %s", p)
    except Exception as exc:
        log.error("Daily export failed: %s", exc)


if __name__ == "__main__":
    log.info("Scheduler starting — interval %d min, DB: %s", INTERVAL_MINUTES, db.DB_PATH)
    scheduler = BlockingScheduler()
    scheduler.add_job(job, "interval", minutes=INTERVAL_MINUTES, max_instances=1)
    for _m in range(1, 6):
        scheduler.add_job(targeted_job, "interval", minutes=_m, max_instances=1,
                          kwargs={"minutes": _m}, id=f"targeted_{_m}min")
    scheduler.add_job(export_job, "cron", hour=6, minute=0, id="daily_export", max_instances=1)
    threading.Thread(target=_refresh_loop, daemon=True).start()
    job()
    scheduler.start()
