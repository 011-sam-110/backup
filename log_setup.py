"""
Shared logging configuration for the EVANTI scraper stack.

Call setup_logging() once at startup (in scheduler.py or scraper.py standalone).
All other modules just do: log = logging.getLogger("evanti.<module>")
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_FILE = LOG_DIR / "scheduler.log"


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s UTC | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # UTC timestamps
    fmt.converter = __import__("time").gmtime

    root = logging.getLogger("evanti")
    root.setLevel(logging.DEBUG)

    # ── stdout handler (shows up in docker compose logs) ──────────────────────
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    # ── rotating file handler (persists across restarts) ──────────────────────
    fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    root.addHandler(sh)
    root.addHandler(fh)
    return root
