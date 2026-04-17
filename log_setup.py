"""
Shared logging configuration for the EVANTI scraper stack.

Call setup_logging() once at startup (in scheduler.py or scraper.py standalone).
All other modules just do: log = logging.getLogger("evanti.<module>")
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))


def setup_logging(log_file: str = "logs/scheduler.log") -> logging.Logger:
    log_path = Path(log_file)
    log_path.parent.mkdir(exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s UTC | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # UTC timestamps
    fmt.converter = __import__("time").gmtime

    # ── stdout handler (shows up in docker compose logs) ──────────────────────
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    # ── rotating file handler (persists across restarts) ──────────────────────
    fh = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # ── evanti logger (our app code) ───────────────────────────────────────────
    evanti = logging.getLogger("evanti")
    evanti.setLevel(logging.DEBUG)
    evanti.propagate = False  # don't double-emit via root
    evanti.addHandler(sh)
    evanti.addHandler(fh)

    # ── root logger — catches uvicorn.*, fastapi.* and anything else ───────────
    # Uvicorn loggers propagate to root by default, so adding our handler here
    # gives timestamps to all uvicorn startup/access/error lines in docker logs.
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(isinstance(h, logging.StreamHandler) and h.stream is sys.stdout
               for h in root.handlers):
        root.addHandler(sh)

    return evanti
