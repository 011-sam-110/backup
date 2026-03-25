# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session start
Read `Projects/Jamie_Dad/Overview.md` and `Projects/Jamie_Dad/Active Work.md` from the Obsidian vault at `C:\Users\sampo\Documents\Obsidian Vault\` before doing anything.

## Commands
```bash
pip install -r requirements.txt
playwright install chromium

python scraper.py       # single scrape run → writes output/chargers_<timestamp>.json
python scheduler.py     # polling mode (default: every 5 min, runs immediately on start)
```

## Architecture
- `scraper.py` — async Playwright script. Launches headless Chromium, navigates to `zapmap.com/live/`, captures XHR/fetch responses via `page.on("response")`, filters to England and 100kW+, writes `output/chargers_<ISO-timestamp>.json`.
- `scheduler.py` — APScheduler `BlockingScheduler` that calls `scrape()` on a configurable interval (`INTERVAL_MINUTES`, default 5).
- `output/` — one JSON file per poll run, named by UTC timestamp.

## Filters
- **England bounding box:** lat 49.9–55.8, lng -5.7–1.8
- **Min power:** 100kW (`MIN_POWER_KW = 100`)

## First-run note
Field names in `filter_chargers()` are guesses. On first run, temporarily print all captured responses to find the real Zapmap API endpoint and field names, then update `filter_chargers()` and `Architecture.md` accordingly.

## Obsidian docs
`C:\Users\sampo\Documents\Obsidian Vault\Projects\Jamie_Dad\` — Overview, Active Work, Architecture, Backlog.
