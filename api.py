"""
FastAPI backend for the EV Hub Utilisation dashboard.

Endpoints:
  GET  /api/stats                       — summary numbers
  GET  /api/stats/deltas                — week-on-week deltas
  GET  /api/hubs                        — all hubs with latest snapshot (or averaged over date range)
  GET  /api/hubs/{uuid}/history         — time-series for one hub
  GET  /api/history?hours=24            — averaged trend across all hubs
  GET  /api/history/daily?days=30       — daily aggregates for growth chart
  GET  /api/hourly-pattern?hours=168    — avg util by hour-of-day
  GET  /api/hourly-heatmap?hours=336    — avg util by (day_of_week, hour)
  GET  /api/reliability?hours=168       — network composition over time
  GET  /api/sparkline?days=7            — global daily sparkline data
  GET  /api/hub-performance?hours=168   — per-hub performance: active%, full-capacity%, visits/day
  GET  /api/connector-types             — distinct connector types with hub counts
  GET  /api/export/snapshots            — raw snapshot dump for Excel export

All time-window endpoints also accept ?start_dt=ISO&end_dt=ISO to query a
specific date range (overrides the hours/days parameter when both are present).

Run with:
    uvicorn api:app --reload --port 8000
"""

import logging
import os
import secrets
import asyncio
import sqlite3
import time
from pathlib import Path

import json
import traceback

from fastapi import FastAPI, HTTPException, Query, Request, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import Optional, List

import db
from log_setup import setup_logging

setup_logging(log_file="logs/api.log")
log = logging.getLogger("evanti.api")

log.info("API starting up — initialising DB...")
db.init_db()
db.purge_non_gb_hubs()
log.info("DB ready.")

SCRAPE_INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", 15))

app = FastAPI(title="EV Hub Utilisation API")

_cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["*"],
)

_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log.error(
        "Unhandled exception on %s %s\n%s",
        request.method, request.url.path,
        traceback.format_exc(),
    )
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Protect all /api/* routes except /api/auth with a bearer token."""
    if _PASSWORD and request.url.path.startswith("/api/") and request.url.path != "/api/auth":
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
        if not secrets.compare_digest(auth[7:], _PASSWORD):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.post("/api/auth")
async def login(body: dict = Body(...)):
    pw = body.get("password", "")
    if not _PASSWORD or not secrets.compare_digest(pw, _PASSWORD):
        await asyncio.sleep(1)
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"token": _PASSWORD}


@app.get("/api/stats")
def stats():
    return db.get_stats()


@app.get("/api/stats/deltas")
def stats_deltas():
    return db.get_stat_deltas()


@app.get("/api/hubs")
def hubs(start_dt: Optional[str] = Query(default=None),
         end_dt: Optional[str] = Query(default=None),
         start_hour: Optional[int] = Query(default=None, ge=0, le=23),
         end_hour: Optional[int] = Query(default=None, ge=0, le=23)):
    if start_dt and end_dt:
        return db.get_hub_averages(start_dt, end_dt, start_hour=start_hour, end_hour=end_hour)
    return db.get_latest_snapshot_per_hub()


@app.get("/api/hubs/{uuid}")
def hub_detail(uuid: str):
    result = db.get_hub_detail(uuid)
    if result is None:
        raise HTTPException(status_code=404, detail="Hub not found")
    return result


@app.get("/api/hubs/{uuid}/session-start")
def hub_session_start(uuid: str):
    return {"session_start": db.get_charging_session_start(uuid)}


@app.get("/api/hubs/{uuid}/groups")
def hub_groups(uuid: str):
    return db.get_hub_group_ids(uuid)


@app.get("/api/hubs/{uuid}/history")
def hub_history(uuid: str,
                hours: int = Query(default=24, ge=1, le=8760),
                start_dt: Optional[str] = Query(default=None),
                end_dt: Optional[str] = Query(default=None)):
    return db.get_hub_history(uuid, hours, start_dt=start_dt, end_dt=end_dt)


@app.get("/api/history")
def history(hours: int = Query(default=24, ge=1, le=8760),
            hub_uuid: Optional[str] = Query(default=None),
            start_dt: Optional[str] = Query(default=None),
            end_dt: Optional[str] = Query(default=None),
            operator: Optional[List[str]] = Query(default=None),
            connector: Optional[str] = Query(default=None),
            min_kw: Optional[float] = Query(default=None),
            max_kw: Optional[float] = Query(default=None),
            min_evses: Optional[int] = Query(default=None),
            max_evses: Optional[int] = Query(default=None),
            start_hour: Optional[int] = Query(default=None, ge=0, le=23),
            end_hour: Optional[int] = Query(default=None, ge=0, le=23),
            group_id: Optional[List[int]] = Query(default=None)):
    return db.get_all_history(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt,
                              operator=operator, connector=connector, min_kw=min_kw, max_kw=max_kw,
                              min_evses=min_evses, max_evses=max_evses,
                              start_hour=start_hour, end_hour=end_hour,
                              group_ids=group_id or None)


@app.get("/api/history/daily")
def history_daily(days: int = Query(default=30, ge=1, le=365),
                  hub_uuid: Optional[str] = Query(default=None),
                  start_dt: Optional[str] = Query(default=None),
                  end_dt: Optional[str] = Query(default=None)):
    return db.get_all_history_daily(days, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt)


@app.get("/api/hourly-pattern")
def hourly_pattern(hours: int = Query(default=168, ge=24, le=8760),
                   hub_uuid: Optional[str] = Query(default=None),
                   start_dt: Optional[str] = Query(default=None),
                   end_dt: Optional[str] = Query(default=None),
                   operator: Optional[List[str]] = Query(default=None),
                   connector: Optional[str] = Query(default=None),
                   min_kw: Optional[float] = Query(default=None),
                   max_kw: Optional[float] = Query(default=None),
                   min_evses: Optional[int] = Query(default=None),
                   max_evses: Optional[int] = Query(default=None),
                   start_hour: Optional[int] = Query(default=None, ge=0, le=23),
                   end_hour: Optional[int] = Query(default=None, ge=0, le=23),
                   group_id: Optional[List[int]] = Query(default=None)):
    return db.get_hourly_pattern(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt,
                                 operator=operator, connector=connector, min_kw=min_kw, max_kw=max_kw,
                                 min_evses=min_evses, max_evses=max_evses,
                                 start_hour=start_hour, end_hour=end_hour,
                                 interval_minutes=SCRAPE_INTERVAL_MINUTES,
                                 group_ids=group_id or None)


@app.get("/api/hourly-heatmap")
def hourly_heatmap(hours: int = Query(default=336, ge=24, le=8760),
                   hub_uuid: Optional[str] = Query(default=None),
                   start_dt: Optional[str] = Query(default=None),
                   end_dt: Optional[str] = Query(default=None)):
    return db.get_hourly_heatmap(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt)


@app.get("/api/reliability")
def reliability(hours: int = Query(default=168, ge=1, le=8760),
                hub_uuid: Optional[str] = Query(default=None),
                start_dt: Optional[str] = Query(default=None),
                end_dt: Optional[str] = Query(default=None),
                operator: Optional[List[str]] = Query(default=None),
                connector: Optional[str] = Query(default=None),
                min_kw: Optional[float] = Query(default=None),
                max_kw: Optional[float] = Query(default=None),
                min_evses: Optional[int] = Query(default=None),
                max_evses: Optional[int] = Query(default=None),
                start_hour: Optional[int] = Query(default=None, ge=0, le=23),
                end_hour: Optional[int] = Query(default=None, ge=0, le=23),
                group_id: Optional[List[int]] = Query(default=None)):
    return db.get_reliability_trend(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt,
                                    operator=operator, connector=connector, min_kw=min_kw, max_kw=max_kw,
                                    min_evses=min_evses, max_evses=max_evses,
                                    start_hour=start_hour, end_hour=end_hour,
                                    group_ids=group_id or None)


@app.get("/api/visits")
def visits(start_dt: Optional[str] = Query(default=None),
           end_dt: Optional[str] = Query(default=None),
           operator: Optional[List[str]] = Query(default=None),
           connector: Optional[str] = Query(default=None),
           min_kw: Optional[float] = Query(default=None),
           max_kw: Optional[float] = Query(default=None),
           min_evses: Optional[int] = Query(default=None),
           max_evses: Optional[int] = Query(default=None),
           start_hour: Optional[int] = Query(default=None, ge=0, le=23),
           end_hour: Optional[int] = Query(default=None, ge=0, le=23),
           group_id: Optional[List[int]] = Query(default=None)):
    return db.get_visit_stats(start_dt or None, end_dt or None, operator, connector, min_kw, max_kw, min_evses, max_evses,
                              start_hour=start_hour, end_hour=end_hour,
                              group_ids=group_id or None)


@app.get("/api/hub-performance")
def hub_performance(hours: int = Query(default=168, ge=24, le=8760),
                    start_dt: Optional[str] = Query(default=None),
                    end_dt: Optional[str] = Query(default=None),
                    operator: Optional[List[str]] = Query(default=None),
                    connector: Optional[str] = Query(default=None),
                    min_kw: Optional[float] = Query(default=None),
                    max_kw: Optional[float] = Query(default=None),
                    min_evses: Optional[int] = Query(default=None),
                    max_evses: Optional[int] = Query(default=None),
                    group_id: Optional[List[int]] = Query(default=None)):
    return db.get_hub_performance(hours, start_dt=start_dt, end_dt=end_dt,
                                   operator=operator, connector=connector,
                                   min_kw=min_kw, max_kw=max_kw,
                                   min_evses=min_evses, max_evses=max_evses,
                                   group_ids=group_id or None)


@app.get("/api/connector-types")
def connector_types():
    return db.get_connector_types()


@app.get("/api/groups")
def list_groups():
    return db.get_groups()


@app.post("/api/groups", status_code=201)
def create_group(body: dict = Body(...)):
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    try:
        return db.create_group(name)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"Group '{name}' already exists")


@app.patch("/api/groups/{group_id}")
def update_group(group_id: int, body: dict = Body(...)):
    name = (body.get("name") or "").strip()
    hf   = body.get("high_frequency")

    if not name and hf is None:
        raise HTTPException(status_code=400, detail="name or high_frequency is required")

    updated = None
    if name:
        updated = db.rename_group(group_id, name)
        if not updated:
            raise HTTPException(status_code=404, detail="Group not found")
    if hf is not None:
        updated = db.set_group_high_frequency(group_id, bool(hf))
        if not updated:
            raise HTTPException(status_code=404, detail="Group not found")

    return updated or db.get_group_by_id(group_id)


@app.delete("/api/groups/{group_id}", status_code=204)
def delete_group(group_id: int):
    db.delete_group(group_id)


@app.get("/api/groups/{group_id}/hubs")
def get_group_hubs(group_id: int):
    return db.get_group_hub_uuids(group_id)


@app.post("/api/groups/{group_id}/hubs", status_code=204)
def add_hubs_to_group(group_id: int, body: dict = Body(...)):
    hub_uuids = body.get("hub_uuids") or []
    if not isinstance(hub_uuids, list):
        raise HTTPException(status_code=400, detail="hub_uuids must be a list")
    db.add_hubs_to_group(group_id, hub_uuids)


@app.delete("/api/groups/{group_id}/hubs/{hub_uuid}", status_code=204)
def remove_hub_from_group(group_id: int, hub_uuid: str):
    db.remove_hub_from_group(group_id, hub_uuid)


@app.get("/api/sparkline")
def sparkline(days: int = Query(default=7, ge=1, le=90)):
    return db.get_global_sparkline(days)


@app.get("/api/export/snapshots")
def export_snapshots(hours: int = Query(default=24, ge=1, le=8760),
                     start_dt: Optional[str] = Query(default=None),
                     end_dt: Optional[str] = Query(default=None)):
    return db.get_all_snapshots(hours, start_dt=start_dt, end_dt=end_dt)


BEARER_MAX_AGE_S = 55 * 60  # matches scraper.py
BEARER_CACHE_FILE = Path("bearer_token.cache")


async def _run_discover():
    """Run discover.py as a subprocess — fires after the endpoint returns."""
    try:
        # Wait for a fresh bearer token (written by scheduler after each scrape).
        # Poll every 30s for up to 20 minutes before giving up.
        for attempt in range(40):
            if BEARER_CACHE_FILE.exists():
                try:
                    age = time.time() - json.loads(BEARER_CACHE_FILE.read_text()).get("ts", 0)
                    if age < BEARER_MAX_AGE_S:
                        log.info("Bearer token cache ready (age %.0fs) — launching discover", age)
                        break
                except Exception:
                    pass
            log.info("_run_discover: waiting for fresh bearer token (attempt %d/40)...", attempt + 1)
            await asyncio.sleep(30)
        else:
            log.error("_run_discover: no fresh bearer token after 20 min — aborting")
            return

        log.info("discover subprocess starting...")
        t0 = time.monotonic()
        proc = await asyncio.create_subprocess_exec(
            "python", "discover.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        elapsed = time.monotonic() - t0
        for line in stdout.decode(errors="replace").splitlines():
            if line.strip():
                log.info("[discover] %s", line)
        for line in stderr.decode(errors="replace").splitlines():
            if line.strip():
                log.warning("[discover stderr] %s", line)
        if proc.returncode != 0:
            log.error("discover subprocess exited with code %d after %.0fs", proc.returncode, elapsed)
        else:
            log.info("discover subprocess finished OK in %.0fs", elapsed)
    except Exception:
        log.error("_run_discover crashed:\n%s", traceback.format_exc())


@app.post("/api/admin/discover")
async def admin_discover(body: dict = Body(...), background_tasks: BackgroundTasks = None):
    """
    Receive a list of hub UUIDs, deduplicate against the DB, write pending_uuids.json,
    and kick off discover.py in the background. Called by parse_har.py --push.
    """
    uuids = body.get("uuids", [])
    if not uuids:
        raise HTTPException(status_code=400, detail="No UUIDs provided")
    known = {h["uuid"] for h in db.get_all_hubs_for_scrape()}
    new_uuids = [u for u in uuids if u not in known]
    if not new_uuids:
        return {"queued": 0, "already_known": len(uuids), "message": "All UUIDs already in DB"}
    Path("pending_uuids.json").write_text(json.dumps(new_uuids))
    background_tasks.add_task(_run_discover)
    return {"queued": len(new_uuids), "already_known": len(uuids) - len(new_uuids)}


@app.post("/api/admin/rediscover")
async def admin_rediscover(body: dict = Body(...), background_tasks: BackgroundTasks = None):
    """
    Force re-discover ALL provided UUIDs, including those already tracked in the DB.
    One-time remediation: re-upserts hub records with corrected CHAdeMO-per-connector
    filtering so EVSE counts and connector_types reflect the current filter logic.
    """
    uuids = body.get("uuids", [])
    if not uuids:
        raise HTTPException(status_code=400, detail="No UUIDs provided")
    deduped = sorted(set(uuids))
    Path("pending_uuids.json").write_text(json.dumps(deduped))
    background_tasks.add_task(_run_discover)
    return {"queued": len(deduped), "message": "Re-discovery queued for all UUIDs (including already-known)"}


# Serve built React frontend — must be last so /api/* routes take priority
_frontend_dist = Path(__file__).parent / "frontend" / "dist"
if _frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dist), html=True), name="static")
