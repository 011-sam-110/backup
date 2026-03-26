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
  GET  /api/export/snapshots            — raw snapshot dump for Excel export

All time-window endpoints also accept ?start_dt=ISO&end_dt=ISO to query a
specific date range (overrides the hours/days parameter when both are present).

Run with:
    uvicorn api:app --reload --port 8000
"""

import os
import secrets
import asyncio
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import Optional

import db

db.init_db()

SCRAPE_INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", 15))

app = FastAPI(title="EV Hub Utilisation API")

_cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")


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
            operator: Optional[str] = Query(default=None),
            connector: Optional[str] = Query(default=None),
            min_kw: Optional[float] = Query(default=None),
            max_kw: Optional[float] = Query(default=None),
            min_evses: Optional[int] = Query(default=None),
            max_evses: Optional[int] = Query(default=None),
            start_hour: Optional[int] = Query(default=None, ge=0, le=23),
            end_hour: Optional[int] = Query(default=None, ge=0, le=23)):
    return db.get_all_history(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt,
                              operator=operator, connector=connector, min_kw=min_kw, max_kw=max_kw,
                              min_evses=min_evses, max_evses=max_evses,
                              start_hour=start_hour, end_hour=end_hour)


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
                   operator: Optional[str] = Query(default=None),
                   connector: Optional[str] = Query(default=None),
                   min_kw: Optional[float] = Query(default=None),
                   max_kw: Optional[float] = Query(default=None),
                   min_evses: Optional[int] = Query(default=None),
                   max_evses: Optional[int] = Query(default=None),
                   start_hour: Optional[int] = Query(default=None, ge=0, le=23),
                   end_hour: Optional[int] = Query(default=None, ge=0, le=23)):
    return db.get_hourly_pattern(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt,
                                 operator=operator, connector=connector, min_kw=min_kw, max_kw=max_kw,
                                 min_evses=min_evses, max_evses=max_evses,
                                 start_hour=start_hour, end_hour=end_hour,
                                 interval_minutes=SCRAPE_INTERVAL_MINUTES)


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
                operator: Optional[str] = Query(default=None),
                connector: Optional[str] = Query(default=None),
                min_kw: Optional[float] = Query(default=None),
                max_kw: Optional[float] = Query(default=None),
                min_evses: Optional[int] = Query(default=None),
                max_evses: Optional[int] = Query(default=None),
                start_hour: Optional[int] = Query(default=None, ge=0, le=23),
                end_hour: Optional[int] = Query(default=None, ge=0, le=23)):
    return db.get_reliability_trend(hours, hub_uuid=hub_uuid, start_dt=start_dt, end_dt=end_dt,
                                    operator=operator, connector=connector, min_kw=min_kw, max_kw=max_kw,
                                    min_evses=min_evses, max_evses=max_evses,
                                    start_hour=start_hour, end_hour=end_hour)


@app.get("/api/visits")
def visits(start_dt: Optional[str] = Query(default=None),
           end_dt: Optional[str] = Query(default=None),
           operator: Optional[str] = Query(default=None),
           connector: Optional[str] = Query(default=None),
           min_kw: Optional[float] = Query(default=None),
           max_kw: Optional[float] = Query(default=None),
           min_evses: Optional[int] = Query(default=None),
           max_evses: Optional[int] = Query(default=None)):
    if not start_dt or not end_dt:
        today = datetime.now(timezone.utc).date()
        start_dt = datetime(today.year, today.month, today.day, tzinfo=timezone.utc).isoformat()
        end_dt = datetime.now(timezone.utc).isoformat()
    return db.get_visit_stats(start_dt, end_dt, operator, connector, min_kw, max_kw, min_evses, max_evses)


@app.get("/api/sparkline")
def sparkline(days: int = Query(default=7, ge=1, le=90)):
    return db.get_global_sparkline(days)


@app.get("/api/export/snapshots")
def export_snapshots(hours: int = Query(default=24, ge=1, le=8760),
                     start_dt: Optional[str] = Query(default=None),
                     end_dt: Optional[str] = Query(default=None)):
    return db.get_all_snapshots(hours, start_dt=start_dt, end_dt=end_dt)


# Serve built React frontend — must be last so /api/* routes take priority
_frontend_dist = Path(__file__).parent / "frontend" / "dist"
if _frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dist), html=True), name="static")
