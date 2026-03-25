"""
SQLite persistence layer for EV hub utilisation data.

Tables:
  hubs       — static hub info, upserted on each scrape
  snapshots  — one row per hub per scrape run (counts + utilisation_pct)
"""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path("chargers.db")


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _parse_dt(iso: str) -> str:
    """Normalise an ISO datetime string (handles Z suffix) to stored format."""
    return datetime.fromisoformat(iso.replace('Z', '+00:00')).isoformat()


def _hour_filter(params: list, start_hour: int | None, end_hour: int | None,
                 col: str = "scraped_at") -> str:
    """Returns SQL fragment restricting to hours-of-day range (0–23)."""
    if start_hour is None or end_hour is None:
        return ""
    params.extend([start_hour, end_hour])
    return f" AND CAST(strftime('%H', {col}) AS INTEGER) BETWEEN ? AND ?"


def _hub_subquery(params: list, operator: str | None, connector: str | None,
                  min_kw: float | None, max_kw: float | None) -> str:
    """Returns SQL fragment restricting snapshots to hub UUIDs matching the given attributes."""
    conditions = []
    if operator:
        conditions.append("LOWER(operator) = LOWER(?)")
        params.append(operator)
    if connector:
        conditions.append("connector_types LIKE ?")
        params.append(f'%{connector}%')
    if min_kw is not None:
        conditions.append("max_power_kw >= ?")
        params.append(min_kw)
    if max_kw is not None:
        conditions.append("max_power_kw <= ?")
        params.append(max_kw)
    if not conditions:
        return ""
    return " AND hub_uuid IN (SELECT uuid FROM hubs WHERE " + " AND ".join(conditions) + ")"


def init_db() -> None:
    con = _connect()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS hubs (
            uuid            TEXT PRIMARY KEY,
            latitude        REAL,
            longitude       REAL,
            max_power_kw    REAL,
            total_evses     INTEGER,
            connector_types TEXT,
            location_raw    TEXT,
            first_seen_at   TEXT,
            last_seen_at    TEXT,
            hub_name        TEXT DEFAULT NULL,
            operator        TEXT DEFAULT NULL,
            address           TEXT DEFAULT NULL,
            city              TEXT DEFAULT NULL,
            postal_code       TEXT DEFAULT NULL,
            user_rating       REAL DEFAULT NULL,
            user_rating_count INTEGER DEFAULT NULL,
            is_24_7           INTEGER DEFAULT NULL,
            pricing           TEXT DEFAULT NULL,
            payment_methods   TEXT DEFAULT NULL,
            devices_raw_loc   TEXT DEFAULT NULL,
            latest_devices_status TEXT DEFAULT NULL
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            hub_uuid            TEXT NOT NULL REFERENCES hubs(uuid),
            scraped_at          TEXT NOT NULL,
            available_count     INTEGER,
            charging_count      INTEGER,
            inoperative_count   INTEGER,
            out_of_order_count  INTEGER,
            unknown_count       INTEGER,
            utilisation_pct     REAL
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_hub_time
            ON snapshots(hub_uuid, scraped_at);

        CREATE INDEX IF NOT EXISTS idx_snapshots_time
            ON snapshots(scraped_at);
    """)
    # Migration guard for existing databases
    try:
        con.execute("ALTER TABLE hubs ADD COLUMN hub_name TEXT DEFAULT NULL")
        con.commit()
    except Exception:
        pass  # column already exists
    try:
        con.execute("ALTER TABLE hubs ADD COLUMN operator TEXT DEFAULT NULL")
        con.commit()
    except Exception:
        pass  # column already exists
    for col, typedef in [
        ("address",           "TEXT DEFAULT NULL"),
        ("city",              "TEXT DEFAULT NULL"),
        ("postal_code",       "TEXT DEFAULT NULL"),
        ("user_rating",       "REAL DEFAULT NULL"),
        ("user_rating_count", "INTEGER DEFAULT NULL"),
        ("is_24_7",           "INTEGER DEFAULT NULL"),
        ("pricing",           "TEXT DEFAULT NULL"),
        ("payment_methods",   "TEXT DEFAULT NULL"),
        ("devices_raw_loc",   "TEXT DEFAULT NULL"),
    ]:
        try:
            con.execute(f"ALTER TABLE hubs ADD COLUMN {col} {typedef}")
            con.commit()
        except Exception:
            pass
    try:
        con.execute("ALTER TABLE hubs ADD COLUMN latest_devices_status TEXT DEFAULT NULL")
        con.commit()
    except Exception:
        pass
    con.commit()
    con.close()


def upsert_hubs(records: list[dict]) -> None:
    con = _connect()
    now = datetime.now(timezone.utc).isoformat()
    for r in records:
        con.execute("""
            INSERT INTO hubs (uuid, latitude, longitude, max_power_kw, total_evses,
                              connector_types, location_raw, first_seen_at, last_seen_at,
                              hub_name, operator,
                              address, city, postal_code,
                              user_rating, user_rating_count, is_24_7,
                              pricing, payment_methods, devices_raw_loc,
                              latest_devices_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uuid) DO UPDATE SET
                latitude        = excluded.latitude,
                longitude       = excluded.longitude,
                max_power_kw    = excluded.max_power_kw,
                total_evses     = excluded.total_evses,
                connector_types = excluded.connector_types,
                location_raw    = excluded.location_raw,
                last_seen_at    = excluded.last_seen_at,
                hub_name        = COALESCE(excluded.hub_name, hub_name),
                operator        = COALESCE(excluded.operator, operator),
                address         = COALESCE(excluded.address, address),
                city            = COALESCE(excluded.city, city),
                postal_code     = COALESCE(excluded.postal_code, postal_code),
                user_rating     = COALESCE(excluded.user_rating, user_rating),
                user_rating_count = COALESCE(excluded.user_rating_count, user_rating_count),
                is_24_7         = COALESCE(excluded.is_24_7, is_24_7),
                pricing         = COALESCE(excluded.pricing, pricing),
                payment_methods = COALESCE(excluded.payment_methods, payment_methods),
                devices_raw_loc       = COALESCE(excluded.devices_raw_loc, devices_raw_loc),
                latest_devices_status = excluded.latest_devices_status
        """, (
            r["uuid"],
            r["latitude"],
            r["longitude"],
            r["max_power_kw"],
            r["total_evses"],
            json.dumps(r.get("connector_types", [])),
            json.dumps(r.get("location_raw", {})),
            now,
            now,
            r.get("hub_name"),
            r.get("operator"),
            r.get("address"),
            r.get("city"),
            r.get("postal_code"),
            r.get("user_rating"),
            r.get("user_rating_count"),
            r.get("is_24_7"),
            json.dumps(r.get("pricing", [])),
            json.dumps(r.get("payment_methods", [])),
            json.dumps(r.get("devices_raw_loc", [])),
            json.dumps(r.get("devices", [])),
        ))
    con.commit()
    con.close()


def insert_snapshots(records: list[dict]) -> None:
    con = _connect()
    rows = []
    for r in records:
        charging    = r.get("charging_count")     or 0
        available   = r.get("available_count")    or 0
        inoperative = r.get("inoperative_count")  or 0
        oos         = r.get("out_of_order_count") or 0
        unknown     = r.get("unknown_count")      or 0
        total_status = available + charging + inoperative + oos + unknown
        util_pct = round(charging / total_status * 100, 2) if total_status > 0 else 0.0
        rows.append((
            r["uuid"],
            r["scraped_at"],
            r.get("available_count", 0),
            charging,
            r.get("inoperative_count", 0),
            r.get("out_of_order_count", 0),
            r.get("unknown_count", 0),
            util_pct,
        ))
    con.executemany("""
        INSERT INTO snapshots
            (hub_uuid, scraped_at, available_count, charging_count,
             inoperative_count, out_of_order_count, unknown_count, utilisation_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    con.commit()
    con.close()


def _deserialise_hub(d: dict) -> dict:
    d["connector_types"] = json.loads(d["connector_types"] or "[]")
    d["location_raw"] = json.loads(d["location_raw"] or "{}")
    d["pricing"] = json.loads(d["pricing"] or "[]")
    d["payment_methods"] = json.loads(d["payment_methods"] or "[]")
    d["devices_raw_loc"] = json.loads(d["devices_raw_loc"] or "[]")
    d["latest_devices_status"] = json.loads(d["latest_devices_status"] or "[]")
    return d


def get_latest_snapshot_per_hub() -> list[dict]:
    """Return each hub's most recent snapshot merged with hub static data."""
    con = _connect()
    rows = con.execute("""
        SELECT
            h.uuid, h.hub_name, h.operator, h.latitude, h.longitude, h.max_power_kw,
            h.total_evses, h.connector_types, h.location_raw,
            h.address, h.city, h.postal_code, h.user_rating, h.user_rating_count,
            h.is_24_7, h.pricing, h.payment_methods, h.devices_raw_loc, h.latest_devices_status,
            s.scraped_at, s.available_count, s.charging_count,
            s.inoperative_count, s.out_of_order_count, s.unknown_count,
            s.utilisation_pct
        FROM hubs h
        LEFT JOIN snapshots s ON s.hub_uuid = h.uuid
            AND s.scraped_at = (
                SELECT MAX(scraped_at) FROM snapshots WHERE hub_uuid = h.uuid
            )
        ORDER BY s.utilisation_pct DESC NULLS LAST
    """).fetchall()
    con.close()
    return [_deserialise_hub(dict(row)) for row in rows]


def get_hub_averages(start_dt: str, end_dt: str,
                     start_hour: int | None = None, end_hour: int | None = None) -> list[dict]:
    """Return per-hub average utilisation over a date range (same shape as get_latest_snapshot_per_hub)."""
    s = _parse_dt(start_dt)
    e = _parse_dt(end_dt)
    join_params = [s, e]
    hour_filter = _hour_filter(join_params, start_hour, end_hour, col="s.scraped_at")
    con = _connect()
    rows = con.execute(f"""
        SELECT
            h.uuid, h.hub_name, h.operator, h.latitude, h.longitude, h.max_power_kw,
            h.total_evses, h.connector_types, h.location_raw,
            h.address, h.city, h.postal_code, h.user_rating, h.user_rating_count,
            h.is_24_7, h.pricing, h.payment_methods, h.devices_raw_loc, h.latest_devices_status,
            MAX(s.scraped_at) AS scraped_at,
            ROUND(AVG(s.available_count))    AS available_count,
            ROUND(AVG(s.charging_count))     AS charging_count,
            ROUND(AVG(s.inoperative_count))  AS inoperative_count,
            ROUND(AVG(s.out_of_order_count)) AS out_of_order_count,
            ROUND(AVG(s.unknown_count))      AS unknown_count,
            ROUND(100.0 * SUM(s.charging_count) /
                  NULLIF(SUM(s.available_count + s.charging_count + s.inoperative_count +
                             s.out_of_order_count + s.unknown_count), 0), 2) AS utilisation_pct
        FROM hubs h
        LEFT JOIN snapshots s ON s.hub_uuid = h.uuid
            AND s.scraped_at >= ? AND s.scraped_at <= ?{hour_filter}
        GROUP BY h.uuid
        ORDER BY utilisation_pct DESC NULLS LAST
    """, join_params).fetchall()
    con.close()
    return [_deserialise_hub(dict(row)) for row in rows]


def get_hub_history(uuid: str, hours: int = 24,
                    start_dt: str | None = None, end_dt: str | None = None) -> list[dict]:
    """Return snapshots for one hub over the last N hours (or a specific date range)."""
    if start_dt and end_dt:
        where_time = "scraped_at >= ? AND scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt), uuid]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "scraped_at >= ?"
        params = [cutoff, uuid]
    con = _connect()
    rows = con.execute(f"""
        SELECT scraped_at, available_count, charging_count,
               inoperative_count, out_of_order_count, unknown_count,
               utilisation_pct
        FROM snapshots
        WHERE {where_time} AND hub_uuid = ?
        ORDER BY scraped_at
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_all_history(hours: int = 24, hub_uuid: str | None = None,
                    start_dt: str | None = None, end_dt: str | None = None,
                    operator: str | None = None, connector: str | None = None,
                    min_kw: float | None = None, max_kw: float | None = None,
                    start_hour: int | None = None, end_hour: int | None = None) -> list[dict]:
    """
    Return one row per scrape run (keyed by scraped_at) with weighted average
    utilisation and totals across all hubs.
    """
    if start_dt and end_dt:
        where_time = "scraped_at >= ? AND scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "scraped_at >= ?"
        params = [cutoff]
    hub_filter = ""
    if hub_uuid:
        hub_filter = " AND hub_uuid = ?"
        params.append(hub_uuid)
    hub_attr_filter = _hub_subquery(params, operator, connector, min_kw, max_kw)
    hour_filter = _hour_filter(params, start_hour, end_hour)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            scraped_at,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_utilisation_pct,
            SUM(charging_count)   AS total_charging,
            SUM(available_count)  AS total_available,
            COUNT(*)              AS hub_count
        FROM snapshots
        WHERE {where_time}{hub_filter}{hub_attr_filter}{hour_filter}
        GROUP BY scraped_at
        ORDER BY scraped_at
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_all_history_daily(days: int = 30, hub_uuid: str | None = None,
                          start_dt: str | None = None, end_dt: str | None = None) -> list[dict]:
    """One row per calendar day — for the weekly/monthly growth trend chart."""
    if start_dt and end_dt:
        where_time = "scraped_at >= ? AND scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        where_time = "scraped_at >= ?"
        params = [cutoff]
    hub_filter = ""
    if hub_uuid:
        hub_filter = " AND hub_uuid = ?"
        params.append(hub_uuid)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            DATE(scraped_at) AS date,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_utilisation_pct,
            CAST(SUM(charging_count) AS INTEGER) AS total_charging,
            COUNT(DISTINCT scraped_at) AS hub_count
        FROM snapshots
        WHERE {where_time}{hub_filter}
        GROUP BY DATE(scraped_at)
        ORDER BY date
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_hourly_pattern(hours: int = 168, hub_uuid: str | None = None,
                       start_dt: str | None = None, end_dt: str | None = None,
                       operator: str | None = None, connector: str | None = None,
                       min_kw: float | None = None, max_kw: float | None = None,
                       start_hour: int | None = None, end_hour: int | None = None) -> list[dict]:
    """
    Return average utilisation grouped by hour-of-day (0–23),
    built from the last N hours of data (default 7 days).
    """
    if start_dt and end_dt:
        where_time = "scraped_at >= ? AND scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "scraped_at >= ?"
        params = [cutoff]
    hub_filter = ""
    if hub_uuid:
        hub_filter = " AND hub_uuid = ?"
        params.append(hub_uuid)
    hub_attr_filter = _hub_subquery(params, operator, connector, min_kw, max_kw)
    hour_filter = _hour_filter(params, start_hour, end_hour)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            CAST(strftime('%H', scraped_at) AS INTEGER) AS hour,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_utilisation_pct,
            COUNT(*) AS data_points
        FROM snapshots
        WHERE {where_time}{hub_filter}{hub_attr_filter}{hour_filter}
        GROUP BY hour
        ORDER BY hour
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_hourly_heatmap(hours: int = 336, hub_uuid: str | None = None,
                       start_dt: str | None = None, end_dt: str | None = None) -> list[dict]:
    """
    Return avg utilisation grouped by (day_of_week, hour) for a 7×24 heatmap.
    day_of_week: 0=Sunday … 6=Saturday (SQLite strftime('%w') convention).
    """
    if start_dt and end_dt:
        where_time = "scraped_at >= ? AND scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "scraped_at >= ?"
        params = [cutoff]
    hub_filter = ""
    if hub_uuid:
        hub_filter = " AND hub_uuid = ?"
        params.append(hub_uuid)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            CAST(strftime('%w', scraped_at) AS INTEGER) AS day_of_week,
            CAST(strftime('%H', scraped_at) AS INTEGER) AS hour,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_utilisation_pct,
            COUNT(*) AS data_points
        FROM snapshots
        WHERE {where_time}{hub_filter}
        GROUP BY day_of_week, hour
        ORDER BY day_of_week, hour
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_reliability_trend(hours: int = 168, hub_uuid: str | None = None,
                          start_dt: str | None = None, end_dt: str | None = None,
                          operator: str | None = None, connector: str | None = None,
                          min_kw: float | None = None, max_kw: float | None = None,
                          start_hour: int | None = None, end_hour: int | None = None) -> list[dict]:
    """
    One row per scrape run showing each status as % of total EVSEs across all hubs.
    Uses snapshot-time counts as denominator (no join with hubs required).
    """
    if start_dt and end_dt:
        where_time = "scraped_at >= ? AND scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "scraped_at >= ?"
        params = [cutoff]
    hub_filter = ""
    if hub_uuid:
        hub_filter = " AND hub_uuid = ?"
        params.append(hub_uuid)
    hub_attr_filter = _hub_subquery(params, operator, connector, min_kw, max_kw)
    hour_filter = _hour_filter(params, start_hour, end_hour)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            scraped_at,
            ROUND(100.0 * SUM(charging_count)     / NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS charging_pct,
            ROUND(100.0 * SUM(available_count)    / NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS available_pct,
            ROUND(100.0 * SUM(inoperative_count)  / NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS inoperative_pct,
            ROUND(100.0 * SUM(out_of_order_count) / NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS oos_pct,
            ROUND(100.0 * SUM(unknown_count)      / NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS unknown_pct
        FROM snapshots
        WHERE {where_time}{hub_filter}{hub_attr_filter}{hour_filter}
        GROUP BY scraped_at
        ORDER BY scraped_at
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_stat_deltas() -> dict:
    """Compare current 7-day avg utilisation vs prior 7-day avg."""
    con = _connect()
    current = con.execute("""
        SELECT ROUND(100.0 * SUM(charging_count) /
                     NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_util,
               SUM(charging_count) AS total_charging
        FROM snapshots
        WHERE scraped_at >= datetime('now', '-7 days')
    """).fetchone()
    prior = con.execute("""
        SELECT ROUND(100.0 * SUM(charging_count) /
                     NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_util,
               SUM(charging_count) AS total_charging
        FROM snapshots
        WHERE scraped_at >= datetime('now', '-14 days')
          AND scraped_at < datetime('now', '-7 days')
    """).fetchone()
    con.close()
    cur_util = current["avg_util"] or 0.0
    pri_util = prior["avg_util"] or 0.0
    cur_charging = current["total_charging"] or 0
    pri_charging = prior["total_charging"] or 0
    return {
        "util_delta_pp": round(cur_util - pri_util, 2),
        "charging_delta": cur_charging - pri_charging,
        "has_prior_data": pri_util > 0,
    }


def get_global_sparkline(days: int = 7) -> list[dict]:
    """Daily avg utilisation for the last N days — used for stat card sparklines."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    con = _connect()
    rows = con.execute("""
        SELECT
            DATE(scraped_at) AS date,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + inoperative_count + out_of_order_count + unknown_count), 0), 2) AS avg_utilisation_pct
        FROM snapshots
        WHERE scraped_at >= ?
        GROUP BY DATE(scraped_at)
        ORDER BY date
    """, (cutoff,)).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_all_snapshots(hours: int = 24,
                      start_dt: str | None = None, end_dt: str | None = None) -> list[dict]:
    """All raw snapshots joined with hub info — used for Excel export."""
    if start_dt and end_dt:
        where_time = "s.scraped_at >= ? AND s.scraped_at <= ?"
        params = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "s.scraped_at >= ?"
        params = [cutoff]
    con = _connect()
    rows = con.execute(f"""
        SELECT h.uuid, h.hub_name, h.operator, h.latitude, h.longitude, h.max_power_kw, h.total_evses,
               h.connector_types,
               s.scraped_at, s.available_count, s.charging_count,
               s.inoperative_count, s.out_of_order_count, s.unknown_count,
               s.utilisation_pct
        FROM snapshots s
        JOIN hubs h ON h.uuid = s.hub_uuid
        WHERE {where_time}
        ORDER BY s.scraped_at DESC, h.uuid
    """, params).fetchall()
    con.close()
    result = []
    for row in rows:
        d = dict(row)
        d["connector_types"] = json.loads(d["connector_types"] or "[]")
        result.append(d)
    return result


def get_stats() -> dict:
    con = _connect()
    hub_count = con.execute("SELECT COUNT(*) FROM hubs").fetchone()[0]
    last_scraped = con.execute(
        "SELECT MAX(scraped_at) FROM snapshots"
    ).fetchone()[0]

    # stats from the most recent scrape run only
    if last_scraped:
        agg = con.execute("""
            SELECT
                ROUND(100.0 * SUM(charging_count) /
                      NULLIF(SUM(available_count + charging_count +
                                 inoperative_count + out_of_order_count +
                                 unknown_count), 0), 2) AS avg_utilisation_pct,
                SUM(charging_count)            AS total_charging,
                SUM(available_count + charging_count +
                    inoperative_count + out_of_order_count +
                    unknown_count)             AS total_evses
            FROM snapshots
            WHERE scraped_at = ?
        """, (last_scraped,)).fetchone()
        avg_util = agg["avg_utilisation_pct"] or 0.0
        total_charging = agg["total_charging"] or 0
        total_evses = agg["total_evses"] or 0
    else:
        avg_util = 0.0
        total_charging = 0
        total_evses = 0

    snapshot_count = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    con.close()
    return {
        "total_hubs": hub_count,
        "avg_utilisation_pct": avg_util,
        "total_charging_evses": total_charging,
        "total_evses": total_evses,
        "last_scraped_at": last_scraped,
        "total_snapshots": snapshot_count,
    }
