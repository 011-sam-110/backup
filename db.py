"""
SQLite persistence layer for EV hub utilisation data.

Tables:
  hubs       — static hub info, upserted on each scrape
  snapshots  — one row per hub per scrape run (counts + utilisation_pct)
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", 15))
EVSE_EVENT_RETENTION_DAYS = int(os.getenv("EVSE_EVENT_RETENTION_DAYS", "30"))
SNAPSHOT_RETENTION_DAYS = int(os.getenv("SNAPSHOT_RETENTION_DAYS", "90"))

log = logging.getLogger("evanti.db")

DB_PATH = Path(os.getenv("DATABASE_PATH", "chargers.db"))


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
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


def _hub_subquery(params: list, operator: str | list[str] | None, connector: str | None,
                  min_kw: float | None, max_kw: float | None,
                  min_evses: int | None = None, max_evses: int | None = None,
                  group_ids: list[int] | None = None) -> str:
    """Returns SQL fragment restricting snapshots to hub UUIDs matching the given attributes."""
    conditions = []
    if operator:
        ops = [operator] if isinstance(operator, str) else list(operator)
        if ops:
            placeholders = ",".join("?" * len(ops))
            conditions.append(f"LOWER(operator) IN ({placeholders})")
            params.extend(op.lower() for op in ops)
    if connector:
        conditions.append("connector_types LIKE ?")
        params.append(f'%{connector}%')
    if min_kw is not None:
        conditions.append("max_power_kw >= ?")
        params.append(min_kw)
    if max_kw is not None:
        conditions.append("max_power_kw <= ?")
        params.append(max_kw)
    if min_evses is not None:
        conditions.append("total_evses >= ?")
        params.append(min_evses)
    if max_evses is not None:
        conditions.append("total_evses <= ?")
        params.append(max_evses)
    if group_ids:
        gp = ",".join("?" * len(group_ids))
        conditions.append(f"uuid IN (SELECT hub_uuid FROM group_hubs WHERE group_id IN ({gp}))")
        params.extend(group_ids)
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
            utilisation_pct     REAL,
            estimated_kwh       REAL
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_hub_time
            ON snapshots(hub_uuid, scraped_at);

        CREATE INDEX IF NOT EXISTS idx_snapshots_time
            ON snapshots(scraped_at);

        CREATE TABLE IF NOT EXISTS visits (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            hub_uuid    TEXT NOT NULL REFERENCES hubs(uuid),
            started_at  TEXT NOT NULL,
            ended_at    TEXT,
            dwell_min   INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_visits_hub_start
            ON visits(hub_uuid, started_at);

        CREATE TABLE IF NOT EXISTS groups (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT NOT NULL UNIQUE,
            created_at     TEXT NOT NULL,
            high_frequency INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS group_hubs (
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            hub_uuid TEXT    NOT NULL REFERENCES hubs(uuid) ON DELETE CASCADE,
            PRIMARY KEY (group_id, hub_uuid)
        );

        CREATE INDEX IF NOT EXISTS idx_group_hubs_group
            ON group_hubs(group_id);

        CREATE TABLE IF NOT EXISTS evse_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            evse_uuid  TEXT NOT NULL,
            hub_uuid   TEXT NOT NULL REFERENCES hubs(uuid),
            scraped_at TEXT NOT NULL,
            status     TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_evse_events_evse_time
            ON evse_events(evse_uuid, scraped_at);

        CREATE INDEX IF NOT EXISTS idx_evse_events_hub_time
            ON evse_events(hub_uuid, scraped_at);
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
    try:
        con.execute("ALTER TABLE snapshots ADD COLUMN estimated_kwh REAL")
        con.commit()
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE visits ADD COLUMN evse_uuid TEXT DEFAULT NULL")
        con.commit()
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE groups ADD COLUMN high_frequency INTEGER NOT NULL DEFAULT 0")
        con.commit()
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE snapshots ADD COLUMN source TEXT NOT NULL DEFAULT 'full'")
        con.commit()
    except Exception:
        pass
    con.commit()
    con.close()


def purge_non_gb_hubs() -> int:
    """
    Remove hubs (and all their snapshots + visits) that fall outside the
    current Great Britain filter — tightened western/eastern bounds and
    Isle of Man exclusion zone. Returns the number of hubs deleted.
    """
    con = _connect()
    bad_uuids = con.execute("""
        SELECT uuid FROM hubs
        WHERE
            -- Outside tightened bounding box
            latitude  < 49.9  OR latitude  > 61.0
            OR longitude < -5.85 OR longitude > 1.75
            -- Isle of Man exclusion zone
            OR (latitude BETWEEN 53.9 AND 54.5 AND longitude BETWEEN -4.9 AND -4.0)
    """).fetchall()
    uuids = [r["uuid"] for r in bad_uuids]
    if uuids:
        placeholders = ",".join("?" * len(uuids))
        con.execute(f"DELETE FROM evse_events WHERE hub_uuid IN ({placeholders})", uuids)
        con.execute(f"DELETE FROM visits     WHERE hub_uuid IN ({placeholders})", uuids)
        con.execute(f"DELETE FROM snapshots  WHERE hub_uuid IN ({placeholders})", uuids)
        con.execute(f"DELETE FROM hubs       WHERE uuid     IN ({placeholders})", uuids)
        con.commit()
        log.info("purge_non_gb_hubs: removed %d hubs outside valid GB area", len(uuids))
    con.close()
    return len(uuids)


def upsert_hubs(records: list[dict]) -> None:
    con = _connect()
    now = datetime.now(timezone.utc).isoformat()
    for r in records:
        con.execute("""
            INSERT INTO hubs (uuid, latitude, longitude, max_power_kw, total_evses,
                              connector_types, first_seen_at, last_seen_at,
                              hub_name, operator,
                              address, city, postal_code,
                              user_rating, user_rating_count, is_24_7,
                              pricing, payment_methods, devices_raw_loc,
                              latest_devices_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uuid) DO UPDATE SET
                latitude        = excluded.latitude,
                longitude       = excluded.longitude,
                max_power_kw    = excluded.max_power_kw,
                total_evses     = excluded.total_evses,
                connector_types = excluded.connector_types,
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
            json.dumps(r["devices_raw_loc"]) if r.get("devices_raw_loc") else None,
            json.dumps(r.get("devices", [])),
        ))
    con.commit()
    con.close()


_KWH_COEFFICIENT = 0.7


def insert_snapshots(records: list[dict], *, source: str = 'full') -> None:
    con = _connect()

    interval_hours = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "15")) / 60.0

    # Batch-fetch max_power_kw for records that don't carry it (db-only records)
    uuids_needing_power = [r["uuid"] for r in records if "max_power_kw" not in r]
    power_map: dict[str, float] = {}
    if uuids_needing_power:
        placeholders = ",".join("?" * len(uuids_needing_power))
        rows_p = con.execute(
            f"SELECT uuid, max_power_kw FROM hubs WHERE uuid IN ({placeholders})",
            uuids_needing_power,
        ).fetchall()
        power_map = {row["uuid"]: (row["max_power_kw"] or 0.0) for row in rows_p}

    rows = []
    for r in records:
        charging    = r.get("charging_count")     or 0
        available   = r.get("available_count")    or 0
        inoperative = r.get("inoperative_count")  or 0
        oos         = r.get("out_of_order_count") or 0
        unknown     = r.get("unknown_count")      or 0
        total_status = available + charging + unknown
        util_pct = round(charging / total_status * 100, 2) if total_status > 0 else 0.0
        try:
            max_kw = r.get("max_power_kw") or power_map.get(r["uuid"], 0.0)
            estimated_kwh = round(charging * max_kw * interval_hours * _KWH_COEFFICIENT, 2)
        except Exception:
            estimated_kwh = None
        rows.append((
            r["uuid"],
            r["scraped_at"],
            r.get("available_count", 0),
            charging,
            r.get("inoperative_count", 0),
            r.get("out_of_order_count", 0),
            r.get("unknown_count", 0),
            util_pct,
            estimated_kwh,
            source,
        ))
    count_before = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    con.executemany("""
        INSERT INTO snapshots
            (hub_uuid, scraped_at, available_count, charging_count,
             inoperative_count, out_of_order_count, unknown_count, utilisation_pct,
             estimated_kwh, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    con.commit()
    count_after = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    log.info("insert_snapshots: %d → %d (+%d)", count_before, count_after, count_after - count_before)
    purge_old_snapshots(con)
    con.commit()
    con.close()


def _detect_visits(records: list[dict], con: sqlite3.Connection) -> None:
    """Deprecated: superseded by detect_evse_changes(). Legacy visits have evse_uuid=NULL.
    Inferred visits from charging_count deltas — systematically undercounts due to net-delta
    cancellation. Kept for reference; no longer called."""
    for r in records:
        uuid = r["uuid"]
        scraped_at = r["scraped_at"]
        curr_charging = r.get("charging_count") or 0

        prev_row = con.execute("""
            SELECT charging_count FROM snapshots
            WHERE hub_uuid = ? AND scraped_at < ?
            ORDER BY scraped_at DESC LIMIT 1
        """, (uuid, scraped_at)).fetchone()

        if prev_row is None:
            continue

        prev_charging = prev_row["charging_count"] or 0
        delta = curr_charging - prev_charging

        if delta > 0:
            con.executemany(
                "INSERT INTO visits (hub_uuid, started_at) VALUES (?, ?)",
                [(uuid, scraped_at)] * delta,
            )
        elif delta < 0:
            open_visits = con.execute("""
                SELECT id, started_at FROM visits
                WHERE hub_uuid = ? AND ended_at IS NULL
                ORDER BY started_at ASC
                LIMIT ?
            """, (uuid, abs(delta))).fetchall()
            for v in open_visits:
                start = datetime.fromisoformat(v["started_at"].replace('Z', '+00:00'))
                end = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                dwell_min = round((end - start).total_seconds() / 60)
                con.execute(
                    "UPDATE visits SET ended_at = ?, dwell_min = ? WHERE id = ?",
                    (scraped_at, dwell_min, v["id"]),
                )

    con.commit()


def _deserialise_hub(d: dict) -> dict:
    d["connector_types"] = json.loads(d["connector_types"] or "[]")
    d["pricing"] = json.loads(d["pricing"] or "[]")
    d["payment_methods"] = json.loads(d["payment_methods"] or "[]")
    return d


def get_latest_snapshot_per_hub() -> list[dict]:
    """Return each hub's most recent snapshot merged with hub static data."""
    con = _connect()
    rows = con.execute("""
        SELECT
            h.uuid, h.hub_name, h.operator, h.latitude, h.longitude, h.max_power_kw,
            h.total_evses, h.connector_types,
            h.address, h.city, h.postal_code, h.user_rating, h.user_rating_count,
            h.is_24_7, h.pricing, h.payment_methods,
            s.scraped_at, s.available_count, s.charging_count,
            s.inoperative_count, s.out_of_order_count, s.unknown_count,
            ROUND(100.0 * s.charging_count /
                  NULLIF(s.available_count + s.charging_count + s.unknown_count, 0), 2) AS utilisation_pct
        FROM hubs h
        LEFT JOIN snapshots s ON s.hub_uuid = h.uuid
            AND s.scraped_at = (
                SELECT MAX(scraped_at) FROM snapshots WHERE hub_uuid = h.uuid
            )
        ORDER BY utilisation_pct DESC NULLS LAST
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
            h.total_evses, h.connector_types,
            h.address, h.city, h.postal_code, h.user_rating, h.user_rating_count,
            h.is_24_7, h.pricing, h.payment_methods,
            MAX(s.scraped_at) AS scraped_at,
            ROUND(AVG(s.available_count))    AS available_count,
            ROUND(AVG(s.charging_count))     AS charging_count,
            ROUND(AVG(s.inoperative_count))  AS inoperative_count,
            ROUND(AVG(s.out_of_order_count)) AS out_of_order_count,
            ROUND(AVG(s.unknown_count))      AS unknown_count,
            ROUND(100.0 * SUM(s.charging_count) /
                  NULLIF(SUM(s.available_count + s.charging_count + s.unknown_count), 0), 2) AS utilisation_pct
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
                    min_evses: int | None = None, max_evses: int | None = None,
                    start_hour: int | None = None, end_hour: int | None = None,
                    group_ids: list[int] | None = None,
                    source_filter: str | None = 'full') -> list[dict]:
    """
    Return one row per scrape run (keyed by scraped_at) with weighted average
    utilisation and totals across all hubs.

    source_filter: 'full' (default) excludes targeted 1-min snapshots from the
    trend graph so partial-network rows don't distort the utilisation average.
    Pass None to include all sources.
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
    hub_attr_filter = _hub_subquery(params, operator, connector, min_kw, max_kw, min_evses, max_evses, group_ids=group_ids)
    hour_filter = _hour_filter(params, start_hour, end_hour)
    source_clause = ""
    if source_filter:
        source_clause = " AND source = ?"
        params.append(source_filter)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            scraped_at,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_utilisation_pct,
            SUM(charging_count)              AS total_charging,
            SUM(available_count)             AS total_available,
            COUNT(*)                         AS hub_count,
            ROUND(SUM(estimated_kwh), 1)     AS total_estimated_kwh
        FROM snapshots
        WHERE {where_time}{hub_filter}{hub_attr_filter}{hour_filter}{source_clause}
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
                  NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_utilisation_pct,
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
                       min_evses: int | None = None, max_evses: int | None = None,
                       start_hour: int | None = None, end_hour: int | None = None,
                       interval_minutes: int = INTERVAL_MINUTES,
                       group_ids: list[int] | None = None) -> list[dict]:
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
    hub_attr_filter = _hub_subquery(params, operator, connector, min_kw, max_kw, min_evses, max_evses, group_ids=group_ids)
    hour_filter = _hour_filter(params, start_hour, end_hour)
    params.append(interval_minutes)
    con = _connect()
    rows = con.execute(f"""
        SELECT
            CAST(strftime('%H', scraped_at) AS INTEGER) AS hour,
            ROUND(100.0 * SUM(charging_count) /
                  NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_utilisation_pct,
            ROUND(AVG(estimated_kwh) * (60.0 / ?), 1) AS avg_est_kw,
            COUNT(*) AS data_points
        FROM snapshots
        WHERE {where_time}{hub_filter}{hub_attr_filter}{hour_filter}
        GROUP BY hour
        ORDER BY hour
    """, params).fetchall()
    result = [dict(r) for r in rows]

    # Merge avg visit starts per hour from visits table
    if start_dt and end_dt:
        v_start = _parse_dt(start_dt)
        v_end = _parse_dt(end_dt)
    else:
        v_start = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        v_end = datetime.now(timezone.utc).isoformat()
    vp: list = [v_start, v_end]
    v_hub_filter = _hub_subquery(vp, operator, connector, min_kw, max_kw, min_evses, max_evses)
    if hub_uuid:
        v_hub_filter += " AND hub_uuid = ?"
        vp.append(hub_uuid)
    visit_rows = con.execute(f"""
        SELECT
            CAST(strftime('%H', started_at) AS INTEGER) AS hour,
            COUNT(*) AS total_starts,
            COUNT(DISTINCT DATE(started_at)) AS day_count
        FROM visits
        WHERE started_at >= ? AND started_at <= ?{v_hub_filter}
        GROUP BY hour
    """, vp).fetchall()
    visit_map = {r["hour"]: round(r["total_starts"] / max(r["day_count"], 1), 1) for r in visit_rows}
    for row in result:
        row["avg_visit_starts"] = visit_map.get(row["hour"], 0)

    con.close()
    return result


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
                  NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_utilisation_pct,
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
                          min_evses: int | None = None, max_evses: int | None = None,
                          start_hour: int | None = None, end_hour: int | None = None,
                          group_ids: list[int] | None = None) -> list[dict]:
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
    hub_attr_filter = _hub_subquery(params, operator, connector, min_kw, max_kw, min_evses, max_evses, group_ids=group_ids)
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
                     NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_util,
               SUM(charging_count) AS total_charging
        FROM snapshots
        WHERE scraped_at >= datetime('now', '-7 days')
    """).fetchone()
    prior = con.execute("""
        SELECT ROUND(100.0 * SUM(charging_count) /
                     NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_util,
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
                  NULLIF(SUM(available_count + charging_count + unknown_count), 0), 2) AS avg_utilisation_pct
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


def get_hub_detail(uuid: str) -> dict | None:
    """Return full hub data for one hub including device blobs — used by the detail modal."""
    con = _connect()
    row = con.execute("""
        SELECT h.uuid, h.hub_name, h.operator, h.latitude, h.longitude, h.max_power_kw,
               h.total_evses, h.connector_types, h.address, h.city, h.postal_code,
               h.user_rating, h.user_rating_count, h.is_24_7, h.pricing, h.payment_methods,
               h.devices_raw_loc, h.latest_devices_status,
               s.scraped_at, s.available_count, s.charging_count,
               s.inoperative_count, s.out_of_order_count, s.unknown_count, s.utilisation_pct
        FROM hubs h
        LEFT JOIN snapshots s ON s.hub_uuid = h.uuid
            AND s.scraped_at = (SELECT MAX(scraped_at) FROM snapshots WHERE hub_uuid = h.uuid)
        WHERE h.uuid = ?
    """, (uuid,)).fetchone()
    con.close()
    if not row:
        return None
    d = dict(row)
    d["connector_types"]       = json.loads(d["connector_types"] or "[]")
    d["pricing"]               = json.loads(d["pricing"] or "[]")
    d["payment_methods"]       = json.loads(d["payment_methods"] or "[]")
    d["devices_raw_loc"]       = json.loads(d["devices_raw_loc"] or "[]")
    d["latest_devices_status"] = json.loads(d["latest_devices_status"] or "[]")
    return d


def update_latest_devices_status(records: list[dict]) -> None:
    """Update only latest_devices_status for hubs that don't go through a full upsert."""
    if not records:
        return
    con = _connect()
    for r in records:
        con.execute(
            "UPDATE hubs SET latest_devices_status = ? WHERE uuid = ?",
            (json.dumps(r.get("devices", [])), r["uuid"])
        )
    con.commit()
    con.close()


def detect_evse_changes(records: list[dict], con: sqlite3.Connection) -> None:
    """Compare incoming per-EVSE statuses against the previously stored
    latest_devices_status for each hub. Writes change events to evse_events
    and opens/closes per-EVSE visits accordingly.

    Must be called BEFORE upsert_hubs() so that latest_devices_status still
    holds the previous scrape's state.
    """
    if not records:
        return

    hub_uuids = [r["uuid"] for r in records]
    placeholders = ",".join("?" * len(hub_uuids))
    rows = con.execute(
        f"SELECT uuid, latest_devices_status FROM hubs WHERE uuid IN ({placeholders})",
        hub_uuids,
    ).fetchall()

    # Build {hub_uuid → {evse_uuid → status}} from stored previous state
    old_status_map: dict[str, dict[str, str]] = {}
    for row in rows:
        raw = row["latest_devices_status"]
        if not raw:
            continue
        try:
            devices = json.loads(raw)
        except Exception:
            continue
        evse_map: dict[str, str] = {}
        for device in devices:
            for evse in device.get("evses", []):
                evse_id = evse.get("evse_uuid")
                status = (evse.get("network_status") or "UNKNOWN").upper()
                if evse_id:
                    evse_map[evse_id] = status
        old_status_map[row["uuid"]] = evse_map

    event_rows: list[tuple] = []          # (evse_uuid, hub_uuid, scraped_at, status)
    visit_opens: list[tuple] = []         # (hub_uuid, evse_uuid, scraped_at)
    visit_closes: list[tuple] = []        # (evse_uuid, scraped_at)

    for r in records:
        hub_uuid = r["uuid"]
        scraped_at = r["scraped_at"]
        old_evse_map = old_status_map.get(hub_uuid)  # None if hub is brand-new
        new_devices = r.get("devices", [])

        for device in new_devices:
            for evse in device.get("evses", []):
                evse_uuid = evse.get("evse_uuid")
                if not evse_uuid:
                    continue
                new_status = (evse.get("network_status") or "UNKNOWN").upper()

                if old_evse_map is None:
                    # Hub seen for the first time — record event, no visit action
                    event_rows.append((evse_uuid, hub_uuid, scraped_at, new_status))
                    continue

                old_status = old_evse_map.get(evse_uuid)

                if old_status is None:
                    # EVSE is brand-new within an existing hub — record event, no visit action
                    event_rows.append((evse_uuid, hub_uuid, scraped_at, new_status))
                    continue

                if old_status == new_status:
                    continue  # no change — skip

                event_rows.append((evse_uuid, hub_uuid, scraped_at, new_status))

                if new_status == "CHARGING":
                    visit_opens.append((hub_uuid, evse_uuid, scraped_at))
                if old_status == "CHARGING":
                    visit_closes.append((evse_uuid, scraped_at))

    if event_rows:
        con.executemany(
            "INSERT INTO evse_events (evse_uuid, hub_uuid, scraped_at, status) VALUES (?, ?, ?, ?)",
            event_rows,
        )

    for hub_uuid, evse_uuid, scraped_at in visit_opens:
        con.execute(
            "INSERT INTO visits (hub_uuid, evse_uuid, started_at) VALUES (?, ?, ?)",
            (hub_uuid, evse_uuid, scraped_at),
        )

    for evse_uuid, close_scraped_at in visit_closes:
        open_visit = con.execute("""
            SELECT id, started_at FROM visits
            WHERE evse_uuid = ? AND ended_at IS NULL
            ORDER BY started_at DESC LIMIT 1
        """, (evse_uuid,)).fetchone()
        if open_visit:
            start = datetime.fromisoformat(open_visit["started_at"].replace('Z', '+00:00'))
            end = datetime.fromisoformat(close_scraped_at.replace('Z', '+00:00'))
            dwell_min = round((end - start).total_seconds() / 60)
            con.execute(
                "UPDATE visits SET ended_at = ?, dwell_min = ? WHERE id = ?",
                (close_scraped_at, dwell_min, open_visit["id"]),
            )

    log.info(
        "detect_evse_changes: %d events, %d visits opened, %d visits closed",
        len(event_rows), len(visit_opens), len(visit_closes),
    )


def close_stale_visits(con: sqlite3.Connection, max_hours: int = 12) -> None:
    """Force-close any visits that have been open longer than max_hours.
    At 100kW+ no session should last more than a few hours; long-open visits
    represent missed close events (scraper downtime, API gaps, etc.).
    dwell_min is set to NULL to signal that the duration is unreliable.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_hours)).isoformat()
    stale = con.execute(
        "SELECT id FROM visits WHERE ended_at IS NULL AND started_at < ?", (cutoff,)
    ).fetchall()
    if not stale:
        return
    now_str = datetime.now(timezone.utc).isoformat()
    ids = [r["id"] for r in stale]
    ph = ",".join("?" * len(ids))
    con.execute(
        f"UPDATE visits SET ended_at = ?, dwell_min = NULL WHERE id IN ({ph})",
        [now_str] + ids,
    )
    log.info("close_stale_visits: force-closed %d visits open > %dh", len(ids), max_hours)


def purge_old_evse_events(con: sqlite3.Connection) -> None:
    """Delete evse_events older than EVSE_EVENT_RETENTION_DAYS (default 30)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=EVSE_EVENT_RETENTION_DAYS)).isoformat()
    cur = con.execute("DELETE FROM evse_events WHERE scraped_at < ?", (cutoff,))
    if cur.rowcount:
        log.info("purge_old_evse_events: deleted %d rows older than %d days",
                 cur.rowcount, EVSE_EVENT_RETENTION_DAYS)


def purge_old_snapshots(con: sqlite3.Connection) -> None:
    """Delete snapshots older than SNAPSHOT_RETENTION_DAYS (default 90)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)).isoformat()
    cur = con.execute("DELETE FROM snapshots WHERE scraped_at < ?", (cutoff,))
    if cur.rowcount:
        log.info("purge_old_snapshots: deleted %d rows older than %d days",
                 cur.rowcount, SNAPSHOT_RETENTION_DAYS)


def process_evse_events(records: list[dict]) -> None:
    """Entry point called from scraper BEFORE upsert_hubs.
    Detects per-EVSE status changes, opens/closes visits, closes stale visits,
    and purges old events — all in a single transaction.
    """
    con = _connect()
    try:
        detect_evse_changes(records, con)
        close_stale_visits(con)
        purge_old_evse_events(con)
        con.commit()
    finally:
        con.close()


def get_all_hubs_for_scrape() -> list[dict]:
    """Return all tracked hub UUIDs and key static fields."""
    con = _connect()
    rows = con.execute(
        "SELECT uuid, latitude, longitude, max_power_kw, total_evses, connector_types FROM hubs"
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_groups() -> list[dict]:
    """Return all groups with their hub counts."""
    con = _connect()
    rows = con.execute("""
        SELECT g.id, g.name, g.created_at, g.high_frequency,
               COUNT(gh.hub_uuid) AS hub_count
        FROM groups g
        LEFT JOIN group_hubs gh ON gh.group_id = g.id
        GROUP BY g.id
        ORDER BY g.created_at
    """).fetchall()
    con.close()
    return [dict(r) for r in rows]


def create_group(name: str) -> dict:
    """Create a new group and return it."""
    con = _connect()
    now = datetime.now(timezone.utc).isoformat()
    try:
        cur = con.execute(
            "INSERT INTO groups (name, created_at) VALUES (?, ?)", (name.strip(), now)
        )
        con.commit()
        row = con.execute(
            "SELECT id, name, created_at, 0 AS hub_count FROM groups WHERE id = ?",
            (cur.lastrowid,)
        ).fetchone()
        con.close()
        return dict(row)
    except Exception:
        con.close()
        raise


def rename_group(group_id: int, name: str) -> dict | None:
    con = _connect()
    cur = con.execute("UPDATE groups SET name = ? WHERE id = ?", (name.strip(), group_id))
    con.commit()
    if cur.rowcount == 0:
        con.close()
        return None
    row = con.execute(
        "SELECT id, name, created_at FROM groups WHERE id = ?", (group_id,)
    ).fetchone()
    con.close()
    return dict(row) if row else None


def delete_group(group_id: int) -> None:
    con = _connect()
    con.execute("DELETE FROM group_hubs WHERE group_id = ?", (group_id,))
    con.execute("DELETE FROM groups WHERE id = ?", (group_id,))
    con.commit()
    con.close()


def add_hubs_to_group(group_id: int, hub_uuids: list[str]) -> None:
    con = _connect()
    con.executemany(
        "INSERT OR IGNORE INTO group_hubs (group_id, hub_uuid) VALUES (?, ?)",
        [(group_id, uuid) for uuid in hub_uuids],
    )
    con.commit()
    con.close()


def remove_hub_from_group(group_id: int, hub_uuid: str) -> None:
    con = _connect()
    con.execute(
        "DELETE FROM group_hubs WHERE group_id = ? AND hub_uuid = ?",
        (group_id, hub_uuid),
    )
    con.commit()
    con.close()


def get_group_hub_uuids(group_id: int) -> list[str]:
    con = _connect()
    rows = con.execute(
        "SELECT hub_uuid FROM group_hubs WHERE group_id = ?", (group_id,)
    ).fetchall()
    con.close()
    return [r["hub_uuid"] for r in rows]


def get_group_by_id(group_id: int) -> dict | None:
    con = _connect()
    row = con.execute("SELECT * FROM groups WHERE id = ?", (group_id,)).fetchone()
    con.close()
    return dict(row) if row else None


def set_group_high_frequency(group_id: int, value: bool) -> dict | None:
    con = _connect()
    cur = con.execute(
        "UPDATE groups SET high_frequency = ? WHERE id = ?",
        (1 if value else 0, group_id),
    )
    con.commit()
    if cur.rowcount == 0:
        con.close()
        return None
    row = con.execute("SELECT * FROM groups WHERE id = ?", (group_id,)).fetchone()
    con.close()
    return dict(row) if row else None


def get_high_frequency_hub_uuids() -> list[str]:
    """Return UUIDs of all hubs belonging to at least one high-frequency group."""
    con = _connect()
    rows = con.execute("""
        SELECT DISTINCT gh.hub_uuid
        FROM group_hubs gh
        JOIN groups g ON g.id = gh.group_id
        WHERE g.high_frequency = 1
    """).fetchall()
    con.close()
    return [r["hub_uuid"] for r in rows]


def get_charging_session_start(hub_uuid: str) -> str | None:
    """Return when the current charging session was first detected by our scraper.
    Finds the first snapshot in the current unbroken charging run by locating the
    most recent transition from a lower charging_count to the current one.
    Returns None if hub is not currently charging or has no snapshot history."""
    con = _connect()
    try:
        row = con.execute(
            "SELECT charging_count FROM snapshots WHERE hub_uuid=? ORDER BY scraped_at DESC LIMIT 1",
            (hub_uuid,)
        ).fetchone()
        if not row or row["charging_count"] == 0:
            return None
        current_charging = row["charging_count"]

        transition = con.execute("""
            SELECT scraped_at FROM snapshots
            WHERE hub_uuid=? AND charging_count < ?
            ORDER BY scraped_at DESC LIMIT 1
        """, (hub_uuid, current_charging)).fetchone()

        after = transition["scraped_at"] if transition else "1970-01-01"
        first_in_run = con.execute("""
            SELECT scraped_at FROM snapshots
            WHERE hub_uuid=? AND scraped_at > ? AND charging_count >= ?
            ORDER BY scraped_at ASC LIMIT 1
        """, (hub_uuid, after, current_charging)).fetchone()

        return first_in_run["scraped_at"] if first_in_run else None
    finally:
        con.close()


def get_hub_group_ids(hub_uuid: str) -> list[int]:
    """Return the group IDs that a hub belongs to."""
    con = _connect()
    rows = con.execute(
        "SELECT group_id FROM group_hubs WHERE hub_uuid = ?", (hub_uuid,)
    ).fetchall()
    con.close()
    return [r["group_id"] for r in rows]


def get_visit_stats(start_dt: str | None = None, end_dt: str | None = None,
                    operator: str | None = None, connector: str | None = None,
                    min_kw: float | None = None, max_kw: float | None = None,
                    min_evses: int | None = None, max_evses: int | None = None,
                    start_hour: int | None = None, end_hour: int | None = None,
                    group_ids: list[int] | None = None) -> list[dict]:
    """Per-hub visit counts and average dwell time for a date range (or all time if no dates given)."""
    params: list = []
    date_filter = ""
    if start_dt and end_dt:
        s = _parse_dt(start_dt)
        e = _parse_dt(end_dt)
        params = [s, e]
        date_filter = "started_at >= ? AND started_at <= ?"
    hour_filter = _hour_filter(params, start_hour, end_hour, col="started_at").lstrip(" AND ")
    hub_filter = _hub_subquery(params, operator, connector, min_kw, max_kw, min_evses, max_evses, group_ids=group_ids)
    where = " AND ".join(filter(None, [date_filter, hour_filter, hub_filter.lstrip(" AND ")]))
    where_clause = f"WHERE {where}" if where else ""
    con = _connect()
    rows = con.execute(f"""
        SELECT
            hub_uuid,
            COUNT(CASE WHEN ended_at IS NOT NULL THEN 1 END)       AS visit_count,
            ROUND(AVG(CASE WHEN ended_at IS NOT NULL
                           THEN dwell_min END))                    AS avg_dwell_min,
            COUNT(CASE WHEN ended_at IS NULL THEN 1 END)           AS active_visits
        FROM visits
        {where_clause}
        GROUP BY hub_uuid
    """, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


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
