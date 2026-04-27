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
TARGETED_SNAPSHOT_RETENTION_DAYS = int(os.getenv("TARGETED_SNAPSHOT_RETENTION_DAYS", "30"))

log = logging.getLogger("evanti.db")

DB_PATH = Path(os.getenv("DATABASE_PATH", "chargers.db"))


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
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
        conditions.append("uuid IN (SELECT hub_uuid FROM hub_connectors WHERE connector_type = ?)")
        params.append(connector)
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


MIN_EVSES = 6               # hubs below this qualifying EVSE count are not surfaced by the API
EXCLUDED_CONNECTORS = {"CHADEMO"}  # connector types stripped from all API responses
MIN_SHARED_POWER_W = 150_000       # 150kW minimum per EVSE — tracks all rapid/ultra-rapid CCS2 units

# ---------------------------------------------------------------------------
# Schema versioning
# Each entry is a single SQL statement applied exactly once, in order.
# To add a migration: append a new key (next integer) — never edit existing ones.
# ---------------------------------------------------------------------------
_MIGRATIONS: dict[int, str | list[str]] = {
    # ── Historical single-column additions (1–16) ──────────────────────────
    1:  "ALTER TABLE hubs ADD COLUMN hub_name TEXT DEFAULT NULL",
    2:  "ALTER TABLE hubs ADD COLUMN operator TEXT DEFAULT NULL",
    3:  "ALTER TABLE hubs ADD COLUMN address TEXT DEFAULT NULL",
    4:  "ALTER TABLE hubs ADD COLUMN city TEXT DEFAULT NULL",
    5:  "ALTER TABLE hubs ADD COLUMN postal_code TEXT DEFAULT NULL",
    6:  "ALTER TABLE hubs ADD COLUMN user_rating REAL DEFAULT NULL",
    7:  "ALTER TABLE hubs ADD COLUMN user_rating_count INTEGER DEFAULT NULL",
    8:  "ALTER TABLE hubs ADD COLUMN is_24_7 INTEGER DEFAULT NULL",
    9:  "ALTER TABLE hubs ADD COLUMN pricing TEXT DEFAULT NULL",
    10: "ALTER TABLE hubs ADD COLUMN payment_methods TEXT DEFAULT NULL",
    11: "ALTER TABLE hubs ADD COLUMN devices_raw_loc TEXT DEFAULT NULL",
    12: "ALTER TABLE hubs ADD COLUMN latest_devices_status TEXT DEFAULT NULL",
    13: "ALTER TABLE snapshots ADD COLUMN estimated_kwh REAL",
    14: "ALTER TABLE visits ADD COLUMN evse_uuid TEXT DEFAULT NULL",
    15: "ALTER TABLE groups ADD COLUMN high_frequency INTEGER NOT NULL DEFAULT 0",
    16: "ALTER TABLE snapshots ADD COLUMN source TEXT NOT NULL DEFAULT 'full'",

    # ── Structural improvements ────────────────────────────────────────────

    # 17: Rebuild snapshots — add snapshot_max_kw audit column, CHECK on source,
    #     and covering index for analytics queries.
    17: [
        "DROP TABLE IF EXISTS snapshots_new",
        """CREATE TABLE snapshots_new (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            hub_uuid            TEXT NOT NULL REFERENCES hubs(uuid),
            scraped_at          TEXT NOT NULL,
            available_count     INTEGER,
            charging_count      INTEGER,
            inoperative_count   INTEGER,
            out_of_order_count  INTEGER,
            unknown_count       INTEGER,
            utilisation_pct     REAL,
            estimated_kwh       REAL,
            snapshot_max_kw     REAL,
            source              TEXT NOT NULL DEFAULT 'full'
                                    CHECK(source IN ('full', 'targeted'))
        )""",
        """INSERT INTO snapshots_new
               (id, hub_uuid, scraped_at, available_count, charging_count,
                inoperative_count, out_of_order_count, unknown_count,
                utilisation_pct, estimated_kwh, snapshot_max_kw, source)
           SELECT id, hub_uuid, scraped_at, available_count, charging_count,
                  inoperative_count, out_of_order_count, unknown_count,
                  utilisation_pct, estimated_kwh, NULL, source
           FROM snapshots""",
        "DROP TABLE snapshots",
        "ALTER TABLE snapshots_new RENAME TO snapshots",
        "CREATE INDEX idx_snapshots_hub_time ON snapshots(hub_uuid, scraped_at)",
        "CREATE INDEX idx_snapshots_time ON snapshots(scraped_at)",
        "CREATE INDEX idx_snapshots_covering ON snapshots(hub_uuid, scraped_at, utilisation_pct, charging_count, source)",
    ],

    # 18: Rebuild evse_events — enforce valid status values via CHECK constraint.
    18: [
        "DROP TABLE IF EXISTS evse_events_new",
        """CREATE TABLE evse_events_new (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            evse_uuid  TEXT NOT NULL,
            hub_uuid   TEXT NOT NULL REFERENCES hubs(uuid),
            scraped_at TEXT NOT NULL,
            status     TEXT NOT NULL
                           CHECK(status IN ('AVAILABLE', 'CHARGING', 'INOPERATIVE',
                                            'OUTOFORDER', 'UNKNOWN'))
        )""",
        """INSERT INTO evse_events_new
               (id, evse_uuid, hub_uuid, scraped_at, status)
           SELECT id, evse_uuid, hub_uuid, scraped_at, status
           FROM evse_events""",
        "DROP TABLE evse_events",
        "ALTER TABLE evse_events_new RENAME TO evse_events",
        "CREATE INDEX idx_evse_events_evse_time ON evse_events(evse_uuid, scraped_at)",
        "CREATE INDEX idx_evse_events_hub_time ON evse_events(hub_uuid, scraped_at)",
    ],

    # 19: Rebuild groups — enforce high_frequency is strictly 0 or 1.
    19: [
        "DROP TABLE IF EXISTS groups_new",
        """CREATE TABLE groups_new (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT NOT NULL UNIQUE,
            created_at     TEXT NOT NULL,
            high_frequency INTEGER NOT NULL DEFAULT 0
                               CHECK(high_frequency IN (0, 1))
        )""",
        """INSERT INTO groups_new (id, name, created_at, high_frequency)
           SELECT id, name, created_at, high_frequency FROM groups""",
        "DROP TABLE groups",
        "ALTER TABLE groups_new RENAME TO groups",
    ],

    # 20: Create hub_connectors junction table — normalises connector_types TEXT/JSON
    #     into exact-match indexed rows, replacing the LIKE anti-pattern in queries.
    #     Populated from existing connector_types JSON via json_each.
    20: [
        """CREATE TABLE IF NOT EXISTS hub_connectors (
            hub_uuid       TEXT NOT NULL REFERENCES hubs(uuid),
            connector_type TEXT NOT NULL,
            PRIMARY KEY (hub_uuid, connector_type)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_hub_connectors_type ON hub_connectors(connector_type)",
        """INSERT OR IGNORE INTO hub_connectors (hub_uuid, connector_type)
           SELECT h.uuid, j.value
           FROM hubs h, json_each(h.connector_types) j
           WHERE h.connector_types IS NOT NULL AND h.connector_types != '[]'""",
    ],
    # 21: One-time data cleanup after CHAdeMO exclusion + 300kW power-sharing rules were added.
    #     Strips excluded connectors from existing hub records and recomputes total_evses
    #     from devices_raw_loc so the >= MIN_EVSES API filter operates on accurate counts.
    21: "_python_migration_21",  # resolved to _migration_21_evse_cleanup by _run_migrations

    # 22: Power threshold lowered from 300kW to 150kW — recompute total_evses so hubs with
    #     dedicated 150kW CCS2 units (previously excluded) now qualify for tracking.
    22: "_python_migration_22",  # resolved to _migration_22_power_threshold_update

    # 23: Replace high_frequency boolean on groups with scrape_interval (1–5 min, NULL = off).
    23: [
        "DROP TABLE IF EXISTS groups_new",
        """CREATE TABLE groups_new (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL UNIQUE,
            created_at      TEXT NOT NULL,
            scrape_interval INTEGER DEFAULT NULL
                                CHECK(scrape_interval IN (1,2,3,4,5))
        )""",
        """INSERT INTO groups_new (id, name, created_at, scrape_interval)
           SELECT id, name, created_at,
                  CASE WHEN high_frequency = 1 THEN 1 ELSE NULL END
           FROM groups""",
        "DROP TABLE groups",
        "ALTER TABLE groups_new RENAME TO groups",
    ],

    # 24: App-wide key-value settings table. Seeded with targeted_scraping_enabled = 1.
    24: [
        """CREATE TABLE IF NOT EXISTS app_settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""",
        "INSERT OR IGNORE INTO app_settings (key, value) VALUES ('targeted_scraping_enabled', '1')",
    ],

    # 25: Polling-interval experiment tables.
    #     targeted_evse_events — independent per-interval event log (prior state never crosses intervals).
    #     targeted_visits — independent per-interval visit tracking.
    #     targeted_snapshots — raw charging counts per poll for the timeline export sheet.
    #     experiment_hubs — created here but superseded by migration 26; use groups.scrape_interval instead.
    25: [
        """CREATE TABLE IF NOT EXISTS experiment_hubs (
            hub_uuid          TEXT    NOT NULL REFERENCES hubs(uuid) ON DELETE CASCADE,
            poll_interval_min INTEGER NOT NULL CHECK(poll_interval_min IN (1, 3, 5)),
            added_at          TEXT    NOT NULL,
            PRIMARY KEY (hub_uuid, poll_interval_min)
        )""",
        """CREATE TABLE IF NOT EXISTS targeted_evse_events (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_interval_min INTEGER NOT NULL CHECK(poll_interval_min IN (1, 3, 5)),
            evse_uuid         TEXT    NOT NULL,
            hub_uuid          TEXT    NOT NULL REFERENCES hubs(uuid),
            scraped_at        TEXT    NOT NULL,
            status            TEXT    NOT NULL
                                  CHECK(status IN ('AVAILABLE', 'CHARGING', 'INOPERATIVE',
                                                   'OUTOFORDER', 'UNKNOWN'))
        )""",
        "CREATE INDEX IF NOT EXISTS idx_tgt_evse_evt_lookup ON targeted_evse_events(poll_interval_min, evse_uuid, scraped_at)",
        "CREATE INDEX IF NOT EXISTS idx_tgt_evse_evt_hub    ON targeted_evse_events(hub_uuid, poll_interval_min, scraped_at)",
        """CREATE TABLE IF NOT EXISTS targeted_visits (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_interval_min INTEGER NOT NULL CHECK(poll_interval_min IN (1, 3, 5)),
            hub_uuid          TEXT    NOT NULL REFERENCES hubs(uuid),
            evse_uuid         TEXT    NOT NULL,
            started_at        TEXT    NOT NULL,
            ended_at          TEXT,
            dwell_min         INTEGER
        )""",
        "CREATE INDEX IF NOT EXISTS idx_tgt_visits_hub  ON targeted_visits(hub_uuid, poll_interval_min, started_at)",
        "CREATE INDEX IF NOT EXISTS idx_tgt_visits_evse ON targeted_visits(poll_interval_min, evse_uuid, started_at)",
        """CREATE TABLE IF NOT EXISTS targeted_snapshots (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_interval_min INTEGER NOT NULL CHECK(poll_interval_min IN (1, 3, 5)),
            hub_uuid          TEXT    NOT NULL REFERENCES hubs(uuid),
            scraped_at        TEXT    NOT NULL,
            charging_count    INTEGER,
            utilisation_pct   REAL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_tgt_snapshots ON targeted_snapshots(hub_uuid, poll_interval_min, scraped_at)",
    ],

    # 26: Drop experiment_hubs — hub-to-interval assignment is handled by groups.scrape_interval.
    26: "DROP TABLE IF EXISTS experiment_hubs",
}


def _migration_21_evse_cleanup(con: sqlite3.Connection) -> None:
    """One-time data fix: strip excluded connectors and recompute total_evses.

    Runs when the image is first deployed after the CHAdeMO exclusion rules
    were introduced.  Updates three things:
      1. connector_types JSON column  — removes excluded types (e.g. CHADEMO)
      2. hub_connectors junction table — deletes excluded-type rows
      3. total_evses                   — recomputed from devices_raw_loc using current
                                         EXCLUDED_CONNECTORS + MIN_SHARED_POWER_W rules
    """
    # 1. Clean connector_types JSON
    con.execute("""
        UPDATE hubs
        SET connector_types = (
            SELECT json_group_array(value)
            FROM json_each(hubs.connector_types)
            WHERE value NOT IN ('CHADEMO')
        )
        WHERE connector_types LIKE '%CHADEMO%'
    """)
    # 2. Clean hub_connectors
    try:
        con.execute("DELETE FROM hub_connectors WHERE connector_type = 'CHADEMO'")
    except Exception:
        pass  # table may not exist on very old schemas — scraper will rebuild it

    # 3. Recount total_evses from devices_raw_loc applying current exclusion rules
    rows = con.execute(
        "SELECT uuid, devices_raw_loc FROM hubs WHERE devices_raw_loc IS NOT NULL"
    ).fetchall()
    updates = []
    for row in rows:
        raw = json.loads(row["devices_raw_loc"] or "[]")
        count = 0
        for dev in raw:
            for evse in dev.get("evses", []):
                conns = [c for c in evse.get("connectors", []) if c.get("standard") not in EXCLUDED_CONNECTORS]
                if not conns:
                    continue
                if max((c.get("max_electric_power") or 0 for c in conns), default=0) < MIN_SHARED_POWER_W:
                    continue
                count += 1
        updates.append((count, row["uuid"]))
    if updates:
        con.executemany("UPDATE hubs SET total_evses = ? WHERE uuid = ?", updates)
    log.info("Migration 21: stripped excluded connectors, recomputed total_evses for %d hubs",
             len(updates))


def _migration_22_power_threshold_update(con: sqlite3.Connection) -> None:
    """Recompute total_evses after lowering the power threshold from 300kW to 150kW.

    Migration 21 set total_evses using MIN_SHARED_POWER_W = 300_000, which excluded
    the majority of dedicated 150kW CCS2 EVSEs.  This migration recounts from
    devices_raw_loc using the updated MIN_SHARED_POWER_W = 150_000 threshold.
    """
    rows = con.execute(
        "SELECT uuid, devices_raw_loc FROM hubs WHERE devices_raw_loc IS NOT NULL"
    ).fetchall()
    updates = []
    for row in rows:
        raw = json.loads(row["devices_raw_loc"] or "[]")
        count = 0
        for dev in raw:
            for evse in dev.get("evses", []):
                conns = [c for c in evse.get("connectors", []) if c.get("standard") not in EXCLUDED_CONNECTORS]
                if not conns:
                    continue
                if max((c.get("max_electric_power") or 0 for c in conns), default=0) < MIN_SHARED_POWER_W:
                    continue
                count += 1
        updates.append((count, row["uuid"]))
    if updates:
        con.executemany("UPDATE hubs SET total_evses = ? WHERE uuid = ?", updates)
    log.info("Migration 22: recomputed total_evses (150kW threshold) for %d hubs", len(updates))


_PYTHON_MIGRATIONS = {
    "_python_migration_21": _migration_21_evse_cleanup,
    "_python_migration_22": _migration_22_power_threshold_update,
}


def _run_migrations(con: sqlite3.Connection) -> None:
    """Apply any pending schema migrations and record them in schema_version."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version    INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
    """)
    current: int = con.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
    now = datetime.now(timezone.utc).isoformat()
    for version, payload in sorted(_MIGRATIONS.items()):
        if version <= current:
            continue
        if isinstance(payload, str) and payload.startswith("_python_migration_"):
            # Python callable migration — resolved via _PYTHON_MIGRATIONS registry
            fn = _PYTHON_MIGRATIONS.get(payload)
            if fn:
                fn(con)
        elif isinstance(payload, str):
            # Single-statement SQL migration — catch duplicate-column errors for the
            # historical ALTER TABLE shims (migrations 1–16) that may already be
            # applied on older databases.
            try:
                con.execute(payload)
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise
        else:
            # Multi-statement migration (e.g. table rebuild) — all errors propagate.
            for stmt in payload:
                con.execute(stmt)
        con.execute(
            "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
            (version, now),
        )
        con.commit()
        log.debug("db migration %d applied", version)


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
            estimated_kwh       REAL,
            snapshot_max_kw     REAL,
            source              TEXT NOT NULL DEFAULT 'full'
                                    CHECK(source IN ('full', 'targeted'))
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_hub_time
            ON snapshots(hub_uuid, scraped_at);

        CREATE INDEX IF NOT EXISTS idx_snapshots_time
            ON snapshots(scraped_at);

        CREATE INDEX IF NOT EXISTS idx_snapshots_covering
            ON snapshots(hub_uuid, scraped_at, utilisation_pct, charging_count);

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
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL UNIQUE,
            created_at      TEXT NOT NULL,
            scrape_interval INTEGER DEFAULT NULL
                                CHECK(scrape_interval IN (1,2,3,4,5))
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
                           CHECK(status IN ('AVAILABLE', 'CHARGING', 'INOPERATIVE',
                                            'OUTOFORDER', 'UNKNOWN'))
        );

        CREATE INDEX IF NOT EXISTS idx_evse_events_evse_time
            ON evse_events(evse_uuid, scraped_at);

        CREATE INDEX IF NOT EXISTS idx_evse_events_hub_time
            ON evse_events(hub_uuid, scraped_at);

        CREATE TABLE IF NOT EXISTS hub_connectors (
            hub_uuid       TEXT NOT NULL REFERENCES hubs(uuid),
            connector_type TEXT NOT NULL,
            PRIMARY KEY (hub_uuid, connector_type)
        );

        CREATE INDEX IF NOT EXISTS idx_hub_connectors_type
            ON hub_connectors(connector_type);
    """)
    _run_migrations(con)
    result = con.execute("PRAGMA quick_check").fetchone()
    if result and result[0] != "ok":
        log.error("DB integrity check FAILED: %s — scheduler will continue but DB may be corrupt", result[0])
    else:
        log.debug("DB integrity check passed")
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
        connector_types = r.get("connector_types", [])
        con.execute("DELETE FROM hub_connectors WHERE hub_uuid = ?", (r["uuid"],))
        if connector_types:
            con.executemany(
                "INSERT OR IGNORE INTO hub_connectors (hub_uuid, connector_type) VALUES (?, ?)",
                [(r["uuid"], ct) for ct in connector_types],
            )
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
            max_kw = None
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
            max_kw or None,   # snapshot_max_kw — NULL when kW is unknown/zero
            source,
        ))
    count_before = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    con.executemany("""
        INSERT INTO snapshots
            (hub_uuid, scraped_at, available_count, charging_count,
             inoperative_count, out_of_order_count, unknown_count, utilisation_pct,
             estimated_kwh, snapshot_max_kw, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    purge_old_snapshots(con)
    con.commit()
    count_after = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    log.info("insert_snapshots: %d → %d (+%d)", count_before, count_after, count_after - count_before)
    if source == 'full':
        rows = con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()
        if rows:
            busy, log_pages, ckpt_pages = rows[0][0], rows[0][1], rows[0][2]
            if busy > 0 or log_pages != ckpt_pages:
                log.warning(
                    "wal_checkpoint incomplete: busy=%d log_pages=%d checkpointed=%d",
                    busy, log_pages, ckpt_pages,
                )
            else:
                log.debug("wal_checkpoint: log_pages=%d checkpointed=%d", log_pages, ckpt_pages)
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
    d["connector_types"] = [ct for ct in json.loads(d["connector_types"] or "[]")
                            if ct not in EXCLUDED_CONNECTORS]
    d["pricing"] = json.loads(d["pricing"] or "[]")
    d["payment_methods"] = json.loads(d["payment_methods"] or "[]")
    return d


def get_latest_snapshot_per_hub() -> list[dict]:
    """Return each hub's most recent snapshot merged with hub static data."""
    con = _connect()
    rows = con.execute(f"""
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
        WHERE h.total_evses >= {MIN_EVSES}
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
        WHERE h.total_evses >= {MIN_EVSES}
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


def get_connector_types() -> list[dict]:
    """Distinct connector types present in the DB with hub counts, ordered by prevalence."""
    con = _connect()
    rows = con.execute("""
        SELECT connector_type, COUNT(DISTINCT hub_uuid) AS hub_count
        FROM hub_connectors
        GROUP BY connector_type
        ORDER BY hub_count DESC
    """).fetchall()
    con.close()
    return [dict(r) for r in rows]


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
    d["connector_types"]  = [ct for ct in json.loads(d["connector_types"] or "[]")
                             if ct not in EXCLUDED_CONNECTORS]
    d["pricing"]          = json.loads(d["pricing"] or "[]")
    d["payment_methods"]  = json.loads(d["payment_methods"] or "[]")

    # Filter device blobs at read time — cleans up stale DB rows that pre-date
    # the scraper-level exclusion rules (excluded connectors + sub-150kW power).
    # devices_raw_loc: connectors are objects with 'standard'+'max_electric_power'.
    # latest_devices_status: connectors are plain strings (no power info), so we
    # cross-reference qualifying EVSE UUIDs derived from devices_raw_loc.
    raw = json.loads(d["devices_raw_loc"] or "[]")
    qualifying_evse_uuids: set = set()
    filtered_raw = []
    for dev in raw:
        kept = [
            e for e in dev.get("evses", [])
            if not {c.get("standard") for c in e.get("connectors", [])} & EXCLUDED_CONNECTORS
            and max((c.get("max_electric_power") or 0 for c in e.get("connectors", [])), default=0)
                >= MIN_SHARED_POWER_W
        ]
        if kept:
            filtered_raw.append({**dev, "evses": kept})
            for e in kept:
                qualifying_evse_uuids.add(e.get("uuid"))
    d["devices_raw_loc"] = filtered_raw

    live = json.loads(d["latest_devices_status"] or "[]")
    filtered_live = []
    for dev in live:
        kept = [
            e for e in dev.get("evses", [])
            if not set(e.get("connectors", [])) & EXCLUDED_CONNECTORS
            and (not qualifying_evse_uuids or e.get("evse_uuid") in qualifying_evse_uuids)
        ]
        if kept:
            filtered_live.append({**dev, "evses": kept})
    d["latest_devices_status"] = filtered_live

    return d


def get_devices_raw_for_hubs(uuids: list[str]) -> dict[str, list]:
    """Return {hub_uuid: devices_raw_loc} for the given hub UUIDs.

    Used by the scraper's db-only snapshot loop to determine qualifying EVSE UUIDs
    (power + connector filter) without re-fetching location details from the API.
    """
    if not uuids:
        return {}
    placeholders = ",".join("?" * len(uuids))
    con = _connect()
    rows = con.execute(
        f"SELECT uuid, devices_raw_loc FROM hubs WHERE uuid IN ({placeholders})",
        uuids,
    ).fetchall()
    con.close()
    return {row["uuid"]: json.loads(row["devices_raw_loc"] or "[]") for row in rows}


def get_hub_performance(hours: int = 168,
                        start_dt: str | None = None, end_dt: str | None = None,
                        operator: str | list[str] | None = None,
                        connector: str | None = None,
                        min_kw: float | None = None, max_kw: float | None = None,
                        min_evses: int | None = None, max_evses: int | None = None,
                        group_ids: list[int] | None = None) -> list[dict]:
    """Per-hub performance stats covering three metrics:
    - active_pct: % of scrape intervals where charging_count > 0 (site sustaining traffic)
    - full_capacity_pct / full_capacity_hours: time at max capacity
    - visits_per_day / avg_dwell_min: visit throughput from the visits table

    All three metrics use the same date window.
    """
    interval_minutes = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "15"))

    if start_dt and end_dt:
        where_time = "s.scraped_at >= ? AND s.scraped_at <= ?"
        snap_params: list = [_parse_dt(start_dt), _parse_dt(end_dt)]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        where_time = "s.scraped_at >= ?"
        snap_params = [cutoff]

    hub_attr_filter = _hub_subquery(snap_params, operator, connector, min_kw, max_kw, min_evses, max_evses, group_ids=group_ids)
    # _hub_subquery references columns via hub_uuid; swap to s.hub_uuid context
    snap_hub_filter = hub_attr_filter.replace(" AND hub_uuid IN ", " AND s.hub_uuid IN ")

    con = _connect()
    snap_rows = con.execute(f"""
        SELECT
            h.uuid,
            h.hub_name,
            h.operator,
            h.max_power_kw,
            h.total_evses,
            COUNT(s.id) AS total_snapshots,
            SUM(CASE WHEN s.charging_count > 0 THEN 1 ELSE 0 END) AS active_snapshots,
            ROUND(100.0 * SUM(CASE WHEN s.charging_count > 0 THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(s.id), 0), 1) AS active_pct,
            SUM(CASE WHEN h.total_evses > 0
                          AND s.charging_count >= h.total_evses THEN 1 ELSE 0 END) AS full_capacity_snapshots,
            ROUND(100.0 * SUM(CASE WHEN h.total_evses > 0
                                        AND s.charging_count >= h.total_evses THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(s.id), 0), 1) AS full_capacity_pct,
            ROUND(1.0 * SUM(CASE WHEN h.total_evses > 0
                                      AND s.charging_count >= h.total_evses THEN 1 ELSE 0 END)
                  * {interval_minutes} / 60.0, 1) AS full_capacity_hours,
            ROUND(100.0 * SUM(s.charging_count) /
                  NULLIF(SUM(s.available_count + s.charging_count + s.unknown_count), 0), 1) AS avg_utilisation_pct
        FROM snapshots s
        JOIN hubs h ON h.uuid = s.hub_uuid
        WHERE {where_time}{snap_hub_filter}
        GROUP BY h.uuid
        ORDER BY active_pct DESC
    """, snap_params).fetchall()

    # Build visits stats over the same window
    if start_dt and end_dt:
        v_start = _parse_dt(start_dt)
        v_end = _parse_dt(end_dt)
    else:
        v_start = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        v_end = datetime.now(timezone.utc).isoformat()
    v_params: list = [v_start, v_end]
    v_hub_filter = _hub_subquery(v_params, operator, connector, min_kw, max_kw, min_evses, max_evses, group_ids=group_ids)
    visit_rows = con.execute(f"""
        SELECT
            hub_uuid,
            COUNT(*) AS total_visits,
            COUNT(DISTINCT DATE(started_at)) AS visit_days,
            ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT DATE(started_at)), 0), 1) AS visits_per_day,
            ROUND(AVG(CASE WHEN dwell_min IS NOT NULL AND dwell_min > 0 THEN dwell_min END), 0) AS avg_dwell_min
        FROM visits
        WHERE started_at >= ? AND started_at <= ?{v_hub_filter}
        GROUP BY hub_uuid
    """, v_params).fetchall()
    con.close()

    visit_map = {r["hub_uuid"]: dict(r) for r in visit_rows}

    result = []
    for row in snap_rows:
        d = dict(row)
        v = visit_map.get(d["uuid"], {})
        d["total_visits"]   = v.get("total_visits", 0)
        d["visits_per_day"] = v.get("visits_per_day", 0.0)
        d["avg_dwell_min"]  = v.get("avg_dwell_min")
        result.append(d)
    return result


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

    _VALID_STATUSES = frozenset({"AVAILABLE", "CHARGING", "INOPERATIVE", "OUTOFORDER", "UNKNOWN"})

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
                raw_status = (evse.get("network_status") or "UNKNOWN").upper()
                new_status = raw_status if raw_status in _VALID_STATUSES else "UNKNOWN"

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
    cutoff_full = (datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)).isoformat()
    cutoff_targeted = (datetime.now(timezone.utc) - timedelta(days=TARGETED_SNAPSHOT_RETENTION_DAYS)).isoformat()
    cur = con.execute("DELETE FROM snapshots WHERE source='full' AND scraped_at < ?", (cutoff_full,))
    if cur.rowcount:
        log.info("purge_old_snapshots: deleted %d full rows older than %d days", cur.rowcount, SNAPSHOT_RETENTION_DAYS)
    cur = con.execute("DELETE FROM snapshots WHERE source='targeted' AND scraped_at < ?", (cutoff_targeted,))
    if cur.rowcount:
        log.info("purge_old_snapshots: deleted %d targeted rows older than %d days", cur.rowcount, TARGETED_SNAPSHOT_RETENTION_DAYS)


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
        SELECT g.id, g.name, g.created_at, g.scrape_interval,
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
            "SELECT id, name, created_at, NULL AS scrape_interval, 0 AS hub_count FROM groups WHERE id = ?",
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


def set_group_scrape_interval(group_id: int, minutes: int | None) -> dict | None:
    """Set scrape_interval for a group. minutes must be 1–5 or None to disable."""
    con = _connect()
    cur = con.execute(
        "UPDATE groups SET scrape_interval = ? WHERE id = ?",
        (minutes, group_id),
    )
    con.commit()
    if cur.rowcount == 0:
        con.close()
        return None
    row = con.execute("SELECT * FROM groups WHERE id = ?", (group_id,)).fetchone()
    con.close()
    return dict(row) if row else None


def get_hubs_for_scrape_interval(minutes: int) -> list[str]:
    """Return distinct hub UUIDs for all groups with the given scrape_interval."""
    con = _connect()
    rows = con.execute("""
        SELECT DISTINCT gh.hub_uuid
        FROM group_hubs gh
        JOIN groups g ON g.id = gh.group_id
        WHERE g.scrape_interval = ?
    """, (minutes,)).fetchall()
    con.close()
    return [r["hub_uuid"] for r in rows]


def get_all_targeted_hub_uuids() -> set[str]:
    """Return UUIDs of all hubs covered by any targeted scrape group (scrape_interval IS NOT NULL)."""
    con = _connect()
    rows = con.execute("""
        SELECT DISTINCT gh.hub_uuid
        FROM group_hubs gh
        JOIN groups g ON g.id = gh.group_id
        WHERE g.scrape_interval IS NOT NULL
    """).fetchall()
    con.close()
    return {r["hub_uuid"] for r in rows}


def get_setting(key: str, default: str = "") -> str:
    con = _connect()
    row = con.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    con.close()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    con = _connect()
    con.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    con.commit()
    con.close()


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


# ---------------------------------------------------------------------------
# Polling-interval experiment — independent per-interval tracking
# Hub assignment is managed through groups.scrape_interval (existing groups UI).
# ---------------------------------------------------------------------------

def get_hubs_for_scrape_interval(scrape_interval: int) -> list[str]:
    """Return UUIDs of all hubs in any group assigned to this scrape_interval."""
    con = _connect()
    rows = con.execute("""
        SELECT DISTINCT gh.hub_uuid
        FROM group_hubs gh
        JOIN groups g ON g.id = gh.group_id
        WHERE g.scrape_interval = ?
    """, (scrape_interval,)).fetchall()
    con.close()
    return [r["hub_uuid"] for r in rows]


def detect_targeted_evse_changes(
    records: list[dict], poll_interval_min: int, con: sqlite3.Connection
) -> None:
    """Independent visit detection for one polling interval.

    Reads prior EVSE state exclusively from targeted_evse_events for this interval —
    never touches hubs.latest_devices_status or the main evse_events table.
    """
    if not records:
        return

    hub_uuids = [r["uuid"] for r in records]
    ph = ",".join("?" * len(hub_uuids))

    # Latest known status per EVSE for this interval
    prev_rows = con.execute(f"""
        SELECT t.evse_uuid, t.hub_uuid, t.status
        FROM targeted_evse_events t
        INNER JOIN (
            SELECT evse_uuid, MAX(scraped_at) AS max_at
            FROM targeted_evse_events
            WHERE poll_interval_min = ? AND hub_uuid IN ({ph})
            GROUP BY evse_uuid
        ) latest ON t.evse_uuid = latest.evse_uuid AND t.scraped_at = latest.max_at
        WHERE t.poll_interval_min = ?
    """, [poll_interval_min] + hub_uuids + [poll_interval_min]).fetchall()

    old_status_map: dict[str, dict[str, str]] = {}
    for row in prev_rows:
        old_status_map.setdefault(row["hub_uuid"], {})[row["evse_uuid"]] = row["status"]

    _VALID = frozenset({"AVAILABLE", "CHARGING", "INOPERATIVE", "OUTOFORDER", "UNKNOWN"})

    event_rows: list[tuple] = []
    visit_opens: list[tuple] = []   # (hub_uuid, evse_uuid, scraped_at)
    visit_closes: list[tuple] = []  # (evse_uuid, scraped_at)

    for r in records:
        hub_uuid  = r["uuid"]
        scraped_at = r["scraped_at"]
        old_evse_map = old_status_map.get(hub_uuid)

        for device in r.get("devices", []):
            for evse in device.get("evses", []):
                evse_uuid = evse.get("evse_uuid")
                if not evse_uuid:
                    continue
                raw = (evse.get("network_status") or "UNKNOWN").upper()
                new_status = raw if raw in _VALID else "UNKNOWN"

                if old_evse_map is None:
                    # First poll for this hub at this interval — record state, no visit action
                    event_rows.append((poll_interval_min, evse_uuid, hub_uuid, scraped_at, new_status))
                    continue

                old_status = old_evse_map.get(evse_uuid)
                if old_status is None:
                    # New EVSE within an existing hub — record state, no visit action
                    event_rows.append((poll_interval_min, evse_uuid, hub_uuid, scraped_at, new_status))
                    continue

                if old_status == new_status:
                    continue

                event_rows.append((poll_interval_min, evse_uuid, hub_uuid, scraped_at, new_status))

                if new_status == "CHARGING":
                    visit_opens.append((hub_uuid, evse_uuid, scraped_at))
                if old_status == "CHARGING":
                    visit_closes.append((evse_uuid, scraped_at))

    if event_rows:
        con.executemany(
            "INSERT INTO targeted_evse_events "
            "(poll_interval_min, evse_uuid, hub_uuid, scraped_at, status) VALUES (?,?,?,?,?)",
            event_rows,
        )

    for hub_uuid, evse_uuid, scraped_at in visit_opens:
        con.execute(
            "INSERT INTO targeted_visits (poll_interval_min, hub_uuid, evse_uuid, started_at) "
            "VALUES (?, ?, ?, ?)",
            (poll_interval_min, hub_uuid, evse_uuid, scraped_at),
        )

    for evse_uuid, close_scraped_at in visit_closes:
        open_visit = con.execute("""
            SELECT id, started_at FROM targeted_visits
            WHERE poll_interval_min = ? AND evse_uuid = ? AND ended_at IS NULL
            ORDER BY started_at DESC LIMIT 1
        """, (poll_interval_min, evse_uuid)).fetchone()
        if open_visit:
            start = datetime.fromisoformat(open_visit["started_at"].replace("Z", "+00:00"))
            end   = datetime.fromisoformat(close_scraped_at.replace("Z", "+00:00"))
            con.execute(
                "UPDATE targeted_visits SET ended_at = ?, dwell_min = ? WHERE id = ?",
                (close_scraped_at, round((end - start).total_seconds() / 60), open_visit["id"]),
            )

    log.info(
        "detect_targeted_evse_changes [%dmin]: %d events, %d opens, %d closes",
        poll_interval_min, len(event_rows), len(visit_opens), len(visit_closes),
    )


def close_stale_targeted_visits(
    poll_interval_min: int, con: sqlite3.Connection, max_hours: int = 12
) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_hours)).isoformat()
    ids = [
        r["id"] for r in con.execute(
            "SELECT id FROM targeted_visits "
            "WHERE poll_interval_min = ? AND ended_at IS NULL AND started_at < ?",
            (poll_interval_min, cutoff),
        ).fetchall()
    ]
    if ids:
        ph = ",".join("?" * len(ids))
        con.execute(
            f"UPDATE targeted_visits SET ended_at = ?, dwell_min = NULL WHERE id IN ({ph})",
            [cutoff] + ids,
        )
        log.info("close_stale_targeted_visits [%dmin]: force-closed %d visits", poll_interval_min, len(ids))


def insert_targeted_snapshots(records: list[dict], poll_interval_min: int, con: sqlite3.Connection) -> None:
    if not records:
        return
    hub_uuids = [r["uuid"] for r in records]
    ph = ",".join("?" * len(hub_uuids))
    evse_counts = {
        r["uuid"]: r["total_evses"]
        for r in con.execute(f"SELECT uuid, total_evses FROM hubs WHERE uuid IN ({ph})", hub_uuids).fetchall()
    }
    rows = []
    for r in records:
        total = evse_counts.get(r["uuid"]) or 0
        util  = round(100.0 * r["charging_count"] / total, 1) if total > 0 else None
        rows.append((poll_interval_min, r["uuid"], r["scraped_at"], r["charging_count"], util))
    con.executemany(
        "INSERT INTO targeted_snapshots "
        "(poll_interval_min, hub_uuid, scraped_at, charging_count, utilisation_pct) VALUES (?,?,?,?,?)",
        rows,
    )


def purge_old_targeted_data(con: sqlite3.Connection) -> None:
    evt_cutoff  = (datetime.now(timezone.utc) - timedelta(days=EVSE_EVENT_RETENTION_DAYS)).isoformat()
    snap_cutoff = (datetime.now(timezone.utc) - timedelta(days=TARGETED_SNAPSHOT_RETENTION_DAYS)).isoformat()
    cur = con.execute("DELETE FROM targeted_evse_events WHERE scraped_at < ?", (evt_cutoff,))
    if cur.rowcount:
        log.info("purge_old_targeted_data: deleted %d targeted_evse_events", cur.rowcount)
    cur = con.execute("DELETE FROM targeted_snapshots WHERE scraped_at < ?", (snap_cutoff,))
    if cur.rowcount:
        log.info("purge_old_targeted_data: deleted %d targeted_snapshots", cur.rowcount)


def process_targeted_evse_events(records: list[dict], poll_interval_min: int) -> None:
    """Entry point called from scraper after each targeted poll.

    Writes to both the main visits table (for Live Status) and the targeted_*
    tables (for interval comparison). detect_evse_changes must run before
    update_latest_devices_status so it reads the previous state correctly.
    """
    con = _connect()
    try:
        detect_evse_changes(records, con)
        detect_targeted_evse_changes(records, poll_interval_min, con)
        close_stale_targeted_visits(poll_interval_min, con)
        insert_targeted_snapshots(records, poll_interval_min, con)
        purge_old_targeted_data(con)
        con.commit()
    finally:
        con.close()
    update_latest_devices_status(records)


def get_interval_comparison_data(hub_uuid: str, hours: int = 24) -> dict:
    """Return per-interval visit counts + raw timeline snapshots for one hub."""
    con = _connect()
    window_start = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    visit_rows = con.execute("""
        SELECT poll_interval_min,
               COUNT(*)                                             AS total_visits,
               COUNT(DISTINCT DATE(started_at))                     AS visit_days,
               ROUND(AVG(CASE WHEN ended_at IS NOT NULL
                               THEN dwell_min END))                 AS avg_dwell_min
        FROM targeted_visits
        WHERE hub_uuid = ? AND started_at >= ?
        GROUP BY poll_interval_min
    """, (hub_uuid, window_start)).fetchall()

    snap_rows = con.execute("""
        SELECT poll_interval_min, scraped_at, charging_count, utilisation_pct
        FROM targeted_snapshots
        WHERE hub_uuid = ? AND scraped_at >= ?
        ORDER BY scraped_at, poll_interval_min
    """, (hub_uuid, window_start)).fetchall()

    con.close()

    visit_by_interval = {r["poll_interval_min"]: dict(r) for r in visit_rows}
    snapshots_by_interval: dict[int, list] = {}
    for r in snap_rows:
        snapshots_by_interval.setdefault(r["poll_interval_min"], []).append({
            "scraped_at": r["scraped_at"],
            "charging_count": r["charging_count"],
            "utilisation_pct": r["utilisation_pct"],
        })

    return {
        "hub_uuid": hub_uuid,
        "hours": hours,
        "visit_stats": visit_by_interval,
        "snapshots": snapshots_by_interval,
    }


def get_stats() -> dict:
    con = _connect()
    hub_count = con.execute(
        "SELECT COUNT(*) FROM hubs WHERE total_evses >= ?", (MIN_EVSES,)
    ).fetchone()[0]
    last_scraped = con.execute(
        "SELECT MAX(scraped_at) FROM snapshots WHERE source = 'full'"
    ).fetchone()[0]

    # stats from the most recent full scrape run only (excludes targeted 1-min snapshots)
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
            WHERE scraped_at = ? AND source = 'full'
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
