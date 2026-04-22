"""
SQLite storage layer — schema management, non-destructive migrations,
and query functions for the earnings events database.

The database is the source of truth for workflow state and historical memory.
Google Calendar is the source of truth for published event state.
"""

import sqlite3
import logging
from pathlib import Path

from config import DB_PATH

logger = logging.getLogger("earnings_agent")

# ---------------------------------------------------------------------------
# Schema version tracking
# ---------------------------------------------------------------------------

CURRENT_SCHEMA_VERSION = 8  # Bump when adding migrations

_MIGRATIONS = {
    # Version 7 → 8: URL of the press release that confirmed this event's
    # date, when one is detected by the RSS announcement scanner. Stored
    # so we can show provenance in alerts and avoid re-confirming via RSS
    # on events that are already anchored to a specific announcement.
    8: [
        "ALTER TABLE events ADD COLUMN announcement_url TEXT",
    ],

    # Version 6 → 7: Backfill date_confirmed from event_hour. Every row
    # whose hour is bmo/amc/dmh is retroactively flagged confirmed — the
    # v6 migration only added the column, not the initial values, so
    # rows that don't upsert on the next sync (skip_count path) would
    # otherwise stay at the default 0.
    7: [
        "UPDATE events SET date_confirmed = 1 "
        "WHERE LOWER(COALESCE(event_hour, '')) IN ('bmo', 'amc', 'dmh')",
    ],

    # Version 5 → 6: Confirmed vs estimated flag. Set to 1 when Finnhub's
    # `hour` field is populated (bmo/amc/dmh) — indicates the company has
    # announced timing, which in practice means the date is confirmed too.
    # Empty hour = Finnhub is projecting from historical cadence; the
    # release date has not been announced by the company.
    6: [
        "ALTER TABLE events ADD COLUMN date_confirmed INTEGER NOT NULL DEFAULT 0",
    ],

    # Version 4 → 5: Record the last yfinance date(s) that the cross-check
    # alerted on for each event. Lets the B1 cross-check suppress daily
    # repeat alerts when the disagreement state hasn't changed, and re-fire
    # when yfinance updates or agreement is restored.
    5: [
        "ALTER TABLE events ADD COLUMN last_xcheck_yf_dates TEXT",
    ],

    # Version 3 → 4: Human override flag. When date_locked = 1, the sync
    # and reconcile jobs will not move the calendar event's date even if
    # Finnhub disagrees. Used when the user has verified the date via IR
    # and considers Finnhub to be wrong.
    4: [
        "ALTER TABLE events ADD COLUMN date_locked INTEGER NOT NULL DEFAULT 0",
    ],

    # Version 2 → 3: Track how many consecutive runs a Tier 1/2 event has
    # gone missing from Finnhub, so we can alert when a name persistently
    # disappears (possible data loss or coverage drop).
    3: [
        "ALTER TABLE events ADD COLUMN unseen_run_count INTEGER NOT NULL DEFAULT 0",
    ],

    # Version 1 → 2: Add new columns and tables for the earnings intelligence system.
    # Also transitions the dedup key from UNIQUE(ticker, quarter) to UNIQUE(ticker, event_date).
    2: [
        # Add columns to events table (safe if they already exist — checked before running)
        "ALTER TABLE events ADD COLUMN tier INTEGER NOT NULL DEFAULT 3",
        "ALTER TABLE events ADD COLUMN source_fingerprint TEXT",
        "ALTER TABLE events ADD COLUMN company_name TEXT",
        "ALTER TABLE events ADD COLUMN ir_url TEXT",
        "ALTER TABLE events ADD COLUMN call_url TEXT",
        "ALTER TABLE events ADD COLUMN ticktick_task_id TEXT",

        # Add unique index on (ticker, event_date) for the new dedup key.
        # The old UNIQUE(ticker, quarter) constraint remains but is harmless.
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_ticker_date ON events(ticker, event_date)",

        # estimate_history table — consensus snapshots for revision tracking
        """CREATE TABLE IF NOT EXISTS estimate_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            event_date TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            eps_estimate REAL,
            revenue_estimate REAL,
            UNIQUE(ticker, event_date, snapshot_date)
        )""",

        # predictions table — user predictions and outcomes
        """CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            event_date TEXT NOT NULL,
            predicted_direction TEXT,
            predicted_eps REAL,
            predicted_revenue REAL,
            predicted_ebitda REAL,
            position_stance TEXT,
            thesis_note TEXT,
            prediction_date TEXT DEFAULT (datetime('now')),
            actual_direction TEXT,
            was_correct INTEGER,
            post_earnings_move_pct REAL,
            UNIQUE(ticker, event_date)
        )""",

        # review_status table — TickTick task tracking
        """CREATE TABLE IF NOT EXISTS review_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            event_date TEXT NOT NULL,
            ticktick_task_id TEXT,
            reviewed INTEGER DEFAULT 0,
            reviewed_at TEXT,
            UNIQUE(ticker, event_date)
        )""",
    ],
}


def _get_schema_version(conn: sqlite3.Connection) -> int:
    """Get the current schema version from the database."""
    try:
        cur = conn.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        # schema_version table doesn't exist yet
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int):
    """Record the schema version."""
    conn.execute(
        "INSERT INTO schema_version (version, applied_at) VALUES (?, datetime('now'))",
        (version,),
    )
    conn.commit()


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Check if a table exists."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _run_migrations(conn: sqlite3.Connection, current_version: int):
    """Run all pending migrations."""
    for version in sorted(_MIGRATIONS.keys()):
        if version <= current_version:
            continue

        logger.info(f"Running migration to schema version {version}...")
        for sql in _MIGRATIONS[version]:
            sql_stripped = sql.strip()
            # Skip ALTER TABLE ADD COLUMN if column already exists
            if sql_stripped.upper().startswith("ALTER TABLE") and "ADD COLUMN" in sql_stripped.upper():
                parts = sql_stripped.split()
                # Parse: ALTER TABLE <table> ADD COLUMN <column> ...
                try:
                    table_idx = next(i for i, p in enumerate(parts) if p.upper() == "TABLE") + 1
                    col_idx = next(i for i, p in enumerate(parts) if p.upper() == "COLUMN") + 1
                    table_name = parts[table_idx]
                    col_name = parts[col_idx]
                    if _column_exists(conn, table_name, col_name):
                        logger.debug(f"  Column {table_name}.{col_name} already exists, skipping")
                        continue
                except (StopIteration, IndexError):
                    pass  # Can't parse — just try to run it

            try:
                conn.execute(sql)
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    logger.debug(f"  Column already exists, skipping: {e}")
                elif "already exists" in str(e).lower():
                    logger.debug(f"  Object already exists, skipping: {e}")
                else:
                    raise

        _set_schema_version(conn, version)
        logger.info(f"Migration to schema version {version} complete.")


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Initialize the SQLite database with non-destructive migrations.

    - Creates the schema from scratch if the database is new.
    - Applies incremental migrations for existing databases.
    - Never drops tables or deletes data.
    """
    conn = sqlite3.connect(str(db_path))

    # Ensure schema_version table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
    """)
    conn.commit()

    current_version = _get_schema_version(conn)

    if not _table_exists(conn, "events"):
        # Fresh database — create the full schema
        conn.execute("""
            CREATE TABLE events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT    NOT NULL,
                quarter         TEXT,
                event_date      TEXT    NOT NULL,
                event_hour      TEXT,
                gcal_id         TEXT,
                eps_estimate    REAL,
                eps_actual      REAL,
                rev_estimate    REAL,
                rev_actual      REAL,
                reported        INTEGER NOT NULL DEFAULT 0,
                tier            INTEGER NOT NULL DEFAULT 3,
                source_fingerprint TEXT,
                company_name    TEXT,
                ir_url          TEXT,
                call_url        TEXT,
                ticktick_task_id TEXT,
                unseen_run_count INTEGER NOT NULL DEFAULT 0,
                date_locked     INTEGER NOT NULL DEFAULT 0,
                last_xcheck_yf_dates TEXT,
                date_confirmed  INTEGER NOT NULL DEFAULT 0,
                announcement_url TEXT,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(ticker, event_date)
            )
        """)

        # Create all other tables
        for sql_list in _MIGRATIONS.values():
            for sql in sql_list:
                if sql.strip().upper().startswith("CREATE TABLE"):
                    try:
                        conn.execute(sql)
                    except sqlite3.OperationalError:
                        pass  # Already exists

        _set_schema_version(conn, CURRENT_SCHEMA_VERSION)
        conn.commit()
        logger.info(f"Created fresh database at schema version {CURRENT_SCHEMA_VERSION}")
    elif current_version < CURRENT_SCHEMA_VERSION:
        _run_migrations(conn, current_version)
    else:
        logger.debug(f"Database at schema version {current_version}, no migrations needed")

    return conn


# ---------------------------------------------------------------------------
# Event queries
# ---------------------------------------------------------------------------


def find_existing_event(conn: sqlite3.Connection, ticker: str, event_date: str) -> dict | None:
    """Look up an existing event by ticker + event_date. Returns dict or None."""
    cur = conn.execute(
        "SELECT id, ticker, quarter, event_date, event_hour, gcal_id, "
        "eps_estimate, eps_actual, rev_estimate, rev_actual, reported, "
        "tier, company_name, ticktick_task_id, date_locked, date_confirmed "
        "FROM events WHERE ticker = ? AND event_date = ?",
        (ticker, event_date),
    )
    row = cur.fetchone()
    if row:
        return {
            "id": row[0], "ticker": row[1], "quarter": row[2],
            "event_date": row[3], "event_hour": row[4], "gcal_id": row[5],
            "eps_estimate": row[6], "eps_actual": row[7],
            "rev_estimate": row[8], "rev_actual": row[9],
            "reported": bool(row[10]), "tier": row[11],
            "company_name": row[12], "ticktick_task_id": row[13],
            "date_locked": bool(row[14]),
            "date_confirmed": bool(row[15]),
        }
    return None


def find_event_by_ticker_quarter(conn: sqlite3.Connection, ticker: str, quarter: str) -> dict | None:
    """Legacy lookup by ticker + quarter (for backward compatibility with old data)."""
    cur = conn.execute(
        "SELECT id, ticker, quarter, event_date, event_hour, gcal_id, "
        "eps_estimate, eps_actual, rev_estimate, rev_actual, reported "
        "FROM events WHERE ticker = ? AND quarter = ?",
        (ticker, quarter),
    )
    row = cur.fetchone()
    if row:
        return {
            "id": row[0], "ticker": row[1], "quarter": row[2],
            "event_date": row[3], "event_hour": row[4], "gcal_id": row[5],
            "eps_estimate": row[6], "eps_actual": row[7],
            "rev_estimate": row[8], "rev_actual": row[9],
            "reported": bool(row[10]),
        }
    return None


def find_event_for_ticker_near_date(
    conn: sqlite3.Connection, ticker: str, event_date: str, window_days: int = 14
) -> dict | None:
    """
    Find an existing event for a ticker within a date window.
    Useful when Finnhub reports a slightly different date for the same earnings event.
    """
    cur = conn.execute(
        "SELECT id, ticker, quarter, event_date, event_hour, gcal_id, "
        "eps_estimate, eps_actual, rev_estimate, rev_actual, reported, tier, "
        "date_locked "
        "FROM events WHERE ticker = ? "
        "AND julianday(event_date) BETWEEN julianday(?) - ? AND julianday(?) + ? "
        "ORDER BY ABS(julianday(event_date) - julianday(?)) LIMIT 1",
        (ticker, event_date, window_days, event_date, window_days, event_date),
    )
    row = cur.fetchone()
    if row:
        return {
            "id": row[0], "ticker": row[1], "quarter": row[2],
            "event_date": row[3], "event_hour": row[4], "gcal_id": row[5],
            "eps_estimate": row[6], "eps_actual": row[7],
            "rev_estimate": row[8], "rev_actual": row[9],
            "reported": bool(row[10]), "tier": row[11],
            "date_locked": bool(row[12]),
        }
    return None


def upsert_event(
    conn: sqlite3.Connection,
    ticker: str,
    event_date: str,
    event_hour: str | None,
    gcal_id: str | None,
    *,
    quarter: str | None = None,
    eps_estimate: float | None = None,
    eps_actual: float | None = None,
    rev_estimate: float | None = None,
    rev_actual: float | None = None,
    reported: bool = False,
    tier: int = 3,
    company_name: str | None = None,
    source_fingerprint: str | None = None,
):
    """
    Insert or update an event, keyed on (ticker, event_date).

    The `date_confirmed` column is derived from `event_hour`: Finnhub
    populates `bmo`/`amc`/`dmh` only when the company has announced
    timing, which in practice signals the date is confirmed.
    """
    if source_fingerprint is None:
        source_fingerprint = f"{ticker}:{event_date}"

    date_confirmed = int((event_hour or "").lower() in ("bmo", "amc", "dmh"))

    # Try update first (handles both old UNIQUE(ticker, quarter) and new UNIQUE(ticker, event_date))
    cur = conn.execute(
        "SELECT id FROM events WHERE ticker = ? AND event_date = ?",
        (ticker, event_date),
    )
    existing = cur.fetchone()

    if existing:
        conn.execute(
            """
            UPDATE events SET
                event_hour       = ?,
                gcal_id          = COALESCE(?, gcal_id),
                quarter          = COALESCE(?, quarter),
                eps_estimate     = COALESCE(?, eps_estimate),
                eps_actual       = COALESCE(?, eps_actual),
                rev_estimate     = COALESCE(?, rev_estimate),
                rev_actual       = COALESCE(?, rev_actual),
                reported         = ?,
                tier             = ?,
                company_name     = COALESCE(?, company_name),
                source_fingerprint = ?,
                date_confirmed   = ?,
                updated_at       = datetime('now')
            WHERE ticker = ? AND event_date = ?
            """,
            (event_hour, gcal_id, quarter,
             eps_estimate, eps_actual, rev_estimate, rev_actual,
             int(reported), tier, company_name, source_fingerprint,
             date_confirmed,
             ticker, event_date),
        )
    else:
        # For old DBs with UNIQUE(ticker, quarter), delete any existing row
        # for this ticker+quarter before inserting with the new date
        if quarter:
            conn.execute(
                "DELETE FROM events WHERE ticker = ? AND quarter = ? AND event_date != ?",
                (ticker, quarter, event_date),
            )
        conn.execute(
            """
            INSERT INTO events (ticker, event_date, event_hour, gcal_id, quarter,
                                eps_estimate, eps_actual, rev_estimate, rev_actual,
                                reported, tier, company_name, source_fingerprint,
                                date_confirmed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ticker, event_date, event_hour, gcal_id, quarter,
             eps_estimate, eps_actual, rev_estimate, rev_actual,
             int(reported), tier, company_name, source_fingerprint,
             date_confirmed),
        )

    conn.commit()


# ---------------------------------------------------------------------------
# Estimate history (for building revision trends)
# ---------------------------------------------------------------------------


def record_estimate_snapshot(
    conn: sqlite3.Connection,
    ticker: str,
    event_date: str,
    snapshot_date: str,
    eps_estimate: float | None,
    revenue_estimate: float | None,
):
    """Record a point-in-time consensus estimate snapshot."""
    conn.execute(
        """
        INSERT INTO estimate_history (ticker, event_date, snapshot_date,
                                       eps_estimate, revenue_estimate)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ticker, event_date, snapshot_date) DO UPDATE SET
            eps_estimate     = excluded.eps_estimate,
            revenue_estimate = excluded.revenue_estimate
        """,
        (ticker, event_date, snapshot_date, eps_estimate, revenue_estimate),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Date-lock helpers (D2: human override when Finnhub is wrong)
# ---------------------------------------------------------------------------


def is_ticker_date_locked(
    conn: sqlite3.Connection, ticker: str, ref_date: str, window_days: int = 30
) -> bool:
    """
    Return True if any event for `ticker` within `window_days` of `ref_date`
    is date-locked. Used as a ticker-wide safety net when DB and calendar
    event dates have drifted apart and exact-date lookup might miss the
    locked row.
    """
    cur = conn.execute(
        "SELECT 1 FROM events WHERE ticker = ? AND date_locked = 1 "
        "AND julianday(event_date) BETWEEN julianday(?) - ? AND julianday(?) + ? "
        "LIMIT 1",
        (ticker.upper(), ref_date, window_days, ref_date, window_days),
    )
    return cur.fetchone() is not None


def set_date_lock(
    conn: sqlite3.Connection, ticker: str, event_date: str, locked: bool
) -> bool:
    """Set or clear the date_locked flag. Returns True if a row was affected."""
    cur = conn.execute(
        "UPDATE events SET date_locked = ?, updated_at = datetime('now') "
        "WHERE ticker = ? AND event_date = ?",
        (1 if locked else 0, ticker.upper(), event_date),
    )
    conn.commit()
    return cur.rowcount > 0


def list_locked_events(conn: sqlite3.Connection) -> list[dict]:
    """Return all events currently date-locked."""
    cur = conn.execute(
        "SELECT ticker, event_date, event_hour, tier, company_name "
        "FROM events WHERE date_locked = 1 "
        "ORDER BY event_date, ticker"
    )
    return [
        {
            "ticker": r[0],
            "event_date": r[1],
            "event_hour": r[2],
            "tier": r[3],
            "company_name": r[4],
        }
        for r in cur.fetchall()
    ]


# ---------------------------------------------------------------------------
# Quarter helpers (kept here since it's used in DB context)
# ---------------------------------------------------------------------------


def date_to_quarter(d: str) -> str:
    """
    Derive a reporting quarter label from an earnings date string (YYYY-MM-DD).

    Earnings released in Jan-Mar typically report Q4 of prior year,
    Apr-Jun report Q1, Jul-Sep report Q2, Oct-Dec report Q3.

    This is a rough mapping — some companies have odd fiscal years — but
    it's used for display purposes. The dedup key is (ticker, event_date),
    not (ticker, quarter).
    """
    from datetime import date
    dt = date.fromisoformat(d)
    month = dt.month

    if month <= 3:
        return f"{dt.year - 1}Q4"
    elif month <= 6:
        return f"{dt.year}Q1"
    elif month <= 9:
        return f"{dt.year}Q2"
    else:
        return f"{dt.year}Q3"
