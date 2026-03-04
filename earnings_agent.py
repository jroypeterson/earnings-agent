"""
Earnings Release Calendar Agent
================================
Polls Finnhub for upcoming earnings dates for a watchlist of tickers
and creates events on a Google Calendar. Tracks by ticker+quarter so
that if a release date changes, the old calendar event is automatically
deleted and replaced with the updated date.

After earnings are reported, updates calendar events with actual results
and beat/miss versus consensus estimates.

Usage:
    python earnings_agent.py              # Run once (normal mode)
    python earnings_agent.py --dry-run    # Preview without creating calendar events
    python earnings_agent.py --backfill   # Also look back 30 days for any missed earnings
    python earnings_agent.py --cleanup    # Delete duplicate events from Google Calendar
    python earnings_agent.py --cleanup --dry-run  # Preview which duplicates would be deleted
"""

import os
import sys
import json
import time
import sqlite3
import logging
import argparse
from datetime import datetime, timedelta, date
from pathlib import Path

import finnhub
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
GOOGLE_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
TICKERS_FILE = Path(__file__).parent / "tickers.txt"


def load_tickers() -> list[str]:
    """Load tickers from tickers.txt (one per line), falling back to .env."""
    if TICKERS_FILE.exists():
        tickers = [
            line.strip().upper()
            for line in TICKERS_FILE.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if tickers:
            return tickers
    # Fallback to .env
    return [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]


TICKERS = load_tickers()

DB_PATH = Path(__file__).parent / "earnings_events.db"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Suppress noisy Google API cache warning
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# Quarter helpers
# ---------------------------------------------------------------------------


def date_to_quarter(d: str) -> str:
    """
    Derive a reporting quarter label from an earnings date string (YYYY-MM-DD).

    Earnings released in Jan-Mar typically report Q4 of prior year,
    Apr-Jun report Q1, Jul-Sep report Q2, Oct-Dec report Q3.

    This is a rough mapping — some companies have odd fiscal years — but
    it's sufficient for deduplication (we just need a stable key per
    earnings cycle for each ticker).
    """
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


# ---------------------------------------------------------------------------
# Database helpers  (SQLite — tracks by ticker + quarter)
# ---------------------------------------------------------------------------


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Initialise the SQLite database and return a connection."""
    conn = sqlite3.connect(str(db_path))

    cursor = conn.execute("PRAGMA table_info(events)")
    columns = [row[1] for row in cursor.fetchall()]

    if not columns:
        # Fresh database
        conn.execute("""
            CREATE TABLE events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT    NOT NULL,
                quarter         TEXT    NOT NULL,
                event_date      TEXT    NOT NULL,
                event_hour      TEXT,
                gcal_id         TEXT,
                eps_estimate    REAL,
                eps_actual      REAL,
                rev_estimate    REAL,
                rev_actual      REAL,
                reported        INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(ticker, quarter)
            )
        """)
        conn.commit()
    elif "reported" not in columns:
        # Migrate from older schema
        logger.info("🔄 Migrating database to add actuals tracking...")
        conn.execute("DROP TABLE events")
        conn.execute("""
            CREATE TABLE events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT    NOT NULL,
                quarter         TEXT    NOT NULL,
                event_date      TEXT    NOT NULL,
                event_hour      TEXT,
                gcal_id         TEXT,
                eps_estimate    REAL,
                eps_actual      REAL,
                rev_estimate    REAL,
                rev_actual      REAL,
                reported        INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(ticker, quarter)
            )
        """)
        conn.commit()
        logger.info("✅ Database migrated. Old events cleared — will re-sync on next run.")

    return conn


def find_existing_event(conn: sqlite3.Connection, ticker: str, quarter: str) -> dict | None:
    """Look up an existing event by ticker + quarter. Returns dict or None."""
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


def upsert_event(
    conn: sqlite3.Connection,
    ticker: str,
    quarter: str,
    event_date: str,
    event_hour: str | None,
    gcal_id: str | None,
    eps_estimate: float | None = None,
    eps_actual: float | None = None,
    rev_estimate: float | None = None,
    rev_actual: float | None = None,
    reported: bool = False,
):
    """Insert or update an event."""
    conn.execute(
        """
        INSERT INTO events (ticker, quarter, event_date, event_hour, gcal_id,
                            eps_estimate, eps_actual, rev_estimate, rev_actual, reported)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, quarter) DO UPDATE SET
            event_date   = excluded.event_date,
            event_hour   = excluded.event_hour,
            gcal_id      = excluded.gcal_id,
            eps_estimate = excluded.eps_estimate,
            eps_actual   = excluded.eps_actual,
            rev_estimate = excluded.rev_estimate,
            rev_actual   = excluded.rev_actual,
            reported     = excluded.reported,
            updated_at   = datetime('now')
        """,
        (ticker, quarter, event_date, event_hour, gcal_id,
         eps_estimate, eps_actual, rev_estimate, rev_actual, int(reported)),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Finnhub: fetch earnings calendar
# ---------------------------------------------------------------------------


def fetch_earnings(
    client: finnhub.Client,
    tickers: list[str],
    from_date: str,
    to_date: str,
) -> list[dict]:
    """
    Query Finnhub earnings calendar in date-range chunks to stay under
    the 1500-result cap, then filter client-side for our watchlist.
    """
    logger.info(f"Querying Finnhub earnings calendar: {from_date} → {to_date}")

    CHUNK_DAYS = 7
    ticker_set = {t.upper() for t in tickers}

    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    matched = []
    total_fetched = 0

    while start < end:
        chunk_end = min(start + timedelta(days=CHUNK_DAYS), end)
        chunk_from = start.isoformat()
        chunk_to = chunk_end.isoformat()

        try:
            result = client.earnings_calendar(
                _from=chunk_from,
                to=chunk_to,
                symbol="",
                international=False,
            )
            all_earnings = result.get("earningsCalendar", [])
            chunk_matches = [
                e for e in all_earnings
                if e.get("symbol", "").upper() in ticker_set
            ]
            total_fetched += len(all_earnings)
            matched.extend(chunk_matches)

            logger.info(
                f"  📅 {chunk_from} → {chunk_to}: "
                f"{len(all_earnings)} total, {len(chunk_matches)} matched"
            )

            if len(all_earnings) >= 1500:
                logger.warning(
                    f"  ⚠️  Chunk returned 1500 results (cap hit). "
                    f"Consider reducing CHUNK_DAYS below {CHUNK_DAYS}."
                )

        except Exception as exc:
            logger.error(f"  ❌ Chunk {chunk_from} → {chunk_to} failed: {exc}")

        start = chunk_end
        time.sleep(1)

    logger.info(
        f"Scanned {total_fetched} total earnings across all chunks. "
        f"Matched {len(matched)} events for {len(tickers)} tickers."
    )
    return matched


# ---------------------------------------------------------------------------
# Event description builder
# ---------------------------------------------------------------------------


def build_description(
    ticker: str,
    hour: str | None,
    eps_estimate: float | None,
    eps_actual: float | None,
    revenue_estimate: float | None,
    revenue_actual: float | None,
) -> str:
    """Build the calendar event description, including actuals if available."""
    timing_labels = {
        "bmo": "Before Market Open",
        "amc": "After Market Close",
        "dmh": "During Market Hours",
    }
    timing_str = timing_labels.get(hour, "Time TBD")

    lines = [
        f"Ticker: {ticker}",
        f"Timing: {timing_str}",
    ]

    has_actuals = eps_actual is not None or revenue_actual is not None

    # --- EPS section ---
    if eps_actual is not None and eps_estimate is not None:
        diff = eps_actual - eps_estimate
        pct = (diff / abs(eps_estimate) * 100) if eps_estimate != 0 else 0
        if diff > 0:
            verdict = f"✅ BEAT by ${abs(diff):.2f} ({abs(pct):.1f}%)"
        elif diff < 0:
            verdict = f"❌ MISS by ${abs(diff):.2f} ({abs(pct):.1f}%)"
        else:
            verdict = "➖ IN LINE"
        lines.append(f"\nEPS: ${eps_actual:.2f} actual vs ${eps_estimate:.2f} est — {verdict}")
    elif eps_actual is not None:
        lines.append(f"\nEPS Actual: ${eps_actual:.2f}")
    elif eps_estimate is not None:
        lines.append(f"EPS Estimate: ${eps_estimate:.2f}")

    # --- Revenue section ---
    if revenue_actual is not None and revenue_estimate is not None:
        act_b = revenue_actual / 1_000_000_000
        est_b = revenue_estimate / 1_000_000_000
        diff = revenue_actual - revenue_estimate
        pct = (diff / abs(revenue_estimate) * 100) if revenue_estimate != 0 else 0
        if diff > 0:
            verdict = f"✅ BEAT by {abs(pct):.1f}%"
        elif diff < 0:
            verdict = f"❌ MISS by {abs(pct):.1f}%"
        else:
            verdict = "➖ IN LINE"
        lines.append(f"Revenue: ${act_b:.2f}B actual vs ${est_b:.2f}B est — {verdict}")
    elif revenue_actual is not None:
        rev_b = revenue_actual / 1_000_000_000
        lines.append(f"Revenue Actual: ${rev_b:.2f}B")
    elif revenue_estimate is not None:
        rev_b = revenue_estimate / 1_000_000_000
        lines.append(f"Revenue Estimate: ${rev_b:.2f}B")

    if has_actuals:
        lines.append("\n📋 REPORTED")

    lines.append(f"\nSource: Finnhub | Auto-generated by Earnings Agent")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Google Calendar: create, update & delete events
# ---------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/calendar.events"]


def get_calendar_service():
    """Build an authenticated Google Calendar API service."""
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=SCOPES
    )
    return build("calendar", "v3", credentials=creds)


def find_calendar_event(service, calendar_id: str, ticker: str, quarter: str, earnings_date: str):
    """
    Search Google Calendar for an existing earnings event matching ticker+quarter.

    Uses extendedProperties to filter. Returns the event dict if found, None otherwise.
    """
    dt = date.fromisoformat(earnings_date)
    time_min = (dt - timedelta(days=90)).isoformat() + "T00:00:00Z"
    time_max = (dt + timedelta(days=90)).isoformat() + "T00:00:00Z"

    try:
        result = service.events().list(
            calendarId=calendar_id,
            privateExtendedProperty=f"ticker={ticker}",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=50,
        ).execute()

        for event in result.get("items", []):
            props = event.get("extendedProperties", {}).get("private", {})
            if props.get("quarter") == quarter:
                logger.info(f"🔍 Found existing calendar event for {ticker} {quarter} via API")
                return event
    except Exception as exc:
        logger.warning(f"⚠️  Calendar API lookup failed for {ticker} {quarter}: {exc}")

    return None


def delete_calendar_event(service, calendar_id: str, gcal_id: str):
    """Delete an event from Google Calendar."""
    service.events().delete(calendarId=calendar_id, eventId=gcal_id).execute()


def update_calendar_event_description(
    service, calendar_id: str, gcal_id: str,
    new_summary: str, new_description: str,
    ticker: str | None = None, quarter: str | None = None,
):
    """Update an existing calendar event's summary, description, and extended properties."""
    event = service.events().get(calendarId=calendar_id, eventId=gcal_id).execute()
    event["summary"] = new_summary
    event["description"] = new_description

    # Ensure extendedProperties are present (backfill for pre-existing events)
    if ticker and quarter:
        event["extendedProperties"] = {
            "private": {
                "earningsAgent": "true",
                "ticker": ticker,
                "quarter": quarter,
            }
        }

    service.events().update(
        calendarId=calendar_id, eventId=gcal_id, body=event
    ).execute()


def create_calendar_event(
    service,
    calendar_id: str,
    ticker: str,
    quarter: str,
    earnings_date: str,
    hour: str | None,
    eps_estimate: float | None = None,
    eps_actual: float | None = None,
    revenue_estimate: float | None = None,
    revenue_actual: float | None = None,
) -> str | None:
    """
    Create an all-day event (or timed event if hour is known) on Google Calendar.
    Returns the created event's Google Calendar ID.
    """
    has_actuals = eps_actual is not None or revenue_actual is not None
    summary = f"{'✅' if has_actuals else '📊'} {ticker} Earnings Release"
    description = build_description(
        ticker, hour, eps_estimate, eps_actual, revenue_estimate, revenue_actual
    )

    extended_props = {
        "private": {
            "earningsAgent": "true",
            "ticker": ticker,
            "quarter": quarter,
        }
    }

    if hour == "bmo":
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": f"{earnings_date}T07:00:00", "timeZone": "America/New_York"},
            "end": {"dateTime": f"{earnings_date}T07:15:00", "timeZone": "America/New_York"},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 60}]},
            "extendedProperties": extended_props,
        }
    elif hour == "amc":
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": f"{earnings_date}T16:30:00", "timeZone": "America/New_York"},
            "end": {"dateTime": f"{earnings_date}T16:45:00", "timeZone": "America/New_York"},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 60}]},
            "extendedProperties": extended_props,
        }
    else:
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"date": earnings_date},
            "end": {"date": earnings_date},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 720}]},
            "extendedProperties": extended_props,
        }

    created = service.events().insert(calendarId=calendar_id, body=event_body).execute()
    logger.info(f"✅ Created calendar event: {summary} on {earnings_date}")
    return created.get("id")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def _parse_ticker_from_summary(summary: str) -> str | None:
    """
    Extract ticker from event summaries like '📊 AAPL Earnings Release'
    or '✅ AAPL Earnings Release'.
    """
    import re
    m = re.match(r"^[^\w]*(\w+)\s+Earnings\s+Release", summary)
    return m.group(1).upper() if m else None


def _dedup_group(
    events: list[dict],
    label: str,
    cal_service,
    conn,
    dry_run: bool,
) -> int:
    """
    Given a list of duplicate events for the same label, keep the newest
    and delete the rest. Returns number of events deleted.
    """
    if len(events) <= 1:
        return 0

    # Sort by created time (newest first) — keep the first, delete the rest
    events.sort(key=lambda e: e.get("created", ""), reverse=True)
    keep = events[0]
    dupes = events[1:]

    logger.info(
        f"  {label}: {len(events)} events found, "
        f"keeping {keep['id']}, deleting {len(dupes)} duplicate(s)"
    )

    deleted = 0
    for dupe in dupes:
        if dry_run:
            logger.info(f"    [dry-run] Would delete {dupe['id']} ({dupe.get('summary', '')})")
        else:
            try:
                delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, dupe["id"])
                logger.info(f"    Deleted {dupe['id']}")
            except Exception as exc:
                logger.error(f"    Failed to delete {dupe['id']}: {exc}")
                continue
        deleted += 1

    # Sync local DB to the kept event
    if not dry_run:
        start = keep.get("start", {})
        event_date = start.get("date") or start.get("dateTime", "")[:10]
        if event_date:
            props = keep.get("extendedProperties", {}).get("private", {})
            ticker = props.get("ticker") or _parse_ticker_from_summary(keep.get("summary", ""))
            quarter = props.get("quarter") or (date_to_quarter(event_date) if event_date else None)
            if ticker and quarter:
                upsert_event(conn, ticker, quarter, event_date, None, keep["id"])

    return deleted


def cleanup_duplicates(dry_run: bool = False):
    """
    Scan Google Calendar for duplicate earnings events and delete extras.

    Pass 1: Events with extendedProperties (earningsAgent=true) — groups by ticker+quarter.
    Pass 2: Legacy events without extendedProperties — matched by summary pattern
            ("TICKER Earnings Release") and event date.
    """
    if not GOOGLE_CALENDAR_ID:
        logger.error("Missing GOOGLE_CALENDAR_ID")
        sys.exit(1)

    cal_service = get_calendar_service()
    conn = init_db()

    today = date.today()
    time_min = (today - timedelta(days=365)).isoformat() + "T00:00:00Z"
    time_max = (today + timedelta(days=365)).isoformat() + "T00:00:00Z"

    deleted_count = 0

    # ── Pass 1: events with extendedProperties ────────────────────────────
    logger.info("Pass 1: Scanning events with extendedProperties...")
    tagged_events = []
    tagged_ids = set()
    page_token = None

    while True:
        result = cal_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            privateExtendedProperty="earningsAgent=true",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=250,
            pageToken=page_token,
        ).execute()

        tagged_events.extend(result.get("items", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"  Found {len(tagged_events)} tagged events")

    groups: dict[str, list[dict]] = {}
    for event in tagged_events:
        tagged_ids.add(event["id"])
        props = event.get("extendedProperties", {}).get("private", {})
        ticker = props.get("ticker")
        quarter = props.get("quarter")
        if not ticker or not quarter:
            continue
        key = f"{ticker}|{quarter}"
        groups.setdefault(key, []).append(event)

    for key, events in groups.items():
        deleted_count += _dedup_group(events, key.replace("|", " "), cal_service, conn, dry_run)

    # ── Pass 2: legacy events (no extendedProperties) ─────────────────────
    logger.info("Pass 2: Scanning legacy events by summary pattern...")
    legacy_events = []
    page_token = None

    while True:
        result = cal_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            q="Earnings Release",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=250,
            pageToken=page_token,
        ).execute()

        legacy_events.extend(result.get("items", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    # Filter out events already handled in pass 1
    legacy_events = [e for e in legacy_events if e["id"] not in tagged_ids]
    logger.info(f"  Found {len(legacy_events)} legacy events (without extendedProperties)")

    # Group by ticker + event date (best we can do without quarter metadata)
    legacy_groups: dict[str, list[dict]] = {}
    for event in legacy_events:
        ticker = _parse_ticker_from_summary(event.get("summary", ""))
        if not ticker:
            continue
        start = event.get("start", {})
        event_date = start.get("date") or start.get("dateTime", "")[:10]
        if not event_date:
            continue
        key = f"{ticker}|{event_date}"
        legacy_groups.setdefault(key, []).append(event)

    for key, events in legacy_groups.items():
        deleted_count += _dedup_group(events, key.replace("|", " "), cal_service, conn, dry_run)

    logger.info("=" * 50)
    if dry_run:
        logger.info(f"Cleanup dry run: would delete {deleted_count} duplicate(s)")
    else:
        logger.info(f"Cleanup complete: deleted {deleted_count} duplicate(s)")
    logger.info("=" * 50)

    conn.close()


def run(dry_run: bool = False, backfill: bool = False):
    """Main entry point."""

    # --- Validate config ---
    missing = []
    if not FINNHUB_API_KEY:
        missing.append("FINNHUB_API_KEY")
    if not GOOGLE_CALENDAR_ID:
        missing.append("GOOGLE_CALENDAR_ID")
    if not TICKERS:
        missing.append("TICKERS")
    if missing:
        logger.error(f"Missing required config: {', '.join(missing)}")
        logger.error("Copy .env.example to .env and fill in your values.")
        sys.exit(1)

    # --- Set up clients ---
    fh_client = finnhub.Client(api_key=FINNHUB_API_KEY)
    conn = init_db()

    if not dry_run:
        cal_service = get_calendar_service()
    else:
        cal_service = None

    # --- Determine date range ---
    # Always look back 14 days to catch recently reported actuals
    today = date.today()
    if backfill:
        from_date = (today - timedelta(days=30)).isoformat()
    else:
        from_date = (today - timedelta(days=14)).isoformat()
    to_date = (today + timedelta(days=90)).isoformat()

    # --- Fetch and process ---
    earnings = fetch_earnings(fh_client, TICKERS, from_date, to_date)

    new_count = 0
    updated_count = 0
    actuals_count = 0
    skip_count = 0

    for e in earnings:
        ticker = e["symbol"].upper()
        earnings_date = e["date"]
        hour = e.get("hour")
        eps_est = e.get("epsEstimate")
        eps_act = e.get("epsActual")
        rev_est = e.get("revenueEstimate")
        rev_act = e.get("revenueActual")
        quarter = date_to_quarter(earnings_date)

        has_actuals = eps_act is not None or rev_act is not None
        existing = find_existing_event(conn, ticker, quarter)

        if existing:
            # --- Check if actuals just came in ---
            if has_actuals and not existing["reported"]:
                logger.info(
                    f"📋 Actuals in: {ticker} {quarter} — "
                    f"EPS: ${eps_act:.2f} vs ${eps_est:.2f} est"
                    if eps_act is not None and eps_est is not None
                    else f"📋 Actuals in: {ticker} {quarter}"
                )

                gcal_id = existing["gcal_id"]

                if not dry_run and gcal_id:
                    try:
                        new_summary = f"✅ {ticker} Earnings Release"
                        new_description = build_description(
                            ticker, hour, eps_est, eps_act, rev_est, rev_act
                        )
                        update_calendar_event_description(
                            cal_service, GOOGLE_CALENDAR_ID,
                            gcal_id, new_summary, new_description,
                            ticker=ticker, quarter=quarter,
                        )
                        logger.info(f"📝 Updated calendar event with actuals for {ticker}")
                    except Exception as exc:
                        logger.error(f"❌ Failed to update event for {ticker}: {exc}")

                upsert_event(
                    conn, ticker, quarter, earnings_date, hour, existing["gcal_id"],
                    eps_est, eps_act, rev_est, rev_act, reported=True,
                )
                actuals_count += 1
                continue

            # --- Check if date or timing changed ---
            date_changed = existing["event_date"] != earnings_date
            hour_changed = existing["event_hour"] != hour

            if not date_changed and not hour_changed:
                skip_count += 1
                continue

            old_date = existing["event_date"]
            old_gcal_id = existing["gcal_id"]

            if date_changed:
                logger.info(
                    f"📅 Date changed: {ticker} {quarter} moved from "
                    f"{old_date} → {earnings_date}"
                )
            if hour_changed:
                logger.info(
                    f"🕐 Timing changed: {ticker} {quarter} on {earnings_date} "
                    f"({existing['event_hour'] or 'TBD'} → {hour or 'TBD'})"
                )

            if not dry_run and old_gcal_id:
                try:
                    delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, old_gcal_id)
                    logger.info(f"🗑️  Deleted old calendar event for {ticker} on {old_date}")
                except Exception as exc:
                    logger.warning(f"⚠️  Could not delete old event for {ticker}: {exc}")

            gcal_id = None
            if not dry_run:
                try:
                    gcal_id = create_calendar_event(
                        cal_service, GOOGLE_CALENDAR_ID, ticker, quarter,
                        earnings_date, hour, eps_est, eps_act, rev_est, rev_act,
                    )
                except Exception as exc:
                    logger.error(f"❌ Failed to create updated event for {ticker}: {exc}")
                    continue

            upsert_event(
                conn, ticker, quarter, earnings_date, hour, gcal_id,
                eps_est, eps_act, rev_est, rev_act, reported=has_actuals,
            )
            updated_count += 1

        else:
            # --- Brand new event (not in local DB) ---
            # Check Google Calendar API to prevent duplicates when DB is lost
            if not dry_run:
                cal_event = find_calendar_event(
                    cal_service, GOOGLE_CALENDAR_ID, ticker, quarter, earnings_date,
                )
                if cal_event:
                    # Event already exists on calendar — backfill local DB and skip
                    gcal_id = cal_event.get("id")
                    upsert_event(
                        conn, ticker, quarter, earnings_date, hour, gcal_id,
                        eps_est, eps_act, rev_est, rev_act, reported=has_actuals,
                    )
                    logger.info(f"♻️  Backfilled DB from calendar for {ticker} {quarter}")
                    skip_count += 1
                    continue

            logger.info(f"🆕 New earnings: {ticker} {quarter} on {earnings_date} ({hour or 'time TBD'})")

            gcal_id = None
            if not dry_run:
                try:
                    gcal_id = create_calendar_event(
                        cal_service, GOOGLE_CALENDAR_ID, ticker, quarter,
                        earnings_date, hour, eps_est, eps_act, rev_est, rev_act,
                    )
                except Exception as exc:
                    logger.error(f"❌ Failed to create calendar event for {ticker}: {exc}")
                    continue

            upsert_event(
                conn, ticker, quarter, earnings_date, hour, gcal_id,
                eps_est, eps_act, rev_est, rev_act, reported=has_actuals,
            )
            new_count += 1

    # --- Summary ---
    logger.info("=" * 50)
    logger.info(
        f"Done! {new_count} new, {updated_count} updated, "
        f"{actuals_count} actuals added, {skip_count} unchanged."
    )
    if dry_run:
        logger.info("(Dry run — no calendar events were actually created, updated, or deleted)")
    logger.info("=" * 50)

    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Earnings Release Calendar Agent")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview earnings without creating calendar events",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Also check the past 30 days for any missed earnings",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Scan calendar for duplicate events and delete extras",
    )
    args = parser.parse_args()

    if args.cleanup:
        cleanup_duplicates(dry_run=args.dry_run)
    else:
        run(dry_run=args.dry_run, backfill=args.backfill)
