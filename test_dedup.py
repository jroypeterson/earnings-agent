"""
Tests that the Calendar API deduplication logic prevents duplicate events
when the local SQLite DB is lost.

Tests use the backward-compatible wrappers in earnings_agent.py to verify
that the new modular code preserves existing behavior.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call
from pathlib import Path

import earnings_agent as ea
from storage import init_db as init_db_new, find_event_by_ticker_quarter, date_to_quarter
from calendar_sync import find_calendar_event as find_cal_new, CalendarError


def make_mock_calendar_service(existing_events=None):
    """Build a mock Google Calendar service."""
    service = MagicMock()

    # events().list().execute() — returns existing_events or empty
    list_result = {"items": existing_events or []}
    service.events().list.return_value.execute.return_value = list_result
    # Need to handle chained calls properly
    service.events.return_value.list.return_value.execute.return_value = list_result

    # events().insert().execute() — returns a fake created event
    insert_counter = {"n": 0}

    def fake_insert(**kwargs):
        insert_counter["n"] += 1
        mock_resp = MagicMock()
        mock_resp.execute.return_value = {"id": f"new_gcal_{insert_counter['n']}"}
        return mock_resp

    service.events.return_value.insert.side_effect = fake_insert

    # events().get().execute() — for update_calendar_event_description
    service.events.return_value.get.return_value.execute.return_value = {
        "summary": "", "description": "",
    }
    service.events.return_value.update.return_value.execute.return_value = {}
    service.events.return_value.delete.return_value.execute.return_value = {}

    return service, insert_counter


def make_in_memory_db():
    """Create a fresh in-memory SQLite DB with the earnings schema.

    Mirrors storage.init_db's fresh-DB CREATE TABLE — every schema bump
    must add the new column here too. Most modern tests should use
    init_db(":memory:") instead of this fixture, which migrates from
    scratch via _MIGRATIONS and is automatically forward-compatible.
    """
    conn = sqlite3.connect(":memory:")
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
            event_hour_yf   TEXT,
            call_datetime_utc TEXT,
            call_source     TEXT,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(ticker, event_date)
        )
    """)
    conn.commit()
    return conn


# ── Test 1: find_calendar_event returns matching event ────────────────────

def test_find_calendar_event_returns_match():
    existing = {
        "id": "gcal_abc123",
        "summary": "AAPL Earnings",
        "start": {"date": "2026-02-01"},
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "AAPL", "quarter": "2025Q4"}
        },
    }
    service, _ = make_mock_calendar_service(existing_events=[existing])

    result = ea.find_calendar_event(service, "cal_id", "AAPL", "2025Q4", "2026-02-01")
    assert result is not None
    assert result["id"] == "gcal_abc123"
    print("PASS: find_calendar_event returns matching event")


# ── Test 2: find_calendar_event returns None when no match ────────────────

def test_find_calendar_event_no_match():
    # Event for a different date (more than 7 days away)
    existing = {
        "id": "gcal_xyz",
        "summary": "AAPL Earnings",
        "start": {"date": "2025-10-15"},
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "AAPL", "quarter": "2025Q3"}
        },
    }
    service, _ = make_mock_calendar_service(existing_events=[existing])

    result = ea.find_calendar_event(service, "cal_id", "AAPL", "2025Q4", "2026-02-01")
    assert result is None
    print("PASS: find_calendar_event returns None for non-matching date")


# ── Test 3: create_calendar_event includes extendedProperties ─────────────

def test_create_includes_extended_properties():
    service, _ = make_mock_calendar_service()

    gcal_id = ea.create_calendar_event(
        service, "cal_id", "MSFT", "2025Q4", "2026-01-28", "amc",
    )

    # Check that insert was called with extendedProperties in the body
    insert_call = service.events.return_value.insert.call_args
    body = insert_call.kwargs.get("body") or insert_call[1].get("body")
    assert "extendedProperties" in body
    props = body["extendedProperties"]["private"]
    assert props["ticker"] == "MSFT"
    assert props["quarter"] == "2025Q4"
    assert props["earningsAgent"] == "true"
    print("PASS: create_calendar_event includes extendedProperties")


# ── Test 4: DB miss + Calendar hit → no duplicate created ─────────────────

def test_no_duplicate_when_db_lost_but_calendar_has_event():
    """
    Simulates the core bug scenario:
    - Local DB is empty (lost/expired)
    - Google Calendar already has the event from a previous run
    - Agent should NOT create a duplicate
    """
    conn = make_in_memory_db()

    # Calendar already has this event (with start date for matching)
    existing_cal_event = {
        "id": "gcal_existing_123",
        "summary": "GOOG Earnings",
        "start": {"date": "2026-02-05"},
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "GOOG", "quarter": "2025Q4"}
        },
    }
    service, insert_counter = make_mock_calendar_service(existing_events=[existing_cal_event])

    # Verify DB is empty
    assert ea.find_existing_event(conn, "GOOG", "2025Q4") is None

    # Simulate what run() does for a "new" event (no DB hit)
    ticker = "GOOG"
    quarter = "2025Q4"
    earnings_date = "2026-02-05"
    hour = "amc"

    # Step 1: Check calendar API (the dedup fallback)
    cal_event = ea.find_calendar_event(service, "cal_id", ticker, quarter, earnings_date)

    if cal_event:
        # Backfill DB, do NOT create
        gcal_id = cal_event.get("id")
        ea.upsert_event(conn, ticker, quarter, earnings_date, hour, gcal_id)
        created = False
    else:
        # Would create — but this path should NOT be taken
        ea.create_calendar_event(service, "cal_id", ticker, quarter, earnings_date, hour)
        created = True

    assert not created, "Should NOT have created a new event — calendar already had it"
    assert insert_counter["n"] == 0, "insert() should not have been called"

    # DB should now have the backfilled entry
    db_event = ea.find_existing_event(conn, "GOOG", "2025Q4")
    assert db_event is not None
    assert db_event["gcal_id"] == "gcal_existing_123"

    conn.close()
    print("PASS: No duplicate created when DB lost but calendar has event")


# ── Test 5: DB miss + Calendar miss → event IS created ────────────────────

def test_creates_event_when_truly_new():
    """When neither DB nor Calendar has the event, it should be created."""
    conn = make_in_memory_db()
    service, insert_counter = make_mock_calendar_service(existing_events=[])

    ticker = "NVDA"
    quarter = "2025Q4"
    earnings_date = "2026-02-26"
    hour = "amc"

    # DB miss
    assert ea.find_existing_event(conn, ticker, quarter) is None

    # Calendar miss
    cal_event = ea.find_calendar_event(service, "cal_id", ticker, quarter, earnings_date)
    assert cal_event is None

    # Should create
    gcal_id = ea.create_calendar_event(service, "cal_id", ticker, quarter, earnings_date, hour)
    assert gcal_id is not None
    assert insert_counter["n"] == 1

    conn.close()
    print("PASS: Event created when truly new (no DB, no calendar)")


# ── Test 6: cleanup_duplicates deletes extras, keeps newest ───────────────

def test_cleanup_deletes_duplicates():
    """
    Simulates 3 events for the same ticker+date on the calendar.
    cleanup_duplicates should delete the 2 older ones and keep the newest.
    """
    dupe1 = {
        "id": "gcal_old_1",
        "created": "2026-01-01T00:00:00Z",
        "summary": "AAPL Earnings",
        "start": {"date": "2026-02-01"},
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "AAPL", "quarter": "2025Q4"}
        },
    }
    dupe2 = {
        "id": "gcal_old_2",
        "created": "2026-01-05T00:00:00Z",
        "summary": "AAPL Earnings",
        "start": {"date": "2026-02-01"},
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "AAPL", "quarter": "2025Q4"}
        },
    }
    keeper = {
        "id": "gcal_newest",
        "created": "2026-01-10T00:00:00Z",
        "summary": "AAPL Earnings",
        "start": {"date": "2026-02-01"},
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "AAPL", "quarter": "2025Q4"}
        },
    }

    # Track which event IDs get deleted
    deleted_ids = []

    service = MagicMock()
    service.events.return_value.list.return_value.execute.return_value = {
        "items": [dupe1, keeper, dupe2],  # intentionally unordered
    }

    def fake_delete(**kwargs):
        deleted_ids.append(kwargs.get("eventId"))
        mock_resp = MagicMock()
        mock_resp.execute.return_value = {}
        return mock_resp

    service.events.return_value.delete.side_effect = fake_delete

    # Use in-memory DB with new schema
    conn = make_in_memory_db()
    wrapper = MagicMock(wraps=conn)
    wrapper.close = MagicMock()  # no-op close so we can inspect DB after

    with patch("calendar_sync.get_calendar_service", return_value=service), \
         patch("calendar_sync.GOOGLE_CALENDAR_ID", "cal_id"):
        from calendar_sync import cleanup_duplicates
        cleanup_duplicates(wrapper, dry_run=False)

    assert "gcal_old_1" in deleted_ids, "Oldest dupe should be deleted"
    assert "gcal_old_2" in deleted_ids, "Middle dupe should be deleted"
    assert "gcal_newest" not in deleted_ids, "Newest event should be kept"
    assert len(deleted_ids) == 2

    # DB should have the keeper
    cur = conn.execute(
        "SELECT gcal_id FROM events WHERE ticker = 'AAPL' AND event_date = '2026-02-01'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "gcal_newest"

    conn.close()
    print("PASS: cleanup_duplicates deletes extras, keeps newest")


# ── Test 7: Coverage Manager tier resolution ────────────────────────────

def test_coverage_tier_resolution():
    """Test that tier resolution works correctly."""
    from coverage import TickerInfo, get_tickers_by_tier, get_ticker_info

    coverage = [
        TickerInfo("UNH", 1, "UnitedHealth", "Healthcare Services", "Mgd Care"),
        TickerInfo("ISRG", 2, "Intuitive Surgical", "MedTech", "Robotics"),
        TickerInfo("MSFT", 3, "Microsoft", "Tech", ""),
    ]

    # Tier 1+2 only
    t12 = get_tickers_by_tier(coverage, max_tier=2)
    assert "UNH" in t12
    assert "ISRG" in t12
    assert "MSFT" not in t12

    # All tiers
    t_all = get_tickers_by_tier(coverage, max_tier=3)
    assert len(t_all) == 3

    # Lookup
    info = get_ticker_info(coverage, "UNH")
    assert info is not None
    assert info.tier == 1
    assert info.sector == "Healthcare Services"

    assert get_ticker_info(coverage, "FAKE") is None

    print("PASS: Coverage tier resolution works correctly")


# ── Test 8: Non-destructive migration preserves data ────────────────────

def test_migration_preserves_data():
    """Test that migrating from old schema to new schema preserves data."""
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    try:
        # Create old-schema database with data
        conn = sqlite3.connect(str(db_path))
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
        conn.execute(
            "INSERT INTO events (ticker, quarter, event_date, event_hour, gcal_id, eps_estimate) "
            "VALUES ('AAPL', '2025Q4', '2026-02-01', 'amc', 'gcal_123', 2.35)"
        )
        conn.commit()
        conn.close()

        # Now open with new init_db — should migrate without dropping
        from storage import init_db
        conn = init_db(db_path)

        # Original data should still be there
        cur = conn.execute("SELECT ticker, quarter, event_date, eps_estimate FROM events WHERE ticker = 'AAPL'")
        row = cur.fetchone()
        assert row is not None, "Migration should preserve existing data"
        assert row[0] == "AAPL"
        assert row[1] == "2025Q4"
        assert row[2] == "2026-02-01"
        assert row[3] == 2.35

        # New columns should exist
        cur = conn.execute("PRAGMA table_info(events)")
        columns = [r[1] for r in cur.fetchall()]
        assert "tier" in columns, "tier column should exist after migration"
        assert "source_fingerprint" in columns, "source_fingerprint column should exist"
        assert "company_name" in columns, "company_name column should exist"

        # New tables should exist
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r[0] for r in cur.fetchall()}
        assert "estimate_history" in tables
        assert "predictions" in tables
        assert "review_status" in tables

        conn.close()
        print("PASS: Non-destructive migration preserves data")
    finally:
        os.unlink(db_path)


# ── Test 9: date_to_quarter mapping ────────────────────────────────────

def test_date_to_quarter():
    """Test quarter derivation from earnings dates."""
    assert date_to_quarter("2026-01-15") == "2025Q4"
    assert date_to_quarter("2026-02-01") == "2025Q4"
    assert date_to_quarter("2026-03-31") == "2025Q4"
    assert date_to_quarter("2026-04-01") == "2026Q1"
    assert date_to_quarter("2026-07-20") == "2026Q2"
    assert date_to_quarter("2026-10-15") == "2026Q3"
    print("PASS: date_to_quarter mapping correct")


# ── Test 10: TickTick task title formatting ────────────────────────────

def test_ticktick_task_title():
    """Test task title generation for TickTick."""
    from ticktick import build_task_title

    title = build_task_title("UNH", "2026-04-21", "bmo")
    assert "UNH" in title
    assert "Q1 2026" in title
    assert "Apr 21" in title
    assert "BMO" in title

    title_amc = build_task_title("AAPL", "2026-04-30", "amc")
    assert "AAPL" in title_amc
    assert "AMC" in title_amc

    title_tbd = build_task_title("MSFT", "2026-07-22", None)
    assert "MSFT" in title_tbd
    assert "BMO" not in title_tbd
    assert "AMC" not in title_tbd

    print("PASS: TickTick task title formatting correct")


# ── Test 11: TickTick task content has checklist ──────────────────────

def test_ticktick_task_content():
    """Test task content includes estimates and review checklist."""
    from ticktick import build_task_content

    content = build_task_content(
        ticker="UNH",
        hour="bmo",
        eps_estimate=7.14,
        revenue_estimate=109_200_000_000,
        company_name="UnitedHealth Group",
        tier=1,
    )
    assert "UnitedHealth Group" in content
    assert "EPS $7.14" in content
    assert "Rev $109.20B" in content
    assert "Read transcript" in content
    assert "Update model" in content  # Tier 1 only

    content_t2 = build_task_content(
        ticker="BSX", hour="amc", tier=2,
    )
    assert "Update model" not in content_t2  # Tier 2 doesn't get this
    assert "Read transcript" in content_t2

    print("PASS: TickTick task content has checklist and estimates")


# ── Test 12: TickTick dedup skips existing tasks ──────────────────────

def test_ticktick_dedup_skips_existing():
    """Events with ticktick_task_id should be skipped."""
    from ticktick import sync_ticktick_tasks
    from unittest.mock import patch

    conn = make_in_memory_db()

    events = [
        {
            "ticker": "UNH", "event_date": "2026-04-21", "event_hour": "bmo",
            "eps_estimate": 7.14, "rev_estimate": 109_200_000_000,
            "tier": 1, "company_name": "UnitedHealth",
            "ticktick_task_id": "existing_task_123",  # Already has a task
        },
        {
            "ticker": "AAPL", "event_date": "2026-04-30", "event_hour": "amc",
            "eps_estimate": 1.63, "rev_estimate": 94_000_000_000,
            "tier": 1, "company_name": "Apple",
            "ticktick_task_id": None,  # Needs a task
        },
    ]

    # Dry run — should skip UNH (has task), "create" AAPL
    with patch("ticktick.get_ticktick_config", return_value={"token": "fake", "list_id": "fake_list"}):
        stats = sync_ticktick_tasks(conn, events, dry_run=True)

    assert stats["skipped"] == 1, "UNH should be skipped (has existing task)"
    assert stats["created"] == 1, "AAPL should be created"

    conn.close()
    print("PASS: TickTick dedup skips events with existing tasks")


# ── Test 13: TickTick quarterly list name generation ──────────────────

def test_ticktick_quarter_list_name():
    """Test quarterly list name generation with reporting quarter + tier + position."""
    from ticktick import _quarter_list_name

    # Tier 1 with no position falls back to legacy "Core Watchlist" label.
    assert _quarter_list_name("2026-04-30", tier=1) == "1Q26 Earnings - Core Watchlist"

    # Tier 1 with position splits into Portfolio / Researching lists.
    assert _quarter_list_name("2026-04-30", tier=1, position="Portfolio") == "1Q26 Earnings - Portfolio"
    assert _quarter_list_name("2026-04-30", tier=1, position="Researching") == "1Q26 Earnings - Researching"

    # Tier 2 unchanged — HC Svcs & MedTech.
    assert _quarter_list_name("2026-04-30", tier=2) == "1Q26 Earnings - HC Svcs & MedTech"

    # Tier 3 has no suffix.
    assert _quarter_list_name("2026-04-30", tier=3) == "1Q26 Earnings"

    # Reporting-quarter math unchanged — Jan-Mar releases report Q4 of prior year, etc.
    assert _quarter_list_name("2026-07-15", tier=2) == "2Q26 Earnings - HC Svcs & MedTech"
    assert _quarter_list_name("2026-10-15", tier=1, position="Portfolio") == "3Q26 Earnings - Portfolio"
    assert _quarter_list_name("2026-01-15", tier=2) == "4Q25 Earnings - HC Svcs & MedTech"

    # Unknown position falls back to legacy "Core Watchlist".
    assert _quarter_list_name("2026-04-30", tier=1, position="Unknown") == "1Q26 Earnings - Core Watchlist"

    print("PASS: TickTick quarterly list name generation correct")


# ── expected_calendar_state / calendar_event_drift_kind ──────────────────


def _make_cal_event(*, summary, description, start, props):
    return {
        "summary": summary,
        "description": description,
        "start": start,
        "extendedProperties": {"private": props},
    }


def test_expected_state_confirmed_amc():
    from calendar_sync import expected_calendar_state

    summary, description, props = expected_calendar_state(
        "AAPL", "amc", 1.50, None, 89e9, None,
        quarter="2026Q1", tier=1, source_fingerprint="AAPL:2026-05-04",
    )
    assert summary == "AAPL Earnings Release"
    assert "Status: Confirmed" in description
    assert props == {
        "earningsAgent": "true",
        "ticker": "AAPL",
        "source_fingerprint": "AAPL:2026-05-04",
        "tier": "1",
        "quarter": "2026Q1",
    }


def test_expected_state_estimated_no_hour():
    from calendar_sync import expected_calendar_state

    summary, description, _ = expected_calendar_state(
        "UFPT", "", 0.5, None, 100e6, None,
        quarter="2026Q1", tier=2, source_fingerprint="UFPT:2026-05-05",
    )
    assert summary == "UFPT Earnings Release (est.)"
    assert "Status: Estimated" in description


def test_expected_state_reported_drops_est_marker():
    from calendar_sync import expected_calendar_state

    summary, description, _ = expected_calendar_state(
        "NVDA", "amc", 5.0, 5.20, 30e9, 32e9,
        quarter="2026Q1", tier=1, source_fingerprint="NVDA:2026-05-20",
    )
    assert summary == "NVDA Rpt'd Earnings"
    assert "\nREPORTED" in description


def test_parse_ticker_handles_both_title_formats():
    """Reported title shortened from '[REPORTED] X Earnings Release' to
    'X Rpt'd Earnings' — parser must accept both during the transition."""
    from calendar_sync import parse_ticker_from_summary
    # New compact form
    assert parse_ticker_from_summary("NVDA Rpt'd Earnings") == "NVDA"
    # Legacy upcoming form
    assert parse_ticker_from_summary("AAPL Earnings Release") == "AAPL"
    # Legacy upcoming + estimated marker
    assert parse_ticker_from_summary("UFPT Earnings Release (est.)") == "UFPT"
    # Legacy reported form
    assert parse_ticker_from_summary("[REPORTED] MSFT Earnings Release") == "MSFT"
    # Non-matching summary
    assert parse_ticker_from_summary("Daily standup") is None


def test_drift_kind_fresh():
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "AAPL", "amc", 1.50, None, 89e9, None,
        quarter="2026Q1", tier=1, source_fingerprint="AAPL:2026-05-04",
    )
    ev = _make_cal_event(
        summary=s, description=d,
        start={"dateTime": "2026-05-04T16:30:00-04:00", "timeZone": "America/New_York"},
        props=p,
    )
    assert calendar_event_drift_kind(ev, s, d, p, "amc") == "fresh"


def test_drift_kind_text_only_summary():
    """Fingerprint or title backfill. Shape unchanged."""
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "AAPL", "amc", 1.50, None, 89e9, None,
        quarter="2026Q1", tier=1, source_fingerprint="AAPL:2026-05-04",
    )
    ev = _make_cal_event(
        summary="AAPL Earnings Release (est.)",  # stale est. marker
        description=d,
        start={"dateTime": "2026-05-04T16:30:00-04:00", "timeZone": "America/New_York"},
        props=p,
    )
    assert calendar_event_drift_kind(ev, s, d, p, "amc") == "text"


def test_drift_kind_text_only_props():
    """Tier reclassification. Shape and visible text unchanged but private tier diff."""
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "AAPL", "amc", 1.50, None, 89e9, None,
        quarter="2026Q1", tier=1, source_fingerprint="AAPL:2026-05-04",
    )
    stale_props = dict(p, tier="2")
    ev = _make_cal_event(
        summary=s, description=d,
        start={"dateTime": "2026-05-04T16:30:00-04:00", "timeZone": "America/New_York"},
        props=stale_props,
    )
    assert calendar_event_drift_kind(ev, s, d, p, "amc") == "text"


def test_drift_kind_shape_all_day_to_amc():
    """The bug at the heart of the calendar accuracy issue.

    Event was created when Finnhub had no hour (all-day). Finnhub later
    populated hour=amc. update_calendar_event_description wouldn't move the
    event to 4:30 PM — it would only swap the title. Detect as 'shape' so
    the fix path delete+recreates.
    """
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "ARDT", "amc", 0.5, None, 200e6, None,
        quarter="2026Q1", tier=2, source_fingerprint="ARDT:2026-05-05",
    )
    ev = _make_cal_event(
        summary="ARDT Earnings Release (est.)",
        description="...stale...",
        start={"date": "2026-05-05"},  # all-day
        props={**p, "tier": "2"},
    )
    assert calendar_event_drift_kind(ev, s, d, p, "amc") == "shape"


def test_drift_kind_shape_bmo_to_amc():
    """Confirmed-to-confirmed timing flip. Title is identical (no est. either
    side); only the dateTime would differ. Reviewer's specific concern."""
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "AAPL", "amc", 1.50, None, 89e9, None,
        quarter="2026Q1", tier=1, source_fingerprint="AAPL:2026-05-04",
    )
    ev = _make_cal_event(
        summary=s,  # identical
        description=d,  # build_description differs only in Timing: line — but
                        # for this test we're proving the shape check fires
                        # even when text didn't drift. Use the canonical
                        # description for the bmo case.
        start={"dateTime": "2026-05-04T07:00:00-04:00", "timeZone": "America/New_York"},
        props=p,
    )
    # We expect amc but event is at bmo time -> shape drift
    assert calendar_event_drift_kind(ev, s, d, p, "amc") == "shape"


def test_drift_kind_shape_amc_to_all_day():
    """Reverse of the all-day->amc case; e.g. Finnhub revoked timing."""
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "FOO", "", 0.5, None, 100e6, None,
        quarter="2026Q1", tier=2, source_fingerprint="FOO:2026-05-05",
    )
    ev = _make_cal_event(
        summary="FOO Earnings Release",
        description="...",
        start={"dateTime": "2026-05-05T16:30:00-04:00", "timeZone": "America/New_York"},
        props=p,
    )
    assert calendar_event_drift_kind(ev, s, d, p, "") == "shape"


# ── Coverage staleness ────────────────────────────────────────────────────


def test_coverage_freshness_manifest_fresh(tmp_path, monkeypatch):
    import coverage
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / "manifest.json").write_text(
        '{"schema_version": 2, "generated_at": "%s"}' %
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path))
    h = coverage.compute_coverage_freshness()
    assert not h.stale
    assert h.source == "manifest"
    assert h.age_days < 1


def test_coverage_freshness_manifest_stale(tmp_path, monkeypatch):
    import coverage
    exports = tmp_path / "exports"
    exports.mkdir()
    old = (datetime.now(timezone.utc) - timedelta(days=10))
    (exports / "manifest.json").write_text(
        '{"schema_version": 2, "generated_at": "%s"}' %
        old.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path))
    h = coverage.compute_coverage_freshness()
    assert h.stale
    assert h.source == "manifest"
    assert h.age_days > 7


def test_coverage_freshness_falls_back_to_mtime(tmp_path, monkeypatch):
    import coverage, os, time
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / "universe.csv").write_text("Ticker\nABC\n")
    # No manifest. Should fall back to mtime (which is now).
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path))
    h = coverage.compute_coverage_freshness()
    assert not h.stale
    assert h.source == "mtime"


def test_coverage_freshness_missing(tmp_path, monkeypatch):
    import coverage
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path / "nonexistent"))
    h = coverage.compute_coverage_freshness()
    assert h.stale
    assert h.source == "missing"
    assert h.age_days is None


def test_coverage_alert_deduplicates_per_day(tmp_path, monkeypatch):
    """Verify kv_store dedup so we don't spam Slack across multiple runs."""
    import main, coverage
    from storage import init_db, kv_get
    from datetime import date as _date

    conn = init_db(":memory:")
    health = coverage.CoverageHealth(
        stale=True, age_days=10.0, source="manifest",
        message="test stale",
    )

    posted = []
    def fake_post(*args, **kwargs):
        posted.append(args)
        class R:
            def read(self): return b""
        return R()

    # Force webhook present + intercept the network call
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", "https://example.invalid/webhook")
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_post)

    # First call: posts and writes dedup key
    main._alert_coverage_stale_if_needed(conn, health)
    assert len(posted) == 1
    today = _date.today().isoformat()
    assert kv_get(conn, f"coverage_stale_alerted:{today}") == "alerted"

    # Second call same day: no additional post
    main._alert_coverage_stale_if_needed(conn, health)
    assert len(posted) == 1


# ── yfinance hour fallback ────────────────────────────────────────────────


def test_infer_hour_bmo_at_8_30_et():
    """The market-session boundary catches 8:30 ET releases as BMO,
    which the original <9 / >16 heuristic would have classified as None."""
    from market_data import infer_hour_from_datetime
    from zoneinfo import ZoneInfo
    dt = datetime(2026, 5, 5, 8, 30, tzinfo=ZoneInfo("America/New_York"))
    assert infer_hour_from_datetime(dt) == "bmo"


def test_infer_hour_bmo_at_6_30_et():
    from market_data import infer_hour_from_datetime
    from zoneinfo import ZoneInfo
    dt = datetime(2026, 5, 5, 6, 30, tzinfo=ZoneInfo("America/New_York"))
    assert infer_hour_from_datetime(dt) == "bmo"


def test_infer_hour_amc_at_4_05_et():
    from market_data import infer_hour_from_datetime
    from zoneinfo import ZoneInfo
    dt = datetime(2026, 5, 5, 16, 5, tzinfo=ZoneInfo("America/New_York"))
    assert infer_hour_from_datetime(dt) == "amc"


def test_infer_hour_midsession_returns_none():
    """Mid-session times fall outside both windows -> None
    (don't make confident BMO/AMC calls for unusual release times)."""
    from market_data import infer_hour_from_datetime
    from zoneinfo import ZoneInfo
    dt = datetime(2026, 5, 5, 11, 0, tzinfo=ZoneInfo("America/New_York"))
    assert infer_hour_from_datetime(dt) is None


def test_infer_hour_boundary_at_9_30_et_is_not_bmo():
    """09:30 ET is market open; treat as not-pre-market."""
    from market_data import infer_hour_from_datetime
    from zoneinfo import ZoneInfo
    dt = datetime(2026, 5, 5, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    assert infer_hour_from_datetime(dt) is None


def test_infer_hour_boundary_at_4_00_et_is_amc():
    """16:00 ET is market close; treat as post-market."""
    from market_data import infer_hour_from_datetime
    from zoneinfo import ZoneInfo
    dt = datetime(2026, 5, 5, 16, 0, tzinfo=ZoneInfo("America/New_York"))
    assert infer_hour_from_datetime(dt) == "amc"


def test_expected_state_yfinance_provenance():
    """When hour_yf provides timing and Finnhub didn't, the description's
    Status: line shows 'Confirmed (yfinance)' and ext-props gain hour_source."""
    from calendar_sync import expected_calendar_state
    summary, description, props = expected_calendar_state(
        "UFPT", None, 0.5, None, 100e6, None,
        quarter="2026Q1", tier=2,
        source_fingerprint="UFPT:2026-05-05",
        hour_yf="amc",
    )
    assert summary == "UFPT Earnings Release", summary
    assert "Status: Confirmed (yfinance)" in description
    assert props["hour_source"] == "yfinance"


def test_expected_state_finnhub_wins_over_yfinance():
    """When both Finnhub and yfinance have hour, Finnhub wins and
    description says plain 'Confirmed' (no provenance suffix)."""
    from calendar_sync import expected_calendar_state
    summary, description, props = expected_calendar_state(
        "AAPL", "amc", 1.5, None, 89e9, None,
        quarter="2026Q1", tier=1,
        source_fingerprint="AAPL:2026-05-04",
        hour_yf="bmo",  # disagreement; Finnhub takes priority
    )
    assert "Status: Confirmed" in description
    assert "yfinance" not in description
    assert "hour_source" not in props


def test_drift_kind_yf_fallback_recreates_all_day_event():
    """The UFPT fix scenario end-to-end at the helper level:
    existing all-day event, yfinance now says amc -> shape drift,
    delete+recreate path will get the right time block."""
    from calendar_sync import expected_calendar_state, calendar_event_drift_kind

    s, d, p = expected_calendar_state(
        "UFPT", None, 0.5, None, 100e6, None,
        quarter="2026Q1", tier=2,
        source_fingerprint="UFPT:2026-05-05",
        hour_yf="amc",
    )
    ev = _make_cal_event(
        summary="UFPT Earnings Release (est.)",
        description="...stale...",
        start={"date": "2026-05-05"},  # all-day, what we'd have created
        props={"earningsAgent": "true", "ticker": "UFPT",
               "source_fingerprint": "UFPT:2026-05-05", "tier": "2",
               "quarter": "2026Q1"},
    )
    effective_hour = None or "amc"
    assert calendar_event_drift_kind(ev, s, d, p, effective_hour) == "shape"


# ── Schema invariant: fresh-DB CREATE matches end of migrations ───────────


def test_fresh_db_schema_matches_migration_path():
    """Catches schema bumps where someone updates the migration list but
    forgets to add the column to the fresh-DB CREATE TABLE (or vice versa).
    """
    from storage import init_db, _MIGRATIONS

    # Path A: fresh DB
    fresh_conn = init_db(":memory:")
    fresh_cols = {row[1] for row in fresh_conn.execute("PRAGMA table_info(events)")}

    # Path B: simulate an old DB and migrate up. Use the v2 baseline shape.
    old_conn = sqlite3.connect(":memory:")
    old_conn.execute("""
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
            created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(ticker, event_date)
        )
    """)
    # Apply ALL migrations in version order
    for version in sorted(_MIGRATIONS.keys()):
        for sql in _MIGRATIONS[version]:
            try:
                old_conn.execute(sql)
            except sqlite3.OperationalError:
                # Some migrations are CREATE TABLE for separate tables
                # and may already exist; skip.
                pass
    migrated_cols = {row[1] for row in old_conn.execute("PRAGMA table_info(events)")}

    missing_in_fresh = migrated_cols - fresh_cols
    missing_in_migration = fresh_cols - migrated_cols

    assert not missing_in_fresh, (
        f"Columns missing from fresh-DB CREATE TABLE but present after migrations: "
        f"{missing_in_fresh}. Update storage.init_db's CREATE TABLE."
    )
    assert not missing_in_migration, (
        f"Columns in fresh-DB CREATE TABLE but no migration adds them: "
        f"{missing_in_migration}. Add an ALTER TABLE migration."
    )


# ── Press release vs Conference call rendering ───────────────────────────


def test_description_no_call_uses_legacy_timing_label():
    """Back-compat: when no call info, description uses 'Timing:' label."""
    from calendar_sync import build_description
    desc = build_description("XYZ", "amc", 1.0, None, 1e9, None,
                             earnings_date="2026-05-15", call_datetime_utc=None)
    assert "Timing: After Market Close" in desc
    assert "Press release:" not in desc
    assert "Conference call:" not in desc


def test_description_same_day_call_renders_compactly():
    """Same-day case: 'Conference call: 5:00 PM ET (same day)'."""
    from calendar_sync import build_description
    desc = build_description("NVDA", "amc", 5.0, None, 30e9, None,
                             earnings_date="2026-05-20",
                             call_datetime_utc="2026-05-20T21:00:00+00:00")
    assert "Press release: After Market Close" in desc
    assert "Conference call: 5:00 PM ET (same day)" in desc


def test_description_split_day_call_includes_weekday_and_date():
    """Split-day case (UFPT): 'Conference call: Tue May 5 8:30 AM ET'."""
    from calendar_sync import build_description
    desc = build_description("UFPT", "amc", 0.5, None, 1e8, None,
                             hour_source="yfinance",
                             earnings_date="2026-05-04",
                             call_datetime_utc="2026-05-05T12:30:00+00:00")
    assert "Press release: After Market Close" in desc
    assert "Conference call: Tue May 5 8:30 AM ET" in desc
    assert "Status: Confirmed (yfinance)" in desc


def test_description_unparseable_call_omits_line():
    """Junk in call_datetime_utc -> no call line, no exception."""
    from calendar_sync import build_description
    desc = build_description("XYZ", "amc", 1.0, None, 1e9, None,
                             earnings_date="2026-05-15",
                             call_datetime_utc="not-a-datetime")
    assert "Conference call:" not in desc
    # Falls back to legacy "Timing:" label since call_line returned None
    assert "Timing: After Market Close" in desc


# ── Cross-check split-day detection ───────────────────────────────────────


def test_split_day_disagreement_routes_to_split_day_verdict():
    """When split_day_call_date is set, verdict text says split-day, not conflict."""
    from notifications import DisagreementRow, _xcheck_verdict
    from datetime import date as _date
    r = DisagreementRow(
        ticker="UFPT",
        company_name="UFP Technologies",
        finnhub_date="2026-05-05",
        yf_dates=[_date(2026, 5, 4)],
        tier=2,
        finnhub_confirmed=False,
        split_day_call_date="2026-05-05",
    )
    verdict = _xcheck_verdict(r)
    assert "split-day" in verdict.lower()
    assert "no action needed" in verdict.lower()


def test_normal_disagreement_keeps_conflict_verdict():
    """Without split_day_call_date, the verdict logic falls through to the
    original Finnhub-vs-yfinance conflict messaging."""
    from notifications import DisagreementRow, _xcheck_verdict
    from datetime import date as _date
    r = DisagreementRow(
        ticker="XYZ",
        company_name="XYZ Corp",
        finnhub_date="2026-05-05",
        yf_dates=[_date(2026, 5, 12)],  # week off, not a split-day pattern
        tier=2,
        finnhub_confirmed=True,
    )
    verdict = _xcheck_verdict(r)
    assert "split-day" not in verdict.lower()
    assert "Finnhub" in verdict


def test_split_day_summary_header_softens_messaging():
    """All-split-day case: header says 'informational', not 'disagreement'."""
    from notifications import DisagreementRow, build_crosscheck_summary_blocks
    from datetime import date as _date
    r = DisagreementRow(
        ticker="UFPT", company_name="", finnhub_date="2026-05-05",
        yf_dates=[_date(2026, 5, 4)], tier=2,
        split_day_call_date="2026-05-05",
    )
    blocks = build_crosscheck_summary_blocks([r], _date(2026, 5, 4))
    header_text = blocks[0]["text"]["text"]
    assert "split-day" in header_text.lower()
    assert "informational" in header_text.lower()


# ── EDGAR 8-K tiebreaker ──────────────────────────────────────────────────


def test_edgar_release_date_overrides_split_day_in_verdict():
    """EDGAR 2.02 takes priority — even over the split-day classifier."""
    from notifications import DisagreementRow, _xcheck_verdict
    from datetime import date as _date
    r = DisagreementRow(
        ticker="UFPT", company_name="UFP Technologies",
        finnhub_date="2026-05-05",
        yf_dates=[_date(2026, 5, 4)],
        tier=2,
        # Both split-day AND EDGAR set; EDGAR wins.
        split_day_call_date="2026-05-05",
        edgar_release_date="2026-05-04",
    )
    verdict = _xcheck_verdict(r)
    assert "EDGAR confirms 2026-05-04" in verdict
    assert "Auto-locked" in verdict
    assert "yfinance correct" in verdict
    assert "split-day" not in verdict.lower()


def test_edgar_confirms_finnhub():
    """EDGAR matches Finnhub's date — verdict says Finnhub correct."""
    from notifications import DisagreementRow, _xcheck_verdict
    from datetime import date as _date
    r = DisagreementRow(
        ticker="XYZ", company_name="",
        finnhub_date="2026-05-04",
        yf_dates=[_date(2026, 5, 6)],
        tier=2,
        edgar_release_date="2026-05-04",
    )
    verdict = _xcheck_verdict(r)
    assert "Finnhub correct" in verdict
    assert "yfinance off" in verdict


def test_edgar_disagrees_with_both():
    """EDGAR has a date neither Finnhub nor yfinance agrees with."""
    from notifications import DisagreementRow, _xcheck_verdict
    from datetime import date as _date
    r = DisagreementRow(
        ticker="XYZ", company_name="",
        finnhub_date="2026-05-04",
        yf_dates=[_date(2026, 5, 6)],
        tier=2,
        edgar_release_date="2026-05-05",
    )
    verdict = _xcheck_verdict(r)
    assert "EDGAR confirms 2026-05-05" in verdict
    assert "both Finnhub and yfinance off" in verdict


def test_find_earnings_release_filing_window():
    """Verify find_earnings_release_filing filters fetch_8k_filings to the window."""
    import edgar_client
    from edgar_client import find_earnings_release_filing, Filing8K
    from datetime import date as _date
    from unittest.mock import patch

    fake = [
        Filing8K(form="8-K", filing_date="2026-05-04", accession="X1",
                 primary_doc_title="Earnings", items=("2.02",)),
        Filing8K(form="8-K", filing_date="2026-04-25", accession="X0",
                 primary_doc_title="Older earnings", items=("2.02",)),
        Filing8K(form="8-K", filing_date="2026-05-10", accession="X2",
                 primary_doc_title="Future earnings", items=("2.02",)),
    ]
    with patch.object(edgar_client, "fetch_8k_filings", return_value=fake):
        # Window includes May 4 -> matches X1
        result = find_earnings_release_filing("XYZ", _date(2026, 5, 3), _date(2026, 5, 5))
        assert result is not None
        assert result.accession == "X1"

        # Window excludes both -> None
        result = find_earnings_release_filing("XYZ", _date(2026, 6, 1), _date(2026, 6, 5))
        assert result is None

        # Empty fetch -> None
        with patch.object(edgar_client, "fetch_8k_filings", return_value=[]):
            result = find_earnings_release_filing("XYZ", _date(2026, 5, 3), _date(2026, 5, 5))
            assert result is None


# ── Run all tests ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_find_calendar_event_returns_match()
    test_find_calendar_event_no_match()
    test_create_includes_extended_properties()
    test_no_duplicate_when_db_lost_but_calendar_has_event()
    test_creates_event_when_truly_new()
    test_cleanup_deletes_duplicates()
    test_coverage_tier_resolution()
    test_migration_preserves_data()
    test_date_to_quarter()
    test_ticktick_task_title()
    test_ticktick_task_content()
    test_ticktick_dedup_skips_existing()
    test_ticktick_quarter_list_name()
    test_expected_state_confirmed_amc()
    test_expected_state_estimated_no_hour()
    test_expected_state_reported_drops_est_marker()
    test_drift_kind_fresh()
    test_drift_kind_text_only_summary()
    test_drift_kind_text_only_props()
    test_drift_kind_shape_all_day_to_amc()
    test_drift_kind_shape_bmo_to_amc()
    test_drift_kind_shape_amc_to_all_day()
    print("\nAll 21 tests passed!")
