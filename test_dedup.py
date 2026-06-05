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
    """Test quarterly list name generation with reporting quarter + tier."""
    from ticktick import _quarter_list_name

    # Tier 1 → Core Watchlist (Portfolio + Researching consolidated)
    assert _quarter_list_name("2026-04-30", tier=1) == "1Q26 Earnings - Core Watchlist - Positions/Researching"

    # `position` is accepted but ignored — all Tier 1 land in the same list.
    assert _quarter_list_name("2026-04-30", tier=1, position="Portfolio") == "1Q26 Earnings - Core Watchlist - Positions/Researching"
    assert _quarter_list_name("2026-04-30", tier=1, position="Researching") == "1Q26 Earnings - Core Watchlist - Positions/Researching"
    assert _quarter_list_name("2026-04-30", tier=1, position="Unknown") == "1Q26 Earnings - Core Watchlist - Positions/Researching"

    # Tier 2 unchanged — HC Svcs & MedTech.
    assert _quarter_list_name("2026-04-30", tier=2) == "1Q26 Earnings - HC Svcs & MedTech"

    # Tier 3 has no suffix.
    assert _quarter_list_name("2026-04-30", tier=3) == "1Q26 Earnings"

    # Reporting-quarter math unchanged — Jan-Mar releases report Q4 of prior year, etc.
    assert _quarter_list_name("2026-07-15", tier=2) == "2Q26 Earnings - HC Svcs & MedTech"
    assert _quarter_list_name("2026-10-15", tier=1, position="Portfolio") == "3Q26 Earnings - Core Watchlist - Positions/Researching"
    assert _quarter_list_name("2026-01-15", tier=2) == "4Q25 Earnings - HC Svcs & MedTech"

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


def test_expected_state_estimated_past_date_drops_est_marker():
    """A Finnhub-estimated (empty-hour) event whose date has already passed must
    NOT keep the '(est.)' marker — an estimate only makes sense for a future
    date. With no actuals yet, the title is the neutral 'Earnings Release' and
    the status reads 'Date passed (results pending)', not 'Estimated'."""
    from datetime import date, timedelta
    from calendar_sync import expected_calendar_state

    past = (date.today() - timedelta(days=10)).isoformat()
    summary, description, _ = expected_calendar_state(
        "UFPT", "", 0.5, None, 100e6, None,
        quarter="2026Q1", tier=2, source_fingerprint=f"UFPT:{past}",
        earnings_date=past,
    )
    assert summary == "UFPT Earnings Release"
    assert "(est.)" not in summary
    assert "Status: Date passed (results pending)" in description
    assert "Status: Estimated" not in description


def test_expected_state_estimated_future_date_keeps_est_marker():
    """The mirror case: an estimated event still in the future keeps '(est.)'
    and the 'Estimated' status — the fix must not strip the marker early."""
    from datetime import date, timedelta
    from calendar_sync import expected_calendar_state

    future = (date.today() + timedelta(days=30)).isoformat()
    summary, description, _ = expected_calendar_state(
        "UFPT", "", 0.5, None, 100e6, None,
        quarter="2026Q1", tier=2, source_fingerprint=f"UFPT:{future}",
        earnings_date=future,
    )
    assert summary == "UFPT Earnings Release (est.)"
    assert "Status: Estimated" in description


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


def test_reported_row_survives_same_quarter_phantom_upsert():
    """Regression for the ICLR re-post spam (2026-06).

    Finnhub double-lists date-flapping names: a real reported event (with
    actuals) plus a phantom forward listing with no actuals, both mapping to
    the same reporting quarter. The old same-quarter cleanup DELETE in
    upsert_event would wipe the reported row when the phantom was inserted,
    so the actuals re-posted every run. The DELETE must now spare reported=1
    rows.
    """
    from storage import (
        init_db, upsert_event, find_existing_event,
        find_reported_event_for_quarter, date_to_quarter,
    )

    conn = init_db(":memory:")

    # Real ICON results land and get marked reported.
    upsert_event(
        conn, "ICLR", "2026-05-27", "amc", None,
        quarter=date_to_quarter("2026-05-27"),
        eps_estimate=3.18, eps_actual=2.52,
        rev_estimate=2.0e9, rev_actual=2.11e9,
        reported=True, tier=2, company_name="ICON PLC",
    )
    assert find_existing_event(conn, "ICLR", "2026-05-27")["reported"] is True

    # A phantom forward listing (no actuals, same quarter, different date) is
    # upserted as a brand-new row — the INSERT branch runs its quarter cleanup.
    upsert_event(
        conn, "ICLR", "2026-06-02", "amc", None,
        quarter=date_to_quarter("2026-06-02"),
        eps_estimate=2.59, reported=False, tier=2, company_name="ICON PLC",
    )

    # The reported actuals row must still be intact.
    survived = find_existing_event(conn, "ICLR", "2026-05-27")
    assert survived is not None, "reported actuals row was clobbered by phantom"
    assert survived["reported"] is True
    assert survived["eps_actual"] == 2.52

    # And it is discoverable as the quarter's reported event.
    rq = find_reported_event_for_quarter(conn, "ICLR", date_to_quarter("2026-05-27"))
    assert rq is not None and rq["event_date"] == "2026-05-27"


def _fh(sym, d, **kw):
    base = {"symbol": sym, "date": d, "hour": kw.get("hour", ""),
            "epsEstimate": kw.get("epsEstimate"), "epsActual": kw.get("epsActual"),
            "revenueEstimate": kw.get("revenueEstimate"),
            "revenueActual": kw.get("revenueActual")}
    return base


def _fmp(sym, d, **kw):
    return {"symbol": sym, "date": d, "hour": None,
            "epsEstimate": kw.get("epsEstimate"), "epsActual": kw.get("epsActual"),
            "revenueEstimate": kw.get("revenueEstimate"),
            "revenueActual": kw.get("revenueActual"), "source": "fmp"}


def test_merge_fmp_actuals_beat_finnhub_lag():
    """The FIVE case: Finnhub has the event without actuals on the wrong day;
    FMP has it WITH actuals on the right day (same quarter). FMP wins."""
    from fmp_client import merge_earnings
    fh = [_fh("FIVE", "2026-06-02", epsEstimate=1.77)]
    fmp = [_fmp("FIVE", "2026-06-03", epsEstimate=1.77, epsActual=2.22,
                revenueActual=1.285e9)]
    m = merge_earnings(fh, fmp)
    assert len(m) == 1
    assert m[0]["date"] == "2026-06-03"          # FMP's real report date
    assert m[0]["epsActual"] == 2.22             # FMP's actuals
    assert "fmp" in m[0]["source"]


def test_merge_breadth_fmp_only_name_included():
    """A name only FMP lists is carried through (the breadth win)."""
    from fmp_client import merge_earnings
    m = merge_earnings([], [_fmp("AAPL", "2026-05-01", epsActual=1.6)])
    assert [e["symbol"] for e in m] == ["AAPL"]


def test_merge_shared_upcoming_keeps_finnhub_date_authority():
    """When both have an UPCOMING (no-actuals) event, Finnhub's date + hour win
    (date arbitration stays with Finnhub/cross-check, dodging FMP date errors);
    FMP only fills missing estimates."""
    from fmp_client import merge_earnings
    fh = [_fh("MDT", "2026-07-10", hour="bmo")]                       # no estimate
    fmp = [_fmp("MDT", "2026-07-12", epsEstimate=1.5)]                # diff date
    m = merge_earnings(fh, fmp)
    assert len(m) == 1
    assert m[0]["date"] == "2026-07-10" and m[0]["hour"] == "bmo"
    assert m[0]["epsEstimate"] == 1.5            # filled from FMP


def test_merge_fmp_actuals_win_preserves_finnhub_hour():
    """Regression: when an FMP actuals row wins over a Finnhub row that knew the
    session (amc), the merged row must keep the Finnhub hour — otherwise the
    post-earnings move window + deferral get the session wrong."""
    from fmp_client import merge_earnings
    fh = [_fh("MDT", "2026-05-20", hour="amc", epsEstimate=1.5)]   # amc, no actuals
    fmp = [_fmp("MDT", "2026-05-21", epsEstimate=1.5, epsActual=1.6)]
    m = merge_earnings(fh, fmp)
    assert len(m) == 1
    assert m[0]["date"] == "2026-05-21"          # FMP's real report date wins
    assert m[0]["epsActual"] == 1.6
    assert m[0]["hour"] == "amc"                  # Finnhub hour preserved


def test_fmp_partial_failure_counts(monkeypatch):
    """A partial outage (some chunks fail) is reported via FMPFetch counts so
    the caller can alarm, instead of silently shrinking the merge."""
    import fmp_client
    import urllib.error

    monkeypatch.setattr(fmp_client, "FMP_API_KEY", "test-key")
    monkeypatch.setattr(fmp_client, "_CHUNK_SLEEP", 0)

    class _FakeResp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"[]"

    calls = {"n": 0}
    def fake_urlopen(url, timeout=30):
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeResp()            # chunk 1 ok (empty)
        raise urllib.error.URLError("boom")  # chunk 2 fails
    monkeypatch.setattr(fmp_client.urllib.request, "urlopen", fake_urlopen)

    res = fmp_client.fetch_fmp_earnings(["FIVE"], "2026-06-01", "2026-06-09")
    assert res.total_chunks == 2
    assert res.failed_chunks == 1
    assert res.events == []


def test_merge_finnhub_actuals_kept_filled_from_fmp():
    """When Finnhub already has actuals, keep its row but backfill any missing
    fields (e.g. revenue) from FMP."""
    from fmp_client import merge_earnings
    fh = [_fh("XYZ", "2026-05-10", epsEstimate=1.0, epsActual=1.1)]   # no revenue
    fmp = [_fmp("XYZ", "2026-05-10", epsActual=1.1, revenueActual=5e8)]
    m = merge_earnings(fh, fmp)
    assert m[0]["epsActual"] == 1.1
    assert m[0]["revenueActual"] == 5e8          # filled from FMP


def test_record_actuals_new_name_not_silently_reported(monkeypatch):
    """Regression: a brand-new event with actuals (e.g. FMP surfaced a name
    Finnhub never listed) must be queued for a beat/miss post and left
    reported=0 until Slack — NOT silently marked reported."""
    import main
    from storage import init_db, find_existing_event
    from datetime import date as _date

    conn = init_db(":memory:")
    monkeypatch.setattr(main, "fetch_post_earnings_move", lambda *a, **k: 5.0)  # move avail
    sync = []
    queued = main._record_actuals(
        conn, _date(2026, 6, 4), sync,
        ticker="AAPL", earnings_date="2026-05-01", hour="amc", quarter="2026Q1",
        eps_est=1.5, eps_act=1.6, rev_est=9e10, rev_act=9.1e10, tier=1,
        company_name="Apple", source_fingerprint="AAPL:2026-05-01",
        hour_yf=None, call_dt_iso=None, call_source=None, gcal_id=None,
        info=None, dry_run=False,
    )
    assert queued is True
    row = find_existing_event(conn, "AAPL", "2026-05-01")
    assert row is not None and row["reported"] is False   # not silently reported
    assert row["eps_actual"] == 1.6
    assert [r.ticker for r in sync] == ["AAPL"]            # queued for Slack


def test_record_actuals_defers_without_move(monkeypatch):
    """No stock-move yet (recent AMC) -> deferred: persisted but not queued,
    still reported=0 for the next sweep."""
    import main
    from storage import init_db, find_existing_event
    from datetime import date as _date

    conn = init_db(":memory:")
    monkeypatch.setattr(main, "fetch_post_earnings_move", lambda *a, **k: None)
    sync = []
    queued = main._record_actuals(
        conn, _date(2026, 6, 4), sync,
        ticker="XYZ", earnings_date="2026-06-03", hour="amc", quarter="2026Q1",
        eps_est=1.0, eps_act=1.1, rev_est=None, rev_act=None, tier=2,
        company_name="XYZ", source_fingerprint="XYZ:2026-06-03",
        hour_yf=None, call_dt_iso=None, call_source=None, gcal_id=None,
        info=None, dry_run=False,
    )
    assert queued is False and sync == []
    assert find_existing_event(conn, "XYZ", "2026-06-03")["reported"] is False


def test_move_calendar_no_orphan_on_create_failure(monkeypatch):
    """Create-first: if the new event can't be created, the OLD event is left
    untouched (no orphan) and its id is returned."""
    import main
    from calendar_sync import CalendarError

    deleted = []
    def boom_create(*a, **k):
        raise CalendarError("create failed")
    monkeypatch.setattr(main, "create_calendar_event", boom_create)
    monkeypatch.setattr(main, "delete_calendar_event",
                        lambda *a, **k: deleted.append(a))
    gcal_id, created = main._move_calendar_event(
        object(), "FIVE", "OLD", "2026-06-03", "amc",
        quarter="2026Q1", eps_est=1.0, eps_act=1.1, rev_est=None, rev_act=None,
        tier=1, source_fingerprint="FIVE:2026-06-03", hour_yf=None, call_dt_iso=None,
    )
    assert gcal_id == "OLD"    # keep old pointer
    assert created is False     # signals failure so same-date callers can retry
    assert deleted == []       # old event NOT deleted -> no orphan


def test_move_calendar_success_deletes_old(monkeypatch):
    """Create-first happy path: new event created, then old one deleted."""
    import main
    monkeypatch.setattr(main, "create_calendar_event", lambda *a, **k: "NEW")
    deleted = []
    monkeypatch.setattr(main, "delete_calendar_event",
                        lambda svc, cal, gid: deleted.append(gid))
    gcal_id, created = main._move_calendar_event(
        object(), "FIVE", "OLD", "2026-06-03", "amc",
        quarter="2026Q1", eps_est=1.0, eps_act=1.1, rev_est=None, rev_act=None,
        tier=1, source_fingerprint="FIVE:2026-06-03", hour_yf=None, call_dt_iso=None,
    )
    assert gcal_id == "NEW" and created is True
    assert deleted == ["OLD"]


def test_edgar_auto_correction_calendar_fail_does_not_move_or_lock(monkeypatch):
    """EDGAR correction locks the new date, so reconcile can't heal a calendar
    failure. If the Calendar create fails, the DB must stay put (old row, not
    locked) and the function returns False for retry."""
    import main
    from storage import init_db, find_existing_event, upsert_event
    from calendar_sync import CalendarError

    conn = init_db(":memory:")
    upsert_event(conn, "FIVE", "2026-06-02", "amc", "OLD", quarter="2026Q1",
                 eps_estimate=1.0, reported=False, tier=1, company_name="Five Below")
    monkeypatch.setattr(main, "fetch_yfinance_hour_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_yfinance_call_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "create_calendar_event",
                        lambda *a, **k: (_ for _ in ()).throw(CalendarError("boom")))
    deleted = []
    monkeypatch.setattr(main, "delete_calendar_event", lambda *a, **k: deleted.append(a))

    result = main._apply_edgar_auto_correction(
        conn, object(), "FIVE", "2026-06-02", "2026-05-27")
    assert result is False
    old = find_existing_event(conn, "FIVE", "2026-06-02")
    assert old is not None
    assert old["date_locked"] is False and old["gcal_id"] == "OLD"   # DB untouched
    assert find_existing_event(conn, "FIVE", "2026-05-27") is None    # not moved
    assert deleted == []                                              # old event intact


def test_edgar_auto_correction_success_moves_and_locks(monkeypatch):
    """Happy path: calendar created at the new date, then DB moved + locked,
    old row + calendar event removed."""
    import main
    from storage import init_db, find_existing_event, upsert_event

    conn = init_db(":memory:")
    upsert_event(conn, "FIVE", "2026-06-02", "amc", "OLD", quarter="2026Q1",
                 eps_estimate=1.0, reported=False, tier=1, company_name="Five Below")
    monkeypatch.setattr(main, "fetch_yfinance_hour_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_yfinance_call_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "create_calendar_event", lambda *a, **k: "NEW")
    deleted = []
    monkeypatch.setattr(main, "delete_calendar_event",
                        lambda svc, cal, gid: deleted.append(gid))

    result = main._apply_edgar_auto_correction(
        conn, object(), "FIVE", "2026-06-02", "2026-05-27")
    assert result is True
    new = find_existing_event(conn, "FIVE", "2026-05-27")
    assert new is not None and new["date_locked"] is True and new["gcal_id"] == "NEW"
    assert find_existing_event(conn, "FIVE", "2026-06-02") is None
    assert deleted == ["OLD"]


def test_edgar_date_corroborated_logic():
    """Corroboration gate: only an EDGAR date within ±1d of a yfinance date
    counts as corroborated; a third distinct date or no yfinance does not."""
    from main import _edgar_date_corroborated
    from datetime import date as _date
    assert _edgar_date_corroborated("2026-06-03", [_date(2026, 6, 3)]) is True
    assert _edgar_date_corroborated("2026-06-03", [_date(2026, 6, 2)]) is True  # 1d
    assert _edgar_date_corroborated("2026-06-10", [_date(2026, 6, 2)]) is False  # 3rd date
    assert _edgar_date_corroborated("2026-06-03", []) is False                   # no yf
    assert _edgar_date_corroborated("not-a-date", [_date(2026, 6, 3)]) is False


def test_run_safeguard_alerts_and_reraises(monkeypatch):
    """A failing safeguard must post a degraded-health alert AND re-raise so
    the workflow step fails loud (never silently continue-on-error)."""
    import main
    posted = []
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", "https://example.invalid/wh")
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "post_slack",
                        lambda wh, blocks, fallback: posted.append((wh, fallback)))

    def boom():
        raise RuntimeError("yfinance exploded")

    import pytest as _pytest
    with _pytest.raises(RuntimeError):
        main._run_safeguard("cross-check", boom)
    assert len(posted) == 1
    assert "Safeguard degraded" in posted[0][1]
    assert "cross-check" in posted[0][1]


def test_run_safeguard_passes_through_success(monkeypatch):
    """No alert, returns the wrapped value when the safeguard succeeds."""
    import main
    posted = []
    monkeypatch.setattr(main, "post_slack",
                        lambda *a, **k: posted.append(a))
    assert main._run_safeguard("ok", lambda: 42) == 42
    assert posted == []


def test_populate_db_only_folds_to_dry_run(monkeypatch):
    """--populate-db-only is an alias that sets dry_run, so no external writes."""
    import main, sys
    captured = {}
    monkeypatch.setattr(main, "run", lambda **kw: captured.update(kw))
    # Avoid touching coverage/DB — run() is fully stubbed above.
    monkeypatch.setattr(sys, "argv", ["main.py", "--populate-db-only", "--no-ticktick"])
    main.main()
    assert captured.get("dry_run") is True


def test_edgar_fallback_blind_sweep_flags_unlisted_tier1(monkeypatch):
    """The DB-independent Tier-1 sweep must flag a Tier-1 name that filed an
    8-K 2.02 but has NO DB row (Finnhub never listed it), and must NOT flag a
    name whose quarter is already recorded as reported."""
    import main
    from storage import init_db, upsert_event, date_to_quarter
    from edgar_client import Filing8K
    from types import SimpleNamespace

    conn = init_db(":memory:")
    # BBB already reported this quarter (filing date 2026-06-03 -> 2026Q1).
    upsert_event(conn, "BBB", "2026-06-03", "amc", None,
                 quarter=date_to_quarter("2026-06-03"), eps_actual=1.0,
                 reported=True, tier=1, company_name="Bravo Inc")

    coverage = [
        SimpleNamespace(ticker="AAA", tier=1, company_name="Alpha Inc"),
        SimpleNamespace(ticker="BBB", tier=1, company_name="Bravo Inc"),
        SimpleNamespace(ticker="CCC", tier=1, company_name="Charlie Inc"),
        SimpleNamespace(ticker="DDD", tier=2, company_name="Delta Inc"),  # T2: not swept
    ]
    monkeypatch.setattr(main, "load_coverage", lambda: coverage)
    monkeypatch.setattr(main, "init_db", lambda *a, **k: conn)
    monkeypatch.setattr(main, "get_cik", lambda t: "1234567")

    def fake_filing(ticker, start, end):
        if ticker in ("AAA", "BBB"):
            return Filing8K(form="8-K", filing_date="2026-06-03",
                            accession="0001234567-26-000001",
                            primary_doc_title="Q1", items=("2.02",))
        return None
    monkeypatch.setattr(main, "find_earnings_release_filing", fake_filing)

    posted = {}
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", "https://example.invalid/wh")
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", None)
    monkeypatch.setattr(main, "post_slack",
                        lambda wh, blocks, fallback: posted.update(fallback=fallback))

    main.run_edgar_results_fallback(dry_run=False, skip_heartbeat=True)

    fb = posted.get("fallback", "")
    assert "AAA" in fb, "unlisted Tier-1 filer should be flagged"
    assert "BBB" not in fb, "already-reported quarter should be skipped"
    assert "CCC" not in fb and "DDD" not in fb


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
    """Split-day case (UFPT): AMC release day X, BMO call day X+1.

    Uses a date relative to today so the 'Confirmed' (upcoming) status path is
    always exercised — a hardcoded past date instead drifts into the
    'Date passed (results pending)' branch once it ages.
    """
    from calendar_sync import build_description
    from datetime import date as _date, timedelta as _td, datetime as _dt, timezone as _tz
    from zoneinfo import ZoneInfo

    rel = _date.today() + _td(days=30)
    call_day = rel + _td(days=1)
    # 8:30 AM ET on day X+1 — build in ET then store as UTC so the displayed
    # "8:30 AM ET" is DST-correct regardless of when the test runs.
    call_et = _dt(call_day.year, call_day.month, call_day.day, 8, 30,
                  tzinfo=ZoneInfo("America/New_York"))
    desc = build_description("UFPT", "amc", 0.5, None, 1e8, None,
                             hour_source="yfinance",
                             earnings_date=rel.isoformat(),
                             call_datetime_utc=call_et.astimezone(_tz.utc).isoformat())
    expected_call = (
        f"Conference call: {call_day.strftime('%a %b ')}{call_day.day} 8:30 AM ET"
    )
    assert "Press release: After Market Close" in desc
    assert expected_call in desc
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


# ── IR email announcement detection ───────────────────────────────────────


def _make_gmail_msg(subject="", body="", sender="noreply@notified.com",
                    received_date=None, msg_id="abc123", thread_id="thr456"):
    from gmail_client import GmailMessage
    from datetime import date as _date
    return GmailMessage(
        id=msg_id, thread_id=thread_id,
        sender=sender, subject=subject, body=body,
        received_date=received_date or _date(2026, 5, 1),
    )


def test_ir_email_classic_pre_announcement():
    """Typical IR alert: 'AcmeCo Schedules Q1 2026 Earnings Conference Call'."""
    from gmail_client import detect_earnings_announcement
    from datetime import date as _date
    msg = _make_gmail_msg(
        subject="AcmeCo Schedules First Quarter 2026 Earnings Conference Call for May 8",
        body="AcmeCo announces it will release first quarter 2026 financial results on May 8, 2026 before market open.",
    )
    announced, matched = detect_earnings_announcement(msg, _date(2026, 5, 8))
    assert matched
    assert announced == _date(2026, 5, 8)


def test_ir_email_rejects_earnings_preview_noise():
    """Analyst commentary that mentions earnings — should NOT match."""
    from gmail_client import detect_earnings_announcement
    from datetime import date as _date
    msg = _make_gmail_msg(
        subject="AcmeCo Q1 Earnings Preview: What to Expect",
        body="With earnings on deck, analysts expect the company to beat consensus.",
    )
    _, matched = detect_earnings_announcement(msg, _date(2026, 5, 8))
    assert not matched


def test_ir_email_rejects_wrong_quarter():
    """Q4 release announcement when we're looking for Q1 — reject."""
    from gmail_client import detect_earnings_announcement
    from datetime import date as _date
    msg = _make_gmail_msg(
        subject="AcmeCo to Release Fourth Quarter 2025 Results on February 14",
        body="AcmeCo will report fourth quarter 2025 earnings on February 14, 2026.",
    )
    # event is in Q1 reporting window (May), so Q4 announcement shouldn't match
    _, matched = detect_earnings_announcement(msg, _date(2026, 5, 8))
    assert not matched


def test_extract_sender_email():
    from gmail_client import extract_sender_email
    assert extract_sender_email("Notified <noreply@notified.com>") == "noreply@notified.com"
    assert extract_sender_email("noreply@globenewswire.com") == "noreply@globenewswire.com"
    assert extract_sender_email("Q4 IR Updates <ir@q4inc.com>") == "ir@q4inc.com"


def test_is_known_ir_sender():
    from gmail_client import is_known_ir_sender
    assert is_known_ir_sender("noreply@notified.com")
    assert is_known_ir_sender("ir-noreply@globenewswire.com")
    assert is_known_ir_sender("anything@q4inc.com")
    # Not a known IR platform
    assert not is_known_ir_sender("ceo@acmeco.com")
    assert not is_known_ir_sender("newsletter@medtech-dive.com")


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
    test_expected_state_estimated_past_date_drops_est_marker()
    test_expected_state_estimated_future_date_keeps_est_marker()
    test_drift_kind_fresh()
    test_drift_kind_text_only_summary()
    test_drift_kind_text_only_props()
    test_drift_kind_shape_all_day_to_amc()
    test_drift_kind_shape_bmo_to_amc()
    test_drift_kind_shape_amc_to_all_day()
    print("\nAll 23 tests passed!")
