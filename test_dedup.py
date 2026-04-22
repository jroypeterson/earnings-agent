"""
Tests that the Calendar API deduplication logic prevents duplicate events
when the local SQLite DB is lost.

Tests use the backward-compatible wrappers in earnings_agent.py to verify
that the new modular code preserves existing behavior.
"""

import sqlite3
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
    """Create a fresh in-memory SQLite DB with the earnings schema."""
    conn = sqlite3.connect(":memory:")
    # Use the new schema with UNIQUE(ticker, event_date) but also keep quarter column
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

    # Reporting quarter: Apr-Jun releases report Q1
    assert _quarter_list_name("2026-04-30", tier=1) == "1Q26 Earnings - Core Watchlist"
    assert _quarter_list_name("2026-04-30", tier=2) == "1Q26 Earnings - HC Svcs & MedTech"

    # Jul-Sep releases report Q2
    assert _quarter_list_name("2026-07-15", tier=2) == "2Q26 Earnings - HC Svcs & MedTech"

    # Oct-Dec releases report Q3
    assert _quarter_list_name("2026-10-15", tier=1) == "3Q26 Earnings - Core Watchlist"

    # Jan-Mar releases report Q4 of prior year
    assert _quarter_list_name("2026-01-15", tier=2) == "4Q25 Earnings - HC Svcs & MedTech"

    print("PASS: TickTick quarterly list name generation correct")


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
    print("\nAll 13 tests passed!")
