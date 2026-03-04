"""
Tests that the Calendar API deduplication logic in earnings_agent.py
prevents duplicate events when the local SQLite DB is lost.
"""

import sqlite3
from unittest.mock import MagicMock, patch, call
from pathlib import Path

import earnings_agent as ea


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
    return conn


# ── Test 1: find_calendar_event returns matching event ────────────────────

def test_find_calendar_event_returns_match():
    existing = {
        "id": "gcal_abc123",
        "summary": "AAPL Earnings",
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
    # Event for different quarter
    existing = {
        "id": "gcal_xyz",
        "summary": "AAPL Earnings",
        "extendedProperties": {
            "private": {"earningsAgent": "true", "ticker": "AAPL", "quarter": "2025Q3"}
        },
    }
    service, _ = make_mock_calendar_service(existing_events=[existing])

    result = ea.find_calendar_event(service, "cal_id", "AAPL", "2025Q4", "2026-02-01")
    assert result is None
    print("PASS: find_calendar_event returns None for non-matching quarter")


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

    # Calendar already has this event
    existing_cal_event = {
        "id": "gcal_existing_123",
        "summary": "GOOG Earnings",
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
    Simulates 3 events for the same ticker+quarter on the calendar.
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

    # Wrap the real connection so we can intercept close()
    conn = make_in_memory_db()
    wrapper = MagicMock(wraps=conn)
    wrapper.close = MagicMock()  # no-op close so we can inspect DB after

    with patch.object(ea, "get_calendar_service", return_value=service), \
         patch.object(ea, "init_db", return_value=wrapper), \
         patch.object(ea, "GOOGLE_CALENDAR_ID", "cal_id"):
        ea.cleanup_duplicates(dry_run=False)

    assert "gcal_old_1" in deleted_ids, "Oldest dupe should be deleted"
    assert "gcal_old_2" in deleted_ids, "Middle dupe should be deleted"
    assert "gcal_newest" not in deleted_ids, "Newest event should be kept"
    assert len(deleted_ids) == 2

    # DB should have the keeper
    db_event = ea.find_existing_event(conn, "AAPL", "2025Q4")
    assert db_event is not None
    assert db_event["gcal_id"] == "gcal_newest"

    conn.close()
    print("PASS: cleanup_duplicates deletes extras, keeps newest")


# ── Run all tests ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_find_calendar_event_returns_match()
    test_find_calendar_event_no_match()
    test_create_includes_extended_properties()
    test_no_duplicate_when_db_lost_but_calendar_has_event()
    test_creates_event_when_truly_new()
    test_cleanup_deletes_duplicates()
    print("\nAll 6 deduplication tests passed!")
