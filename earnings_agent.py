"""
Earnings Release Calendar Agent (Legacy Entry Point)
=====================================================
This file is preserved for backward compatibility.
All logic has been moved to the modular package:
  - config.py         — configuration and environment
  - coverage.py       — Coverage Manager integration and tier resolution
  - storage.py        — SQLite database and migrations
  - finnhub_client.py — Finnhub API client
  - calendar_sync.py  — Google Calendar operations
  - main.py           — CLI entry point and orchestrator

Usage (either works):
    python main.py [--dry-run] [--backfill] [--cleanup]
    python earnings_agent.py [--dry-run] [--backfill] [--cleanup]
"""

# Re-export key functions for backward compatibility with tests and scripts
from storage import (
    init_db,
    find_existing_event as _find_existing_new,
    find_event_by_ticker_quarter,
    upsert_event as _upsert_new,
    date_to_quarter,
)
from calendar_sync import (
    get_calendar_service,
    find_calendar_event as _find_calendar_new,
    create_calendar_event as _create_calendar_new,
    update_calendar_event_description,
    delete_calendar_event,
    build_description,
    cleanup_duplicates as _cleanup_new,
    parse_ticker_from_summary as _parse_ticker_from_summary,
)
from config import GOOGLE_CALENDAR_ID, TICKERS_FILE


# ---------------------------------------------------------------------------
# Backward-compatible wrappers
# These maintain the old function signatures so existing tests keep working
# until they are updated to use the new modules directly.
# ---------------------------------------------------------------------------


def find_existing_event(conn, ticker, quarter):
    """Legacy wrapper: look up by ticker + quarter."""
    return find_event_by_ticker_quarter(conn, ticker, quarter)


def upsert_event(
    conn, ticker, quarter, event_date, event_hour, gcal_id,
    eps_estimate=None, eps_actual=None, rev_estimate=None, rev_actual=None,
    reported=False,
):
    """Legacy wrapper: upsert with old positional signature."""
    _upsert_new(
        conn, ticker, event_date, event_hour, gcal_id,
        quarter=quarter, eps_estimate=eps_estimate, eps_actual=eps_actual,
        rev_estimate=rev_estimate, rev_actual=rev_actual, reported=reported,
    )


def find_calendar_event(service, calendar_id, ticker, quarter, earnings_date):
    """Legacy wrapper: find by ticker + quarter (now finds by ticker + date proximity)."""
    return _find_calendar_new(service, calendar_id, ticker, earnings_date)


def create_calendar_event(
    service, calendar_id, ticker, quarter, earnings_date, hour,
    eps_estimate=None, eps_actual=None, revenue_estimate=None, revenue_actual=None,
):
    """Legacy wrapper: create with old positional signature."""
    return _create_calendar_new(
        service, calendar_id, ticker, earnings_date, hour,
        quarter=quarter, eps_estimate=eps_estimate, eps_actual=eps_actual,
        revenue_estimate=revenue_estimate, revenue_actual=revenue_actual,
    )


def load_tickers():
    """Legacy: load tickers from tickers.txt, falling back to .env."""
    import os
    if TICKERS_FILE.exists():
        tickers = [
            line.strip().upper()
            for line in TICKERS_FILE.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if tickers:
            return tickers
    return [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]


# Run via main.py when executed directly
if __name__ == "__main__":
    from main import main
    main()
