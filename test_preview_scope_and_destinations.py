"""Selection scope + per-destination dedup for the consensus preview.

Two behaviours JP asked for on 2026-09-09, neither of which had any test:

  1. The preview covers Portfolio / Researching / Ready to Buy by POSITION,
     so the 10 Researching names that are not Core=Y (and therefore not
     Tier 1) still get previewed.
  2. #street-account and #portfolio each keep their own posted-ledger, so a
     failure on one is never recorded as a success for the other.
"""

import sqlite3

import pytest

from coverage import TickerInfo
from consensus_preview import (
    PREVIEW_POSITIONS,
    select_upcoming_reporters,
)
import main
from main import (
    PREVIEW_DEST_PORTFOLIO,
    PREVIEW_DEST_STREET_ACCOUNT,
    _preview_kv_key,
)


def _conn_with_events(rows):
    """In-memory events table carrying just the columns the selector reads."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE events ("
        " ticker TEXT, event_date TEXT, event_hour TEXT, event_hour_yf TEXT,"
        " tier INTEGER, company_name TEXT, eps_estimate REAL,"
        " rev_estimate REAL, call_datetime_utc TEXT, reported INTEGER,"
        " closed_reason TEXT)"
    )
    conn.executemany(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    return conn


def _event(ticker, event_date, tier):
    return (ticker, event_date, "amc", None, tier, ticker, None, None,
            None, 0, None)


@pytest.fixture
def today_iso():
    from datetime import date
    return date.today().isoformat()


# --- 1. Scope ---------------------------------------------------------------

def test_non_core_researching_name_is_previewed_by_position(today_iso):
    """The exact gap JP's ask closes: Tier 2 by the tier rule, but Researching.

    Before the position arm existed this name was silently absent from every
    preview -- the tier rule gates Researching on Core=Y.
    """
    conn = _conn_with_events([_event("UBER", today_iso, 2)])
    coverage = [TickerInfo("UBER", 2, "Uber", "Tech", "", position="Researching")]

    got = select_upcoming_reporters(
        conn, coverage, days_ahead=3, max_tier=1, positions=PREVIEW_POSITIONS
    )

    assert [r.ticker for r in got] == ["UBER"]
    assert got[0].position == "Researching"


def test_position_arm_only_widens_never_narrows(today_iso):
    """A Tier-1 name with no Position list still qualifies via the tier arm."""
    conn = _conn_with_events([_event("CPRT", today_iso, 1)])
    coverage = [TickerInfo("CPRT", 1, "Copart", "Industrials", "", position="")]

    got = select_upcoming_reporters(
        conn, coverage, days_ahead=3, max_tier=1, positions=PREVIEW_POSITIONS
    )

    assert [r.ticker for r in got] == ["CPRT"]


def test_unwatched_name_is_still_excluded(today_iso):
    """Widening must not turn the preview into the whole universe."""
    conn = _conn_with_events([_event("RANDOM", today_iso, 3)])
    coverage = [TickerInfo("RANDOM", 3, "Random", "Tech", "",
                           position="Following for Interest")]

    got = select_upcoming_reporters(
        conn, coverage, days_ahead=3, max_tier=1, positions=PREVIEW_POSITIONS
    )

    assert got == []


def test_omitting_positions_preserves_the_old_tier_only_behaviour(today_iso):
    conn = _conn_with_events([_event("UBER", today_iso, 2)])
    coverage = [TickerInfo("UBER", 2, "Uber", "Tech", "", position="Researching")]

    assert select_upcoming_reporters(
        conn, coverage, days_ahead=3, max_tier=1
    ) == []


# --- 2. Per-destination dedup ----------------------------------------------

def test_street_account_keeps_its_original_key_shape():
    """Re-keying would orphan every existing mark and re-post the open window."""
    assert (_preview_kv_key(PREVIEW_DEST_STREET_ACCOUNT, "CPRT", "2026-09-10")
            == "consensus_preview_posted:CPRT:2026-09-10")


def test_the_two_destinations_never_share_a_key():
    sa = _preview_kv_key(PREVIEW_DEST_STREET_ACCOUNT, "CPRT", "2026-09-10")
    pf = _preview_kv_key(PREVIEW_DEST_PORTFOLIO, "CPRT", "2026-09-10")
    assert sa != pf, (
        "One shared key means a failed post to either channel is remembered "
        "as a success for both"
    )
