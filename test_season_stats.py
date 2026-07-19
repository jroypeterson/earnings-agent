"""Tests for the earnings season-funnel population stats (compute_season_stats)."""
import os
import sqlite3
import tempfile
from datetime import date

import storage


def _db():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "e.db")
    storage.init_db(p)
    return sqlite3.connect(p)


def _add(conn, ticker, event_date, *, reported=0, tier=2):
    conn.execute(
        "INSERT INTO events (ticker, event_date, quarter, reported, tier) "
        "VALUES (?, ?, ?, ?, ?)",
        (ticker.upper(), event_date, storage.date_to_quarter(event_date), reported, tier),
    )
    conn.commit()


def test_reconciliation_identity_holds():
    conn = _db()
    # Season = 2026Q2 (a mid-July as_of maps there). Add scheduled + reported names.
    for t, rep in [("AAA", 1), ("BBB", 1), ("CCC", 0), ("DDD", 0)]:
        _add(conn, t, "2026-08-05", reported=rep)
    s = storage.compute_season_stats(conn, date(2026, 7, 19))
    assert s["season"] == "2026Q2"
    assert s["expected"] == 4 and s["reported"] == 2 and s["remaining"] == 2
    assert s["reported"] + s["remaining"] == s["expected"]


def test_also_reported_new_fmp_only_reporter_not_dropped():
    """A name announced in the batch with NO pre-scheduled event must count in
    BOTH expected and reported — never omitted, never an empty-season message
    while it is being announced (the Codex finding)."""
    conn = _db()  # no scheduled events at all this season
    s = storage.compute_season_stats(
        conn, date(2026, 7, 19), universe_tickers=["NEWCO"], also_reported=["NEWCO"]
    )
    assert s["expected"] == 1 and s["reported"] == 1 and s["remaining"] == 0
    assert s["reported"] + s["remaining"] == s["expected"]
    assert s["no_date"] == 0  # it reported -> has a date, not "no date"


def test_also_reported_mixes_with_scheduled():
    conn = _db()
    _add(conn, "AAA", "2026-08-05", reported=1)   # already flagged in DB
    _add(conn, "BBB", "2026-08-05", reported=0)   # scheduled, not yet reported
    # This batch announces BBB (in-season, not yet flagged) + ZZZ (brand new).
    s = storage.compute_season_stats(conn, date(2026, 7, 19), also_reported=["BBB", "ZZZ"])
    assert s["expected"] == 3           # AAA, BBB, ZZZ
    assert s["reported"] == 3           # AAA(db) + BBB(also) + ZZZ(also)
    assert s["remaining"] == 0
    assert s["reported"] + s["remaining"] == s["expected"]


def test_empty_season_is_zeroed_not_crash():
    conn = _db()
    s = storage.compute_season_stats(conn, date(2026, 7, 19))
    assert s["expected"] == 0 and s["reported"] == 0 and s["remaining"] == 0
