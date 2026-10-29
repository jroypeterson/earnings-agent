"""Board #380: consensus snapshots for names OUTSIDE the coverage universe.

The sync fetches both calendars unfiltered and splits the result. Three things
must hold, and each test below fails if one of them breaks:

  1. a non-universe name gets an estimate_history row and NOTHING else - no
     events row, no calendar write, no notification;
  2. the universe pipeline sees exactly what it saw before;
  3. wide rows are change-only, so the shared CI artifact does not grow by
     ~3,800 rows a day for no information.
"""
import sqlite3
from datetime import date

import finnhub_client
import fmp_client
import main
import storage
from test_run_integration import _ev, _run_env, _tkr


def _hist(db_path, ticker):
    c = sqlite3.connect(db_path)
    try:
        return c.execute(
            "SELECT event_date, snapshot_date, eps_estimate, revenue_estimate "
            "FROM estimate_history WHERE ticker = ? ORDER BY snapshot_date",
            (ticker,)).fetchall()
    finally:
        c.close()


def test_run_snapshots_a_non_universe_name_and_touches_nothing_else(
        monkeypatch, tmp_path):
    future = date.fromordinal(date.today().toordinal() + 10).isoformat()
    seen = {}

    def fake_source(*a, **k):
        seen["match_all"] = k.get("match_all")
        return [_ev("HCA", future), _ev("ZZZZ", future, eps_est=2.5, rev_est=7e8)]

    db_path, rec = _run_env(
        monkeypatch, tmp_path, coverage=[_tkr("HCA")], events=[])
    # _run_env already ran once with no events; rerun with the wide feed.
    monkeypatch.setattr(main, "_fetch_earnings_source", fake_source)
    main.run(skip_ticktick=True, skip_heartbeat=True)

    assert seen["match_all"] is True, "the sync must fetch unfiltered"
    assert _hist(db_path, "ZZZZ") == [(future, date.today().isoformat(), 2.5, 7e8)]
    c = sqlite3.connect(db_path)
    try:
        assert c.execute(
            "SELECT COUNT(*) FROM events WHERE ticker = 'ZZZZ'").fetchone()[0] == 0
        assert c.execute(
            "SELECT COUNT(*) FROM events WHERE ticker = 'HCA'").fetchone()[0] == 1
    finally:
        c.close()
    assert all(t != "ZZZZ" for t, _ in rec["created"])


def test_wide_snapshots_are_change_only(tmp_path):
    conn = storage.init_db(str(tmp_path / "ea.db"))
    ev = {"symbol": "ZZZZ", "date": "2026-11-05",
          "epsEstimate": 1.0, "revenueEstimate": 5e8}
    assert storage.record_wide_estimate_snapshots(conn, [ev], "2026-10-27") == 1
    # Unchanged the next day: nothing written.
    assert storage.record_wide_estimate_snapshots(conn, [ev], "2026-10-28") == 0
    # Revised: written, and the pre-print reader sees the revision.
    ev2 = dict(ev, epsEstimate=1.1)
    assert storage.record_wide_estimate_snapshots(conn, [ev2], "2026-10-29") == 1
    rows = conn.execute(
        "SELECT snapshot_date, eps_estimate FROM estimate_history "
        "WHERE ticker='ZZZZ' ORDER BY snapshot_date").fetchall()
    assert rows == [("2026-10-27", 1.0), ("2026-10-29", 1.1)]


def test_wide_snapshots_skip_rows_with_no_estimate(tmp_path):
    conn = storage.init_db(str(tmp_path / "ea.db"))
    ev = {"symbol": "ZZZZ", "date": "2026-11-05",
          "epsEstimate": None, "revenueEstimate": None}
    assert storage.record_wide_estimate_snapshots(conn, [ev], "2026-10-27") == 0


def test_wide_heartbeat_rewrites_an_unchanged_estimate_weekly(tmp_path):
    # followthrough.consensus_revision needs >=2 rows after a print even
    # when consensus never moves; pure change-only would starve it.
    conn = storage.init_db(str(tmp_path / "ea.db"))
    ev = {"symbol": "ZZZZ", "date": "2026-12-05",
          "epsEstimate": 1.0, "revenueEstimate": 5e8}
    storage.record_wide_estimate_snapshots(conn, [ev], "2026-11-01")
    assert storage.record_wide_estimate_snapshots(conn, [ev], "2026-11-07") == 0
    assert storage.record_wide_estimate_snapshots(conn, [ev], "2026-11-08") == 1


class _FinnhubFake:
    def earnings_calendar(self, **k):
        return {"earningsCalendar": [
            {"symbol": "AAA", "date": "2026-11-01"},
            {"symbol": "BBB", "date": "2026-11-01"},
            {"symbol": "", "date": "2026-11-01"},      # unusable
            {"symbol": "CCC", "date": None},          # unusable
        ]}


def test_finnhub_fetch_earnings_match_all_through_the_public_seam(monkeypatch):
    monkeypatch.setattr(finnhub_client.time, "sleep", lambda *_: None)
    wide = finnhub_client.fetch_earnings(
        _FinnhubFake(), ["AAA"], "2026-11-01", "2026-11-02", match_all=True)
    # Every usable row kept, malformed rows dropped before the merge.
    assert sorted(e["symbol"] for e in wide) == ["AAA", "BBB"]
    narrow = finnhub_client.fetch_earnings(
        _FinnhubFake(), ["AAA"], "2026-11-01", "2026-11-02")
    assert [e["symbol"] for e in narrow] == ["AAA"]


def test_fetch_earnings_source_forwards_match_all(monkeypatch):
    seen = []
    monkeypatch.setattr(main, "fetch_earnings",
                        lambda *a, **k: seen.append(("fh", k.get("match_all"))) or [])
    monkeypatch.setattr(main, "FMP_API_KEY", "k")
    monkeypatch.setattr(
        main, "fetch_fmp_earnings",
        lambda *a, **k: seen.append(("fmp", k.get("match_all")))
        or fmp_client.FMPFetch(events=[], failed_chunks=0, total_chunks=1))
    main._fetch_earnings_source(object(), ["AAA"], "2026-11-01", "2026-11-02",
                                match_all=True)
    assert seen == [("fh", True), ("fmp", True)]


def test_fmp_match_all_keeps_every_event(monkeypatch):
    import io, json
    rows = [{"symbol": s, "date": "2026-11-03", "epsEstimated": 1.0}
            for s in ("AAA", "BBB")]

    class _R(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(fmp_client, "FMP_API_KEY", "k")
    monkeypatch.setattr(fmp_client.time, "sleep", lambda *_: None)
    monkeypatch.setattr(fmp_client.urllib.request, "urlopen",
                        lambda *a, **k: _R(json.dumps(rows).encode()))
    narrow = fmp_client.fetch_fmp_earnings(["AAA"], "2026-11-03", "2026-11-03")
    wide = fmp_client.fetch_fmp_earnings(
        ["AAA"], "2026-11-03", "2026-11-03", match_all=True)
    assert {e["symbol"] for e in narrow.events} == {"AAA"}
    assert {e["symbol"] for e in wide.events} == {"AAA", "BBB"}
