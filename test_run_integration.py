"""
End-to-end integration tests for main.run() (the daily sync).

These exercise the REAL run() branch logic, the REAL helpers
(_record_actuals, _move_calendar_event, the same-quarter phantom guard) and a
REAL on-disk SQLite DB. Only true I/O is stubbed: the calendar API, Slack, the
Finnhub/FMP fetch (events are injected via _fetch_earnings_source), and yfinance.

This is the run()-level coverage the helper-level tests can't give — it verifies
the branches are wired to the right helpers in the right order:
  - FMP-corrected actuals on a new date MOVE the calendar event (create-first)
    and the DB/calendar agree.
  - reported flips to 1 only after Slack succeeds (and stays 0 on failure).
  - a brand-new FMP-only result is posted, not silently reported.
  - a same-quarter phantom is skipped with no calendar churn.
"""
import sqlite3
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

import main
import storage


def _tkr(ticker, tier=1, name=None):
    return SimpleNamespace(
        ticker=ticker, tier=tier, company_name=name or ticker,
        sector="Healthcare Services", subsector="", position="Portfolio",
    )


class _FakeCal:
    """Minimal Google Calendar service stub for the preflight. `events` maps
    gcal_id -> 'YYYY-MM-DD' start date (default empty = no tagged events)."""
    def __init__(self, events=None):
        self._items = [
            {"id": gid, "start": {"date": d}} for gid, d in (events or {}).items()
        ]
    def events(self):
        return self
    def list(self, **kwargs):
        return self
    def execute(self):
        return {"items": self._items}


def _run_env(monkeypatch, tmp_path, *, coverage, events,
             seed=None, cal_events=None, notify_ok=True, move=5.0,
             find_event=None, create_fails=False):
    """Wire all I/O stubs, seed the DB, run main.run(), and return
    (db_path, recorded) where recorded captures calendar + notify activity."""
    db_path = str(tmp_path / "ea.db")

    # Seed pre-existing DB rows (e.g. a Finnhub event already stored).
    if seed:
        c = storage.init_db(db_path)
        for row in seed:
            storage.upsert_event(c, **row)
        c.close()

    monkeypatch.setattr(main, "init_db", lambda *a, **k: storage.init_db(db_path))
    monkeypatch.setattr(main, "load_coverage", lambda: coverage)
    monkeypatch.setattr(main, "FINNHUB_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", "cal")
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "get_finnhub_client", lambda: object())
    monkeypatch.setattr(main, "get_calendar_service", lambda: _FakeCal(cal_events))
    monkeypatch.setattr(main, "compute_coverage_freshness", lambda: None)
    monkeypatch.setattr(main, "_alert_coverage_stale_if_needed", lambda *a, **k: None)
    monkeypatch.setattr(main, "_alert_coverage_changes_if_needed", lambda *a, **k: None)
    monkeypatch.setattr(main, "_fetch_earnings_source", lambda *a, **k: events)
    monkeypatch.setattr(main, "fetch_yfinance_hour_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_yfinance_call_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_post_earnings_move", lambda *a, **k: move)
    monkeypatch.setattr(main, "expected_calendar_state",
                        lambda *a, **k: ("summary", "description", {}))

    recorded = {"created": [], "deleted": [], "updated": [], "notify": []}

    def fake_create(svc, cal, ticker, ev_date, hour, **k):
        recorded["created"].append((ticker, ev_date))
        if create_fails:
            from calendar_sync import CalendarError
            raise CalendarError("create failed")
        return f"NEW-{ticker}-{ev_date}"
    def fake_delete(svc, cal, gid):
        recorded["deleted"].append(gid)
    def fake_update(svc, cal, gid, *a, **k):
        recorded["updated"].append(gid)
    def fake_notify(conn, rows, when):
        recorded["notify"].extend(r.ticker for r in rows)
        return notify_ok

    monkeypatch.setattr(main, "create_calendar_event", fake_create)
    monkeypatch.setattr(main, "delete_calendar_event", fake_delete)
    monkeypatch.setattr(main, "update_calendar_event_description", fake_update)
    monkeypatch.setattr(main, "notify_results", fake_notify)
    if find_event is not None:
        monkeypatch.setattr(main, "find_calendar_event", find_event)

    main.run(skip_ticktick=True, skip_heartbeat=True)
    return db_path, recorded


def _row(db_path, ticker, ev_date):
    conn = sqlite3.connect(db_path)
    try:
        return storage.find_existing_event(conn, ticker, ev_date)
    finally:
        conn.close()


def _ev(symbol, d, *, eps_act=None, rev_act=None, hour="amc",
        eps_est=1.0, rev_est=1e9):
    return {"symbol": symbol, "date": d, "hour": hour,
            "epsEstimate": eps_est, "epsActual": eps_act,
            "revenueEstimate": rev_est, "revenueActual": rev_act}


# ---------------------------------------------------------------------------


def test_run_fmp_corrected_date_moves_calendar_and_marks_reported(monkeypatch, tmp_path):
    """FIVE-style: DB/calendar on the old (Finnhub) date 6/02; merge delivers
    actuals on the real date 6/03. run() should MOVE the calendar (create new,
    delete old), store the row at 6/03 pointing at the new event, post, and
    mark reported=1 after the post succeeds."""
    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("FIVE", tier=1)],
        seed=[dict(ticker="FIVE", event_date="2026-06-02", event_hour="amc",
                   gcal_id="OLD", quarter="2026Q1", eps_estimate=1.0,
                   reported=False, tier=1, company_name="Five Below")],
        events=[_ev("FIVE", "2026-06-03", eps_act=2.22, rev_act=1.28e9)],
    )
    # Calendar moved: created at 6/03, old event deleted.
    assert ("FIVE", "2026-06-03") in rec["created"]
    assert "OLD" in rec["deleted"]
    # DB now on the real date, pointing at the NEW event, reported after post.
    new = _row(db, "FIVE", "2026-06-03")
    assert new is not None and new["reported"] is True
    assert new["eps_actual"] == 2.22
    assert new["gcal_id"] == "NEW-FIVE-2026-06-03"
    # Old-date row gone (no Calendar/DB disagreement left behind).
    assert _row(db, "FIVE", "2026-06-02") is None
    assert rec["notify"] == ["FIVE"]


def test_run_new_fmp_only_actuals_posts_not_silently_reported(monkeypatch, tmp_path):
    """A Tier-1 name Finnhub never listed (no seed) arrives from FMP with
    actuals — must be posted and only then marked reported."""
    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("AAPL", tier=1)],
        events=[_ev("AAPL", "2026-05-01", eps_act=1.6, rev_act=9e10)],
    )
    row = _row(db, "AAPL", "2026-05-01")
    assert row is not None and row["reported"] is True
    assert rec["notify"] == ["AAPL"]
    assert ("AAPL", "2026-05-01") in rec["created"]


def test_run_slack_failure_leaves_reported_zero(monkeypatch, tmp_path):
    """If the Slack post fails, the actuals row must stay reported=0 for the
    next run to retry — never marked-reported-but-unannounced."""
    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("AAPL", tier=1)],
        events=[_ev("AAPL", "2026-05-01", eps_act=1.6, rev_act=9e10)],
        notify_ok=False,
    )
    row = _row(db, "AAPL", "2026-05-01")
    assert row is not None and row["reported"] is False
    assert rec["notify"] == ["AAPL"]   # we did attempt the post


def test_run_same_quarter_phantom_skipped_no_churn(monkeypatch, tmp_path):
    """With the quarter already reported (5/27), a no-actuals same-quarter
    phantom on a flapped date (6/02) is skipped: no calendar churn, no re-post,
    reported row untouched."""
    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("ICLR", tier=2)],
        seed=[dict(ticker="ICLR", event_date="2026-05-27", event_hour="amc",
                   gcal_id="REAL", quarter="2026Q1", eps_estimate=3.2,
                   eps_actual=2.52, reported=True, tier=2, company_name="ICON")],
        events=[_ev("ICLR", "2026-06-02", eps_act=None, rev_act=None)],
    )
    assert rec["created"] == [] and rec["deleted"] == []
    assert rec["notify"] == []
    kept = _row(db, "ICLR", "2026-05-27")
    assert kept is not None and kept["reported"] is True
    assert _row(db, "ICLR", "2026-06-02") is None


def test_run_date_change_moves_calendar_create_first(monkeypatch, tmp_path):
    """An upcoming event whose date moved (no actuals) goes through the
    date-change branch, which must now use the create-first helper: new event
    created at the new date, old one deleted, DB row moved."""
    q = storage.date_to_quarter("2026-06-20")  # same reporting quarter as 6/23
    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("MOVE", tier=1)],
        seed=[dict(ticker="MOVE", event_date="2026-06-20", event_hour="amc",
                   gcal_id="OLD", quarter=q, eps_estimate=1.0,
                   reported=False, tier=1, company_name="Mover")],
        events=[_ev("MOVE", "2026-06-23", eps_act=None, rev_act=None)],
        move=None,
    )
    assert ("MOVE", "2026-06-23") in rec["created"]
    assert "OLD" in rec["deleted"]
    moved = _row(db, "MOVE", "2026-06-23")
    assert moved is not None and moved["reported"] is False
    assert _row(db, "MOVE", "2026-06-20") is None
    assert rec["notify"] == []   # no actuals -> no results post


def test_run_same_date_shape_recreate_failure_does_not_advance_db(monkeypatch, tmp_path):
    """A same-date hour/shape change whose calendar create FAILS must not
    advance the DB hour — otherwise next run's date/hour/stale checks all read
    false and the wrong-shape event is hidden. The row must stay at the old
    hour so the next sync re-detects the drift and retries."""
    q = storage.date_to_quarter("2026-06-25")
    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("SHAPE", tier=1)],
        seed=[dict(ticker="SHAPE", event_date="2026-06-25", event_hour="",
                   gcal_id="OLD", quarter=q, eps_estimate=1.0,
                   reported=False, tier=1, company_name="Shaper")],
        # Same date, hour now known (bmo) -> hour_changed shape recreate.
        events=[_ev("SHAPE", "2026-06-25", eps_act=None, rev_act=None, hour="bmo")],
        move=None,
        create_fails=True,
    )
    row = _row(db, "SHAPE", "2026-06-25")
    assert row is not None
    assert row["event_hour"] == ""          # NOT advanced to bmo -> retriable
    assert row["gcal_id"] == "OLD"          # still points at the intact old event
    assert rec["deleted"] == []             # old event preserved (create failed first)


def test_run_calendar_backfill_actuals_not_silently_reported(monkeypatch, tmp_path):
    """No DB row but a calendar event already exists for this date+ticker
    (the backfill path). With actuals present it must post and mark reported
    only after Slack — not silently set reported=has_actuals."""
    def fake_find(svc, cal, ticker, ev_date, **k):
        # A matching tagged calendar event already exists on this date.
        return {"id": "CALX", "start": {"date": "2026-05-01"},
                "summary": f"{ticker} Earnings", "extendedProperties": {}}

    db, rec = _run_env(
        monkeypatch, tmp_path,
        coverage=[_tkr("BKFL", tier=2)],
        events=[_ev("BKFL", "2026-05-01", eps_act=0.9, rev_act=4e8)],
        find_event=fake_find,
    )
    row = _row(db, "BKFL", "2026-05-01")
    assert row is not None and row["reported"] is True   # reported AFTER post
    assert rec["notify"] == ["BKFL"]                      # beat/miss posted


# --- reconcile_calendar integration -----------------------------------------

class _ReconcileCal:
    """cal_service for run_reconcile_calendar: serves one tagged event."""
    def __init__(self, ticker, gcal_id, start_date):
        self._items = [{
            "id": gcal_id, "start": {"date": start_date},
            "extendedProperties": {"private": {"ticker": ticker}},
        }]
    def events(self):
        return self
    def list(self, **kwargs):
        return self
    def execute(self):
        return {"items": self._items}


def test_reconcile_preserves_reported_and_does_not_flip_on_actuals(monkeypatch, tmp_path):
    """A market-hours drift fix must NOT mark a result reported just because
    Finnhub now carries actuals — reconcile only fixes dates. reported must
    reflect the prior DB state (0 here), and the move is create-first."""
    db_path = str(tmp_path / "rec.db")
    # Seed: future-dated event, reported=0, on the OLD (calendar) date.
    c = storage.init_db(db_path)
    storage.upsert_event(c, ticker="WXYZ", event_date="2026-06-20",
                         event_hour="amc", gcal_id="OLD", quarter="2026Q2",
                         eps_estimate=1.0, reported=False, tier=1,
                         company_name="WXYZ")
    c.close()

    monkeypatch.setattr(main, "init_db", lambda *a, **k: storage.init_db(db_path))
    monkeypatch.setattr(main, "load_coverage", lambda: [_tkr("WXYZ", tier=1)])
    monkeypatch.setattr(main, "FINNHUB_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", "cal")
    monkeypatch.setattr(main, "get_calendar_service",
                        lambda: _ReconcileCal("WXYZ", "OLD", "2026-06-20"))
    monkeypatch.setattr(main, "get_finnhub_client", lambda: object())
    monkeypatch.setattr(main, "is_ticker_date_locked", lambda *a, **k: False)
    monkeypatch.setattr(main, "fetch_yfinance_hour_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_yfinance_call_for_date", lambda *a, **k: None)
    # Finnhub now says a DIFFERENT date AND carries actuals (the risky case).
    monkeypatch.setattr(main, "fetch_earnings", lambda *a, **k: [
        {"symbol": "WXYZ", "date": "2026-06-23", "hour": "amc",
         "epsEstimate": 1.0, "epsActual": 1.2,
         "revenueEstimate": 1e9, "revenueActual": 1.1e9},
    ])
    created, deleted = [], []
    monkeypatch.setattr(main, "create_calendar_event",
                        lambda svc, cal, tk, d, h, **k: created.append((tk, d)) or "NEW")
    monkeypatch.setattr(main, "delete_calendar_event",
                        lambda svc, cal, gid: deleted.append(gid))

    main.run_reconcile_calendar(dry_run=False)

    moved = _row(db_path, "WXYZ", "2026-06-23")
    assert moved is not None
    assert moved["reported"] is False        # NOT flipped by Finnhub actuals
    assert _row(db_path, "WXYZ", "2026-06-20") is None   # old row removed
    assert ("WXYZ", "2026-06-23") in created and "OLD" in deleted  # create-first


# --- run_check_results FMP date-correction --------------------------------

def test_check_results_fmp_corrected_date_moves_calendar_first(monkeypatch, tmp_path):
    """run_check_results must apply the same near-date + calendar-first move as
    run(): if the stored event is FIVE@2026-06-02 but the merged source reports
    actuals on 2026-05-27 (FMP correction), it should MOVE the calendar event
    to 5/27, migrate the DB row there, drop the old row, and mark reported —
    never strand the old calendar event behind a same-quarter reported row."""
    db_path = str(tmp_path / "cr.db")
    c = storage.init_db(db_path)
    storage.upsert_event(c, ticker="FIVE", event_date="2026-06-02",
                         event_hour="amc", gcal_id="OLD",
                         quarter=storage.date_to_quarter("2026-06-02"),
                         eps_estimate=1.0, reported=False, tier=1,
                         company_name="Five Below")
    c.close()

    monkeypatch.setattr(main, "init_db", lambda *a, **k: storage.init_db(db_path))
    monkeypatch.setattr(main, "load_coverage", lambda: [_tkr("FIVE", tier=1)])
    monkeypatch.setattr(main, "FINNHUB_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", "cal")
    monkeypatch.setattr(main, "get_finnhub_client", lambda: object())
    monkeypatch.setattr(main, "get_calendar_service", lambda: object())
    # A real move (not None) so the result isn't deferred (run_check_results
    # defers on target-day with no move) and the Slack block builder is happy.
    monkeypatch.setattr(main, "fetch_post_earnings_move",
                        lambda *a, **k: SimpleNamespace(move_pct=5.0, window_label="1d"))
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", None)
    monkeypatch.setattr(main, "_fetch_earnings_source", lambda *a, **k: [
        {"symbol": "FIVE", "date": "2026-05-27", "hour": "amc",
         "epsEstimate": 1.77, "epsActual": 2.22,
         "revenueEstimate": 1.2e9, "revenueActual": 1.285e9},
    ])
    created, deleted = [], []
    monkeypatch.setattr(main, "create_calendar_event",
                        lambda svc, cal, tk, d, h, **k: created.append((tk, d)) or "NEW")
    monkeypatch.setattr(main, "delete_calendar_event",
                        lambda svc, cal, gid: deleted.append(gid))
    notified = []
    monkeypatch.setattr(main, "notify_results",
                        lambda conn, rows, when: notified.extend(r.ticker for r in rows) or True)

    main.run_check_results(target_date="2026-05-27", skip_heartbeat=True)

    new = _row(db_path, "FIVE", "2026-05-27")
    assert new is not None and new["reported"] is True and new["gcal_id"] == "NEW"
    assert _row(db_path, "FIVE", "2026-06-02") is None          # old row migrated
    assert ("FIVE", "2026-05-27") in created and "OLD" in deleted  # create-first move
    assert notified == ["FIVE"]
