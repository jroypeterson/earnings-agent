"""Board #661: two wrong numbers the #earnings results card had already posted.

1. SNN 2026-08-04: half-year revenue $3,128.8M against a quarterly estimate of $1,640M, posted as a
   +90.8% beat. Every surface that prints a revenue surprise must refuse that pair.
2. ALC / CP / NHP / SHMZF / OSSFF (and DXCM, ENSG this season): after-close prints Finnhub left
   untimed were scored before-open -- the reaction came from the day BEFORE the news -- although
   yfinance had stored `event_hour_yf='amc'` for each.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import date
from types import SimpleNamespace

import pytest

import daily_summary
import main
import notifications
import storage
from coverage import TickerInfo
from season_progress import SeasonRow, collect_season
from surprise import rev_period_mismatch

SNN = (3_128_826_000.0, 1_640_000_000.0)    # the production row, 2026-08-04
ARGX = (2_815_800_000.0, 1_630_000_000.0)   # 1.73x, 2026-07-23
SRPT = (730_800_000.0, 483_600_000.0)       # a REAL 1.51x beat, 2026-05-06
CYTK = (19_400_000.0, 8_700_000.0)          # a real 2.23x launch quarter, but small


# --- the rule ---------------------------------------------------------------

def test_half_year_actuals_against_quarterly_estimates_are_refused():
    assert "half-year" in rev_period_mismatch(*SNN)
    assert "half-year" in rev_period_mismatch(*ARGX)


def test_the_reverse_and_the_full_year_shapes_are_refused():
    assert "full-year" in rev_period_mismatch(4.1e9, 1.0e9)
    assert "quarterly figure against a half-year" in rev_period_mismatch(0.5e9, 1.0e9)


def test_a_real_large_beat_is_still_a_beat():
    assert rev_period_mismatch(*SRPT) is None
    assert rev_period_mismatch(1.32e9, 1.0e9) is None      # Bloom Energy's +32%


def test_a_small_company_can_really_double_its_estimate():
    assert rev_period_mismatch(*CYTK) is None


def test_a_missing_side_is_not_a_mismatch():
    assert rev_period_mismatch(None, 1.6e9) is None
    assert rev_period_mismatch(3.1e9, None) is None


# --- every surface that prints a revenue surprise ----------------------------

def _result(actual, estimate):
    return notifications.ResultRow(
        ticker="SNN", company_name="Smith & Nephew PLC", event_date="2026-08-04", event_hour="bmo",
        eps_actual=0.946, eps_estimate=0.939, rev_actual=actual, rev_estimate=estimate, tier=2,
        move=SimpleNamespace(move_pct=1.2, window_label="1d"))


def test_the_earnings_card_says_not_compared_and_shows_no_colour():
    line = notifications._format_results_line(_result(*SNN))
    assert "90.8" not in line and "not compared" in line
    rev_square = line.split()[1]
    assert rev_square == "⬜"


def test_the_earnings_card_still_scores_a_real_beat():
    line = notifications._format_results_line(_result(*SRPT))
    assert "+51.1%" in line and line.split()[1] == "\U0001F7E9"


def test_the_long_form_revenue_line_says_not_compared():
    assert "not compared" in notifications._fmt_actual_vs_estimate_rev(*SNN)
    assert "beat" in notifications._fmt_actual_vs_estimate_rev(*SRPT)


def test_the_daily_summary_says_not_compared():
    assert "not compared" in daily_summary._rev_pct(*SNN)
    assert daily_summary._rev_marker(*SNN) == "⬜"
    assert daily_summary._rev_pct(*SRPT) == " (+51.1%)"


def test_season_progress_gives_no_surprise_for_a_cross_period_pair():
    row = SeasonRow("SNN", "Smith & Nephew", "Portfolio", "2026-08-04", "bmo", True, True, False,
                    rev_estimate=SNN[1], rev_actual=SNN[0])
    assert row.rev_surprise_pct is None
    row.rev_actual, row.rev_estimate = SRPT
    assert row.rev_surprise_pct == pytest.approx(51.1, abs=0.1)


# --- after-close prints Finnhub left untimed ---------------------------------

def test_record_actuals_scores_an_untimed_print_by_the_yfinance_session(monkeypatch):
    seen = []
    monkeypatch.setattr(main, "fetch_post_earnings_move",
                        lambda t, d, timing: seen.append(timing) or 5.0)
    main._record_actuals(
        storage.init_db(":memory:"), date(2026, 8, 12), [],
        ticker="ALC", earnings_date="2026-08-10", hour=None, quarter="2026Q2",
        eps_est=1.0, eps_act=1.1, rev_est=None, rev_act=None, tier=2, company_name="Alcon",
        source_fingerprint="ALC:2026-08-10", hour_yf="amc", call_dt_iso=None, call_source=None,
        gcal_id=None, info=None, dry_run=False)
    assert seen == ["amc"]


def test_finnhub_session_still_wins_over_yfinance(monkeypatch):
    seen = []
    monkeypatch.setattr(main, "fetch_post_earnings_move",
                        lambda t, d, timing: seen.append(timing) or 5.0)
    main._record_actuals(
        storage.init_db(":memory:"), date(2026, 8, 12), [],
        ticker="ALC", earnings_date="2026-08-10", hour="bmo", quarter="2026Q2",
        eps_est=1.0, eps_act=1.1, rev_est=None, rev_act=None, tier=2, company_name="Alcon",
        source_fingerprint="ALC:2026-08-10", hour_yf="amc", call_dt_iso=None, call_source=None,
        gcal_id=None, info=None, dry_run=False)
    assert seen == ["bmo"]


def test_check_results_scores_an_untimed_print_by_the_yfinance_session(monkeypatch, tmp_path):
    db_path = str(tmp_path / "amc.db")
    c = storage.init_db(db_path)
    storage.upsert_event(c, ticker="CP", event_date="2026-10-28", event_hour=None, gcal_id="G",
                         quarter=storage.date_to_quarter("2026-10-28"), eps_estimate=1.0,
                         reported=False, tier=2, company_name="Canadian Pacific", event_hour_yf="amc")
    c.close()
    monkeypatch.setattr(main, "init_db", lambda *a, **k: storage.init_db(db_path))
    monkeypatch.setattr(main, "load_coverage", lambda: [SimpleNamespace(
        ticker="CP", tier=2, company_name="Canadian Pacific", sector="Industrials", subsector="",
        position="Portfolio")])
    monkeypatch.setattr(main, "FINNHUB_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", "cal")
    monkeypatch.setattr(main, "get_finnhub_client", lambda: object())
    monkeypatch.setattr(main, "get_calendar_service", lambda: object())
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", None)
    monkeypatch.setattr(main, "update_calendar_event_description", lambda *a, **k: None)
    monkeypatch.setattr(main, "_fetch_earnings_source", lambda *a, **k: [
        {"symbol": "CP", "date": "2026-10-28", "hour": "", "epsEstimate": 1.0, "epsActual": 1.1,
         "revenueEstimate": None, "revenueActual": None}])
    seen = []
    monkeypatch.setattr(main, "fetch_post_earnings_move", lambda t, d, timing: seen.append(timing)
                        or SimpleNamespace(move_pct=2.0, window_label="1d"))
    monkeypatch.setattr(main, "notify_results", lambda *a, **k: True)

    main.run_check_results(target_date="2026-10-28", skip_heartbeat=True)

    assert seen and set(seen) == {"amc"}


def test_season_progress_reads_the_yfinance_session_when_finnhub_has_none():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "e.db")
    storage.init_db(p)
    conn = sqlite3.connect(p)
    conn.execute("INSERT INTO events (ticker, quarter, event_date, event_hour, event_hour_yf, reported, "
                 "tier, company_name, date_confirmed, date_locked) VALUES "
                 "('DXCM','2026Q3','2026-10-29',NULL,'amc',1,1,'DexCom',1,0)")
    conn.commit()
    cov = {"DXCM": TickerInfo(ticker="DXCM", tier=1, company_name="DexCom", sector="Healthcare",
                              subsector="", position="Portfolio")}
    progress = collect_season(conn, cov, date(2026, 11, 1))
    assert [r.event_hour for r in progress.reported] == ["amc"]
