"""#712 follow-up: refused foreign names are dated by their HOME listing, outside the events DB."""
from __future__ import annotations

import json
import sqlite3
from datetime import date

import pytest

import calendar_sync
import home_listing
import ticktick

TODAY = date(2026, 10, 29)
REFUSED = {
    "CSL": {"company": "CSL Ltd", "exchange": "ASX", "tier": 2, "sector": "Biopharma",
            "position": "", "reason": "SEC gives CSL to CARLISLE COMPANIES INC"},
    "MED": {"company": "Medartis Holding AG", "exchange": "SIX", "tier": 2, "sector": "MedTech",
            "position": "", "reason": "SEC gives MED to MEDIFAST INC"},
    "ZZZ": {"company": "Nowhere AG", "exchange": "Mars Exchange", "tier": 2, "sector": "",
            "position": "", "reason": "x"},
}


def test_home_symbol_from_coverage_managers_exchange():
    assert home_listing.home_symbol("CSL", "ASX") == "CSL.AX"
    assert home_listing.home_symbol("UCB", "Euronext Brussels") == "UCB.BR"
    assert home_listing.home_symbol("MOVE", "SIX") == "MOVE.SW"
    assert home_listing.home_symbol("ROG.SW", "SIX") == "ROG.SW"
    assert home_listing.home_symbol("ZZZ", "Mars Exchange") is None


def test_lookup_asks_by_home_symbol_and_says_why_when_it_cannot():
    asked = []
    found = home_listing.lookup(REFUSED, TODAY,
                                next_date=lambda s, t: asked.append(s) or {"CSL.AX": "2027-02-09"}.get(s))
    assert asked == ["CSL.AX", "MED.SW"]          # never the bare US symbol
    assert found["CSL"]["next_date"] == "2027-02-09"
    assert found["MED"]["next_date"] is None and "no future date" in found["MED"]["note"]
    assert "no Yahoo suffix" in found["ZZZ"]["note"]


def test_a_failed_lookup_keeps_the_last_known_date_marked_stale():
    first = home_listing.write_export(home_listing.lookup(REFUSED, TODAY, next_date=lambda s, t: "2027-02-09"))
    def boom(s, t):
        raise RuntimeError("yahoo down")
    second = home_listing.write_export(home_listing.lookup(REFUSED, TODAY, next_date=boom), first)
    assert second["names"]["CSL"]["next_date"] == "2027-02-09"
    assert second["names"]["CSL"]["stale_since"]
    assert json.loads(home_listing.EXPORT.read_text())["names"]["CSL"]["next_date"] == "2027-02-09"


class Cal:
    def __init__(self):
        self.created, self.deleted = [], []


@pytest.fixture
def fakes(monkeypatch):
    cal = Cal()
    tasks = []
    monkeypatch.setattr(calendar_sync, "create_calendar_event",
                        lambda svc, cid, t, d, h, **kw: cal.created.append((t, d)) or f"g-{t}-{d}")
    monkeypatch.setattr(calendar_sync, "delete_calendar_event",
                        lambda svc, cid, gid: cal.deleted.append(gid))
    monkeypatch.setattr(ticktick, "find_or_create_list", lambda tok, name: f"list:{name}")
    monkeypatch.setattr(ticktick, "create_task",
                        lambda tok, lid, title, content, due, tags=None: tasks.append((lid, title, due)) or f"task{len(tasks)}")
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE kv_store (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    return cal, tasks, conn


def _found(d, tier=2):
    return {"CSL": {"company": "CSL Ltd", "home_symbol": "CSL.AX", "next_date": d, "tier": tier,
                    "sector": "Biopharma", "position": "", "reason": "r"}}


def test_sync_creates_once_then_is_idempotent(fakes):
    cal, tasks, conn = fakes
    for _ in range(2):
        home_listing.sync(conn, _found("2027-02-09"), cal_service=object(), calendar_id="c", ticktick_token="t")
    assert cal.created == [("CSL", "2027-02-09")]
    assert len(tasks) == 1
    lid, title, due = tasks[0]
    assert due is None and "(" not in title      # undated until the company confirms
    assert "4Q26" in lid


def test_a_moved_date_moves_the_calendar_event(fakes, monkeypatch):
    cal, tasks, conn = fakes
    monkeypatch.setattr(home_listing, "date", type("D", (), {"today": staticmethod(lambda: TODAY)}))
    home_listing.sync(conn, _found("2027-02-09"), cal_service=object(), calendar_id="c")
    home_listing.sync(conn, _found("2027-02-16"), cal_service=object(), calendar_id="c")
    assert cal.created == [("CSL", "2027-02-09"), ("CSL", "2027-02-16")]
    assert cal.deleted == ["g-CSL-2027-02-09"]


def test_tier_3_gets_no_calendar_or_task(fakes):
    cal, tasks, conn = fakes
    home_listing.sync(conn, _found("2027-02-09", tier=3), cal_service=object(), calendar_id="c", ticktick_token="t")
    assert cal.created == [] and tasks == []


def test_dry_run_writes_nothing(fakes):
    cal, tasks, conn = fakes
    home_listing.sync(conn, _found("2027-02-09"), cal_service=object(), calendar_id="c",
                      ticktick_token="t", dry_run=True)
    assert cal.created == [] and tasks == []
    assert conn.execute("SELECT COUNT(*) FROM kv_store").fetchone()[0] == 0


def test_load_coverage_keeps_the_refused_names_details(monkeypatch, tmp_path):
    import coverage
    import edgar_client
    ex = tmp_path / "exports"
    ex.mkdir()
    (ex / "universe.csv").write_text("Ticker,Company Name,Country (Listing),CIK,Exchange\n"
                                     "CSL,CSL Ltd,Australia,,ASX\n", encoding="utf-8")
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path))
    monkeypatch.setattr(edgar_client, "load_ticker_owners",
                        lambda: {"CSL": ("0000790051", "CARLISLE COMPANIES INC")})
    assert coverage.load_coverage() == []
    assert coverage.LAST_REFUSED_INFO["CSL"]["exchange"] == "ASX"
