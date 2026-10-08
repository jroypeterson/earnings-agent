"""Board #450: B2 must ask Finnhub BY SYMBOL before calling a Tier 1/2 event unseen.

Measured 2026-10-08: argenx's 3Q26 event (2026-10-22, company-published) is
absent from Finnhub's US bulk calendar because Finnhub now files it under the
Euronext Brussels primary `ARGX.BR`; `earnings_calendar(symbol="ARGX")` still
returns it on the free tier. B2 had alerted "missing" for 29 straight runs.
"""
from __future__ import annotations

import finnhub_client
from finnhub_client import probe_foreign_listing_dates


class FakeClient:
    def __init__(self, by_symbol, fail=()):
        self.by_symbol = by_symbol
        self.fail = set(fail)
        self.calls = []

    def earnings_calendar(self, _from, to, symbol, international):
        self.calls.append((symbol, _from, to, international))
        if symbol in self.fail:
            raise RuntimeError("boom")
        rows = [r for r in self.by_symbol.get(symbol, [])
                if _from <= r["date"] <= to]
        return {"earningsCalendar": rows}


def _nosleep(monkeypatch):
    monkeypatch.setattr(finnhub_client.time, "sleep", lambda s: None)


# The real 2026-10-08 response shape for symbol=ARGX (estimates are EUR).
ARGX_BR = {"symbol": "ARGX.BR", "date": "2026-10-22", "hour": "", "quarter": 3,
           "year": 2026, "epsEstimate": 7.8662, "revenueEstimate": 1685743300}


def test_foreign_primary_listing_confirms_the_adr_event(monkeypatch):
    _nosleep(monkeypatch)
    c = FakeClient({"ARGX": [ARGX_BR]})
    got = probe_foreign_listing_dates(c, [("ARGX", "2026-10-22")])
    assert got == {("ARGX", "2026-10-22")}
    # Asked by OUR symbol, US scope (international=True 401s on the free tier).
    assert c.calls == [("ARGX", "2026-10-22", "2026-10-22", False)]


def test_a_different_date_is_not_a_confirmation(monkeypatch):
    """A moved date is exactly what the unseen alert exists to surface."""
    _nosleep(monkeypatch)
    moved = dict(ARGX_BR, date="2026-10-29")
    c = FakeClient({"ARGX": [moved]})
    # the fake filters by window, so also feed the row unfiltered to prove the
    # date check itself (not only the query window) rejects it
    c.earnings_calendar = lambda **kw: {"earningsCalendar": [moved]}
    assert probe_foreign_listing_dates(c, [("ARGX", "2026-10-22")]) == set()


def test_an_unrelated_symbol_is_not_a_confirmation(monkeypatch):
    """Only the queried ticker or a dotted listing of it counts: ARGXY or a
    different issuer the vendor returns is not our company."""
    _nosleep(monkeypatch)
    for sym in ("ARGXY", "ARG.BR", "XARGX", "AR.GX"):
        row = dict(ARGX_BR, symbol=sym)
        c = FakeClient({})
        c.earnings_calendar = lambda row=row, **kw: {"earningsCalendar": [row]}
        assert probe_foreign_listing_dates(c, [("ARGX", "2026-10-22")]) == set(), sym


def test_plain_us_symbol_still_confirms(monkeypatch):
    _nosleep(monkeypatch)
    c = FakeClient({"ABC": [{"symbol": "ABC", "date": "2026-10-22"}]})
    assert probe_foreign_listing_dates(c, [("ABC", "2026-10-22")]) == {("ABC", "2026-10-22")}


def test_a_probe_failure_leaves_the_pair_unseen_and_continues(monkeypatch):
    _nosleep(monkeypatch)
    c = FakeClient({"ARGX": [ARGX_BR], "BAD": []}, fail={"BAD"})
    got = probe_foreign_listing_dates(
        c, [("BAD", "2026-10-22"), ("ARGX", "2026-10-22")])
    assert got == {("ARGX", "2026-10-22")}


def test_probe_count_is_capped(monkeypatch):
    _nosleep(monkeypatch)
    c = FakeClient({})
    pairs = [(f"T{i}", "2026-10-22") for i in range(25)]
    probe_foreign_listing_dates(c, pairs, max_probes=3)
    assert len(c.calls) == 3


# --- run()-level seam: the REAL B2 loop, with the probe's client injected ---

import sqlite3  # noqa: E402
from datetime import date, timedelta  # noqa: E402

import storage  # noqa: E402
import test_run_integration as tri  # noqa: E402


def _unseen_count(db_path, ticker, ev_date):
    c = sqlite3.connect(db_path)
    try:
        return c.execute(
            "SELECT unseen_run_count FROM events WHERE ticker=? AND event_date=?",
            (ticker, ev_date)).fetchone()[0]
    finally:
        c.close()


def _b2_run(monkeypatch, tmp_path, by_symbol):
    _nosleep(monkeypatch)
    ev_date = (date.today() + timedelta(days=10)).isoformat()
    db_path = str(tmp_path / "ea.db")
    tri._seed_upcoming(db_path, ["ARGX"], ev_date)
    client = FakeClient(by_symbol)
    # _run_env installs get_finnhub_client -> object(); re-point it at the fake
    # by wrapping run so the override lands after _run_env's monkeypatching.
    real_run = tri.main.run

    def run_with_client(*a, **k):
        monkeypatch.setattr(tri.main, "get_finnhub_client", lambda: client)
        return real_run(*a, **k)

    monkeypatch.setattr(tri.main, "run", run_with_client)
    tri._run_env(monkeypatch, tmp_path, coverage=[tri._tkr("ARGX")],
                 events=[tri._ev("OTHER", ev_date)])
    return db_path, ev_date, client


def test_run_does_not_count_an_event_the_symbol_query_confirms(monkeypatch, tmp_path):
    ev_date = (date.today() + timedelta(days=10)).isoformat()
    row = dict(ARGX_BR, date=ev_date)
    db_path, ev_date, client = _b2_run(monkeypatch, tmp_path, {"ARGX": [row]})
    assert [c[0] for c in client.calls] == ["ARGX"]
    assert _unseen_count(db_path, "ARGX", ev_date) == 0


def test_run_does_not_probe_an_event_the_bulk_feed_returned(monkeypatch, tmp_path):
    _nosleep(monkeypatch)
    ev_date = (date.today() + timedelta(days=10)).isoformat()
    db_path = str(tmp_path / "ea.db")
    tri._seed_upcoming(db_path, ["ARGX"], ev_date)
    client = FakeClient({})
    real_run = tri.main.run

    def run_with_client(*a, **k):
        monkeypatch.setattr(tri.main, "get_finnhub_client", lambda: client)
        return real_run(*a, **k)

    monkeypatch.setattr(tri.main, "run", run_with_client)
    tri._run_env(monkeypatch, tmp_path, coverage=[tri._tkr("ARGX")],
                 events=[tri._ev("ARGX", ev_date)])
    assert client.calls == []
    assert _unseen_count(db_path, "ARGX", ev_date) == 0


def test_run_still_counts_an_event_the_vendor_does_not_have(monkeypatch, tmp_path):
    db_path, ev_date, client = _b2_run(monkeypatch, tmp_path, {})
    assert [c[0] for c in client.calls] == ["ARGX"]
    assert _unseen_count(db_path, "ARGX", ev_date) == 1
