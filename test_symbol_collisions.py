"""Board #712: a foreign Coverage Manager name whose symbol a US issuer owns is refused at ingest.

The measured 2026-10-29 shapes: SEC's company_tickers.json gives CSL to Carlisle, UCB to United
Community Banks, MED to Medifast and MOVE to Corvex, while CM lists CSL Ltd (ASX), UCB SA
(Euronext Brussels), Medartis and Medacta (SIX). UCB carries a CM CIK that disagrees; the other
three carry none.
"""
from __future__ import annotations

import sqlite3

import coverage
import storage
from coverage import find_symbol_collisions

OWNERS = {
    "CSL": ("0000790051", "CARLISLE COMPANIES INC"),
    "UCB": ("0000857855", "UNITED COMMUNITY BANKS INC"),
    "MED": ("0000910329", "MEDIFAST INC"),
    "MOVE": ("0001734750", "Corvex, Inc."),
    "VXRT": ("0000072444", "Vaxart, Inc."),
    "AZN": ("0000901832", "ASTRAZENECA PLC"),
    "ROG": ("0000084748", "ROGERS CORP"),
    "SAN": ("0000891478", "BANCO SANTANDER, S.A."),
}


def row(t, name, country, cik=""):
    return {"Ticker": t, "Company Name": name, "Country (Listing)": country, "CIK": cik}


ROWS = [
    row("CSL", "CSL Ltd", "Australia"),
    row("UCB", "UCB SA", "Belgium", "1290640"),
    row("MED", "Medartis Holding AG", "Switzerland"),
    row("MOVE", "Medacta Group SA", "Switzerland"),
    # Non-US listing country, but SEC's CIK is CM's: the same company.
    row("VXRT", "Aviragen Therapeutics", "Canada", "72444"),  # renamed: only the CIK saves it
    # US listing: never refused, whatever the CIK says (CM's CIK can be wrong).
    row("AZN", "Zeneca Group PLC", "United States", "1"),  # name differs too: only the US arm saves it
    # Foreign, no CM CIK, but SEC's owner is the same company by name.
    row("SAN", "Banco Santander SA", "Spain"),
    # Foreign symbol SEC does not list at all: nothing to collide with.
    row("ROG.SW", "Roche Holding AG", "Switzerland"),
]


def test_refuses_exactly_the_four_measured_collisions():
    got = find_symbol_collisions(ROWS, OWNERS)
    assert sorted(got) == ["CSL", "MED", "MOVE", "UCB"]
    assert "CARLISLE" in got["CSL"] and "Australia" in got["CSL"]


def test_a_us_listing_is_never_refused_on_a_cik_disagreement():
    assert "AZN" not in find_symbol_collisions(ROWS, OWNERS)


def test_matching_cik_or_matching_name_is_the_same_company():
    got = find_symbol_collisions(ROWS, OWNERS)
    assert "VXRT" not in got and "SAN" not in got


def test_blank_listing_country_is_not_refused():
    assert find_symbol_collisions([row("CSL", "CSL Ltd", "")], OWNERS) == {}


def _db():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE events (ticker TEXT, event_date TEXT, gcal_id TEXT, reported INT, "
                 "closed_reason TEXT, closed_at TEXT, updated_at TEXT)")
    conn.executemany("INSERT INTO events VALUES (?,?,?,?,?,NULL,NULL)", [
        ("UCB", "2026-10-20", "g1", 1, None),          # reported: a wrong card
        ("MOVE", "2026-11-12", "g2", 0, None),         # future: a wrong reminder
        ("CSL", "2026-10-28", "g3", 0, "delisted"),    # close_departed_events got there first
        ("ABT", "2026-10-15", "g4", 1, None),          # untouched
    ])
    return conn


def _reasons(conn):
    return dict(((t, d), r) for t, d, r in
                conn.execute("SELECT ticker, event_date, closed_reason FROM events"))


def test_closes_reported_future_and_delisted_rows_of_refused_tickers():
    conn = _db()
    closed = storage.close_symbol_collision_events(conn, {"UCB": "x", "MOVE": "x", "CSL": "x"})
    assert sorted((t, d) for t, d, _ in closed) == [
        ("CSL", "2026-10-28"), ("MOVE", "2026-11-12"), ("UCB", "2026-10-20")]
    r = _reasons(conn)
    assert r[("ABT", "2026-10-15")] is None
    assert {r[k] for k in r if k[0] != "ABT"} == {storage.CLOSED_SYMBOL_COLLISION}


def test_reopens_when_the_symbol_is_no_longer_refused_and_is_idempotent():
    conn = _db()
    storage.close_symbol_collision_events(conn, {"UCB": "x", "MOVE": "x"})
    assert storage.close_symbol_collision_events(conn, {"UCB": "x", "MOVE": "x"}) == []
    storage.close_symbol_collision_events(conn, {"UCB": "x"})
    r = _reasons(conn)
    assert r[("MOVE", "2026-11-12")] is None
    assert r[("UCB", "2026-10-20")] == storage.CLOSED_SYMBOL_COLLISION


def test_an_unknown_set_changes_nothing():
    conn = _db()
    storage.close_symbol_collision_events(conn, {"UCB": "x"})
    before = _reasons(conn)
    assert storage.close_symbol_collision_events(conn, None) == []
    assert _reasons(conn) == before


def test_load_coverage_drops_refused_tickers(monkeypatch, tmp_path):
    ex = tmp_path / "exports"
    ex.mkdir()
    (ex / "universe.csv").write_text(
        "Ticker,Company Name,Country (Listing),CIK\n"
        "CSL,CSL Ltd,Australia,\nABT,Abbott Laboratories,United States,1800\n", encoding="utf-8")
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path))
    import edgar_client
    monkeypatch.setattr(edgar_client, "load_ticker_owners", lambda: OWNERS)
    tickers = [t.ticker for t in coverage.load_coverage()]
    assert tickers == ["ABT"]
    assert list(coverage.LAST_SYMBOL_COLLISIONS) == ["CSL"]


def test_load_coverage_records_unknown_when_sec_is_unreadable(monkeypatch, tmp_path):
    ex = tmp_path / "exports"
    ex.mkdir()
    (ex / "universe.csv").write_text("Ticker,Company Name,Country (Listing),CIK\n"
                                     "CSL,CSL Ltd,Australia,\n", encoding="utf-8")
    monkeypatch.setattr(coverage, "COVERAGE_MANAGER_PATH", str(tmp_path))
    import edgar_client
    monkeypatch.setattr(edgar_client, "load_ticker_owners", lambda: None)
    assert [t.ticker for t in coverage.load_coverage()] == ["CSL"]
    assert coverage.LAST_SYMBOL_COLLISIONS is None
