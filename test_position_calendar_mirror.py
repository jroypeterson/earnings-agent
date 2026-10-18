"""Position-calendar mirror: the pure diff and the no-wipe guards."""

import pytest

import position_calendar_mirror as pcm


def _src(eid, ticker, start, summary=None, updated="2026-10-01T00:00:00Z"):
    return {
        "id": eid,
        "summary": summary or f"{ticker} Earnings Release",
        "description": f"Ticker: {ticker}",
        "start": start,
        "end": start,
        "reminders": {"useDefault": False},
        "updated": updated,
        "extendedProperties": {"private": {"earningsAgent": "true", "ticker": ticker}},
    }


def _mirror_of(src, position, mid="m-" ):
    body = pcm.mirror_body(src, position)
    return {"id": mid + src["id"], **body}


TICKERS = {"LLY": "Portfolio", "ENSG": "Researching"}


def test_creates_only_position_names():
    sources = [_src("a", "LLY", {"date": "2026-10-29"}), _src("b", "AAPL", {"date": "2026-10-30"})]
    creates, updates, deletes = pcm.plan_mirror(sources, [], TICKERS)
    assert [c["extendedProperties"]["private"]["mirrorOf"] for c in creates] == ["a"]
    assert creates[0]["extendedProperties"]["private"]["position"] == "Portfolio"
    # A mirror must NOT carry the agent's tag, or the source-side queries would see it.
    assert "earningsAgent" not in creates[0]["extendedProperties"]["private"]
    assert updates == [] and deletes == []


def test_in_sync_mirror_is_a_noop_across_timezones():
    # Source calendar returns UTC; the position calendar returns the same instant in ET.
    src = _src("a", "ENSG", {"dateTime": "2026-11-03T12:00:00Z", "timeZone": "America/New_York"})
    mirror = _mirror_of(src, "Researching")
    mirror["start"] = mirror["end"] = {"dateTime": "2026-11-03T07:00:00-05:00", "timeZone": "America/New_York"}
    assert pcm.plan_mirror([src], [mirror], TICKERS) == ([], [], [])


def test_changed_source_updates_mirror():
    src = _src("a", "LLY", {"date": "2026-10-29"})
    mirror = _mirror_of(src, "Portfolio")
    src = _src("a", "LLY", {"date": "2026-10-30"}, summary="LLY Earnings Release")
    _, updates, _ = pcm.plan_mirror([src], [mirror], TICKERS)
    assert [u[0] for u in updates] == ["m-a"]
    assert updates[0][1]["start"] == {"date": "2026-10-30"}


def test_departed_ticker_and_vanished_source_are_deleted():
    gone = _mirror_of(_src("x", "LLY", {"date": "2026-10-29"}), "Portfolio")
    left = _mirror_of(_src("y", "AAPL", {"date": "2026-10-30"}), "Portfolio")
    _, _, deletes = pcm.plan_mirror([_src("y", "AAPL", {"date": "2026-10-30"})], [gone, left], TICKERS)
    assert {d["id"] for d in deletes} == {"m-x", "m-y"}


def test_source_duplicates_collapse_to_newest():
    # Newest listed FIRST, so "last one wins" cannot pass by accident.
    sources = [
        _src("new", "LLY", {"date": "2026-10-29"}, updated="2026-10-05T00:00:00Z"),
        _src("old", "LLY", {"date": "2026-10-29"}, updated="2026-10-01T00:00:00Z"),
    ]
    stale = _mirror_of(sources[1], "Portfolio")
    creates, _, deletes = pcm.plan_mirror(sources, [stale], TICKERS)
    assert [c["extendedProperties"]["private"]["mirrorOf"] for c in creates] == ["new"]
    assert [d["id"] for d in deletes] == ["m-old"]


def test_duplicate_mirrors_of_one_source_are_pruned():
    src = _src("a", "LLY", {"date": "2026-10-29"})
    m1, m2 = _mirror_of(src, "Portfolio", "m1-"), _mirror_of(src, "Portfolio", "m2-")
    creates, updates, deletes = pcm.plan_mirror([src], [m1, m2], TICKERS)
    assert creates == [] and updates == [] and [d["id"] for d in deletes] == ["m2-a"]


def test_refuses_to_run_on_empty_position_lists(monkeypatch, tmp_path):
    monkeypatch.setattr(pcm, "GOOGLE_CALENDAR_ID", "src")
    monkeypatch.setattr(pcm, "POSITION_CALENDAR_ID", "dst")
    monkeypatch.setattr(pcm, "COVERAGE_MANAGER_PATH", str(tmp_path))
    monkeypatch.setattr(pcm, "get_calendar_service", lambda: pytest.fail("must abort before any API call"))
    with pytest.raises(pcm.MirrorAbort, match="no Position-list tickers"):
        pcm.run_mirror(dry_run=True)


def test_refuses_to_mirror_onto_source(monkeypatch):
    monkeypatch.setattr(pcm, "GOOGLE_CALENDAR_ID", "same")
    monkeypatch.setattr(pcm, "POSITION_CALENDAR_ID", "same")
    with pytest.raises(pcm.MirrorAbort, match="onto itself"):
        pcm.run_mirror(dry_run=True)


class _FakeEvents:
    def __init__(self, by_cal):
        self.by_cal, self.calls = by_cal, []

    def list(self, calendarId, **kw):
        items = self.by_cal.get(calendarId, [])
        return type("R", (), {"execute": lambda s: {"items": items}})()

    def _op(self, name, **kw):
        self.calls.append((name, kw.get("eventId")))
        return type("R", (), {"execute": lambda s: {}})()

    def insert(self, **kw): return self._op("insert", **kw)
    def update(self, **kw): return self._op("update", **kw)
    def delete(self, **kw): return self._op("delete", **kw)


def test_mass_delete_is_blocked_but_creates_still_apply(monkeypatch, tmp_path):
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / "portfolio.json").write_text('{"LLY": {"Core": "Y"}}', encoding="utf-8")
    sources = [_src("a", "LLY", {"date": "2026-10-29"}), _src("z", "AAPL", {"date": "2026-10-29"})]
    # 20 mirrors of names no longer in any Position list -> 20 deletes > cap max(10, 10).
    mirrors = [_mirror_of(_src(f"o{i}", "AAPL", {"date": "2026-10-29"}), "Portfolio") for i in range(20)]
    fake = _FakeEvents({"src": sources, "dst": mirrors})
    monkeypatch.setattr(pcm, "GOOGLE_CALENDAR_ID", "src")
    monkeypatch.setattr(pcm, "POSITION_CALENDAR_ID", "dst")
    monkeypatch.setattr(pcm, "COVERAGE_MANAGER_PATH", str(tmp_path))
    monkeypatch.setattr(pcm, "get_calendar_service", lambda: type("S", (), {"events": lambda s: fake})())
    monkeypatch.setattr(pcm, "DB_PATH", tmp_path / "absent.db")  # never the real events DB
    with pytest.raises(pcm.MirrorAbort, match="blocked"):
        pcm.run_mirror()
    assert [c[0] for c in fake.calls] == ["insert"]

    fake.calls.clear()
    pcm.run_mirror(allow_mass_delete=True)
    assert sum(1 for c in fake.calls if c[0] == "delete") == 20


def _db(tmp_path, rows, heartbeat="2026-10-17"):
    import sqlite3
    conn = sqlite3.connect(tmp_path / "e.db")
    conn.execute(
        "CREATE TABLE events (ticker, quarter, event_date, event_hour, event_hour_yf, "
        "eps_estimate, eps_actual, rev_estimate, rev_actual, tier, source_fingerprint, "
        "call_datetime_utc, closed_reason, updated_at)"
    )
    conn.execute("CREATE TABLE kv_store (key PRIMARY KEY, value)")
    if heartbeat:
        conn.execute("INSERT INTO kv_store VALUES ('last_sync_completed', ?)", (heartbeat,))
    for r in rows:
        conn.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", r)
    return conn


def _row(ticker, date_, tier=3, quarter="2026Q3", closed=None, updated="2026-10-01"):
    return (ticker, quarter, date_, "amc", None, 0.5, None, None, None, tier,
            None, None, closed, updated)


def test_db_arm_renders_only_tier3_open_position_rows(tmp_path):
    conn = _db(tmp_path, [
        _row("ENSG", "2026-11-04"),                    # wanted
        _row("LLY", "2026-10-29", tier=1),             # on the source calendar already
        _row("AAPL", "2026-10-30"),                    # not a Position name
        _row("ENSG", "2026-08-01", quarter="2026Q2", closed="departed"),
    ])
    out = pcm.db_bodies(conn, TICKERS, "2026-07-01", "2027-01-01")
    assert list(out) == ["db:ENSG:2026Q3"]
    body = out["db:ENSG:2026Q3"]
    assert body["summary"].startswith("ENSG Earnings Release")
    assert body["extendedProperties"]["private"]["position"] == "Researching"


def test_db_arm_same_quarter_resolves_to_newest_row(tmp_path):
    conn = _db(tmp_path, [
        _row("ENSG", "2026-11-04", updated="2026-10-05"),
        _row("ENSG", "2026-11-11", updated="2026-10-01"),
    ])
    out = pcm.db_bodies(conn, TICKERS, "2026-07-01", "2027-01-01")
    assert out["db:ENSG:2026Q3"]["start"]["dateTime"].startswith("2026-11-04")


def test_db_heartbeat_freshness(tmp_path):
    from datetime import date
    today = date(2026, 10, 18)
    for name, beat, fresh in [("ok", "2026-10-17", True), ("old", "2026-10-10", False), ("none", None, False)]:
        d = tmp_path / name
        d.mkdir()
        assert pcm._db_is_fresh(_db(d, [], heartbeat=beat), today) is fresh


def test_skipped_db_arm_never_deletes_db_mirrors():
    db_mirror = {"id": "m-db", "summary": "UBER Earnings Release",
                 "extendedProperties": {"private": {"earningsMirror": "true",
                                                    "mirrorOf": "db:UBER:2026Q3"}}}
    _, _, deletes = pcm.plan_mirror([], [db_mirror], TICKERS, db_desired=None)
    assert deletes == []
    # ...but a DB arm that ran and no longer wants it does delete it.
    _, _, deletes = pcm.plan_mirror([], [db_mirror], TICKERS, db_desired={})
    assert [d["id"] for d in deletes] == ["m-db"]
