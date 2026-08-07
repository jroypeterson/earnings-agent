"""An event whose company no longer exists must be able to close.

`reported = 0` was the whole definition of "still waiting to happen", which
stops being true the moment the universe churns: a company that has been
ACQUIRED can never report, so its event sits unreported forever.

Measured 2026-08-06: **51 of 160** past-dated unreported rows belonged to
tickers that had left the coverage universe — APLS (-> Biogen), EXAS
(-> Abbott), FOLD (-> BioMarin), CFLT (-> IBM), SEMR (-> Adobe) — the oldest
98 days. Nothing flagged them, because every alerting loop skips a ticker
absent from `coverage_map`. That guard is correct on its own terms (dead names
would otherwise alert forever), but its side effect is that **leaving the
universe stops the only lane that would notice.**

`closed_reason` is that terminal state. NULL = open.
"""
import sqlite3

import pytest

import storage


def _db():
    conn = storage.init_db(":memory:")
    return conn


def _event(conn, ticker, event_date, **kw):
    cols = {"ticker": ticker, "event_date": event_date, "reported": 0,
            "tier": 1, "quarter": "2026Q2"}
    cols.update(kw)
    conn.execute(
        f"INSERT INTO events ({','.join(cols)}) "
        f"VALUES ({','.join('?' * len(cols))})", tuple(cols.values()))
    conn.commit()


def _open_count(conn):
    return conn.execute(
        f"SELECT COUNT(*) FROM events WHERE {storage.OPEN_EVENT_SQL}").fetchone()[0]


# --- the closing rule ------------------------------------------------------

def test_a_past_event_on_a_departed_ticker_is_closed():
    conn = _db()
    _event(conn, "APLS", "2026-05-01")           # acquired by Biogen
    _event(conn, "ENSG", "2026-05-01")           # still covered
    closed = storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    assert closed == [("APLS", "2026-05-01")]
    assert _open_count(conn) == 1


def test_a_closed_event_leaves_the_open_population():
    conn = _db()
    _event(conn, "EXAS", "2026-05-01")
    assert _open_count(conn) == 1
    storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    assert _open_count(conn) == 0, (
        "the whole point is that it stops counting as pending")


def test_a_FUTURE_event_on_a_departed_ticker_is_left_alone():
    """A ticker can leave the universe for reasons unrelated to the company
    ending — a Position change, a tier edit, a sector reclassification — and a
    future event for such a name may still be perfectly real. Only a
    past-dated event that never reported has no other explanation.
    """
    conn = _db()
    _event(conn, "FOLD", "2026-12-01")
    assert storage.close_departed_events(conn, {"ENSG"}, "2026-08-06") == []
    assert _open_count(conn) == 1


def test_an_event_with_ACTUALS_is_left_alone():
    """Actuals mean it did report; `--check-results` owns that transition, and
    stamping it `delisted` would race the post-then-mark invariant."""
    conn = _db()
    _event(conn, "CFLT", "2026-05-01", eps_actual=1.23)
    assert storage.close_departed_events(conn, {"ENSG"}, "2026-08-06") == []


def test_an_already_reported_event_is_not_reclosed():
    conn = _db()
    _event(conn, "SEMR", "2026-05-01", reported=1)
    assert storage.close_departed_events(conn, {"ENSG"}, "2026-08-06") == []


def test_closing_is_idempotent():
    conn = _db()
    _event(conn, "APLS", "2026-05-01")
    first = storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    second = storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    assert first and second == [], "a closed row must not be closed twice"


def test_the_reason_and_timestamp_are_recorded():
    conn = _db()
    _event(conn, "APLS", "2026-05-01")
    storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    reason, at = conn.execute(
        "SELECT closed_reason, closed_at FROM events WHERE ticker='APLS'"
    ).fetchone()
    assert reason == storage.CLOSED_DELISTED and at


# --- the guard that keeps this from being a mass deletion ------------------

def test_an_EMPTY_universe_closes_nothing():
    """An empty coverage set means the exports read FAILED. Closing every open
    event on that basis is a silent mass deletion of workflow state — the same
    reasoning as the coverage-collapse hard stop, and the 2026-07-26 BOM
    incident is the precedent: a bad `universe.csv` returned an empty set while
    the run stayed green.
    """
    conn = _db()
    for t in ("APLS", "EXAS", "FOLD"):
        _event(conn, t, "2026-05-01")
    assert storage.close_departed_events(conn, set(), "2026-08-06") == []
    assert _open_count(conn) == 3


# --- the invariant that stops this becoming a silent half-fix --------------

def test_no_query_filters_on_a_bare_reported_flag():
    """Every open-event query must go through `OPEN_EVENT_SQL`.

    A terminal state that only SOME call sites honour is worse than none: the
    digest would drop a closed event while TickTick kept nagging about it, or
    the reverse. That is the shape of portfolio_daily's two account rosters,
    where editing one of them was a silent half-fix.

    This scans the live source rather than trusting a checklist, so a query
    added next month cannot quietly skip the new state.
    """
    import pathlib
    import re

    # Both spellings, because the second is what the first draft of this scan
    # MISSED: `scripts/export_upcoming_events.py` filtered on
    # `COALESCE(reported, 0) = 0`, a real query that a literal `reported = 0`
    # search walks straight past.
    PREDICATE = re.compile(r"(?:COALESCE\(\s*reported\s*,[^)]*\)|\breported\b)\s*=\s*0")
    SQLISH = re.compile(r"\b(WHERE|AND|SELECT|UPDATE|DELETE)\b")

    root = pathlib.Path(__file__).resolve().parent
    offenders = []
    for path in list(root.glob("*.py")) + list((root / "scripts").glob("*.py")):
        if path.name.startswith("test_"):
            continue
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or "OPEN_EVENT_SQL" in line:
                continue
            # A SQL keyword on the same line is what separates a real predicate
            # from a log message ("left reported=0 for retry") or a counter
            # (`skipped_already_reported = 0`).
            if PREDICATE.search(line) and SQLISH.search(line):
                offenders.append(f"{path.name}:{i}: {stripped}")
    assert not offenders, (
        "these filter on a bare `reported` flag instead of storage.OPEN_EVENT_SQL, "
        "so a delisted event stays pending for them:\n  " + "\n  ".join(offenders))


def test_the_open_predicate_actually_excludes_closed_rows():
    """Guards against OPEN_EVENT_SQL being edited into something vacuous."""
    conn = _db()
    _event(conn, "A", "2026-05-01")
    _event(conn, "B", "2026-05-01")
    conn.execute("UPDATE events SET closed_reason='delisted' WHERE ticker='B'")
    conn.commit()
    rows = conn.execute(
        f"SELECT ticker FROM events WHERE {storage.OPEN_EVENT_SQL}").fetchall()
    assert [r[0] for r in rows] == ["A"]


def test_migration_adds_the_columns_to_an_existing_db(tmp_path):
    """Non-destructive: an existing v12 database gains the columns without
    losing rows."""
    db = tmp_path / "e.db"
    conn = storage.init_db(db)
    _event(conn, "ENSG", "2026-05-01")
    conn.execute("DELETE FROM schema_version")
    conn.execute("INSERT INTO schema_version VALUES (12, datetime('now'))")
    conn.execute("ALTER TABLE events DROP COLUMN closed_reason")
    conn.execute("ALTER TABLE events DROP COLUMN closed_at")
    conn.commit()
    conn.close()

    conn = storage.init_db(db)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    assert {"closed_reason", "closed_at"} <= cols
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


# --- reversibility, because a blip must not be permanent -------------------

def test_a_ticker_that_returns_to_coverage_is_REOPENED():
    """Codex round 1. The collapse guard only fires on >=25 names AND >20% of
    a tier, so a SINGLE ticker missing from one exports read sails under it.

    Without reopening, that one bad run closes a live event forever and every
    `OPEN_EVENT_SQL` consumer silently drops a name that is right there in the
    universe. A terminal state a transient blip can enter and nothing can leave
    is not a state, it is data loss.
    """
    conn = _db()
    _event(conn, "ENSG", "2026-05-01")
    storage.close_departed_events(conn, {"OTHER"}, "2026-08-06")   # the blip
    assert _open_count(conn) == 0

    storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")    # it's back
    assert _open_count(conn) == 1
    reason, at = conn.execute(
        "SELECT closed_reason, closed_at FROM events WHERE ticker='ENSG'"
    ).fetchone()
    assert reason is None and at is None


def test_reopening_does_not_disturb_a_reported_row():
    conn = _db()
    _event(conn, "ENSG", "2026-05-01", reported=1)
    storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    assert conn.execute(
        "SELECT reported FROM events WHERE ticker='ENSG'").fetchone()[0] == 1


def test_the_ticktick_mutation_query_excludes_closed_rows():
    """Codex round 1. The freshness query was filtered; the query that
    actually drives the mutations was not — so a terminal row would enter the
    unconfirmed branch and strip or rewrite a task that is done.

    It must still see REPORTED rows (it marks their tasks `[REPORTED]`), so
    the filter is `closed_reason IS NULL`, not the full open predicate.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent / "ticktick.py").read_text(
        encoding="utf-8")
    start = src.index("def reconcile_ticktick_tasks(")
    nxt = src.find("\ndef ", start + 1)          # it is currently the last def
    body = src[start:nxt if nxt != -1 else len(src)]
    idx = body.index("FROM events WHERE tier <= 2 AND")
    window = body[idx:idx + 200]
    assert "closed_reason IS NULL" in window, (
        "the mutation source query must exclude closed events:\n" + window)
