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


# NOTE: a `test_the_ticktick_mutation_query_excludes_closed_rows` lived here
# after round 2 and was DELETED in round 3. It asserted that the reconcile
# filters closed rows out — which turned out to be the wrong behaviour, because
# hiding them orphans an already-created task forever. Its replacement is
# `test_the_ticktick_reconcile_SEES_closed_rows_and_completes_them` below.
# Recorded rather than silently dropped: a test that pins the wrong contract is
# worse than no test, and this one was written confidently enough to ship.

def test_a_SAME_DAY_event_is_never_closed():
    """Codex round 2. `event_date <= today` closed same-day events.

    The sync runs at 6/7 AM and 2/3 PM ET. At 6 AM a BMO name's actuals are
    not in yet; an AMC name has not reported at all. A coverage removal landing
    on the morning of an event would have terminally closed a LIVE one — and
    reopening only helps if the ticker comes back, which for a genuine
    same-day removal it does not.
    """
    conn = _db()
    _event(conn, "ENSG", "2026-08-06")
    assert storage.close_departed_events(conn, {"OTHER"}, "2026-08-06") == []
    assert _open_count(conn) == 1
    # The day after, it is genuinely past-dated.
    assert storage.close_departed_events(conn, {"OTHER"}, "2026-08-07")


def test_the_ticktick_CREATION_query_excludes_closed_rows():
    """Codex round 2, and the case the source-scan structurally cannot catch.

    `test_no_query_filters_on_a_bare_reported_flag` looks for queries filtering
    on `reported`; this one filters on neither `reported` nor `closed_reason`,
    so there was nothing for the scan to find. A row closed earlier in the same
    `run()` would have had a TickTick task created for it.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent / "main.py").read_text(
        encoding="utf-8")
    idx = src.index("Gather all Tier 1+2 events that need TickTick tasks")
    window = src[idx:idx + 900]
    assert "closed_reason IS NULL" in window, (
        "the TickTick creation query must not see closed events:\n" + window)


def test_every_events_query_that_drives_a_write_names_the_terminal_state():
    """The generalized version of the two misses above.

    Neither the TickTick creation query nor its reconcile sibling contained a
    `reported` predicate, so scanning for one found neither. Scan instead for
    what they DO have in common: a SELECT over `events` inside a module that
    mutates something external (Calendar, TickTick). Each must name
    `closed_reason`, whether via OPEN_EVENT_SQL or the bare column.
    """
    import pathlib
    import re
    root = pathlib.Path(__file__).resolve().parent
    offenders = []
    for name in ("main.py", "ticktick.py", "consensus_preview.py"):
        text = (root / name).read_text(encoding="utf-8")
        for m in re.finditer(r"FROM events\b", text):
            # A window wide enough to cover a multi-line SQL string.
            window = text[max(0, m.start() - 800):m.start() + 500]
            if "closed_reason" in window or "OPEN_EVENT_SQL" in window:
                continue
            # Three shapes are legitimately unfiltered, checked one by one
            # rather than waved through as a class:
            #   * a targeted DELETE of one known row (ticker + event_date) --
            #     it is removing a specific row, not reading a population;
            #   * a query for REPORTED history -- a closed event is by
            #     definition not reported, so it cannot appear;
            #   * `find_reported_event_for_quarter`-style lookups, same reason.
            tail = text[m.start():m.start() + 300]
            if re.search(r"DELETE FROM events WHERE ticker = \? AND event_date = \?",
                         window):
                continue
            if re.search(r"reported\s*,?\s*0?\s*\)?\s*=\s*1", tail):
                continue
            line = text[:m.start()].count("\n") + 1
            offenders.append(f"{name}:{line}: {tail.splitlines()[0]}")
    assert not offenders, (
        "these SELECT from `events` without naming the terminal state, so a "
        "closed event still reaches an external write:\n  " + "\n  ".join(offenders))


# --- round 3: the state has to reach the two external systems --------------

def test_a_closed_event_is_no_longer_answerable_in_slack():
    """Codex round 3. `list_open_questions` did not filter the terminal state.

    A late `lock <date>` reply on a departed ticker reaches
    `_apply_edgar_auto_correction`, which moves the Calendar entry and INSERTS
    a replacement row carrying the default `closed_reason = NULL` before
    deleting the closed one — resurrecting the event, date-locked, possibly at
    a future date.
    """
    conn = _db()
    _event(conn, "APLS", "2026-05-01", slack_thread_ts="1.1",
           question_state="open")
    assert len(storage.list_open_questions(conn)) == 1
    storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    assert storage.list_open_questions(conn) == []


def test_the_ticktick_reconcile_SEES_closed_rows_and_completes_them():
    """Codex round 3, and a regression the round-1 fix introduced.

    Hiding closed rows from the reconcile is worse than not filtering at all:
    an already-created task is then never completed, never cleared, never
    retitled — it stays overdue and keeps nagging about a company that no
    longer exists, which is the exact cross-consumer inconsistency the terminal
    state exists to remove.

    The CREATION query must not see them; this one has to, to finish what
    creation started. And it COMPLETES, never deletes — the tick is the durable
    record, and a deleted task would be recreated by a later sync.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent / "ticktick.py").read_text(
        encoding="utf-8")
    start = src.index("def reconcile_ticktick_tasks(")
    body = src[start:]
    idx = body.index("FROM events WHERE tier <= 2")
    assert "closed_reason IS NULL" not in body[idx:idx + 200], (
        "the reconcile must SEE closed rows, or their tasks are orphaned")
    assert "closed_reason" in body[:idx + 3000], (
        "...and must select the column so it can act on them")
    assert "complete_task(" in body, "a closed row's task must be completed"
    assert "delete_task" not in body.split("if closed_reason:")[1][:800], (
        "complete, never delete")


# --- round 4: the three ways the completion path was still inert -----------

def test_reopening_drops_the_task_pointer():
    """Codex round 4. Closing COMPLETED the task, and a completed task is
    untouchable by design — creation skips a non-null pointer, the reconcile
    refuses to act on `status == 2`. Keeping the pointer leaves the reopened
    event with no live task and no path to ever getting one.
    """
    conn = _db()
    _event(conn, "ENSG", "2026-05-01", ticktick_task_id="abc123")
    storage.close_departed_events(conn, {"OTHER"}, "2026-08-06")
    storage.close_departed_events(conn, {"ENSG"}, "2026-08-06")
    assert conn.execute(
        "SELECT ticktick_task_id FROM events WHERE ticker='ENSG'"
    ).fetchone()[0] is None, "a fresh task must be creatable"


def test_the_reconcile_selects_closed_rows_regardless_of_its_window():
    """Codex round 4, and the finding that made the round-3 fix nearly inert.

    The reconcile window defaults to a 14-day lookback; `close_departed_events`
    reaches back as far as the backlog goes, and the oldest measured row was 98
    days. Without this, almost none of the backlog's overdue tasks would ever
    be completed.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent / "ticktick.py").read_text(
        encoding="utf-8")
    idx = src.index("FROM events WHERE tier <= 2")
    window = src[idx:idx + 220]
    assert "closed_reason IS NOT NULL OR" in window, (
        "closed rows must be selected outside the date window:\n" + window)


def test_the_closed_branch_runs_AFTER_task_identity_is_resolved():
    """Codex round 4. Run earlier, it skips a row whose DB pointer is null (a
    legacy or repointed task) and completes a non-null pointer against a list
    GUESSED from the current date and tier rather than the list the task was
    actually found in — wrong for anything legacy or tier-moved.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent / "ticktick.py").read_text(
        encoding="utf-8")
    body = src[src.index("def reconcile_ticktick_tasks("):]
    resolved = body.index("pid, pname, task = chosen")
    branch = body.index("if closed_reason:")
    assert branch > resolved, (
        "the closed branch must run after identity resolution, so it uses the "
        "task's real list")
    assert "list_id=pid" in body[branch:branch + 1400], (
        "it must complete against the scanned list, not a guessed one")


def test_a_failed_completion_reddens_the_run():
    """A completion that fails leaves the task open indefinitely. Returning
    False without counting an error would let the job stay green while the
    thing it exists to fix silently did not happen."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parent / "ticktick.py").read_text(
        encoding="utf-8")
    body = src[src.index("if closed_reason:"):]
    tail = body[:1600]
    assert 'stats["errors"] += 1' in tail, (
        "a failed completion must increment errors")
