"""A reported event must close its Slack question thread.

`question_state` documents `resolved` as "action applied (lock, reported, etc.)" — but
nothing ever applied the `reported` half. Two paths mark an event reported:

  * the Slack reply handler (`main.py` ~4442), which calls `update_question_state(...,
    "resolved")` immediately after, and
  * the AUTOMATIC path (`main.py` ~1484), which does not.

So every event that healed itself — an EDGAR 8-K Item 2.02 auto-correction, an IR
announcement, the web resolver — reported its numbers and left its question open forever.

Measured against the LIVE artifact on 2026-08-07: **8 of 21** question-bearing events were
`reported = 1 AND question_state = 'open'`, and **zero** were `reported = 1 AND resolved`, so
the documented transition had never once fired. The oldest (EHAB) had been open since May.
QDEL — the event whose self-healing was being tested — was the eighth instance.

It causes no spurious alerts, because every alert query gates on `OPEN_EVENT_SQL`
(`reported = 0 AND closed_reason IS NULL`). What it costs is signal: the open-question count
was inflated ~38%, burying 10 genuinely open questions among 8 answered ones.
"""
import sqlite3

from storage import init_db, resolve_reported_questions


def _db() -> sqlite3.Connection:
    return init_db(":memory:")


def _event(conn, ticker, *, reported, state, date="2026-08-06", quarter="2026Q2"):
    conn.execute(
        "INSERT INTO events (ticker, quarter, event_date, reported, question_state) "
        "VALUES (?, ?, ?, ?, ?)",
        (ticker, quarter, date, reported, state),
    )
    conn.commit()


def _state(conn, ticker):
    return conn.execute(
        "SELECT question_state FROM events WHERE ticker = ?", (ticker,)).fetchone()[0]


def test_reporting_closes_an_open_question():
    conn = _db()
    _event(conn, "QDEL", reported=1, state="open")
    assert resolve_reported_questions(conn) == 1
    assert _state(conn, "QDEL") == "resolved"


def test_an_unreported_event_keeps_its_open_question():
    """The question is only answered once the numbers are actually out."""
    conn = _db()
    _event(conn, "PENDING", reported=0, state="open")
    assert resolve_reported_questions(conn) == 0
    assert _state(conn, "PENDING") == "open"


def test_a_dismissed_question_is_never_relabelled():
    """`dismissed` means the operator said never alert me about this again. That is already
    terminal and is not ours to overwrite."""
    conn = _db()
    _event(conn, "IGNORED", reported=1, state="dismissed")
    resolve_reported_questions(conn)
    assert _state(conn, "IGNORED") == "dismissed"


def test_monitoring_and_snoozed_both_close_on_report():
    """`wait` and `snooze <n>d` both mean "ask me later". Reporting is the later."""
    conn = _db()
    _event(conn, "WAIT", reported=1, state="monitoring")
    _event(conn, "SNOOZE", reported=1, state="snoozed", date="2026-08-05")
    assert resolve_reported_questions(conn) == 2
    assert _state(conn, "WAIT") == "resolved"
    assert _state(conn, "SNOOZE") == "resolved"


def test_an_event_with_no_question_is_untouched():
    """Most events never had a question. The sweep must not invent state for them."""
    conn = _db()
    _event(conn, "QUIET", reported=1, state=None)
    assert resolve_reported_questions(conn) == 0
    assert _state(conn, "QUIET") is None


def test_the_sweep_is_idempotent():
    """It runs on every daily sync; a second pass must change nothing."""
    conn = _db()
    _event(conn, "QDEL", reported=1, state="open")
    resolve_reported_questions(conn)
    assert resolve_reported_questions(conn) == 0
