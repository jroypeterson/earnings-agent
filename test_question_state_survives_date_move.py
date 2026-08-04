"""
Regression tests for the "Status: New" re-raise that isn't new (board #263 + #274).

**These two board rows are the same bug, and the row that named the cause is #274.**

#263 was filed as "a confirmed date doesn't suppress the missing-from-Finnhub alert —
HCAT re-raised the day after its Aug-6 lock". The live DB on 2026-08-03 says otherwise:

    HCAT  2026-08-12  date_locked=0  date_confirmed=0  question_state=None
    DH    2026-08-12  date_locked=0  date_confirmed=0  question_state=None

There is no Aug-6 row and no lock on HCAT anywhere — and DH, filed separately as #274,
has the byte-identical shape. What actually happened is #274's diagnosis: the vendor
MOVED the date (Finnhub flapped DH Aug 12 -> Aug 5 -> Aug 12 inside 24h), and every
piece of question/alert state is keyed on `(ticker, event_date)`. `upsert_event`'s
same-quarter DELETE therefore destroyed it, and the replacement row was born with
`question_state`, `question_first_seen`, `slack_thread_ts` and `unseen_run_count` all
NULL/0 — so the next run had no idea a conversation was already open and posted a fresh
card reading "Status: *New* — just detected".

The question is about a TICKER'S QUARTER, not about one candidate date. So the fix is
the same rescue `ticktick_task_id` already gets across that DELETE.
"""

from __future__ import annotations

from datetime import date

import main
from storage import init_db, open_question, set_date_lock, upsert_event


def _db():
    return init_db(":memory:")


def _seed(conn, ticker="DH", event_date="2026-08-05", quarter="2026Q2"):
    upsert_event(conn, ticker, event_date, "amc", None, quarter=quarter, tier=2,
                 company_name="Definitive Healthcare")


# --------------------------------------------------------------------------
# The real defect: a date move must not orphan the conversation
# --------------------------------------------------------------------------

def test_open_question_survives_a_vendor_date_move():
    """The DH sequence, exactly: raise a question on Aug 5, vendor moves to Aug 12."""
    conn = _db()
    _seed(conn, event_date="2026-08-05")
    open_question(conn, "DH", "2026-08-05", kind="unseen",
                  thread_ts="1722.0001", channel_id="C0STATUS",
                  first_seen_iso="2026-07-31")

    upsert_event(conn, "DH", "2026-08-12", "amc", None, quarter="2026Q2", tier=2)

    row = conn.execute(
        "SELECT question_state, question_first_seen, slack_thread_ts, "
        "slack_channel_id, slack_question_kind FROM events "
        "WHERE ticker='DH' AND event_date='2026-08-12'").fetchone()
    assert row[0] == "open", "the question was still open; the move must not close it"
    assert row[1] == "2026-07-31", (
        "question_first_seen must carry, or the card renders 'New — just detected' "
        "for a conversation four days old")
    assert row[2] == "1722.0001", "escalation must land in the EXISTING thread"
    assert row[3] == "C0STATUS"
    assert row[4] == "unseen"


def test_the_old_row_is_gone_so_state_is_not_duplicated():
    conn = _db()
    _seed(conn, event_date="2026-08-05")
    open_question(conn, "DH", "2026-08-05", kind="unseen", thread_ts="1722.0001",
                  channel_id="C0STATUS", first_seen_iso="2026-07-31")
    upsert_event(conn, "DH", "2026-08-12", "amc", None, quarter="2026Q2", tier=2)
    n = conn.execute("SELECT COUNT(*) FROM events WHERE ticker='DH'").fetchone()[0]
    assert n == 1


def test_the_miss_counter_survives_the_move():
    """Otherwise a flapping date resets the streak every time and the 2-run
    escalation threshold is never reached — the alert would go permanently silent."""
    conn = _db()
    _seed(conn, event_date="2026-08-05")
    conn.execute("UPDATE events SET unseen_run_count = 3 "
                 "WHERE ticker='DH' AND event_date='2026-08-05'")
    upsert_event(conn, "DH", "2026-08-12", "amc", None, quarter="2026Q2", tier=2)
    got = conn.execute("SELECT unseen_run_count FROM events "
                       "WHERE ticker='DH' AND event_date='2026-08-12'").fetchone()[0]
    assert got == 3


def test_a_flap_back_to_the_original_date_still_keeps_state():
    """Aug 12 -> Aug 5 -> Aug 12 is the observed DH sequence, not a hypothetical."""
    conn = _db()
    _seed(conn, event_date="2026-08-12")
    open_question(conn, "DH", "2026-08-12", kind="unseen", thread_ts="1722.0001",
                  channel_id="C0STATUS", first_seen_iso="2026-07-31")
    upsert_event(conn, "DH", "2026-08-05", "amc", None, quarter="2026Q2", tier=2)
    upsert_event(conn, "DH", "2026-08-12", "amc", None, quarter="2026Q2", tier=2)
    row = conn.execute("SELECT question_state, question_first_seen, slack_thread_ts "
                       "FROM events WHERE ticker='DH' AND event_date='2026-08-12'").fetchone()
    assert row == ("open", "2026-07-31", "1722.0001")


def test_an_answered_question_stays_answered_across_a_move():
    """The worst version: the operator said `ignore` (state: dismissed), the date moved,
    and the alert came back anyway."""
    conn = _db()
    _seed(conn, event_date="2026-08-05")
    open_question(conn, "DH", "2026-08-05", kind="unseen", thread_ts="1722.0001",
                  channel_id="C0STATUS", first_seen_iso="2026-07-31")
    conn.execute("UPDATE events SET question_state='dismissed' "
                 "WHERE ticker='DH' AND event_date='2026-08-05'")
    upsert_event(conn, "DH", "2026-08-12", "amc", None, quarter="2026Q2", tier=2)
    q = conn.execute("SELECT question_state, slack_question_kind FROM events "
                     "WHERE ticker='DH' AND event_date='2026-08-12'").fetchone()
    assert q[0] == "dismissed"
    assert main._unseen_alert_suppressed(
        {"slack_question_kind": q[1], "question_state": q[0]}, date(2026, 8, 4)
    ) is not None, "a dismissed question must still suppress after the move"


def test_a_different_quarter_does_not_inherit_state():
    """State is scoped to the ticker's QUARTER. Next quarter starts clean."""
    conn = _db()
    _seed(conn, event_date="2026-08-05", quarter="2026Q2")
    open_question(conn, "DH", "2026-08-05", kind="unseen", thread_ts="1722.0001",
                  channel_id="C0STATUS", first_seen_iso="2026-07-31")
    upsert_event(conn, "DH", "2026-11-05", "amc", None, quarter="2026Q3", tier=2)
    row = conn.execute("SELECT question_state, slack_thread_ts FROM events "
                       "WHERE ticker='DH' AND event_date='2026-11-05'").fetchone()
    assert row == (None, None)


def test_a_reported_row_does_not_donate_its_state():
    """A reported row is history; the DELETE already spares it and it must not seed a
    new question either."""
    conn = _db()
    upsert_event(conn, "DH", "2026-05-07", "amc", None, quarter="2026Q2", tier=2,
                 reported=True)
    open_question(conn, "DH", "2026-05-07", kind="unseen", thread_ts="OLD",
                  channel_id="C0STATUS", first_seen_iso="2026-05-01")
    upsert_event(conn, "DH", "2026-08-12", "amc", None, quarter="2026Q2", tier=2)
    got = conn.execute("SELECT slack_thread_ts FROM events "
                       "WHERE ticker='DH' AND event_date='2026-08-12'").fetchone()[0]
    assert got is None


# --------------------------------------------------------------------------
# #263's stated rule is still worth having — it closes a DIFFERENT hole
# --------------------------------------------------------------------------

def test_a_locked_date_suppresses_the_unseen_alert_with_no_slack_question():
    """`--lock` from the CLI, an EDGAR auto-correction and the web resolver all pin a
    date without ever creating a Slack question, so `question_state` stays NULL and the
    2026-07-25 suppression could never fire for them. Sync and reconcile SKIP a locked
    event by design, so Finnhub not returning it is the expected consequence of the
    lock rather than news."""
    assert main._unseen_alert_suppressed(None, date(2026, 8, 4), date_locked=1) is not None


def test_company_confirmed_with_an_announcement_suppresses():
    """An IR feed / IR email / high-confidence web resolution is the COMPANY's own
    date — tiers 2-4 of the source hierarchy, all above Finnhub at tier 5."""
    assert main._unseen_alert_suppressed(
        None, date(2026, 8, 4), date_confirmed=1,
        announcement_url="https://ir.example.com/pr") is not None


def test_finnhub_derived_confirmation_alone_does_NOT_suppress():
    """Without an announcement URL, `date_confirmed` came from Finnhub's own `hour`
    field. Finnhub confirming a date and then dropping the event is a real anomaly and
    must still be surfaced — suppressing it would recreate the silent miss."""
    assert main._unseen_alert_suppressed(
        None, date(2026, 8, 4), date_confirmed=1, announcement_url=None) is None


def test_an_unlocked_unconfirmed_row_still_alerts():
    """The HCAT/DH shape itself. Nothing about it should be suppressed — it just must
    not claim to be NEW."""
    assert main._unseen_alert_suppressed(None, date(2026, 8, 4)) is None
