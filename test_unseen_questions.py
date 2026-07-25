"""
Regression tests for the unseen-ticker question lifecycle (board #172).

The bug these guard against, observed live on INMD 2026-07-24: the operator
replied `lock 2026-08-05` in the alert thread, `--check-replies` applied it
(`date_locked=1` persisted), and the SAME alert re-fired hours later with the
counter incremented 7 -> 8 and the card reading "Status: *New* — just
detected". Three independent defects combined to produce that:

  D1  The B2 unseen-detection SELECT in `run()` never looked at
      `question_state`, so an answered question was re-alerted on every daily
      sync. `ignore` / `snooze` / `wait` were therefore silent no-ops for this
      alert class — and their acks ("won't re-alert for this event") were lies.
  D2  Every re-alert called `open_question(...)`, which unconditionally
      overwrote `question_first_seen`, reset `question_state` back to 'open',
      NULLed the reply watermark, and cleared any snooze — resurrecting a
      question the operator had already closed and orphaning their reply in
      the now-superseded thread.
  D3  `build_unseen_thread_blocks(row, today)` was called without
      `first_seen_iso`, so the age line fell through to the "New — just
      detected" default on every card regardless of the question's real age.
"""

from __future__ import annotations

from datetime import date

import main
from notifications import UnseenRow, build_unseen_thread_blocks
from storage import (
    get_question_snapshot,
    init_db,
    open_question,
    update_question_state,
    upsert_event,
    date_to_quarter,
)


def _conn_with_event(ticker="INMD", event_date="2026-08-05"):
    conn = init_db(":memory:")
    upsert_event(
        conn, ticker, event_date, "amc", None,
        quarter=date_to_quarter(event_date),
        reported=False, tier=2, company_name="InMode Ltd.",
    )
    return conn


def _blocks_text(blocks):
    return "\n".join(
        b.get("text", {}).get("text", "")
        for b in blocks
        if isinstance(b.get("text"), dict)
    )


# --------------------------------------------------------------------------
# D2 — open_question must not clobber an existing question
# --------------------------------------------------------------------------


def test_open_question_preserves_first_seen_across_realert():
    """A re-alert must not reset the question's age to today."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18")
    # Same event re-alerts six days later on a brand-new thread.
    open_question(conn, "INMD", "2026-08-05", thread_ts="222.2",
                  kind="unseen", first_seen_iso="2026-07-24")

    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert q["question_first_seen"] == "2026-07-18", (
        "first_seen must survive a re-alert — otherwise every card reads "
        "'just detected' and the operator can't see the question is stale"
    )
    assert q["slack_thread_ts"] == "222.2"


def test_open_question_keeps_reply_watermark_when_thread_unchanged():
    """Re-opening the SAME thread must not replay already-consumed replies."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18")
    conn.execute(
        "UPDATE events SET slack_last_reply_ts = '999.9' WHERE ticker = 'INMD'"
    )
    conn.commit()

    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-24")

    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert q["slack_last_reply_ts"] == "999.9"


def test_open_question_clears_reply_watermark_on_a_new_thread():
    """A genuinely new thread has no consumed replies — watermark must reset."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18")
    conn.execute(
        "UPDATE events SET slack_last_reply_ts = '999.9' WHERE ticker = 'INMD'"
    )
    conn.commit()

    open_question(conn, "INMD", "2026-08-05", thread_ts="333.3",
                  kind="unseen", first_seen_iso="2026-07-24")

    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert q["slack_last_reply_ts"] is None


# --------------------------------------------------------------------------
# D1 — an answered question must suppress the re-alert
# --------------------------------------------------------------------------


def test_no_question_yet_is_not_suppressed():
    assert main._unseen_alert_suppressed(None, date(2026, 7, 24)) is None


def test_dismissed_question_suppresses_forever():
    """`ignore` promises 'won't re-alert for this event'. Keep that promise."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18")
    update_question_state(conn, "INMD", "2026-08-05", "dismissed")

    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert main._unseen_alert_suppressed(q, date(2027, 1, 1)) is not None


def test_resolved_and_monitoring_questions_suppress():
    """`lock` (resolved) and `wait` (monitoring) are both operator answers."""
    for state in ("resolved", "monitoring"):
        conn = _conn_with_event()
        open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                      kind="unseen", first_seen_iso="2026-07-18")
        update_question_state(conn, "INMD", "2026-08-05", state)
        q = get_question_snapshot(conn, "INMD", "2026-08-05")
        assert main._unseen_alert_suppressed(q, date(2026, 7, 24)) is not None, (
            f"a {state} question must not re-alert"
        )


def test_snooze_suppresses_until_it_expires():
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18")
    update_question_state(conn, "INMD", "2026-08-05", "snoozed",
                          snooze_until_iso="2026-07-27")
    q = get_question_snapshot(conn, "INMD", "2026-08-05")

    assert main._unseen_alert_suppressed(q, date(2026, 7, 24)) is not None
    assert main._unseen_alert_suppressed(q, date(2026, 7, 28)) is None


def test_answered_xcheck_question_does_not_suppress_an_unseen_alert():
    """Codex R1 P1: `question_state` is per-EVENT and shared by all three
    question kinds. Suppressing on state alone let a resolved CROSS-CHECK
    question silence a genuine, unrelated missing-from-Finnhub alert — the
    exact silent-miss class this fix exists to remove."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="xcheck", first_seen_iso="2026-07-18")
    update_question_state(conn, "INMD", "2026-08-05", "resolved")

    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert main._unseen_alert_suppressed(q, date(2026, 7, 24)) is None, (
        "an answered xcheck question must not gag the unseen lane"
    )


def test_unseen_alert_never_posts_into_a_foreign_thread():
    """Codex R1 P1: reusing an OPEN xcheck thread for the unseen card would
    leave `--check-replies` parsing replies with the xcheck grammar, which
    rejects unseen-only commands like `reported`."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="xcheck", first_seen_iso="2026-07-18",
                  channel_id="C_STATUS")
    q = get_question_snapshot(conn, "INMD", "2026-08-05")

    assert main._live_unseen_thread(q) is None
    q["slack_question_kind"] = "unseen"
    assert main._live_unseen_thread(q) == ("111.1", "C_STATUS")


def test_escalation_targets_the_threads_own_channel():
    """Codex R2 P1: a `thread_ts` is only valid in the channel it was created
    in. Escalating into the currently-configured channel after a channel
    change yields `thread_not_found` and reds the daily run."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18",
                  channel_id="C_OLD")
    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert main._live_unseen_thread(q) == ("111.1", "C_OLD")


def test_thread_without_a_recorded_channel_is_not_reused():
    """Legacy rows carry no channel — open a fresh question rather than guess."""
    conn = _conn_with_event()
    open_question(conn, "INMD", "2026-08-05", thread_ts="111.1",
                  kind="unseen", first_seen_iso="2026-07-18", channel_id=None)
    q = get_question_snapshot(conn, "INMD", "2026-08-05")
    assert main._live_unseen_thread(q) is None


def test_open_question_escalates_on_a_backoff_not_every_run():
    """An unanswered question stays visible, but must not spam daily."""
    fired, last = [], None
    for n in range(1, 40):
        if main._unseen_escalation_due(n, last):
            fired.append(n)
            last = n  # a DELIVERED escalation moves the anchor
    assert fired == [2, 4, 8, 16, 32], fired


def test_a_failed_escalation_is_still_due_on_the_next_run():
    """Codex R2 P1: the counter commits before the Slack post, so an exact
    `n in {2,4,8,...}` membership test dropped the alert entirely when the
    post at 4 failed — nothing surfaced again until 8."""
    # Delivered at 2, then the post at 4 FAILS so the anchor stays at 2.
    assert main._unseen_escalation_due(4, 2) is True
    assert main._unseen_escalation_due(5, 2) is True, (
        "a failed delivery must leave the escalation due, not skip to 8"
    )


def test_below_the_first_alert_threshold_nothing_fires():
    assert main._unseen_escalation_due(1, None) is False
    assert main._unseen_escalation_due(2, None) is True


def test_escalation_anchor_is_cleared_when_the_provider_recovers():
    """Codex R3 P1: the anchor is scoped to ONE missing streak. If Finnhub
    returns the event (resetting `unseen_run_count`) and it then disappears
    again, a leftover anchor of 8 would gag the whole new streak until 16."""
    from storage import kv_get, kv_set, kv_delete

    conn = _conn_with_event()
    key = main._unseen_escalation_key("INMD", "2026-08-05")
    kv_set(conn, key, "8")
    assert kv_get(conn, key) == "8"

    kv_delete(conn, key)  # what the re-appearance branch now does
    assert kv_get(conn, key) is None
    # A fresh streak is therefore due again at the first alert threshold.
    assert main._unseen_escalation_due(2, None) is True


def test_kv_delete_is_a_noop_when_absent():
    from storage import kv_delete

    conn = _conn_with_event()
    kv_delete(conn, "nothing:here")  # must not raise


# --------------------------------------------------------------------------
# D3 — the card must show the question's real age
# --------------------------------------------------------------------------


def test_unseen_card_reports_real_age_when_first_seen_known():
    row = UnseenRow(ticker="INMD", company_name="InMode Ltd.",
                    event_date="2026-08-05", tier=2, miss_count=8)
    blocks = build_unseen_thread_blocks(
        row, date(2026, 7, 24), first_seen_iso="2026-07-18"
    )
    text = _blocks_text(blocks)
    assert "just detected" not in text, (
        "an 8-run-old question must never render as newly detected"
    )
    assert "6d" in text or "6 d" in text, text
