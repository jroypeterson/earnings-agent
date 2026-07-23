"""Heartbeat status semantics (HEALTH_REPORTING.md §4.2 abnormal-counts rule).

The heartbeat was previously hardcoded green — a zero-event provider outage
heartbeated :white_check_mark:. These tests pin the new status/warnings
rendering and back-compat default.
"""
from unittest import mock

import notifications


def _capture_post(monkeypatch_target="notifications.post_slack"):
    return mock.patch(monkeypatch_target)


def test_default_status_renders_green_ok():
    with _capture_post() as post:
        notifications.post_heartbeat("http://wh", "Daily sync", {"events": 412})
    blocks, fallback = post.call_args.args[1], post.call_args.args[2]
    text = blocks[0]["elements"][0]["text"]
    assert ":white_check_mark:" in text
    assert "OK" in fallback and "PARTIAL" not in fallback


def test_partial_renders_warning_and_reason():
    with _capture_post() as post:
        notifications.post_heartbeat(
            "http://wh", "Daily sync", {"events": 0},
            status="partial",
            warnings=["0 events from Finnhub+FMP across the full window — provider outage?"],
        )
    blocks, fallback = post.call_args.args[1], post.call_args.args[2]
    text = blocks[0]["elements"][0]["text"]
    assert ":warning:" in text and ":white_check_mark:" not in text
    assert "provider outage" in text
    assert "PARTIAL" in fallback


def test_zero_values_still_render_in_counters():
    """Existing contract: None/0 values render so a zero is visibly from the
    run, not a missing field."""
    with _capture_post() as post:
        notifications.post_heartbeat("http://wh", "Results check",
                                     {"new": 0, "pending": 0})
    text = post.call_args.args[1][0]["elements"][0]["text"]
    assert "new: 0" in text and "pending: 0" in text


def test_heartbeat_failure_never_raises():
    with mock.patch("notifications.post_slack",
                    side_effect=notifications.NotificationError("boom")):
        notifications.post_heartbeat("http://wh", "Daily sync", {"events": 1},
                                     status="partial", warnings=["w"])
