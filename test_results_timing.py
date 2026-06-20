"""Tests for the report-timing context segment on the results Slack line.

PROJECT_IDEAS #531: the results post should show when the company reported
(date), whether it was before/after market (BMO/AMC), and when the call was
held (same day vs. the following morning).
"""

from datetime import date

from notifications import (
    ResultRow,
    _fmt_call_compact,
    _fmt_results_timing,
    _format_results_line,
    build_results_slack_blocks,
)


def _row(**kw):
    base = dict(
        ticker="ACME",
        company_name="Acme Corp",
        event_date="2026-06-18",
        event_hour="amc",
        eps_actual=1.10,
        eps_estimate=1.00,
        rev_actual=2.0e9,
        rev_estimate=1.9e9,
        tier=1,
    )
    base.update(kw)
    return ResultRow(**base)


class TestCallCompact:
    def test_none_when_missing(self):
        assert _fmt_call_compact("2026-06-18", None) is None

    def test_same_day_call(self):
        # 2026-06-18 20:30 UTC = 16:30 ET (same day as the AMC release)
        out = _fmt_call_compact("2026-06-18", "2026-06-18T20:30:00+00:00")
        assert out == "call same day 4:30 PM ET"

    def test_next_morning_call(self):
        # 2026-06-19 12:00 UTC = 08:00 ET (the morning after an AMC release)
        out = _fmt_call_compact("2026-06-18", "2026-06-19T12:00:00+00:00")
        assert out == "call Fri 8:00 AM ET"

    def test_handles_zulu_suffix(self):
        out = _fmt_call_compact("2026-06-18", "2026-06-18T20:30:00Z")
        assert out == "call same day 4:30 PM ET"

    def test_junk_returns_none(self):
        assert _fmt_call_compact("2026-06-18", "not-a-date") is None


class TestResultsTiming:
    def test_date_and_session_always_present(self):
        seg = _fmt_results_timing(_row(call_datetime_utc=None))
        assert "Jun 18" in seg
        assert "AMC" in seg
        assert "call" not in seg  # no call info → no call segment

    def test_tbd_when_hour_unknown(self):
        seg = _fmt_results_timing(_row(event_hour=None, call_datetime_utc=None))
        assert "TBD" in seg

    def test_call_appended_when_present(self):
        seg = _fmt_results_timing(
            _row(call_datetime_utc="2026-06-19T12:00:00+00:00")
        )
        assert "AMC" in seg
        assert "call Fri 8:00 AM ET" in seg


class TestResultsLine:
    def test_line_includes_timing_before_metrics(self):
        line = _format_results_line(_row(call_datetime_utc="2026-06-18T20:30:00Z"))
        # Timing must appear, and before the EPS metric segment.
        assert "Jun 18 AMC" in line
        assert "call same day 4:30 PM ET" in line
        assert line.index("AMC") < line.index("EPS")

    def test_line_renders_without_call(self):
        line = _format_results_line(_row(call_datetime_utc=None))
        assert "Jun 18 AMC" in line
        assert "EPS" in line


class TestBlocksSmoke:
    def test_blocks_build_with_timing(self):
        rows = [
            _row(),
            _row(ticker="BETA", event_hour="bmo", call_datetime_utc=None),
        ]
        blocks = build_results_slack_blocks(rows, date(2026, 6, 18))
        text = json.dumps(blocks)
        assert "ACME" in text and "Jun 18" in text


import json  # noqa: E402  (used only in the smoke test above)
