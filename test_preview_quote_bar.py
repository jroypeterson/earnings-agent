"""Consensus preview: the grey quote bar in #earnings-core-coverage (JP 2026-11-16).

JP asked for each company's entry behind Slack's quote bar in that channel, so the preview
reads like earnings_review's short cards beside it. #portfolio and #street-account were not
part of the ask and keep the plain layout.
"""
from datetime import date

from consensus_preview import PreviewRow, build_preview_blocks


def _row(ticker="ISRG", name="Intuitive Surgical"):
    return PreviewRow(ticker=ticker, company_name=name, tier=1,
                      event_date="2026-10-20", event_hour="amc",
                      call_datetime_utc=None, eps_mean=2.69)


def _sections(quote):
    blocks, n = build_preview_blocks([_row(), _row("DXCM", "DexCom")], date(2026, 10, 18), quote=quote)
    assert n == 2
    return [b["text"]["text"].split("\n") for b in blocks if b["type"] == "section"]


def test_core_layout_quotes_every_detail_line_under_a_flush_name():
    secs = _sections(quote=True)
    assert len(secs) == 2
    for (name, *detail), ticker in zip(secs, ("ISRG", "DXCM")):
        assert name.startswith(f"*`{ticker}`") and not name.startswith(">")
        assert len(detail) > 5 and all(ln.startswith("> ") for ln in detail)


def test_default_layout_is_unchanged_for_the_other_channels():
    for name, *detail in _sections(quote=False):
        assert not any(ln.startswith(">") for ln in [name, *detail])
