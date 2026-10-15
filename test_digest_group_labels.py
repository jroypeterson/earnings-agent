"""The weekly digest names its groups in Coverage Manager's terms, never tiers.

JP 2026-10-15: "Fix the slack weekly digest with Tier 1+2 names and instead
call them what they are." Tier 1 is the Position lists, not the Core=Y names,
so "Core Watchlist" was a mislabel as well as jargon.
"""
import json
from datetime import date

from digest import DigestData, EventRow
from notifications import (build_email_html, build_email_text,
                           build_slack_blocks, build_slack_fallback_text)


def _row(t, tier):
    return EventRow(ticker=t, company_name=t + " Co", event_date="2026-10-20",
                    event_hour="amc", eps_estimate=1.0, rev_estimate=None,
                    tier=tier, sector="MedTech", subsector="")


def _digest():
    return DigestData(
        reference_date=date(2026, 10, 18), week_start=date(2026, 10, 19),
        week_end=date(2026, 10, 23), month_end=date(2026, 11, 17),
        tier1_week=[_row("AAA", 1)], tier2_week=[_row("BBB", 2)],
        tier3_week=[_row("CCC", 3)],
        peak_week_start=date(2026, 11, 2), peak_week_count=40,
    )


def test_no_tier_jargon_or_core_watchlist_in_any_digest_rendering():
    d = _digest()
    outputs = [json.dumps(build_slack_blocks(d)), build_email_html(d),
               build_email_text(d), build_slack_fallback_text(d)]
    for out in outputs:
        assert "Tier" not in out
        assert "Core Watchlist" not in out


def test_digest_names_the_groups():
    text = build_email_text(_digest())
    assert "Positions (Portfolio, Researching, Ready to Buy/Short) (1)" in text
    assert "HC Services + MedTech + Core Biopharma (1)" in text
    assert "Rest of coverage (1)" in text
