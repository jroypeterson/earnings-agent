"""
Outbound notification surfaces — Slack webhook posting and email rendering.

Slack posts are synchronous HTTP calls to the configured incoming webhook.
Email sending is not done here; this module produces HTML + plaintext that
the Gmail MCP path turns into a draft for the user to review and send.
"""

from __future__ import annotations

import html
import json
import logging
from datetime import date

import requests

from digest import DigestData, EventRow

logger = logging.getLogger("earnings_agent")

SLACK_TIMEOUT = 10
SLACK_MAX_BLOCKS = 48  # Slack hard cap is 50; leave headroom


class NotificationError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Shared formatting helpers
# ---------------------------------------------------------------------------


def _timing_short(hour: str | None) -> str:
    if not hour:
        return "TBD"
    h = hour.lower()
    if h == "bmo":
        return "BMO"
    if h == "amc":
        return "AMC"
    if h == "dmh":
        return "DMH"
    return hour.upper()


def _fmt_date_safe(iso: str) -> str:
    # Avoid %-d / %#d platform quirks by formatting the day manually.
    d = date.fromisoformat(iso)
    return f"{d.strftime('%a %b')} {d.day}"


def _fmt_estimate_eps(eps: float | None) -> str:
    if eps is None:
        return "–"
    return f"${eps:.2f}"


def _fmt_estimate_rev(rev: float | None) -> str:
    if rev is None:
        return "–"
    if rev >= 1e9:
        return f"${rev / 1e9:.2f}B"
    if rev >= 1e6:
        return f"${rev / 1e6:.1f}M"
    return f"${rev:,.0f}"


# ---------------------------------------------------------------------------
# Slack Block Kit
# ---------------------------------------------------------------------------


# Order timing buckets chronologically within a trading day.
_TIMING_ORDER = {"bmo": 0, "dmh": 1, "amc": 2}


def _timing_sort_key(hour: str | None) -> int:
    if not hour:
        return 99  # TBD / unknown last
    return _TIMING_ORDER.get(hour.lower(), 50)


def _row_line(r: EventRow, show_company: bool) -> str:
    """Render a single event as '{timing}: {ticker} ({company}) — EPS X, Rev Y'."""
    est_parts = []
    if r.eps_estimate is not None:
        est_parts.append(f"EPS {_fmt_estimate_eps(r.eps_estimate)}")
    if r.rev_estimate is not None:
        est_parts.append(f"Rev {_fmt_estimate_rev(r.rev_estimate)}")
    est_str = f" — {', '.join(est_parts)}" if est_parts else ""
    name = f" ({r.company_name})" if show_company and r.company_name else ""
    return f"  {_timing_short(r.event_hour)}: `{r.ticker}`{name}{est_str}"


def _slack_tier_block(
    label: str, rows: list[EventRow], show_company: bool = True
) -> dict | None:
    """Render a tier section grouped by day, timing-ordered within each day."""
    if not rows:
        return None

    # Bucket by event_date
    by_date: dict[str, list[EventRow]] = {}
    for r in rows:
        by_date.setdefault(r.event_date, []).append(r)

    lines = [f"*{label} ({len(rows)})*"]
    for iso_date in sorted(by_date.keys()):
        day_rows = sorted(
            by_date[iso_date],
            key=lambda r: (_timing_sort_key(r.event_hour), r.ticker),
        )
        lines.append(f"_{_fmt_date_safe(iso_date)}_")
        for r in day_rows:
            lines.append(_row_line(r, show_company=show_company))

    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(lines)[:3000]},
    }


def _slack_context_block(digest: DigestData) -> dict | None:
    parts = []
    if digest.sector_clusters:
        cluster_str = ", ".join(f"{c} {s}" for s, c in digest.sector_clusters[:3])
        parts.append(f":chart_with_upwards_trend: Sector clusters this week: {cluster_str}")
    if digest.peak_week_start and digest.peak_week_count > (len(digest.tier1_week) + len(digest.tier2_week)):
        parts.append(
            f":calendar: Peak tracked week begins {_fmt_date_safe(digest.peak_week_start.isoformat())} "
            f"({digest.peak_week_count} Tier 1+2 names)"
        )
    if not parts:
        return None
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "\n".join(parts)}],
    }


def build_slack_blocks(digest: DigestData) -> list[dict]:
    week_end_str = _fmt_date_safe(digest.week_end.isoformat())
    header_text = (
        f"Earnings week of {_fmt_date_safe(digest.week_start.isoformat())} — "
        f"{digest.week_count} releases"
    )
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text[:150]},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Through {week_end_str} · "
                        f"{digest.month_tracked_count} tracked names in next 30 days"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]

    for block in (
        _slack_tier_block("Core Watchlist", digest.tier1_week, show_company=True),
        _slack_tier_block("HC Services + MedTech", digest.tier2_week, show_company=True),
        _slack_tier_block("Other", digest.tier3_week, show_company=True),
    ):
        if block:
            blocks.append(block)

    ctx = _slack_context_block(digest)
    if ctx:
        blocks.append({"type": "divider"})
        blocks.append(ctx)

    if len(blocks) > SLACK_MAX_BLOCKS:
        blocks = blocks[:SLACK_MAX_BLOCKS]
    return blocks


def post_slack(webhook_url: str, blocks: list[dict], fallback_text: str) -> None:
    """POST a Block Kit message. Raises NotificationError on failure."""
    payload = {"text": fallback_text, "blocks": blocks}
    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=SLACK_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise NotificationError(f"Slack POST failed: {exc}") from exc

    if resp.status_code != 200 or resp.text.strip() != "ok":
        raise NotificationError(
            f"Slack webhook returned {resp.status_code}: {resp.text[:200]}"
        )
    logger.info("Slack digest posted successfully")


# ---------------------------------------------------------------------------
# Email rendering
# ---------------------------------------------------------------------------


def _row_table_html(rows: list[EventRow], show_estimates: bool) -> str:
    if not rows:
        return "<p><em>None</em></p>"
    header_cols = "<th>Ticker</th><th>Company</th><th>Date</th><th>Timing</th>"
    if show_estimates:
        header_cols += "<th>EPS Est</th><th>Rev Est</th>"
    cells = []
    for r in rows:
        row_cells = (
            f"<td><code>{html.escape(r.ticker)}</code></td>"
            f"<td>{html.escape(r.company_name or '')}</td>"
            f"<td>{html.escape(_fmt_date_safe(r.event_date))}</td>"
            f"<td>{_timing_short(r.event_hour)}</td>"
        )
        if show_estimates:
            row_cells += (
                f"<td>{_fmt_estimate_eps(r.eps_estimate)}</td>"
                f"<td>{_fmt_estimate_rev(r.rev_estimate)}</td>"
            )
        cells.append(f"<tr>{row_cells}</tr>")
    return (
        "<table border='1' cellpadding='6' cellspacing='0' "
        "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px'>"
        f"<thead><tr>{header_cols}</tr></thead>"
        f"<tbody>{''.join(cells)}</tbody></table>"
    )


def build_email_html(digest: DigestData) -> str:
    week_start = _fmt_date_safe(digest.week_start.isoformat())
    week_end = _fmt_date_safe(digest.week_end.isoformat())

    parts: list[str] = [
        "<div style='font-family:Arial,sans-serif;max-width:760px'>",
        f"<h2 style='margin-bottom:4px'>Earnings week of {week_start}</h2>",
        f"<p style='color:#555;margin-top:0'>Through {week_end} · "
        f"{digest.week_count} releases · "
        f"{digest.month_tracked_count} tracked names in next 30 days</p>",
    ]

    if digest.sector_clusters:
        cluster_str = ", ".join(
            f"<b>{c} {html.escape(s)}</b>" for s, c in digest.sector_clusters[:5]
        )
        parts.append(f"<p>Sector clusters this week: {cluster_str}</p>")

    if digest.peak_week_start and digest.peak_week_count > (len(digest.tier1_week) + len(digest.tier2_week)):
        parts.append(
            f"<p>Peak tracked week begins <b>{_fmt_date_safe(digest.peak_week_start.isoformat())}</b> "
            f"({digest.peak_week_count} Tier 1+2 names reporting)</p>"
        )

    parts.append(f"<h3>Core Watchlist ({len(digest.tier1_week)})</h3>")
    parts.append(_row_table_html(digest.tier1_week, show_estimates=True))

    parts.append(f"<h3>HC Services + MedTech ({len(digest.tier2_week)})</h3>")
    parts.append(_row_table_html(digest.tier2_week, show_estimates=True))

    if digest.tier3_week:
        ticker_list = ", ".join(
            f"<code>{html.escape(r.ticker)}</code>" for r in digest.tier3_week
        )
        parts.append(f"<h3>Other ({len(digest.tier3_week)})</h3>")
        parts.append(f"<p style='font-size:13px;line-height:1.6'>{ticker_list}</p>")

    parts.append("</div>")
    return "".join(parts)


def build_email_text(digest: DigestData) -> str:
    lines = [
        f"Earnings week of {_fmt_date_safe(digest.week_start.isoformat())} "
        f"through {_fmt_date_safe(digest.week_end.isoformat())}",
        f"{digest.week_count} releases · "
        f"{digest.month_tracked_count} tracked names in next 30 days",
        "",
    ]
    if digest.sector_clusters:
        lines.append(
            "Sector clusters this week: "
            + ", ".join(f"{c} {s}" for s, c in digest.sector_clusters[:5])
        )
    if digest.peak_week_start and digest.peak_week_count > (len(digest.tier1_week) + len(digest.tier2_week)):
        lines.append(
            f"Peak tracked week begins {_fmt_date_safe(digest.peak_week_start.isoformat())} "
            f"({digest.peak_week_count} Tier 1+2 names reporting)"
        )
    lines.append("")

    def _dump(label: str, rows: list[EventRow], with_est: bool):
        lines.append(f"{label} ({len(rows)})")
        if not rows:
            lines.append("  (none)")
            return
        for r in rows:
            est = ""
            if with_est and r.eps_estimate is not None:
                est = f" · EPS {_fmt_estimate_eps(r.eps_estimate)}"
            lines.append(
                f"  {r.ticker:<6} {_fmt_date_safe(r.event_date):<12} "
                f"{_timing_short(r.event_hour):<4} {r.company_name}{est}"
            )
        lines.append("")

    _dump("Core Watchlist", digest.tier1_week, with_est=True)
    _dump("HC Services + MedTech", digest.tier2_week, with_est=True)

    if digest.tier3_week:
        lines.append(f"Other ({len(digest.tier3_week)})")
        lines.append("  " + ", ".join(r.ticker for r in digest.tier3_week))
        lines.append("")

    return "\n".join(lines)


def build_slack_fallback_text(digest: DigestData) -> str:
    return (
        f"Earnings week of {_fmt_date_safe(digest.week_start.isoformat())}: "
        f"{digest.week_count} releases "
        f"({len(digest.tier1_week)} Tier 1, {len(digest.tier2_week)} Tier 2)"
    )
