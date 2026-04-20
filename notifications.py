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
from dataclasses import dataclass
from datetime import date

import requests

from digest import DigestData, EventRow
from market_data import PostEarningsMove


@dataclass
class ResultRow:
    ticker: str
    company_name: str
    event_date: str
    event_hour: str | None
    eps_actual: float | None
    eps_estimate: float | None
    rev_actual: float | None
    rev_estimate: float | None
    tier: int
    move: PostEarningsMove | None = None

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


# Timing buckets in chronological order for sub-segmenting within a day.
_TIMING_BUCKETS = [
    ("bmo", "BMO"),
    ("dmh", "DMH"),
    ("amc", "AMC"),
    ("tbd", "TBD"),
]


def _timing_bucket(hour: str | None) -> str:
    if not hour:
        return "tbd"
    h = hour.lower()
    return h if h in {"bmo", "dmh", "amc"} else "tbd"


def _underline(text: str) -> str:
    """Apply Unicode combining low line (U+0332) after each char — renders as underline in Slack."""
    return "".join(c + "\u0332" for c in text)


def _fmt_ytd(pct: float | None) -> str:
    if pct is None:
        return "YTD –"
    if pct < 0:
        return f"YTD ({abs(pct):.1f}%)"
    return f"YTD +{pct:.1f}%"


_BUCKET_SORT = {"bmo": 0, "dmh": 1, "amc": 2, "tbd": 3}
_BUCKET_LABEL = {"bmo": "bmo", "dmh": "dmh", "amc": "amc", "tbd": "tbd"}


def _row_line(r: EventRow, show_company: bool) -> str:
    """Render a single event as '`TICKER` Company — YTD ±X.X% · BMO'."""
    name = f" {r.company_name}" if show_company and r.company_name else ""
    timing = _BUCKET_LABEL[_timing_bucket(r.event_hour)]
    return f"  `{r.ticker}`{name} — {_fmt_ytd(r.ytd_pct)} · {timing}"


def _slack_tier_block(
    label: str, rows: list[EventRow], show_company: bool = True
) -> dict | None:
    """Render a tier: day (underlined) → tickers with YTD + timing at end of each row."""
    if not rows:
        return None

    by_date: dict[str, list[EventRow]] = {}
    for r in rows:
        by_date.setdefault(r.event_date, []).append(r)

    lines = [f"*{label} ({len(rows)})*"]
    for iso_date in sorted(by_date.keys()):
        day_rows = sorted(
            by_date[iso_date],
            key=lambda r: (_BUCKET_SORT[_timing_bucket(r.event_hour)], r.ticker),
        )
        lines.append(f"*{_underline(_fmt_date_safe(iso_date))}*")
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
    logger.info("Slack message posted successfully")


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


# ---------------------------------------------------------------------------
# Post-earnings results
# ---------------------------------------------------------------------------


def _beat_miss_pct(actual: float | None, estimate: float | None) -> float | None:
    if actual is None or estimate is None or estimate == 0:
        return None
    return (actual - estimate) / abs(estimate) * 100


def _fmt_beat_miss(actual: float | None, estimate: float | None) -> str:
    pct = _beat_miss_pct(actual, estimate)
    if pct is None:
        return "–"
    if pct < 0:
        return f"({abs(pct):.1f}%) miss 🟥"
    return f"+{pct:.1f}% beat 🟩"


def _fmt_actual_vs_estimate_eps(actual: float | None, estimate: float | None) -> str:
    if actual is None and estimate is None:
        return "EPS –"
    a = _fmt_estimate_eps(actual) if actual is not None else "–"
    e = _fmt_estimate_eps(estimate) if estimate is not None else "–"
    return f"EPS {a} vs {e} est · {_fmt_beat_miss(actual, estimate)}"


def _fmt_actual_vs_estimate_rev(actual: float | None, estimate: float | None) -> str:
    if actual is None and estimate is None:
        return "Rev –"
    a = _fmt_estimate_rev(actual) if actual is not None else "–"
    e = _fmt_estimate_rev(estimate) if estimate is not None else "–"
    return f"Rev {a} vs {e} est · {_fmt_beat_miss(actual, estimate)}"


def _fmt_move(move: PostEarningsMove | None) -> str:
    if move is None:
        return "Stock: reaction pending"
    if move.move_pct < 0:
        pct_str = f"({abs(move.move_pct):.1f}%)"
    else:
        pct_str = f"+{move.move_pct:.1f}%"
    return f"Stock: {pct_str} ({move.window_label})"


def _results_tier_label(tier: int) -> str:
    return {1: "Core Watchlist", 2: "HC Services + MedTech"}.get(tier, "Other")


def _results_result_block(r: ResultRow) -> dict:
    header = f"*`{r.ticker}`*"
    if r.company_name:
        header += f" · {r.company_name}"
    header += f"  _{_results_tier_label(r.tier)}_"

    lines = [
        header,
        _fmt_actual_vs_estimate_eps(r.eps_actual, r.eps_estimate),
        _fmt_actual_vs_estimate_rev(r.rev_actual, r.rev_estimate),
        _fmt_move(r.move),
    ]
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(lines)[:3000]},
    }


def build_results_slack_blocks(results: list[ResultRow], as_of: date) -> list[dict]:
    header_text = f"Earnings Results — {_fmt_date_safe(as_of.isoformat())}"
    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": header_text[:150]}},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{len(results)} reported",
                }
            ],
        },
        {"type": "divider"},
    ]
    # Sort: Tier 1 first, then Tier 2, then alpha by ticker
    ordered = sorted(results, key=lambda r: (r.tier, r.ticker))
    for r in ordered:
        blocks.append(_results_result_block(r))
        if len(blocks) >= SLACK_MAX_BLOCKS:
            break
    return blocks


def build_results_fallback_text(results: list[ResultRow], as_of: date) -> str:
    beats = sum(
        1 for r in results
        if (_beat_miss_pct(r.eps_actual, r.eps_estimate) or 0) >= 0
    )
    misses = len(results) - beats
    return (
        f"Earnings results {_fmt_date_safe(as_of.isoformat())}: "
        f"{len(results)} reported ({beats} beat, {misses} miss on EPS)"
    )


# ---------------------------------------------------------------------------
# Reconcile drift summary
# ---------------------------------------------------------------------------


@dataclass
class DriftRow:
    ticker: str
    old_date: str
    new_date: str
    hour: str | None
    tier: int


def _days_until(date_str: str, as_of: date) -> int | None:
    try:
        return (date.fromisoformat(date_str) - as_of).days
    except ValueError:
        return None


def build_reconcile_blocks(fixed: list[DriftRow], as_of: date) -> list[dict]:
    """Slack Block Kit message summarizing drift that was detected + fixed."""
    lines = []
    # Show urgent (within 5 business days) first
    def _sort_key(r: DriftRow) -> tuple[int, str]:
        days = _days_until(r.new_date, as_of) or 999
        return (days, r.ticker)

    for r in sorted(fixed, key=_sort_key):
        timing = _timing_short(r.hour)
        days = _days_until(r.new_date, as_of)
        urgency = " :warning:" if days is not None and days <= 5 else ""
        lines.append(
            f"• `{r.ticker}` (T{r.tier}): "
            f"{_fmt_date_safe(r.old_date)} → *{_fmt_date_safe(r.new_date)}* "
            f"({timing}){urgency}"
        )

    header_text = (
        f":calendar: Date changes detected — {len(fixed)} event"
        f"{'s' if len(fixed) != 1 else ''} updated"
    )
    return [
        {"type": "header", "text": {"type": "plain_text", "text": header_text}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Reconciled {_fmt_date_safe(as_of.isoformat())}  ·  "
                        f":warning: = within 5 business days"
                    ),
                }
            ],
        },
    ]


def build_reconcile_fallback(fixed: list[DriftRow], as_of: date) -> str:
    return (
        f"{len(fixed)} earnings date change(s) detected and calendar updated "
        f"on {_fmt_date_safe(as_of.isoformat())}"
    )


# ---------------------------------------------------------------------------
# Cross-check disagreement (B1: Finnhub vs yfinance)
# ---------------------------------------------------------------------------


@dataclass
class DisagreementRow:
    ticker: str
    company_name: str
    finnhub_date: str
    yf_dates: list  # list[date] from yfinance
    tier: int


def _fmt_yf_dates(yf_dates: list) -> str:
    """Render yfinance date(s) as a compact string: 'Apr 21' or 'Apr 20-24'."""
    if not yf_dates:
        return "–"
    if len(yf_dates) == 1:
        return _fmt_date_safe(yf_dates[0].isoformat())
    lo, hi = min(yf_dates), max(yf_dates)
    return f"{_fmt_date_safe(lo.isoformat())}–{_fmt_date_safe(hi.isoformat())}"


def _disagreement_lines(rows: list[DisagreementRow]) -> str:
    lines = []
    for r in sorted(rows, key=lambda x: (x.finnhub_date, x.ticker)):
        co = f" — {r.company_name}" if r.company_name else ""
        lines.append(
            f"• `{r.ticker}`{co}: Finnhub {_fmt_date_safe(r.finnhub_date)} "
            f"·  yfinance {_fmt_yf_dates(r.yf_dates)}"
        )
    return "\n".join(lines)


def build_crosscheck_blocks(rows: list[DisagreementRow], as_of: date) -> list[dict]:
    """Slack message for Finnhub/yfinance source disagreements."""
    t1 = [r for r in rows if r.tier == 1]
    t2 = [r for r in rows if r.tier == 2]

    header = (
        f":mag: Source disagreement: Finnhub vs yfinance "
        f"({len(rows)} event{'s' if len(rows) != 1 else ''})"
    )
    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
    ]

    if t1:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":warning: *Tier 1* ({len(t1)})\n{_disagreement_lines(t1)}"},
        })
    if t2:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Tier 2* ({len(t2)})\n{_disagreement_lines(t2)}"},
        })

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                "Finnhub date was applied automatically. Verify on IR page — "
                "if Finnhub is wrong, run `--lock TICKER:YYYY-MM-DD` to pin the "
                "correct date."
            ),
        }],
    })
    return blocks


def build_crosscheck_fallback(rows: list[DisagreementRow], as_of: date) -> str:
    return (
        f"{len(rows)} Finnhub/yfinance disagreement(s) on upcoming earnings dates "
        f"— verify on IR page"
    )


# ---------------------------------------------------------------------------
# Unseen-ticker alert (B2)
# ---------------------------------------------------------------------------


@dataclass
class UnseenRow:
    ticker: str
    company_name: str
    event_date: str
    tier: int
    miss_count: int


def build_unseen_blocks(rows: list[UnseenRow], as_of: date) -> list[dict]:
    """Slack message for Tier 1/2 events persistently missing from Finnhub."""
    # Sort: Tier 1 before Tier 2, then by soonest event_date, then ticker
    def _sort_key(r: UnseenRow) -> tuple[int, str, str]:
        return (r.tier, r.event_date, r.ticker)

    lines = []
    for r in sorted(rows, key=_sort_key):
        co = f" — {r.company_name}" if r.company_name else ""
        lines.append(
            f"• `{r.ticker}` (T{r.tier}){co} · expected "
            f"{_fmt_date_safe(r.event_date)} · missed {r.miss_count} runs"
        )

    header = (
        f":warning: Tier 1/2 earnings missing from Finnhub "
        f"({len(rows)} event{'s' if len(rows) != 1 else ''})"
    )
    help_text = (
        "Finnhub hasn't returned these events for 2+ consecutive daily syncs. "
        "Possible date move or coverage drop — verify on the IR page."
    )
    return [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": help_text}]},
    ]


def build_unseen_fallback(rows: list[UnseenRow], as_of: date) -> str:
    return (
        f"{len(rows)} Tier 1/2 earnings event(s) missing from Finnhub "
        f"for 2+ runs — verify on IR pages"
    )


# ---------------------------------------------------------------------------
# Workflow heartbeat
# ---------------------------------------------------------------------------


def post_heartbeat(
    webhook_url: str,
    run_name: str,
    stats: dict[str, object],
    *,
    duration_sec: float | None = None,
) -> None:
    """
    Post a compact success heartbeat to Slack.

    run_name: short label shown in the heartbeat header (e.g. "Daily sync").
    stats: ordered dict of label -> value. None/0 values still render so the
        reader can confirm a zero came from the run, not a missing field.
    """
    parts = [f"{label}: {value}" for label, value in stats.items()]
    if duration_sec is not None:
        parts.append(f"{duration_sec:.1f}s")
    detail = "  ·  ".join(parts) if parts else "ok"

    today = date.today().isoformat()
    blocks = [
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f":white_check_mark: *{run_name}* — {today}   ·   {detail}",
                }
            ],
        }
    ]
    fallback = f"{run_name} OK · {today} · {detail}"

    try:
        post_slack(webhook_url, blocks, fallback)
    except NotificationError as exc:
        # Heartbeat failures must not break the calling workflow.
        logger.warning(f"Heartbeat post failed for {run_name!r}: {exc}")
