"""
Slack rendering for the portfolio earnings-season progress + forward calendar.

Split out of ``notifications.py`` (already 1670 lines) rather than grown into
it, mirroring the ``digest.py`` / ``notifications.py`` data-vs-render split at
the season-lane level.

**The tables are fixed-width**, matching the convention JP asked for on the
post-earnings-movers digest 2026-08-04. Three rules there are load-bearing and
are re-implemented here rather than imported, because that project is a
separate repo with no remote and CI has no path to it:

  1. **Column widths are computed once across a whole table.** Per-chunk widths
     turn one table into several unrelated ones the moment it splits.
  2. **An all-empty column is dropped; a blank cell in a kept column means
     *not measurable*, never zero.** A reaction that has not resolved yet is
     blank plus an explicit note, never ``0.0%``.
  3. **Truncation drops whole ROWS with a stated notice.** A mid-string cut
     leaves the code fence open and renders the rest of the digest as code.

The key that explains the columns appears **once**, at the end, one note per
line — repeating it under every table is what made the movers digest unreadable.
"""

from __future__ import annotations

from datetime import date, timedelta

from notifications import _short_company_name
from season_progress import SeasonProgress, SeasonRow

# Slack's hard section cap is 3000 characters; leave headroom for the fence and
# the header line.
_SECTION_MAX = 2800

_HOUR_LABEL = {"bmo": "BMO", "amc": "AMC", "dmh": "DMH"}


# ---------------------------------------------------------------------------
# Fixed-width table primitives
# ---------------------------------------------------------------------------


def render_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Render an aligned fixed-width table as a list of lines.

    Columns that are empty in every row are dropped — carrying a column of
    blanks implies we tried and failed to measure something for each row, which
    is a different (and false) claim from "this measure does not apply here".
    """
    if not rows:
        return []

    keep = [
        i for i in range(len(headers))
        if any((r[i] or "").strip() for r in rows)
    ]
    if not keep:
        return []

    hdr = [headers[i] for i in keep]
    body = [[(r[i] or "") for i in keep] for r in rows]

    # Rule 1: one width map for the whole table, computed before any splitting.
    widths = [
        max(len(hdr[c]), max(len(r[c]) for r in body))
        for c in range(len(hdr))
    ]

    def line(cells: list[str]) -> str:
        return "  ".join(c.ljust(widths[i]) for i, c in enumerate(cells)).rstrip()

    out = [line(hdr), "  ".join("-" * w for w in widths)]
    out.extend(line(r) for r in body)
    return out


def _fenced_sections(title: str, lines: list[str]) -> list[dict]:
    """Wrap table lines in one or more fenced section blocks.

    Rule 3: splits only ever happen BETWEEN whole lines, and a continued chunk
    says so, so a reader never mistakes a split for the end of the table.
    """
    if not lines:
        return []

    blocks: list[dict] = []
    header, sep, body = lines[0], lines[1], lines[2:]
    chunk: list[str] = []
    part = 0

    def flush(final: bool) -> None:
        nonlocal chunk, part
        if not chunk:
            return
        part += 1
        label = title if part == 1 else f"{title} (cont.)"
        text = "*{}*\n```\n{}\n```".format(
            label, "\n".join([header, sep] + chunk)
        )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        chunk = []

    overhead = len(title) + len(header) + len(sep) + 40
    for row in body:
        if chunk and overhead + sum(len(x) + 1 for x in chunk) + len(row) > _SECTION_MAX:
            flush(False)
        chunk.append(row)
    flush(True)
    return blocks


# ---------------------------------------------------------------------------
# Cell formatters
# ---------------------------------------------------------------------------


def _signed(v: float, decimals: int) -> str:
    """Signed number with no negative zero.

    ``f"{-0.004:+.1f}"`` is ``-0.0`` and ``f"{-0.4:+.0f}"`` is ``-0`` — both
    render a value that rounded away to nothing as though its direction still
    mattered. BSX closed -0.004% on its print and the table said ``-0.0%``,
    which reads as a (tiny) decline rather than as flat.
    """
    rounded = round(v, decimals)
    if rounded == 0:
        return f"{abs(rounded):.{decimals}f}"
    return f"{rounded:+.{decimals}f}"


def _fmt_pct(v: float | None, decimals: int = 1) -> str:
    """Blank when unmeasurable — never 0.0, which is a real reading."""
    return "" if v is None else f"{_signed(v, decimals)}%"


def _fmt_sigma(v: float | None) -> str:
    return "" if v is None else _signed(v, 1)


def _fmt_when(row: SeasonRow) -> str:
    if not row.event_date:
        return ""
    d = date.fromisoformat(row.event_date)
    hour = _HOUR_LABEL.get((row.event_hour or "").lower(), "")
    return f"{d.strftime('%a %m-%d')} {hour}".strip()


def _day(d: date) -> str:
    """`Aug 3`, without the platform-specific no-pad directive.

    ``%-d`` is glibc-only and ``%#d`` is Windows-only; this lane is authored on
    Windows and runs on ubuntu CI, so either literal would work in exactly one
    of the two places it has to work.
    """
    return f"{d.strftime('%b')} {d.day}"


def _marker(row: SeasonRow) -> str:
    """A compact direction+magnitude flag so the table scans without reading
    numbers. Doubled when the move cleared 2 sigma. A dot (not a blank) marks
    a row whose reaction is genuinely unmeasurable, so the column is never
    all-empty and silently dropped.

    ASCII only: these cells sit inside a monospace fence where a wide or
    ambiguous-width glyph breaks the column alignment, and the fleet has
    already lost a run to cp1252 on a Windows console. See
    ``feedback_ascii_console_sanitize_data``.
    """
    if row.move_pct is None:
        return "."
    big = row.sigma is not None and abs(row.sigma) >= 2
    glyph = "+" if row.move_pct > 0 else "-"
    return glyph * 2 if big else glyph


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

_REPORTED_HEADERS = [
    "", "TICKER", "COMPANY", "REPORTED", "EPS SURP", "MOVE", "SIGMA", "vs SPY", "LIST",
]


def reported_rows(rows: list[SeasonRow]) -> list[list[str]]:
    out = []
    for r in rows:
        out.append([
            _marker(r),
            r.ticker,
            _short_company_name(r.company_name)[:22],
            _fmt_when(r),
            _fmt_pct(r.eps_surprise_pct, 0),
            _fmt_pct(r.move_pct),
            _fmt_sigma(r.sigma),
            _fmt_pct(r.rel_pct),
            "P" if r.position == "Portfolio" else "R",
        ])
    return out


_UPCOMING_HEADERS = [
    "TICKER", "COMPANY", "EXPECTED", "STATUS", "IN", "LIST",
]


def upcoming_rows(rows: list[SeasonRow], as_of: date) -> list[list[str]]:
    out = []
    for r in rows:
        if r.event_date:
            days = (date.fromisoformat(r.event_date) - as_of).days
            in_ = "today" if days == 0 else (f"{days}d" if days > 0 else f"{-days}d ago")
        else:
            in_ = ""
        out.append([
            r.ticker,
            _short_company_name(r.company_name)[:22],
            _fmt_when(r) or "no date",
            r.status,
            in_,
            "P" if r.position == "Portfolio" else "R",
        ])
    return out


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------


def _funnel_line(p: SeasonProgress) -> str:
    if p.scheduled == 0:
        return (
            f":bar_chart: *{p.season}* — no scheduled reports yet for the "
            f"{p.in_scope} name(s) in Portfolio + Researching."
        )
        # An empty season is stated explicitly rather than rendered as 0%,
        # which would read as "nobody has reported" instead of "nothing is
        # scheduled".
    parts = [
        f"*{len(p.reported)} of {p.scheduled} reported ({p.pct_reported}%)*",
        f"{len(p.upcoming)} still to come",
    ]
    if p.overdue:
        parts.append(f":warning: {len(p.overdue)} past-due with no results")
    if p.no_date:
        parts.append(f"{len(p.no_date)} with no date yet")
    return f":bar_chart: *{p.season} season — Portfolio + Researching* · " + " · ".join(parts)


SEASON_PAGE_URL = "https://jroypeterson.github.io/earnings-agent/season.html"


def _page_link_note(p: SeasonProgress) -> str:
    """Point at the full page from every card.

    JP 2026-08-10 asked for *"a notification that the table is updating"*. A
    post fired on every page rebuild would be noise — the page rebuilds daily
    whether or not anything happened. The card already fires exactly when
    something DID happen, so the honest version is to make the card carry the
    link: the notification and the thing it is notifying about arrive together.
    Also removes the need to remember which channel holds the bookmark.
    """
    return (
        f":bar_chart: Full season table — every one of the {p.in_scope} names, "
        f"sortable and untruncated: <{SEASON_PAGE_URL}|season progress page>. "
        f"Rebuilt each morning from the same data as this card."
    )


def build_reported_ping(rows: list[SeasonRow], p: SeasonProgress) -> list[dict]:
    """The terse #portfolio notification: *"RPD reported"*, then the link.

    JP 2026-08-10: *"I want you to tell me in the portfolio channel when
    earnings are reported for a company in the portfolio or researching. You
    could just say something like RPD reported and then flag the bookmark."*

    So this is deliberately NOT the season table. #portfolio is a P&L channel,
    the full standings already post to #earnings, and the ask was explicitly for
    the short form. It carries the company name (a bare ticker is unreadable at
    a glance for 67 names), whether it printed before or after the close — which
    is what says whether the reaction is knowable yet — and nothing else.

    The move is deliberately absent even when it IS known: putting a number here
    makes this a second, thinner results card competing with the real one, and
    the number changes the moment an AMC name's next close lands.
    """
    if not rows:
        return []

    lines = []
    for r in rows:
        when = {"bmo": "before the open", "amc": "after the close",
                "dmh": "during the session"}.get((r.event_hour or "").lower())
        name = _short_company_name(r.company_name)
        tail = f" _{when}_" if when else ""
        lines.append(f"•  `{r.ticker}`  {name} reported{tail}  · _{r.position}_")

    head = (
        f":mega: *{len(rows)} of your names reported*"
        if len(rows) > 1
        else ":mega: *A name you follow reported*"
    )

    return [
        {"type": "section", "text": {"type": "mrkdwn",
                                     "text": head + "\n" + "\n".join(lines)}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": (
            f"{len(p.reported)} of {p.scheduled} reported this season · "
            f"{len(p.upcoming)} still to come — "
            f"<{SEASON_PAGE_URL}|full table> "
            f"(also the *Earnings in Season Progress* bookmark above). "
            f"Beat/miss and the stock reaction post to <#C0AT6UNGJ5V|earnings>."
        )}]},
    ]


def _denominator_note(p: SeasonProgress) -> str:
    note = (
        f"Denominator = {p.scheduled} name(s) with a scheduled {p.season} date. "
    )
    if p.no_date:
        names = ", ".join(r.ticker for r in p.no_date)
        note += (
            f"{len(p.no_date)} in-scope name(s) have no {p.season} date at all "
            f"({names}) and are EXCLUDED from the percentage rather than counted "
            f"as unreported."
        )
    return note


def _reaction_gap_note(rows: list[SeasonRow]) -> str:
    """Say why a reaction is missing, rather than letting the columns vanish.

    An all-blank reaction column is DROPPED by ``render_table``, so on a card
    whose only reporter printed AMC this evening the MOVE / SIGMA / vs SPY
    columns disappear entirely and the card silently looks like it never
    measured anything. Naming the tickers and the reason turns an absence into
    a stated, temporary fact — and an AMC name's real move arrives on the next
    card, because the watermark settles on the reaction, not on first sighting.
    """
    pending = [r for r in rows if r.move_pct is None and r.reaction_note]
    if not pending:
        return ""

    by_reason: dict[str, list[str]] = {}
    for r in pending:
        by_reason.setdefault(r.reaction_note, []).append(r.ticker)

    parts = [
        f"{', '.join(f'`{t}`' for t in sorted(tickers))} — {reason}"
        for reason, tickers in sorted(by_reason.items())
    ]
    return ":hourglass: *Reaction not yet measurable:* " + " · ".join(parts)


def _key_block() -> dict:
    """Rule: the key appears exactly once, at the end, one note per line."""
    notes = [
        "*Reading the table*",
        "`MOVE` — the post-earnings reaction. BMO/DMH prints are measured from the PRIOR "
        "session's close to the report-day close; AMC prints from the report-day close to "
        "the NEXT session's close.",
        "`SIGMA` — that move in the stock's own trailing 252-day daily standard deviation, "
        "the reaction day excluded. `!!` marks |sigma| >= 2.",
        "`vs SPY` — the same move net of SPY over the identical window. Separates a real "
        "reaction from a tape move.",
        "`EPS SURP` — surprise versus consensus, not growth. Blank when consensus is "
        "missing or non-positive (a percentage against a zero or negative estimate is "
        "not meaningful).",
        "A blank cell means *not measurable*, never zero.",
        "`LIST` — `P` = Portfolio, `R` = Researching.",
    ]
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "\n".join(notes)}],
    }


def build_progress_blocks(
    p: SeasonProgress, fresh_only: bool = False
) -> list[dict]:
    """The progress card.

    ``fresh_only`` renders the weekday version: the names that reported since
    the last post in full detail, plus the standings summary and what is still
    outstanding — rather than reprinting all 50 season-to-date rows every night.
    The Sunday version prints the full reported table.
    """
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Earnings season progress"
                + (" — new results" if fresh_only else ""),
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": _funnel_line(p)}},
    ]

    if p.scheduled == 0:
        blocks.append(
            {"type": "context", "elements": [{"type": "mrkdwn", "text": _denominator_note(p)}]}
        )
        return blocks

    if fresh_only:
        fresh = [r for r in p.reported if r.ticker in p.fresh]
        blocks.extend(
            _fenced_sections(
                f"Reported since the last update ({len(fresh)})",
                render_table(_REPORTED_HEADERS, reported_rows(fresh)),
            )
        )
    else:
        # Most recent first — the triage question is "what just happened".
        ordered = sorted(
            p.reported, key=lambda r: (r.event_date or "", r.ticker), reverse=True
        )
        blocks.extend(
            _fenced_sections(
                f"Reported this season ({len(ordered)})",
                render_table(_REPORTED_HEADERS, reported_rows(ordered)),
            )
        )

    shown = (
        [r for r in p.reported if r.ticker in p.fresh] if fresh_only else p.reported
    )
    note = _reaction_gap_note(shown)
    if note:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": note}]})

    if p.overdue:
        blocks.extend(
            _fenced_sections(
                f":warning: Past-due, no results ({len(p.overdue)})",
                render_table(_UPCOMING_HEADERS, upcoming_rows(p.overdue, p.as_of)),
            )
        )

    blocks.extend(
        _fenced_sections(
            f"Still to report ({len(p.upcoming)})",
            render_table(_UPCOMING_HEADERS, upcoming_rows(p.upcoming, p.as_of)),
        )
    )

    blocks.append(_key_block())
    blocks.append(
        {"type": "context", "elements": [{"type": "mrkdwn", "text": _denominator_note(p)}]}
    )
    blocks.append(
        {"type": "context", "elements": [{"type": "mrkdwn", "text": _page_link_note(p)}]}
    )
    return blocks


def build_forward_calendar_blocks(p: SeasonProgress) -> list[dict]:
    """The Sunday forward calendar: the week ahead day by day, then everything
    else still outstanding this season, so 'when is the rest of it' is answered
    on the same card."""
    week_start = p.as_of + timedelta(days=1)      # Monday, when posted Sunday
    week_end = week_start + timedelta(days=6)

    this_week, later = [], []
    for r in p.upcoming:
        if not r.event_date:
            later.append(r)
            continue
        d = date.fromisoformat(r.event_date)
        (this_week if week_start <= d <= week_end else later).append(r)

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "The week ahead — earnings"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{_day(week_start)} – {_day(week_end)}* · "
                    f"*{len(this_week)}* of your names report this week · "
                    f"{len(later)} more later this season"
                    if this_week
                    else f"*{_day(week_start)} – {_day(week_end)}* · "
                    f"no Portfolio or Researching names report this week · "
                    f"{len(later)} still outstanding this season"
                ),
            },
        },
    ]

    if this_week:
        lines: list[str] = []
        for d_off in range(7):
            day = week_start + timedelta(days=d_off)
            on_day = [r for r in this_week if r.event_date == day.isoformat()]
            if not on_day:
                continue
            lines.append(f"*{day.strftime('%A')} {_day(day)}*")
            for r in sorted(on_day, key=lambda r: (r.position != "Portfolio", r.ticker)):
                hour = _HOUR_LABEL.get((r.event_hour or "").lower(), "time TBD")
                tag = "" if r.status in ("Confirmed", "Locked") else "  _(est.)_"
                lines.append(
                    f"   `{r.ticker}`  {_short_company_name(r.company_name)[:28]} "
                    f"· {hour} · {r.position}{tag}"
                )
            lines.append("")
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines).strip()[:2900]},
            }
        )

    if later:
        blocks.extend(
            _fenced_sections(
                f"Rest of the season ({len(later)})",
                render_table(_UPCOMING_HEADERS, upcoming_rows(later, p.as_of)),
            )
        )

    if p.no_date:
        names = ", ".join(f"`{r.ticker}`" for r in p.no_date)
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f":grey_question: No {p.season} date yet for {names} — "
                            f"not scheduled by any provider, so they are listed here "
                            f"rather than counted as upcoming."
                        ),
                    }
                ],
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "Dates marked _(est.)_ are provider projections from historical "
                        "cadence, not company-announced. Confirmed dates come from the "
                        "company (IR release, IR email, or an SEC filing)."
                    ),
                }
            ],
        }
    )
    return blocks
