"""
Reply parser for Slack thread commands (v9).

Each open question (cross-check disagreement, unseen-ticker, urgent
move) is its own thread. The user replies in-thread with a short
command; this module turns that text into a structured ParsedAction
that the caller can dispatch.

Grammar (case-insensitive; first non-blank line wins):

    lock <YYYY-MM-DD>     # pin to a specific date
    lock fh               # pin to Finnhub's date
    lock yf               # pin to yfinance's earliest candidate
    lock yf <n>           # pin to yfinance's nth candidate (1-indexed)
    confirm fh            # mark Finnhub date as company-confirmed
    wait                  # acknowledge, keep monitoring (state: monitoring)
    snooze <Nd|Nw>        # suppress re-alerts for N days/weeks
    ignore                # close permanently — never re-alert for this event
    reported              # (unseen only) mark reported=1
    ir <url>              # register IR RSS feed for this ticker
    note <text>           # attach a free-text note
    help                  # bot replies with command list
    status                # bot replies with current question state
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date

logger = logging.getLogger("earnings_agent")


# Action kinds — caller switches on these to apply effects.
ACT_LOCK = "lock"
ACT_CONFIRM_FH = "confirm_fh"
ACT_WAIT = "wait"
ACT_SNOOZE = "snooze"
ACT_IGNORE = "ignore"
ACT_REPORTED = "reported"
ACT_IR = "ir"
ACT_NOTE = "note"
ACT_HELP = "help"
ACT_STATUS = "status"
ACT_UNKNOWN = "unknown"


@dataclass
class ReplyContext:
    ticker: str
    event_date: str  # the ISO date currently stored on the event row
    kind: str        # "xcheck" | "unseen" | "urgent"
    finnhub_date: str | None = None  # for `lock fh`
    yf_dates: list[str] = field(default_factory=list)  # ISO strings for `lock yf [n]`


@dataclass
class ParsedAction:
    action: str
    payload: dict | None = None
    ack: str = ""           # short message to post in-thread on success
    error: str | None = None  # set when the input was rejected


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SNOOZE_RE = re.compile(r"^(\d+)([dw])$")
_URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)


def _first_command_line(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith(">"):
            continue
        return s
    return ""


def parse_reply(text: str, ctx: ReplyContext) -> ParsedAction:
    """Parse one reply into a structured action. Never raises."""
    line = _first_command_line(text)
    if not line:
        return ParsedAction(action=ACT_UNKNOWN, error="empty reply")

    parts = line.split()
    cmd = parts[0].lower()
    rest = parts[1:]

    if cmd == "help":
        return ParsedAction(action=ACT_HELP, ack=format_help(ctx.kind))

    if cmd == "status":
        return ParsedAction(action=ACT_STATUS)

    if cmd == "wait":
        return ParsedAction(
            action=ACT_WAIT,
            ack=":eyes: monitoring — won't re-alert until something changes.",
        )

    if cmd == "ignore":
        return ParsedAction(
            action=ACT_IGNORE,
            ack=":mute: dismissed — won't re-alert for this event.",
        )

    if cmd == "snooze":
        if not rest:
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="snooze needs a duration like `snooze 3d` or `snooze 2w`",
            )
        m = _SNOOZE_RE.match(rest[0].lower())
        if not m:
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="snooze duration must be Nd or Nw (e.g. 3d, 2w)",
            )
        n, unit = int(m.group(1)), m.group(2)
        days = n * (7 if unit == "w" else 1)
        if days < 1 or days > 60:
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="snooze must be between 1d and 60d",
            )
        return ParsedAction(
            action=ACT_SNOOZE,
            payload={"days": days},
            ack=f":zzz: snoozed {n}{unit} — will re-check after.",
        )

    if cmd == "confirm" and rest and rest[0].lower() == "fh":
        return ParsedAction(
            action=ACT_CONFIRM_FH,
            ack=":white_check_mark: marked Finnhub date as company-confirmed.",
        )

    if cmd == "reported":
        if ctx.kind != "unseen":
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="`reported` only applies to unseen-ticker alerts",
            )
        return ParsedAction(
            action=ACT_REPORTED,
            ack=":white_check_mark: marked as reported.",
        )

    if cmd == "ir":
        if not rest or not _URL_RE.match(rest[0]):
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="ir needs a URL: `ir https://ir.example.com/rss`",
            )
        url = rest[0]
        return ParsedAction(
            action=ACT_IR,
            payload={"url": url},
            ack=f":satellite_antenna: IR feed registered for `{ctx.ticker}`.",
        )

    if cmd == "note":
        body = " ".join(rest).strip()
        if not body:
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="note needs text: `note checked IR page, date is firm`",
            )
        return ParsedAction(
            action=ACT_NOTE,
            payload={"text": body},
            ack=":memo: note saved.",
        )

    if cmd == "lock":
        return _parse_lock(rest, ctx)

    return ParsedAction(
        action=ACT_UNKNOWN,
        error=f"unknown command `{cmd}` — reply `help` for the list",
    )


def _parse_lock(rest: list[str], ctx: ReplyContext) -> ParsedAction:
    if not rest:
        return ParsedAction(
            action=ACT_UNKNOWN,
            error="lock needs a date or `fh`/`yf` shortcut — try `lock 2026-05-12`",
        )
    arg = rest[0].lower()

    if arg == "fh":
        if not ctx.finnhub_date:
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="`lock fh` not available — Finnhub date not in context",
            )
        return ParsedAction(
            action=ACT_LOCK,
            payload={"date": ctx.finnhub_date},
            ack=f":lock: locked `{ctx.ticker}` → {ctx.finnhub_date} (Finnhub).",
        )

    if arg == "yf":
        if not ctx.yf_dates:
            return ParsedAction(
                action=ACT_UNKNOWN,
                error="`lock yf` not available — no yfinance dates in context",
            )
        if len(rest) >= 2:
            try:
                idx = int(rest[1]) - 1
            except ValueError:
                return ParsedAction(
                    action=ACT_UNKNOWN,
                    error=f"`lock yf <n>` needs an integer; got {rest[1]!r}",
                )
            if idx < 0 or idx >= len(ctx.yf_dates):
                return ParsedAction(
                    action=ACT_UNKNOWN,
                    error=(
                        f"yfinance only has {len(ctx.yf_dates)} candidate(s); "
                        f"index {idx + 1} out of range"
                    ),
                )
            chosen = ctx.yf_dates[idx]
        else:
            chosen = sorted(ctx.yf_dates)[0]
        return ParsedAction(
            action=ACT_LOCK,
            payload={"date": chosen},
            ack=f":lock: locked `{ctx.ticker}` → {chosen} (yfinance).",
        )

    if not _DATE_RE.match(arg):
        return ParsedAction(
            action=ACT_UNKNOWN,
            error=f"date must be YYYY-MM-DD; got {arg!r}",
        )
    try:
        date.fromisoformat(arg)
    except ValueError:
        return ParsedAction(
            action=ACT_UNKNOWN,
            error=f"invalid date {arg!r}",
        )
    return ParsedAction(
        action=ACT_LOCK,
        payload={"date": arg},
        ack=f":lock: locked `{ctx.ticker}` → {arg}.",
    )


# ---------------------------------------------------------------------------
# Help / status text
# ---------------------------------------------------------------------------


_HELP_COMMON = (
    "*Commands:*\n"
    "• `lock YYYY-MM-DD` — pin to a specific date\n"
    "• `wait` — keep monitoring, no re-alert\n"
    "• `snooze 3d` / `snooze 2w` — suppress re-alerts for a while\n"
    "• `ignore` — never re-alert for this event\n"
    "• `note <text>` — attach a free-text note\n"
    "• `help` — this message\n"
    "• `status` — current question state\n"
)

_HELP_XCHECK_EXTRA = (
    "• `lock fh` / `lock yf` / `lock yf <n>` — pin to Finnhub or "
    "yfinance candidate\n"
    "• `confirm fh` — mark Finnhub date as company-confirmed\n"
    "• `ir <url>` — register IR RSS feed for this ticker\n"
)
_HELP_UNSEEN_EXTRA = (
    "• `reported` — mark this event as reported\n"
    "• `ir <url>` — register IR RSS feed for this ticker\n"
)
_HELP_URGENT_EXTRA = (
    "• `confirm fh` — mark Finnhub date as company-confirmed\n"
)


def format_help(kind: str) -> str:
    extra = ""
    if kind == "xcheck":
        extra = _HELP_XCHECK_EXTRA
    elif kind == "unseen":
        extra = _HELP_UNSEEN_EXTRA
    elif kind == "urgent":
        extra = _HELP_URGENT_EXTRA
    return _HELP_COMMON + extra


def format_status(question_row: dict, today: date) -> str:
    """Compose a one-message status reply for a question."""
    state = question_row.get("question_state") or "open"
    first_seen = question_row.get("question_first_seen")
    age = ""
    if first_seen:
        try:
            seen = date.fromisoformat(first_seen)
            d = (today - seen).days
            age = f" · first seen {d}d ago" if d >= 1 else " · first seen today"
        except ValueError:
            pass
    snooze = question_row.get("question_snooze_until")
    snooze_part = f" · snoozed until {snooze}" if snooze else ""
    return (
        f"*Status:* `{state}`{age}{snooze_part}\n"
        f"_Reply `help` for available commands._"
    )
