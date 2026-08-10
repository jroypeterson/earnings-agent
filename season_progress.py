"""
Portfolio earnings-season progress + forward calendar.

JP 2026-08-09: *"During earnings season I want an earnings progress printed so
I know which of my companies have reported during the current earnings season
and which still need to report, and what the stock reaction was after earnings.
This is going to be a way I think about triaging as well. And then I should
also have a calendar of forward-looking earnings posted every Sunday to know
what's upcoming for the week and when the remaining things are supposed to be
reporting."*

Two surfaces, one collector:

  * **Progress** — every in-scope name's season status (reported / still to
    report), with the post-earnings reaction on the reported half. Posts on
    weekday evenings, but ONLY when a name has newly reported since the last
    post (watermark in ``kv_store``), so a quiet week is silent.
  * **Forward calendar** — the same season's unreported half, grouped by day
    for the week ahead and then by week for the rest of the season. Posts every
    Sunday unconditionally, because "nothing changed" is itself the answer the
    Sunday post exists to give.

Scope is **Position in {Portfolio, Researching}** (JP's choice 2026-08-09,
priced at 33 / 67 / 85 names for Portfolio / +Researching / all-five). A name
being researched is one whose print IS the trigger to look, which is what makes
this a triage surface rather than a P&L surface.

Why this lives in earnings_agent and not a new project: ``earnings_events.db``
is **gitignored** and exists only as a GitHub Actions artifact, so no local
clone can ever be freshened by a pull. Measured 2026-08-09 on one 7-day window,
the stale local copy was missing 167 of 467 events and had 54 more on the wrong
date. A local consumer of this data is wrong by construction; this must run in
CI where the artifact is restored. See ``feedback_ci_artifact_local_consumer_pull``.
"""

from __future__ import annotations

import io
import logging
import sqlite3
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass, field
from datetime import date, timedelta

from coverage import TickerInfo
from storage import date_to_quarter, kv_get, kv_set

logger = logging.getLogger("earnings_agent")


# JP's triage scope. Ordered — drives the sort so held names sit above
# researched ones within a day.
SCOPE_POSITIONS: tuple[str, ...] = ("Portfolio", "Researching")

# Watermark key for the daily "has anything newly reported?" gate.
_WATERMARK_KEY = "season_progress:last_reported_set"

# Separate watermark for the terse #portfolio ping. It must NOT reuse the
# settled set: that one deliberately re-surfaces an AMC name a second time when
# its reaction lands, which is right for the table and wrong for a "X reported"
# notification — JP would be told RPD reported on two consecutive evenings.
_ANNOUNCED_KEY = "season_progress:announced_set"

# Trailing window for the reaction sigma. Matches sigma-alert / PEM /
# portfolio_daily so the same number means the same thing across the fleet.
_SIGMA_LOOKBACK_DAYS = 252

# How long to keep re-showing a reported name whose reaction never resolves
# before settling it anyway. Covers a Friday AMC print whose reaction lands
# Monday, plus slack for a holiday.
_REACTION_GIVE_UP_DAYS = 5

# Benchmark for the market-relative column.
_BENCHMARK = "SPY"

# Consensus below this (in dollars per share) makes a percentage surprise a
# statement about the denominator rather than about the quarter. See
# SeasonRow.eps_surprise_pct for the ARXS case that set it.
_MIN_SURPRISE_ESTIMATE = 0.10


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass
class SeasonRow:
    """One in-scope company's state in the current reporting season."""

    ticker: str
    company_name: str
    position: str                    # "Portfolio" | "Researching"
    event_date: str | None           # None when the season has no row at all
    event_hour: str | None           # bmo / amc / dmh / "" / None
    reported: bool
    date_confirmed: bool
    date_locked: bool
    eps_estimate: float | None = None
    eps_actual: float | None = None
    rev_estimate: float | None = None
    rev_actual: float | None = None

    # Reaction — populated by attach_reactions() for reported rows only.
    move_pct: float | None = None
    sigma: float | None = None
    rel_pct: float | None = None      # move minus benchmark over the same window
    window_label: str | None = None   # e.g. "08-04 close -> 08-05 close"
    reaction_note: str | None = None  # why a reaction is absent, when it is
    # Year-to-date total return to the LATEST close (so for a reported name it
    # includes the post-earnings reaction). See _ytd_from_closes.
    ytd_pct: float | None = None

    @property
    def eps_surprise_pct(self) -> float | None:
        """Percent surprise vs consensus, or None when a percentage would not
        mean what a reader will take it to mean.

        Three cases return None rather than a number:

        * **No consensus or no actual** — nothing to compare.
        * **A zero or negative estimate** — the sign of the ratio stops tracking
          beat-vs-miss, so the number would be actively misleading.
        * **A near-zero estimate** (< ``_MIN_SURPRISE_ESTIMATE``). This is the
          one that bites in practice: ARXS reported $0.28 against a $0.0292
          consensus on 2026-07-29, which is a true and useless **+859%** sitting
          in a column where every other row is single or double digits. The
          percentage is unstable in the denominator, not informative about the
          quarter. Suppressed to blank, which this table defines everywhere as
          *not measurable* — never as zero.

        The absolute beat is still visible: ``eps_estimate`` and ``eps_actual``
        are both retained on the row.
        """
        if self.eps_actual is None or self.eps_estimate is None:
            return None
        if self.eps_estimate < _MIN_SURPRISE_ESTIMATE:
            return None
        return (self.eps_actual - self.eps_estimate) / self.eps_estimate * 100

    @property
    def status(self) -> str:
        """Reported / Confirmed / Estimated / No date.

        **Deliberately BINARY on trust** (JP 2026-08-09: *"you have a confirmed
        and locked status… aren't those redundant?"* — he was right).

        `date_confirmed` and `date_locked` are two INDEPENDENT axes, not two
        levels of one scale:

        * `date_confirmed` — the company announced the date. *Evidence.*
        * `date_locked` — the date is pinned so provider syncs cannot move it.
          *Mechanism*, set by an operator lock, a Slack `lock` reply, or a
          corroborated EDGAR 8-K 2.02 / 6-K auto-correction.

        Rendering them as an ordered precedence made them look mutually
        exclusive, so a row that was BOTH (SGRY) displayed only "Locked" and
        read as some *alternative* kind of certainty. Measured across the whole
        DB: 1,465 rows confirmed-not-locked, 15 both, and only **4**
        locked-not-confirmed — so the lock almost never carries trust
        information the confirmed flag doesn't already carry.

        The question this column answers is "can I trust this date?", which has
        two answers. The lock survives as `pinned`, an annotation, because
        "this one was pinned after a provider disagreement" is real provenance
        — just not a third trust level. Same failure as
        `feedback_two_independent_axes_read_as_one`.
        """
        if self.reported:
            return "Reported"
        if self.event_date is None:
            return "No date"
        return "Confirmed" if (self.date_confirmed or self.date_locked) else "Estimated"

    @property
    def pinned(self) -> bool:
        """The date was pinned against provider drift. Provenance detail shown
        as an annotation, never as a separate status — see `status`."""
        return self.date_locked

    @property
    def rev_surprise_pct(self) -> float | None:
        """Revenue surprise vs consensus.

        Same shape as `eps_surprise_pct` but WITHOUT the near-zero floor: a
        revenue consensus is an absolute dollar figure in the millions or
        billions, so the unstable-denominator case that suppresses a 2.9-cent
        EPS estimate simply does not arise. A non-positive estimate is still
        rejected — the sign of the ratio would stop tracking beat-vs-miss.
        """
        if self.rev_actual is None or self.rev_estimate is None:
            return None
        if self.rev_estimate <= 0:
            return None
        return (self.rev_actual - self.rev_estimate) / self.rev_estimate * 100


@dataclass
class SeasonProgress:
    season: str
    as_of: date
    reported: list[SeasonRow] = field(default_factory=list)
    upcoming: list[SeasonRow] = field(default_factory=list)
    overdue: list[SeasonRow] = field(default_factory=list)
    no_date: list[SeasonRow] = field(default_factory=list)
    # Tickers that newly flipped to reported since the last post. Drives the
    # daily gate and the "new since last look" marker.
    fresh: set[str] = field(default_factory=set)

    @property
    def in_scope(self) -> int:
        return (
            len(self.reported) + len(self.upcoming)
            + len(self.overdue) + len(self.no_date)
        )

    @property
    def scheduled(self) -> int:
        """Denominator for the percentage: names with a season date. Names with
        NO date are never folded in silently — they are surfaced separately, so
        a percentage is never inflated by names that simply have no date yet.
        Same denominator discipline as storage.compute_season_stats."""
        return len(self.reported) + len(self.upcoming) + len(self.overdue)

    @property
    def pct_reported(self) -> int | None:
        if self.scheduled == 0:
            return None
        return round(len(self.reported) / self.scheduled * 100)


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


def _is_reported(row: sqlite3.Row) -> bool:
    """A name has reported once its numbers exist, even if the workflow has not
    yet flipped `reported=1`.

    `reported` is a *posting* flag: `_should_defer_post` deliberately holds it
    back for up to 3 days when the post-earnings move is not yet computable (an
    AMC print has no next-day close on the evening of the print). Keying the
    triage table on it alone would show a company that reported this morning as
    "still to report", which is the single thing this table exists to get right.
    So actuals count, and the reaction column carries the pending state instead.
    """
    return bool(
        row["reported"]
        or row["eps_actual"] is not None
        or row["rev_actual"] is not None
    )


def collect_season(
    conn: sqlite3.Connection,
    coverage_map: dict[str, TickerInfo],
    as_of: date,
    positions: tuple[str, ...] = SCOPE_POSITIONS,
) -> SeasonProgress:
    """Assemble the in-scope roster for the season ``as_of`` falls into.

    Every in-scope name lands in exactly one bucket: reported, upcoming,
    overdue (past-dated with no actuals — a date that slipped, or a print we
    missed), or no_date. Nothing is dropped: a name absent from the events
    table entirely still appears, under "no date", because "we have no date for
    ADSK" is a fact JP needs on a triage sheet and silence would hide it.
    """
    season = date_to_quarter(as_of.isoformat())
    wanted = {p.lower() for p in positions}

    scope = {
        t.upper(): info
        for t, info in coverage_map.items()
        if (info.position or "").lower() in wanted
    }
    if not scope:
        # An empty scope means the Coverage Manager read failed or the position
        # lists were renamed — never a real state (JP always holds something).
        # Return an empty progress and let the caller refuse to post; silently
        # rendering "0 of 0 reported" would look like a finished season.
        logger.error(
            "season_progress: no tickers matched positions %s — coverage read "
            "is broken or the Position vocabulary changed; refusing to build",
            ", ".join(positions),
        )
        return SeasonProgress(season=season, as_of=as_of)

    conn.row_factory = sqlite3.Row
    rows = {
        r["ticker"].upper(): r
        for r in conn.execute(
            "SELECT * FROM events WHERE quarter = ?", (season,)
        )
        if r["ticker"] and r["ticker"].upper() in scope
    }

    progress = SeasonProgress(season=season, as_of=as_of)
    today_iso = as_of.isoformat()

    for ticker, info in sorted(scope.items()):
        raw = rows.get(ticker)
        if raw is None:
            progress.no_date.append(
                SeasonRow(
                    ticker=ticker,
                    company_name=info.company_name or ticker,
                    position=info.position,
                    event_date=None,
                    event_hour=None,
                    reported=False,
                    date_confirmed=False,
                    date_locked=False,
                )
            )
            continue

        row = SeasonRow(
            ticker=ticker,
            company_name=(raw["company_name"] or info.company_name or ticker),
            position=info.position,
            event_date=raw["event_date"],
            event_hour=raw["event_hour"],
            reported=_is_reported(raw),
            date_confirmed=bool(raw["date_confirmed"]),
            date_locked=bool(raw["date_locked"]),
            eps_estimate=raw["eps_estimate"],
            eps_actual=raw["eps_actual"],
            rev_estimate=raw["rev_estimate"],
            rev_actual=raw["rev_actual"],
        )

        if row.reported:
            progress.reported.append(row)
        elif row.event_date and row.event_date < today_iso:
            progress.overdue.append(row)
        else:
            progress.upcoming.append(row)

    progress.reported.sort(key=lambda r: (r.event_date or "", r.ticker))
    progress.upcoming.sort(key=lambda r: (r.event_date or "", r.ticker))
    progress.overdue.sort(key=lambda r: (r.event_date or "", r.ticker))
    return progress


# ---------------------------------------------------------------------------
# Reaction
# ---------------------------------------------------------------------------


def _reaction_window(event_date: str, hour: str | None) -> str:
    """Which close the reaction is measured FROM.

    BMO / DMH / unknown anchor on the PRIOR session's close (the news lands
    before or during the session, so the session itself is the reaction).
    AMC anchors on the report-day close (the reaction is the next session).

    Returned as a marker the caller resolves against real trading days — the
    calendar-day arithmetic is done on the price index, never here, so weekends
    and holidays cannot shift a window.
    """
    return "amc" if (hour or "").lower() == "amc" else "bmo"


def attach_reactions(
    rows: list[SeasonRow],
    as_of: date,
    benchmark: str = _BENCHMARK,
    downloader=None,
    with_ytd: bool = False,
) -> None:
    """Populate move_pct / sigma / rel_pct on reported rows, in place.

    ONE batched download covers every ticker plus the benchmark over a window
    wide enough for both the reaction and the trailing sigma — per-ticker
    fetches would be ~50 round trips for JP's book.

    ``with_ytd`` additionally fills ``ytd_pct`` for **every** row passed in,
    reported or not, off that same frame. It is a flag rather than a second
    function so the page cannot end up issuing two downloads of overlapping
    data, and so the reaction math has exactly one implementation.

    Never raises: a ticker whose prices are unavailable keeps `None` metrics and
    gains a `reaction_note` saying so. A blank reaction cell means *not
    measurable*, never zero — the renderer relies on that distinction.
    """
    targets = [r for r in rows if r.reported and r.event_date]
    ytd_targets = [r for r in rows if r.ticker] if with_ytd else []
    if not targets and not ytd_targets:
        return

    tickers = sorted({r.ticker for r in targets} | {r.ticker for r in ytd_targets})

    # Reach back far enough for the trailing sigma AND for the first close of
    # the current year. With no reported rows yet (early season) there is no
    # earliest event to anchor on, so fall back to as_of.
    dated = [r.event_date for r in targets if r.event_date]
    anchor = date.fromisoformat(min(dated)) if dated else as_of
    start = min(
        anchor - timedelta(days=int(_SIGMA_LOOKBACK_DAYS * 1.6) + 10),
        date(as_of.year - 1, 12, 1),
    )
    end = as_of + timedelta(days=2)

    frame = _download(
        sorted(set(tickers) | {benchmark}), start, end, downloader=downloader
    )
    if frame is None:
        for r in targets:
            r.reaction_note = "price download failed"
        logger.warning(
            "season_progress: price download failed for %d ticker(s); reaction "
            "and YTD columns will render blank",
            len(tickers),
        )
        return

    if with_ytd:
        for row in ytd_targets:
            closes = _closes_for(frame, row.ticker, single=(len(tickers) == 1))
            if closes is not None:
                row.ytd_pct = _ytd_from_closes(closes, as_of)

    bench_closes = _closes_for(frame, benchmark, single=(len(tickers) == 0))
    if bench_closes is None:
        logger.warning(
            "season_progress: benchmark %s unavailable — the vs-S&P column will "
            "be blank; absolute moves are unaffected",
            benchmark,
        )

    for row in targets:
        closes = _closes_for(frame, row.ticker, single=(len(tickers) == 1))
        if closes is None or len(closes) < 2:
            row.reaction_note = "no price data"
            continue

        anchor = _reaction_window(row.event_date, row.event_hour)
        computed = _move_from_closes(closes, row.event_date, anchor)
        if computed is None:
            # The comparison close has not posted yet. For an AMC print that is
            # simply "the market has not reacted yet" — a real, temporary state
            # that must read as pending rather than as a missing number.
            row.reaction_note = (
                "reaction pending (AMC — next close not in yet)"
                if anchor == "amc"
                else "reaction pending"
            )
            continue

        move, from_ts, to_ts = computed
        row.move_pct = move
        row.window_label = (
            f"{from_ts.strftime('%m-%d')} close -> {to_ts.strftime('%m-%d')} close"
        )

        sigma = _trailing_sigma(closes, to_ts)
        if sigma:
            row.sigma = move / sigma

        if bench_closes is not None:
            bench = _move_from_closes(bench_closes, row.event_date, anchor)
            if bench is not None:
                row.rel_pct = move - bench[0]


def _download(tickers: list[str], start: date, end: date, downloader=None):
    """Batched yfinance download, silenced. Returns None on any failure."""
    if downloader is not None:
        return downloader(tickers, start, end)

    import yfinance as yf

    buf_out, buf_err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(buf_out), redirect_stderr(buf_err):
            data = yf.download(
                tickers=tickers,
                start=start.isoformat(),
                end=end.isoformat(),
                progress=False,
                auto_adjust=True,
                threads=True,
                group_by="ticker",
            )
    except Exception as exc:  # noqa: BLE001 — never break the digest on a vendor
        logger.warning("season_progress: yfinance download failed: %s", exc)
        return None
    if data is None or data.empty:
        logger.warning("season_progress: yfinance returned an empty frame")
        return None
    return data


def _closes_for(frame, ticker: str, single: bool = False):
    """Extract one ticker's close series. yfinance returns a flat frame for a
    single ticker and a MultiIndex for many — handle both, or a one-name run
    silently yields nothing."""
    try:
        if single and "Close" in frame.columns:
            closes = frame["Close"].dropna()
        else:
            closes = frame[ticker]["Close"].dropna()
    except (KeyError, IndexError, TypeError):
        return None
    if closes is None or len(closes) == 0:
        return None
    return closes


def _move_from_closes(closes, event_date: str, anchor: str):
    """Return (move_pct, from_timestamp, to_timestamp) or None if the closing
    price needed for the comparison has not posted yet.

    Positions are resolved against the actual price index, so a Friday AMC
    print correctly measures to Monday's close and a holiday cannot shift the
    window by a day.
    """
    try:
        ed = date.fromisoformat(event_date)
    except (TypeError, ValueError):
        return None

    idx = closes.index
    # Position of the event day (or the first session at/after it).
    on_or_after = [i for i, ts in enumerate(idx) if ts.date() >= ed]
    if not on_or_after:
        return None
    pos = on_or_after[0]

    if anchor == "amc":
        # Measure the session AFTER the print. If the event day itself is not a
        # session (a Saturday-dated row), pos already points at the next one.
        base_pos = pos if idx[pos].date() == ed else pos - 1
        to_pos = base_pos + 1
    else:
        base_pos = pos - 1          # prior session's close
        to_pos = pos

    if base_pos < 0 or to_pos >= len(closes) or base_pos >= len(closes):
        return None

    base = float(closes.iloc[base_pos])
    later = float(closes.iloc[to_pos])
    if base == 0:
        return None
    return (later - base) / base * 100, idx[base_pos], idx[to_pos]


def _ytd_from_closes(closes, as_of: date) -> float | None:
    """Year-to-date total return: last close of the PRIOR year -> latest close.

    Deliberately anchored on the prior year's final close rather than the first
    close of January — the January 2nd open is not the starting point of the
    year's return, and using it silently drops the first session.

    ⚠ This runs to the LATEST close, so for a company that has already
    reported it INCLUDES its post-earnings reaction. That is the right answer
    for "where does this stock stand now" (the triage question, and the only
    definition that also works for the not-yet-reported table), but it is a
    different measure from post_earnings_movers' `YTD`, which deliberately
    stops at the print so it never includes the move it is explaining. The page
    says so in prose; do not quietly switch one for the other.
    """
    prior_year_end = date(as_of.year - 1, 12, 31)
    try:
        prior = closes[closes.index <= str(prior_year_end)]
        current = closes[closes.index <= str(as_of + timedelta(days=1))]
    except TypeError:
        return None
    if len(prior) == 0 or len(current) == 0:
        return None
    base = float(prior.iloc[-1])
    last = float(current.iloc[-1])
    if base == 0:
        return None
    return (last - base) / base * 100


def _trailing_sigma(closes, reaction_ts) -> float | None:
    """Daily-return standard deviation over the trailing window, EXCLUDING the
    reaction day itself — including it lets a large move inflate the very
    denominator that is supposed to measure how large it is."""
    try:
        prior = closes[closes.index < reaction_ts]
    except TypeError:
        return None
    if len(prior) < 30:
        return None
    window = prior.iloc[-_SIGMA_LOOKBACK_DAYS:]
    returns = window.pct_change().dropna() * 100
    if len(returns) < 30:
        return None
    sd = float(returns.std())
    return sd if sd > 0 else None


# ---------------------------------------------------------------------------
# Daily gate
# ---------------------------------------------------------------------------


def _watermark_key(season: str) -> str:
    return f"{_WATERMARK_KEY}:{season}"


def select_unsettled(
    conn: sqlite3.Connection, progress: SeasonProgress
) -> set[str]:
    """Reported tickers the weekday card has not yet shown *with a resolved
    reaction*.

    A first-sighting watermark is not enough, and getting this wrong ships a
    permanently invisible column. The sequence that breaks it:

      1. A name reports AMC at 16:05 ET. The results sweep flips it at 18:37 ET
         and this card runs at 19:07 ET.
      2. Its reaction is the NEXT session's close, which does not exist yet, so
         the card can only say "pending".
      3. Under a first-sighting watermark the name is now marked seen — and the
         weekday card never shows it again. **Its actual reaction is never
         posted**, on the one surface JP reads daily.

    So the watermark stores the **settled** set: reported AND either
    reaction-resolved or aged out. An AMC name therefore appears twice — once
    the night it reports ("reaction pending"), once the night the move lands —
    and never again.

    A season's first run settles everything silently: announcing fifty names as
    "just reported" would be false, and it mirrors the first-run rule the
    coverage snapshot in main.py already uses.
    """
    key = _watermark_key(progress.season)
    raw = kv_get(conn, key)
    current = {r.ticker for r in progress.reported}

    if raw is None:
        logger.info(
            "season_progress: %s watermark absent — first run will settle all "
            "%d reported name(s) silently; no 'new' claims on a first run",
            progress.season, len(current),
        )
        return set()

    settled = {t for t in raw.split(",") if t}
    return current - settled


def mark_settled(
    conn: sqlite3.Connection, progress: SeasonProgress, as_of: date
) -> None:
    """Advance the watermark to every reported name whose reaction has landed —
    or whose reaction is never going to.

    Called only AFTER a successful post (or on a first-run seed), so a Slack
    failure re-announces rather than silently swallowing a night's reporters.

    A name whose reaction cannot be computed at all — delisted, or a vendor gap
    — would otherwise reappear on the card every single night forever. It is
    settled once its event is `_REACTION_GIVE_UP_DAYS` old, with the card
    having said plainly that the reaction is unavailable. Same shape as the
    3-day cap on `_should_defer_post`.
    """
    key = _watermark_key(progress.season)
    settled = {t for t in (kv_get(conn, key) or "").split(",") if t}

    for row in progress.reported:
        if row.move_pct is not None:
            settled.add(row.ticker)
            continue
        if not row.event_date:
            continue
        age = (as_of - date.fromisoformat(row.event_date)).days
        if age >= _REACTION_GIVE_UP_DAYS:
            logger.warning(
                "season_progress: %s reported %s but no reaction after %d days "
                "(%s) — settling it so the card stops repeating; the reaction "
                "column stays blank rather than showing a number we do not have",
                row.ticker, row.event_date, age, row.reaction_note or "no note",
            )
            settled.add(row.ticker)

    kv_set(conn, key, ",".join(sorted(settled)))


def seed_watermark(conn: sqlite3.Connection, progress: SeasonProgress) -> None:
    """Settle every currently-reported name without announcing any of them.
    Used on a season's first run."""
    kv_set(
        conn,
        _watermark_key(progress.season),
        ",".join(sorted(r.ticker for r in progress.reported)),
    )


def is_seeded(conn: sqlite3.Connection, progress: SeasonProgress) -> bool:
    return kv_get(conn, _watermark_key(progress.season)) is not None


# ---------------------------------------------------------------------------
# The terse "X reported" ping (#portfolio)
# ---------------------------------------------------------------------------


def _announced_key(season: str) -> str:
    return f"{_ANNOUNCED_KEY}:{season}"


def select_unannounced(
    conn: sqlite3.Connection, progress: SeasonProgress
) -> list[SeasonRow]:
    """Reported names that have not yet been announced to #portfolio.

    **Deliberately NOT `select_unsettled`.** That set re-surfaces an AMC name
    the evening its reaction lands, which is correct for the table (the move is
    new information) and wrong here — a bare "RPD reported" posted on two
    consecutive evenings says something false the second time.

    Ordered by report date so a batch reads chronologically.
    """
    key = _announced_key(progress.season)
    raw = kv_get(conn, key)
    already = {t for t in (raw or "").split(",") if t}

    fresh = [r for r in progress.reported if r.ticker not in already]
    fresh.sort(key=lambda r: (r.event_date or "", r.ticker))
    return fresh


def mark_announced(conn: sqlite3.Connection, progress: SeasonProgress) -> None:
    """Record every currently-reported name as announced.

    Called only AFTER a successful post, so a Slack failure re-announces rather
    than swallowing the notification — the same post-then-mark rule the results
    lane uses.

    Marks the WHOLE reported set, not just what was posted: on a season's first
    run there is nothing to announce retroactively, and folding them all in here
    is what stops fifty "X reported" pings on day one.
    """
    kv_set(
        conn,
        _announced_key(progress.season),
        ",".join(sorted(r.ticker for r in progress.reported)),
    )


def is_announce_seeded(conn: sqlite3.Connection, progress: SeasonProgress) -> bool:
    return kv_get(conn, _announced_key(progress.season)) is not None
