"""Pre-print ANNUAL Street consensus snapshots (board #298, Phase A).

The guidance-vs-consensus square compares a company's full-year guide with
the Street's full-year consensus AS IT STOOD BEFORE THE RELEASE. FMP keeps no
estimate history, and the post-print figure has already moved onto the guide
(FIVE: $9.29 pre-print vs $10.25 post-print against a $9.83-10.31 guide, which
inverts the verdict). So the only pre-print consensus that will ever exist is
one captured beforehand. This module captures it, every run, while an event is
ahead; the square (Phase B) only ever READS what was stored here.

Three public pieces:

- ``snapshot_annual_consensus(conn, today, fetcher)`` -- the write side.
- ``pre_release_cutoff(...)`` -- the instant a snapshot stops being pre-print.
- ``latest_pre_release_snapshot(conn, ticker, fiscal_period_end, cutoff)`` --
  the read side, keyed on the FISCAL PERIOD; ``event_date`` is provenance only,
  and bounded to the release's own reporting cycle (Codex round 2).

No rendering. Nothing here posts to Slack or changes a results card.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.error
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from storage import OPEN_EVENT_SQL, kv_get, kv_set

logger = logging.getLogger("earnings_agent")

ET = ZoneInfo("America/New_York")

# (today, today + 4]: both daily crons fire, so an event is snapshotted on
# ~4-5 runs. An UNCONFIRMED date is an estimate that can land earlier or later
# than projected, so its window is widened by 10 days (Fable round 1, M5).
WINDOW_DAYS = 4
UNCONFIRMED_EXTRA_DAYS = 10

# Codex round 2 (2026-09-22): an OPEN event whose cutoff has passed, dated
# within this many days before the in-window event, is the same reporting
# cycle (a locked row + a vendor duplicate, or a moved date). The company may
# already have printed on the earlier date, so the in-window fetch could be
# post-print consensus. Consecutive quarterly prints are ~91 days apart.
SAME_CYCLE_DAYS = 45

# FMP Starter allows 300/min. 4/s is 240/min with headroom for the rest of the
# run; the 429 backoff is the same schedule as notifications.post_with_retry.
FMP_RATE_PER_SEC = 4.0
# Codex round 1 (2026-09-17): a run must never approach the job limit and push
# `Save earnings database` past it. Sustained 429s cost 50 s of backoff PER
# ticker; 400 tickers would be ~5.5 h. So the run stops -- loudly -- after this
# many consecutive transient failures, or after this wall-clock budget.
MAX_CONSECUTIVE_ERRORS = 5
RUN_BUDGET_SECONDS = 15 * 60
FMP_429_BACKOFF = (5, 15, 30)

CURRENCY_CACHE_DAYS = 30
_CURRENCY_KV = "fmp_reported_currency:"

# A status-only row (empty / error) has no fiscal period. It is still written,
# so "we asked and FMP had nothing" and "we asked and FMP refused" are both on
# record, and it can never be returned as a figure: the read side filters to
# fetch_status = 'ok' and a real fiscal_period_end.
STATUS_ROW_PERIOD = ""

# Codex round 2 (2026-09-22): a read is bounded BELOW as well as above. The
# write window is at most 14 days ahead of an event, so an in-cycle capture is
# days old at the cutoff; one older than this is from an earlier cycle (or the
# captures stopped) and is not "the consensus before THIS print". Consecutive
# quarterly prints are ~91 days apart, so 30 cannot reach the previous cycle.
MAX_SNAPSHOT_AGE_DAYS = 30

_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _utc_stamp(dt: datetime) -> str:
    """Fixed-width UTC string, so `taken_at < cutoff` compares correctly as TEXT."""
    if dt.tzinfo is None:
        raise ValueError("naive datetime: a snapshot instant must carry a zone")
    return dt.astimezone(timezone.utc).strftime(_TS_FMT)


# ---------------------------------------------------------------------------
# The cutoff rule
# ---------------------------------------------------------------------------

def _norm_hour(h: Optional[str]) -> str:
    return (h or "").strip().lower()


def pre_release_cutoff(
    event_date: str,
    event_hour: Optional[str],
    event_hour_yf: Optional[str],
    date_confirmed,
) -> datetime:
    """The instant after which a snapshot may already contain the print.

    - **16:00 ET** on the event date for an AMC release.
    - **00:00 ET** on the event date for BMO / DMH / unknown hour, for an
      UNCONFIRMED date (``date_confirmed = 0``), and when ``event_hour_yf`` and
      ``event_hour`` are **both non-empty AND differ** -- a real second opinion
      that disagrees, so the safe side is taken.

    A NULL / empty ``event_hour_yf`` (about 80% of rows) is "no second opinion",
    NOT a disagreement. Reading it as one would drag nearly every AMC cutoff to
    midnight and throw away the day-of snapshot, the freshest pre-print figure.

    AMC is read from ``event_hour`` (Finnhub-canonical) only. yfinance alone
    saying "amc" over an empty Finnhub hour is not enough to move the cutoff
    later -- later is the direction that can admit a post-print figure.

    Computed with ``ZoneInfo`` so DST is right (16:00 ET is 20:00 UTC in
    September and 21:00 UTC in November). Returned as an aware UTC datetime.
    """
    d = date.fromisoformat(event_date)
    hour = _norm_hour(event_hour)
    yf_hour = _norm_hour(event_hour_yf)
    disagree = bool(hour) and bool(yf_hour) and hour != yf_hour
    if hour == "amc" and bool(date_confirmed) and not disagree:
        local = datetime.combine(d, dtime(16, 0), tzinfo=ET)
    else:
        local = datetime.combine(d, dtime(0, 0), tzinfo=ET)
    return local.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Read side
# ---------------------------------------------------------------------------

_SNAPSHOT_COLS = (
    "ticker", "fiscal_period_end", "taken_at", "event_date", "revenue_avg",
    "eps_avg", "num_analysts_revenue", "num_analysts_eps", "currency",
    "fetch_status",
)


def _cycle_floor(conn: sqlite3.Connection, ticker: str, cutoff: datetime) -> datetime:
    """The earliest instant a snapshot can belong to the release at `cutoff`.

    The later of (a) ``cutoff - MAX_SNAPSHOT_AGE_DAYS`` and (b) 00:00 ET on the
    day AFTER the ticker's most recent REPORTED event dated before this one --
    anything taken on or before a previous print day is that print's cycle.
    """
    floor = cutoff - timedelta(days=MAX_SNAPSHOT_AGE_DAYS)
    event_day = cutoff.astimezone(ET).date()
    row = conn.execute(
        "SELECT MAX(event_date) FROM events WHERE ticker = ? AND reported = 1 "
        "AND event_date < ?",
        (ticker, event_day.isoformat()),
    ).fetchone()
    if row and row[0]:
        prev_end = datetime.combine(
            date.fromisoformat(row[0]) + timedelta(days=1), dtime(0, 0), tzinfo=ET)
        floor = max(floor, prev_end.astimezone(timezone.utc))
    return floor


def latest_pre_release_snapshot(
    conn: sqlite3.Connection,
    ticker: str,
    fiscal_period_end: str,
    cutoff: datetime,
) -> Optional[dict]:
    """Newest successful snapshot for this fiscal period taken before `cutoff`
    and inside the release's own reporting cycle.

    ``event_date`` is deliberately NOT in the predicate (Fable round 1, H1):
    FMP moves dates after a snapshot is taken (FIVE 06-02 -> 05-27), and a
    snapshot taken before the move is still pre-print. Matching on it would
    discard exactly those.

    Bounded BELOW by ``_cycle_floor`` (Codex round 2, 2026-09-22): without it a
    quarter-old snapshot from before the PREVIOUS print is still "pre-cutoff"
    and would be served as this release's consensus whenever this cycle's
    captures were missed.

    A newer in-cycle ``empty`` attempt supersedes an older ``ok``: the newest
    answer FMP gave for this release is "no coverage". A newer ``error:*`` does
    NOT -- a 429 says nothing about the consensus, and the older in-cycle
    figure is still pre-print.

    Returns None when nothing qualifies -- the caller must render "no pre-print
    consensus", never fall back to a live, post-print fetch.
    """
    if not fiscal_period_end:
        return None
    floor = _utc_stamp(_cycle_floor(conn, ticker, cutoff))
    ceiling = _utc_stamp(cutoff)
    row = conn.execute(
        f"SELECT {', '.join(_SNAPSHOT_COLS)} FROM consensus_snapshot "
        "WHERE ticker = ? AND fiscal_period_end = ? AND fetch_status = 'ok' "
        "AND taken_at >= ? AND taken_at < ? ORDER BY taken_at DESC LIMIT 1",
        (ticker, fiscal_period_end, floor, ceiling),
    ).fetchone()
    if not row:
        return None
    got = dict(zip(_SNAPSHOT_COLS, row))
    superseded = conn.execute(
        "SELECT 1 FROM consensus_snapshot WHERE ticker = ? "
        "AND fetch_status = 'empty' AND taken_at > ? AND taken_at < ? LIMIT 1",
        (ticker, got["taken_at"], ceiling),
    ).fetchone()
    return None if superseded else got


# ---------------------------------------------------------------------------
# Write side
# ---------------------------------------------------------------------------

def select_snapshot_window(
    conn: sqlite3.Connection, today: date, now: Optional[datetime] = None,
) -> list[tuple[str, str]]:
    """`(ticker, event_date)` for every ticker whose release is still ahead.

    Open rows (``OPEN_EVENT_SQL``) with ``event_date`` in ``(today, today+4]``,
    widened to ``today+14`` for an unconfirmed date, plus today's AMC events.
    ALL tiers: a snapshot not taken now cannot be recovered later, so Tier 2/3
    expansion of the square must not start with no history.

    A row is kept only while ``now`` is before its pre-release cutoff, so a
    today-AMC event is dropped once 16:00 ET passes (or at once, if its hour is
    in dispute) instead of spending a call on a figure the read side would
    reject. One entry per ticker, carrying its NEAREST open event date.
    """
    now = now or datetime.now(timezone.utc)
    window, _blocked = _select_window(conn, today, now)
    return [(t, d) for t, d, _ in window]


def _select_window(conn: sqlite3.Connection, today: date, now: datetime):
    """``(window, blocked)``: the snapshot window, minus every ticker that has a
    past-cutoff OPEN event in the same reporting cycle (``blocked``, as
    ``(ticker, event_date, prior_event_date)``).

    Fail-closed rule for Codex round 2 finding 2: an operator-locked 09-17 AMC
    row plus a vendor duplicate at 09-20 -- on the 09-18 run the locked row is
    no longer in the window, and if the company printed on 09-17 a fetch for
    the 09-20 row is POST-print consensus that would be stored as pre-print.
    An open past row within ``SAME_CYCLE_DAYS`` is exactly "not yet marked
    reported / superseded", so the ticker is skipped until it is resolved.
    """
    window = _window_with_cutoffs(_open_window_rows(conn, today), now)
    if not window:
        return window, []
    lookback = (today - timedelta(days=WINDOW_DAYS + UNCONFIRMED_EXTRA_DAYS
                                  + SAME_CYCLE_DAYS)).isoformat()
    prior: dict[str, list[str]] = {}
    for ticker, d, hour, hour_yf, confirmed in conn.execute(
        "SELECT ticker, event_date, event_hour, event_hour_yf, date_confirmed "
        f"FROM events WHERE {OPEN_EVENT_SQL} AND event_date >= ? "
        "AND event_date <= ?",
        (lookback, today.isoformat()),
    ).fetchall():
        if now >= pre_release_cutoff(d, hour, hour_yf, confirmed):
            prior.setdefault(ticker, []).append(d)
    kept, blocked = [], []
    for ticker, event_date, cutoff in window:
        earliest = (date.fromisoformat(event_date)
                    - timedelta(days=SAME_CYCLE_DAYS)).isoformat()
        hits = sorted(p for p in prior.get(ticker, ())
                      if earliest <= p < event_date)
        if hits:
            blocked.append((ticker, event_date, hits[-1]))
        else:
            kept.append((ticker, event_date, cutoff))
    if blocked:
        logger.warning(
            "Consensus snapshot: %d ticker(s) NOT snapshotted -- an open, "
            "past-cutoff event in the same cycle may already have printed: %s",
            len(blocked), ", ".join(f"{t}@{d} (open {p})" for t, d, p in blocked))
    return kept, blocked


def _window_with_cutoffs(rows, now: datetime) -> list[tuple[str, str, datetime]]:
    """`(ticker, event_date, cutoff)` per ticker, nearest open event first.

    The cutoff travels with the row so the WRITE side can re-check it against
    the moment each fetch actually returned (Codex round 1, 2026-09-17).
    """
    out: dict[str, tuple[str, datetime]] = {}
    for ticker, event_date, hour, hour_yf, confirmed in rows:
        if ticker in out:
            continue  # nearest date already taken (ORDER BY event_date)
        cutoff = pre_release_cutoff(event_date, hour, hour_yf, confirmed)
        if now >= cutoff:
            continue
        out[ticker] = (event_date, cutoff)
    return [(t, d, c) for t, (d, c) in sorted(out.items())]


def _open_window_rows(conn: sqlite3.Connection, today: date) -> list:
    d_start = today.isoformat()
    d_confirmed = (today + timedelta(days=WINDOW_DAYS)).isoformat()
    d_unconfirmed = (
        today + timedelta(days=WINDOW_DAYS + UNCONFIRMED_EXTRA_DAYS)).isoformat()
    rows = conn.execute(
        "SELECT ticker, event_date, event_hour, event_hour_yf, date_confirmed "
        f"FROM events WHERE {OPEN_EVENT_SQL} AND ("
        "  (event_date > ? AND event_date <= ?)"
        "  OR (COALESCE(date_confirmed, 0) = 0 AND event_date > ? AND event_date <= ?)"
        "  OR (event_date = ? AND LOWER(COALESCE(event_hour, '')) = 'amc')"
        ") ORDER BY ticker, event_date",
        (d_start, d_confirmed, d_start, d_unconfirmed, d_start),
    ).fetchall()
    return rows


Fetcher = Callable[[str], "tuple[list, str]"]


def snapshot_annual_consensus(
    conn: sqlite3.Connection,
    today: date,
    fetcher: Fetcher,
    *,
    now: Optional[datetime] = None,
    clock: Optional[Callable[[], datetime]] = None,
    max_consecutive_errors: Optional[int] = None,
    budget_seconds: Optional[float] = None,
) -> dict:
    """Take one snapshot per in-window ticker and store it.

    ``fetcher(ticker)`` returns ``(rows, status)`` -- ``rows`` a list of
    ``consensus_preview.AnnualConsensus``, ``status`` ``ok`` / ``empty`` /
    ``error:<code>`` (see ``fetch_fmp_annual_estimates_checked``). An empty
    window makes ZERO fetcher calls.

    Writes one row per ``(ticker, fiscal_period_end, taken_at)``; an empty or
    failed fetch writes one status row instead (fiscal_period_end ''), so a 429
    is on record as a 429 and never reads as "no coverage". Commits per ticker,
    so a crash part-way keeps what was already captured.

    Returns a summary: ``tickers``, ``ok``, ``empty``, ``errors``,
    ``rows_written`` and ``failed`` (``[(ticker, status), ...]``).
    """
    # Resolved at CALL time so the module constants stay the single knob.
    if max_consecutive_errors is None:
        max_consecutive_errors = MAX_CONSECUTIVE_ERRORS
    if budget_seconds is None:
        budget_seconds = RUN_BUDGET_SECONDS
    if clock is None:
        # Anchor on `now` and advance by REAL elapsed time, so an injected `now`
        # (tests, a pinned run) and the per-fetch stamp stay on one timeline.
        now = now or datetime.now(timezone.utc)
        _t0, _anchor = time.monotonic(), now
        clock = lambda: _anchor + timedelta(seconds=time.monotonic() - _t0)  # noqa: E731
    now = now or clock()
    window, blocked = _select_window(conn, today, now)
    summary = {"tickers": len(window), "ok": 0, "empty": 0, "errors": 0,
               "rows_written": 0, "failed": [], "aborted": None, "skipped": 0,
               "skipped_open_prior": [t for t, _d, _p in blocked]}
    insert = (
        "INSERT OR REPLACE INTO consensus_snapshot (ticker, fiscal_period_end, "
        "taken_at, event_date, revenue_avg, eps_avg, num_analysts_revenue, "
        "num_analysts_eps, currency, fetch_status) VALUES (?,?,?,?,?,?,?,?,?,?)"
    )
    if blocked:
        # On record, so "why is there no snapshot" has an answer -- and never
        # an error: nothing failed, the guard refused.
        stamp = _utc_stamp(now)
        conn.executemany(insert, [
            (t, STATUS_ROW_PERIOD, stamp, d, None, None, None, None, None,
             "skipped:open_prior_event") for t, d, _p in blocked])
        conn.commit()
    if not window:
        logger.info("Consensus snapshot: no open events in the pre-release window")
        return summary
    consecutive_errors = 0
    for i, (ticker, event_date, cutoff) in enumerate(window):
        # Circuit breaker + wall-clock budget (Codex round 1): stop LOUDLY rather
        # than let backoff push the job past `Save earnings database`.
        if consecutive_errors >= max_consecutive_errors:
            summary["aborted"] = f"{consecutive_errors} consecutive fetch errors"
        elif (clock() - now).total_seconds() > budget_seconds:
            summary["aborted"] = f"run budget of {budget_seconds:.0f}s spent"
        if summary["aborted"]:
            summary["skipped"] = len(window) - i
            break
        # Re-check the cutoff BEFORE the metered call (Fable code review, H):
        # a run that straddles 16:00 ET must not spend a request on a figure it
        # will refuse, and a missed cutoff is a SKIP, not a fetch failure -- it
        # must never feed the breaker, the error count or the "retried next
        # run" status post (it is never retried: the release has happened).
        if clock() >= cutoff:
            summary["skipped_post_cutoff"] = summary.get("skipped_post_cutoff", 0) + 1
            continue
        try:
            rows, status = fetcher(ticker)
        except Exception as exc:  # noqa: BLE001 -- one ticker never stops the run
            logger.warning("Consensus snapshot: fetcher raised for %s: %s", ticker, exc)
            rows, status = [], f"error:{type(exc).__name__}"
        if status == "ok" and not rows:
            status = "empty"  # never let an "ok" with nothing in it pass as data
        # Stamp with the moment THIS fetch returned, and refuse a figure that
        # came back after its release's cutoff: a slow run (backoff, a late
        # manual dispatch) must never store post-print consensus as pre-print.
        fetched_at = clock()
        taken_at = _utc_stamp(fetched_at)
        if status == "ok" and fetched_at >= cutoff:
            # Crossed the cutoff DURING the call: refuse the figure, record it,
            # but as a skip (never an error -- see above).
            summary["skipped_post_cutoff"] = summary.get("skipped_post_cutoff", 0) + 1
            conn.execute(insert, (ticker, STATUS_ROW_PERIOD, taken_at, event_date,
                                  None, None, None, None, None, "skipped:post_cutoff"))
            conn.commit()
            continue
        if status.startswith("partial:") and not rows:
            status = "error:" + status[len("partial:"):]
        failed = status.startswith(("error:", "partial:"))
        consecutive_errors = consecutive_errors + 1 if failed else 0

        if status == "ok" or status.startswith("partial:"):
            # A `partial:*` row keeps its figures but is NOT `ok`: the read
            # side filters to ok, and the run counts it as an error.
            conn.executemany(insert, [
                (ticker, r.fiscal_period_end, taken_at, event_date,
                 r.revenue_avg, r.eps_avg, r.revenue_analysts, r.eps_analysts,
                 r.currency, status)
                for r in rows
            ])
            summary["rows_written"] += len(rows)
            if status == "ok":
                summary["ok"] += 1
            else:
                summary["errors"] += 1
                summary["failed"].append((ticker, status))
        else:
            conn.execute(insert, (ticker, STATUS_ROW_PERIOD, taken_at, event_date,
                                  None, None, None, None, None, status))
            summary["rows_written"] += 1
            if status == "empty":
                summary["empty"] += 1
            else:
                summary["errors"] += 1
                summary["failed"].append((ticker, status))
        conn.commit()

    if summary["aborted"]:
        logger.error("Consensus snapshot ABORTED (%s): %d ticker(s) not attempted",
                     summary["aborted"], summary["skipped"])
    logger.info(
        "Consensus snapshot: %d ticker(s) -- %d ok, %d empty, %d error(s)%s",
        summary["tickers"], summary["ok"], summary["empty"],
        summary["errors"],
        (": " + ", ".join(f"{t} {s}" for t, s in summary["failed"][:10])
         if summary["failed"] else ""),
    )
    return summary


# ---------------------------------------------------------------------------
# Production fetcher: FMP, paced
# ---------------------------------------------------------------------------

class FmpPacer:
    """Wrap a ``get_json(url)`` so calls go out at most ``rate_per_sec``, and a
    429 is retried after 5 / 15 / 30 s before the final HTTPError is re-raised
    (so the caller records ``error:429``). Any other error is NOT retried: a
    403 or a malformed key will not fix itself in 50 seconds."""

    def __init__(self, get_json, *, rate_per_sec: float = FMP_RATE_PER_SEC,
                 backoff=FMP_429_BACKOFF, sleep=time.sleep, clock=time.monotonic):
        self._get_json = get_json
        self._interval = 1.0 / rate_per_sec
        self._backoff = tuple(backoff)
        self._sleep = sleep
        self._clock = clock
        self._last: Optional[float] = None

    def _pace(self) -> None:
        if self._last is not None:
            wait = self._last + self._interval - self._clock()
            if wait > 0:
                self._sleep(wait)
        self._last = self._clock()

    def __call__(self, url, *args, **kwargs):
        for attempt in range(len(self._backoff) + 1):
            self._pace()
            try:
                return self._get_json(url, *args, **kwargs)
            except urllib.error.HTTPError as exc:
                if exc.code != 429 or attempt == len(self._backoff):
                    raise
                delay = self._backoff[attempt]
                logger.warning("FMP 429; retrying in %ss (attempt %d/%d)",
                               delay, attempt + 1, len(self._backoff) + 1)
                self._sleep(delay)


def _cached_currency(conn, ticker, api_key, get_json, today: date):
    """``(currency, status)`` -- reported currency, cached 30 days in kv_store.

    ``status`` is ``ok`` / ``absent`` / ``error:<code>`` (see
    ``fetch_fmp_reported_currency_checked``). A failed or absent lookup is NOT
    cached (it is retried next run) and yields None, which the square treats as
    abstain -- never as USD. A FAILED lookup must also never let the snapshot
    pass as ``ok`` (Codex round 2); the caller turns it into a partial status.
    """
    from consensus_preview import fetch_fmp_reported_currency_checked

    key = _CURRENCY_KV + ticker
    raw = kv_get(conn, key)
    if raw:
        try:
            rec = json.loads(raw)
            fetched = date.fromisoformat(rec["fetched"])
            if (today - fetched).days < CURRENCY_CACHE_DAYS and rec.get("currency"):
                return rec["currency"], "ok"
        except (ValueError, KeyError, TypeError):
            pass
    cur, status = fetch_fmp_reported_currency_checked(
        ticker, api_key, get_json=get_json)
    if cur:
        kv_set(conn, key, json.dumps({"currency": cur, "fetched": today.isoformat()}))
    return cur, status


def make_fmp_fetcher(conn: sqlite3.Connection, api_key: str, today: date,
                     pacer: Optional[FmpPacer] = None) -> Fetcher:
    """The production ``fetcher`` for ``snapshot_annual_consensus``: FMP annual
    estimates through the 4/s pacer, joined to the cached reported currency.
    The currency is only looked up after a successful estimates call, so a
    429'd or uncovered ticker costs one request, not two.

    A currency that could not be FETCHED returns the rows with status
    ``partial:currency_<error>`` (Codex round 2): the figures are kept, since a
    pre-print figure cannot be recaptured, but the row is not ``ok`` -- the
    read side never serves it and the run counts it as an error. A currency
    FMP legitimately does not have stays ``ok`` with currency None."""
    from consensus_preview import fetch_fmp_annual_estimates_checked
    import consensus_preview as cp

    pacer = pacer or FmpPacer(lambda url: cp._fmp_get_json(url))

    def fetch(ticker: str):
        rows, status = fetch_fmp_annual_estimates_checked(
            ticker, api_key, get_json=pacer)
        if status != "ok":
            return rows, status
        cur, cur_status = _cached_currency(conn, ticker, api_key, pacer, today)
        for r in rows:
            r.currency = cur
        if cur_status.startswith("error:"):
            return rows, "partial:currency_" + cur_status
        return rows, status

    return fetch
