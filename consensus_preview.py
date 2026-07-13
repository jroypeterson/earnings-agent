"""
Consensus Metrics Preview (StreetAccount Wave 1 §9A).

Builds a pre-earnings brief for each upcoming covered reporter, composed
entirely from FREE / cached data (yfinance + the SQLite events table +
Coverage Manager exports). One brief per upcoming Tier-1 name carries:

  - header: company, event date, BMO/AMC badge, last price ± day change
  - mean consensus Rev + EPS (from the DB events table — zero API calls)
  - QTD price move since the prior quarter-end for TICKER / SPY / sector-ETF
  - options-implied move (ATM straddle mid ÷ spot on the first expiry that
    contains the post-earnings reaction — see compute_implied_move)
  - last-4-quarter post-earnings stock reactions
  - EPS beat rate (up to 20 quarters, yfinance Surprise%)
  - conference-call time (from events.call_datetime_utc)

Two further fields are FMP-Starter-gated (estimate count/low-high range and
the revenue beat rate). They are only fetched when the caller passes an FMP
key AND the kv_store probe flag `fmp_preview_endpoints_ok` == "true"; until
then they render as an explicit "n/a (FMP endpoint unavailable)" — never a
silent blank and never a fabricated number.

Design invariant: NO field is ever silently dropped. Every degraded field
appends a human note to PreviewRow.na_notes and a machine-readable
{field, reason} entry to PreviewRow.na_fields, and renders as an explicit
"n/a (reason)" string. A per-ticker failure never drops the row.

This module is additive — it does not touch the daily run() flow.
"""

from __future__ import annotations

import io
import logging
import sqlite3
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - py<3.9 fallback
    ZoneInfo = None

import yfinance as yf

from coverage import TickerInfo
from market_data import (
    PostEarningsMove,
    fetch_post_earnings_move,
    infer_hour_from_datetime,
)

logger = logging.getLogger("earnings_agent")

SCHEMA_VERSION = 1

# Coverage-Manager sector (or subsector) -> sector-proxy ETF for the QTD row.
# Keys are the canonical CM sector labels. Unknown sector -> no ETF row + note.
SECTOR_ETF: dict[str, str] = {
    "Healthcare Services": "XLV",
    "MedTech": "IHI",
    "Large Pharma": "XLV",
    "Biotech": "XBI",
    "Fintech": "XLF",
}

# Implied-move sanity gates (percent of spot). A straddle mid outside this
# band is almost always a data artifact (stale/one-sided quotes, wrong
# strike), not a real expectation — surface it as n/a rather than a bad number.
_IMPLIED_MOVE_MIN_PCT = 0.5
_IMPLIED_MOVE_MAX_PCT = 40.0
# Nearest common strike must be within this fraction of spot to be "ATM".
_ATM_MAX_STRIKE_DISTANCE = 0.05
# A leg with (ask - bid)/mid above this is flagged "wide market".
_WIDE_MARKET_RATIO = 0.6
# Expiry more than this many days past the reaction date carries extra
# time value; we annotate so the reader knows the move is inflated.
_EXTRA_TIME_VALUE_DAYS = 7


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ImpliedMove:
    """Options-implied one-day move from an ATM straddle.

    pct is None when the move could not be computed for any reason; in that
    case notes[0] carries the "n/a (reason)" string. When pct is present,
    notes holds any annotations ("stale quote", "wide market",
    "(includes N extra days of time value)").
    """
    pct: Optional[float]
    expiry: Optional[str]
    method: str
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.pct is not None


@dataclass
class EstimateRange:
    """FMP analyst-estimate spread (count + low/high). All None => n/a."""
    count: Optional[int]
    low: Optional[float]
    high: Optional[float]

    @property
    def ok(self) -> bool:
        return self.count is not None


@dataclass
class BeatRate:
    """Historical beat rate (beats out of total). beats/total None => n/a."""
    beats: Optional[int]
    total: Optional[int]
    source: str

    @property
    def ok(self) -> bool:
        return self.total is not None and self.total > 0


@dataclass
class QuarterMove:
    """One historical post-earnings stock reaction."""
    event_date: str
    move_pct: float
    window_label: str


@dataclass
class Reporter:
    """A single upcoming reporter selected for a preview (pre-assembly)."""
    ticker: str
    company_name: str
    tier: int
    event_date: str
    event_hour: Optional[str]
    call_datetime_utc: Optional[str]
    eps_estimate: Optional[float]
    rev_estimate: Optional[float]
    sector: str
    subsector: str


@dataclass
class PreviewRow:
    """The fully-assembled preview for one reporter."""
    ticker: str
    company_name: str
    tier: int
    event_date: str
    event_hour: Optional[str]
    call_datetime_utc: Optional[str]

    last_price: Optional[float] = None
    day_change_pct: Optional[float] = None

    eps_mean: Optional[float] = None
    rev_mean: Optional[float] = None
    eps_range: EstimateRange = field(
        default_factory=lambda: EstimateRange(None, None, None)
    )

    qtd_since: Optional[str] = None
    qtd_ticker_pct: Optional[float] = None
    qtd_spx_pct: Optional[float] = None
    sector_etf: Optional[str] = None          # ETF ticker, e.g. "XLV"
    sector_etf_label: Optional[str] = None    # human label, e.g. "Healthcare Services"
    sector_etf_pct: Optional[float] = None

    implied_move: ImpliedMove = field(
        default_factory=lambda: ImpliedMove(None, None, "ATM straddle", ["n/a (not computed)"])
    )

    post_moves: list[QuarterMove] = field(default_factory=list)

    eps_beat: BeatRate = field(default_factory=lambda: BeatRate(None, None, "yfinance"))
    rev_beat: BeatRate = field(default_factory=lambda: BeatRate(None, None, "fmp"))

    na_notes: list[str] = field(default_factory=list)
    na_fields: list[dict] = field(default_factory=list)

    def mark_na(self, field_name: str, reason: str) -> None:
        """Record a degraded/unavailable field once, both human + machine."""
        self.na_notes.append(f"{field_name}: {reason}")
        self.na_fields.append({"field": field_name, "reason": reason})


# ---------------------------------------------------------------------------
# yfinance noise suppression
# ---------------------------------------------------------------------------


@contextmanager
def _silence():
    """Silence yfinance's chatty stdout/stderr (matches market_data idiom)."""
    buf_out, buf_err = io.StringIO(), io.StringIO()
    with redirect_stdout(buf_out), redirect_stderr(buf_err):
        yield


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _parse_iso_date(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def _prior_quarter_end(today: date) -> date:
    """Return the calendar-quarter-end immediately before `today`'s quarter.

    For any day in Q3 (Jul-Sep) this is Jun 30; the base for the QTD move.
    """
    q_start_month = ((today.month - 1) // 3) * 3 + 1
    q_start = date(today.year, q_start_month, 1)
    return q_start - timedelta(days=1)


def sector_etf_for(sector: str, subsector: str) -> Optional[tuple[str, str]]:
    """Map a CM sector (then subsector) to (label, etf_ticker) or None."""
    for key in (sector, subsector):
        if key and key in SECTOR_ETF:
            return key, SECTOR_ETF[key]
    return None


def reaction_date(event_date: str, event_hour: Optional[str]) -> Optional[date]:
    """The trading day whose close reflects the market's reaction.

    bmo / dmh           -> the event date itself. A pre-open (bmo) OR during-
                           market-hours (dmh) release reacts the SAME session.
    amc / blank         -> the next weekday after the event date (post-close or
                           unknown-timing release reacts the following session;
                           unknown is treated as amc so the chosen expiry can
                           never fall BEFORE an actual post-close reaction).
                           Weekend-skip only (no holiday calendar).
    """
    ed = _parse_iso_date(event_date)
    if ed is None:
        return None
    # Mirror market_data.fetch_post_earnings_move: only amc defers to the next
    # session; bmo and dmh both react same-day.
    if (event_hour or "").lower() in ("bmo", "dmh"):
        return ed
    d = ed + timedelta(days=1)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d += timedelta(days=1)
    return d


def _last_close(ticker: str) -> Optional[float]:
    """Most recent daily close from a 5d history (NOT intraday fast_info)."""
    try:
        with _silence():
            hist = yf.Ticker(ticker).history(period="5d", auto_adjust=True)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"5d history failed for {ticker}: {exc}")
        return None
    if hist is None or hist.empty:
        return None
    closes = hist["Close"].dropna()
    if closes.empty:
        return None
    return float(closes.iloc[-1])


def _leg_mid(row) -> tuple[Optional[float], bool, Optional[float]]:
    """Return (mid, is_stale, spread) for one option leg.

    Rule (spec §2):
      bid > 0 and ask >= bid  -> (bid + ask) / 2, not stale, spread = ask - bid
      elif lastPrice > 0      -> lastPrice, STALE (no live two-sided quote),
                                 spread unknown (None)
      else                    -> (None, False, None)  -> leg unusable
    """
    def _f(key: str) -> float:
        try:
            v = row.get(key)
        except AttributeError:  # pandas Series
            v = row[key] if key in row else None
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    bid, ask, last = _f("bid"), _f("ask"), _f("lastPrice")
    if bid > 0 and ask >= bid:
        return (bid + ask) / 2.0, False, (ask - bid)
    if last > 0:
        return last, True, None
    return None, False, None


# ---------------------------------------------------------------------------
# Reporter selection
# ---------------------------------------------------------------------------


def _row_to_reporter(raw: tuple, coverage_map: dict[str, TickerInfo]) -> Reporter:
    (ticker, event_date, event_hour, event_hour_yf, db_tier, company_name,
     eps_est, rev_est, call_dt) = raw
    info = coverage_map.get(ticker)
    # Finnhub-canonical hour first; fall back to the yfinance-inferred hour
    # (v11 event_hour_yf column) when Finnhub left timing blank — the same
    # fallback the rest of the pipeline uses. Without it a bmo/amc name whose
    # timing only lives in event_hour_yf renders TBD and gets a wrong (next-
    # session) reaction date for the implied-move expiry.
    effective_hour = event_hour or event_hour_yf
    return Reporter(
        ticker=ticker,
        company_name=company_name or (info.company_name if info else ""),
        tier=(info.tier if info else (int(db_tier) if db_tier is not None else 99)),
        event_date=event_date,
        event_hour=effective_hour,
        call_datetime_utc=call_dt,
        eps_estimate=eps_est,
        rev_estimate=rev_est,
        sector=(info.sector if info else ""),
        subsector=(info.subsector if info else ""),
    )


_SELECT_COLS = (
    "ticker, event_date, event_hour, event_hour_yf, tier, company_name, "
    "eps_estimate, rev_estimate, call_datetime_utc"
)


def select_upcoming_reporters(
    conn: sqlite3.Connection,
    coverage: list[TickerInfo],
    *,
    days_ahead: int = 3,
    max_tier: int = 1,
    ticker: Optional[str] = None,
) -> list[Reporter]:
    """Select upcoming, not-yet-reported reporters for previewing.

    When `ticker` is given, returns that ticker's single nearest upcoming
    unreported event regardless of tier or window (on-demand preview).
    Otherwise returns every unreported event in [today, today+days_ahead]
    whose Coverage-Manager tier is <= max_tier (Tier-1 only by default).
    """
    coverage_map = {t.ticker: t for t in coverage}
    today = date.today()

    if ticker:
        tk = ticker.strip().upper()
        row = conn.execute(
            f"SELECT {_SELECT_COLS} FROM events "
            "WHERE ticker = ? AND COALESCE(reported, 0) = 0 AND event_date >= ? "
            "ORDER BY event_date LIMIT 1",
            (tk, today.isoformat()),
        ).fetchone()
        if not row:
            logger.warning(f"No upcoming unreported event found for {tk}")
            return []
        return [_row_to_reporter(row, coverage_map)]

    end = (today + timedelta(days=days_ahead)).isoformat()
    rows = conn.execute(
        f"SELECT {_SELECT_COLS} FROM events "
        "WHERE event_date BETWEEN ? AND ? AND COALESCE(reported, 0) = 0 "
        "ORDER BY event_date, ticker",
        (today.isoformat(), end),
    ).fetchall()

    reporters: list[Reporter] = []
    for raw in rows:
        rep = _row_to_reporter(raw, coverage_map)
        if rep.tier <= max_tier:
            reporters.append(rep)
    logger.info(
        f"Selected {len(reporters)} upcoming Tier<= {max_tier} reporter(s) "
        f"in next {days_ahead}d ({len(rows)} events scanned)"
    )
    return reporters


# ---------------------------------------------------------------------------
# Free-data field fetchers
# ---------------------------------------------------------------------------


def fetch_price_snapshot(ticker: str) -> tuple[Optional[float], Optional[float]]:
    """Return (last_close, 1-day_pct_change) from a single 5d history call."""
    try:
        with _silence():
            hist = yf.Ticker(ticker).history(period="5d", auto_adjust=True)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"price snapshot failed for {ticker}: {exc}")
        return None, None
    if hist is None or hist.empty:
        return None, None
    closes = hist["Close"].dropna()
    if closes.empty:
        return None, None
    last = float(closes.iloc[-1])
    if len(closes) >= 2:
        prev = float(closes.iloc[-2])
        day_change = (last - prev) / prev * 100 if prev else None
    else:
        day_change = None
    return last, day_change


def fetch_quarter_performance(
    tickers: list[str], since: date
) -> dict[str, Optional[float]]:
    """QTD % change (close on/after `since` -> latest close) for each ticker.

    One batched yfinance download (the caller pre-adds SPY + sector ETFs so
    the whole panel comes over the wire once). A ticker yfinance can't
    resolve maps to None (caller renders n/a), never raising.
    """
    unique = sorted({t.upper() for t in tickers})
    result: dict[str, Optional[float]] = {t: None for t in unique}
    if not unique:
        return result

    try:
        with _silence():
            data = yf.download(
                tickers=unique,
                start=since.isoformat(),
                progress=False,
                auto_adjust=True,
                threads=True,
                group_by="ticker",
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"QTD batch download failed: {exc}")
        return result

    if data is None or data.empty:
        logger.warning("QTD download returned empty dataset")
        return result

    for t in unique:
        try:
            if len(unique) == 1:
                closes = data["Close"].dropna()
            else:
                closes = data[t]["Close"].dropna()
            if closes.empty:
                continue
            first = float(closes.iloc[0])
            last = float(closes.iloc[-1])
            if first == 0:
                continue
            result[t] = (last - first) / first * 100
        except (KeyError, IndexError, ValueError) as exc:
            logger.debug(f"QTD unavailable for {t}: {exc}")
    return result


def compute_implied_move(
    ticker: str, event_date: str, event_hour: Optional[str]
) -> ImpliedMove:
    """Options-implied move = ATM straddle mid ÷ spot on the first expiry that
    contains the post-earnings reaction date.

    Never raises and never returns a bad number: every failure mode resolves
    to ImpliedMove(pct=None, ...) whose notes[0] is an explicit "n/a (reason)".
    """
    method = "ATM straddle"

    rxn = reaction_date(event_date, event_hour)
    if rxn is None:
        return ImpliedMove(None, None, method, ["n/a (unparseable event date)"])

    try:
        tkr = yf.Ticker(ticker)
        with _silence():
            expiries = list(tkr.options or [])
    except Exception as exc:  # noqa: BLE001
        return ImpliedMove(
            None, None, method,
            [f"n/a (options unavailable: {exc.__class__.__name__})"],
        )
    if not expiries:
        return ImpliedMove(None, None, method, ["n/a (no listed options)"])

    future = sorted(
        e for e in expiries
        if (_parse_iso_date(e) is not None and _parse_iso_date(e) >= rxn)
    )
    if not future:
        return ImpliedMove(
            None, None, method,
            ["n/a (no expiry on or after the reaction date)"],
        )
    expiry = future[0]
    exp_date = _parse_iso_date(expiry)

    spot = _last_close(ticker)
    if spot is None or spot <= 0:
        return ImpliedMove(None, expiry, method, ["n/a (spot price unavailable)"])

    try:
        with _silence():
            chain = tkr.option_chain(expiry)
        calls, puts = chain.calls, chain.puts
    except Exception as exc:  # noqa: BLE001
        return ImpliedMove(
            None, expiry, method,
            [f"n/a (option chain fetch failed: {exc.__class__.__name__})"],
        )

    if calls is None or puts is None or calls.empty or puts.empty:
        return ImpliedMove(None, expiry, method, ["n/a (empty option chain)"])

    call_strikes = set(calls["strike"].tolist())
    put_strikes = set(puts["strike"].tolist())
    common = call_strikes & put_strikes
    if not common:
        return ImpliedMove(
            None, expiry, method,
            ["n/a (no strike present in both calls and puts)"],
        )

    K = min(common, key=lambda k: abs(k - spot))
    if abs(K - spot) / spot > _ATM_MAX_STRIKE_DISTANCE:
        return ImpliedMove(
            None, expiry, method,
            [f"n/a (nearest common strike {K:g} is >5% from spot {spot:.2f})"],
        )

    call_row = calls.loc[calls["strike"] == K].iloc[0]
    put_row = puts.loc[puts["strike"] == K].iloc[0]
    call_mid, call_stale, call_spread = _leg_mid(call_row)
    put_mid, put_stale, put_spread = _leg_mid(put_row)
    if call_mid is None or put_mid is None:
        return ImpliedMove(
            None, expiry, method,
            ["n/a (no usable bid/ask/last on a straddle leg)"],
        )

    pct = (call_mid + put_mid) / spot * 100
    if not (_IMPLIED_MOVE_MIN_PCT <= pct <= _IMPLIED_MOVE_MAX_PCT):
        return ImpliedMove(
            None, expiry, method,
            [f"n/a (implausible straddle: {pct:.1f}%)"],
        )

    notes: list[str] = []
    if call_stale or put_stale:
        notes.append("stale quote")
    for spread, mid in ((call_spread, call_mid), (put_spread, put_mid)):
        if spread is not None and mid > 0 and spread / mid > _WIDE_MARKET_RATIO:
            notes.append("wide market")
            break
    if exp_date is not None:
        extra_days = (exp_date - rxn).days
        if extra_days > _EXTRA_TIME_VALUE_DAYS:
            notes.append(f"(includes {extra_days} extra days of time value)")

    return ImpliedMove(round(pct, 1), expiry, method, notes)


def fetch_recent_post_moves(
    conn: sqlite3.Connection, ticker: str, n: int = 4
) -> list[QuarterMove]:
    """Last `n` post-earnings stock reactions, newest first.

    Prefers the DB's reported events (already timed BMO/AMC), then backfills
    from yfinance's historical earnings dates for names with a thin DB
    history, inferring timing from the timestamp's time-of-day.
    """
    moves: list[QuarterMove] = []
    seen: set[str] = set()

    rows = conn.execute(
        "SELECT event_date, event_hour FROM events "
        "WHERE ticker = ? AND COALESCE(reported, 0) = 1 AND event_date <= date('now') "
        "ORDER BY event_date DESC LIMIT ?",
        (ticker, n * 3),
    ).fetchall()
    for ev_date, ev_hour in rows:
        if len(moves) >= n:
            break
        if ev_date in seen:
            continue
        mv = fetch_post_earnings_move(ticker, ev_date, ev_hour)
        if mv is not None:
            moves.append(QuarterMove(ev_date, mv.move_pct, mv.window_label))
            seen.add(ev_date)

    if len(moves) < n:
        try:
            with _silence():
                ed = yf.Ticker(ticker).get_earnings_dates(limit=max(n * 3, 12))
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"earnings-date backfill failed for {ticker}: {exc}")
            ed = None
        if ed is not None and not ed.empty:
            today = date.today()
            for idx in ed.index:
                if len(moves) >= n:
                    break
                try:
                    d = idx.date()
                except AttributeError:
                    continue
                if d >= today:  # only settled, past reactions
                    continue
                iso = d.isoformat()
                if iso in seen:
                    continue
                try:
                    hour = infer_hour_from_datetime(idx.to_pydatetime())
                except Exception:  # noqa: BLE001
                    hour = None
                mv = fetch_post_earnings_move(ticker, iso, hour)
                if mv is not None:
                    moves.append(QuarterMove(iso, mv.move_pct, mv.window_label))
                    seen.add(iso)

    moves.sort(key=lambda m: m.event_date, reverse=True)
    return moves[:n]


def fetch_eps_beat_history(ticker: str, max_q: int = 20) -> BeatRate:
    """EPS beat rate over up to `max_q` reported quarters (yfinance Surprise%).

    A positive Surprise(%) is a beat. Rows with NaN surprise (the upcoming,
    not-yet-reported quarter) are excluded. Returns BeatRate with beats/total
    None when yfinance has no usable surprise history (caller renders n/a).
    """
    try:
        with _silence():
            ed = yf.Ticker(ticker).get_earnings_dates(limit=max_q)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"EPS beat history failed for {ticker}: {exc}")
        return BeatRate(None, None, "yfinance")
    if ed is None or ed.empty or "Surprise(%)" not in ed.columns:
        return BeatRate(None, None, "yfinance")
    # yfinance does not strictly honor `limit` (it pads future/extra rows), so
    # cap to the most recent max_q reported quarters after dropping the
    # not-yet-reported (NaN-surprise) rows. Index is newest-first.
    surprises = ed["Surprise(%)"].dropna().head(max_q)
    total = int(len(surprises))
    if total == 0:
        return BeatRate(None, None, "yfinance")
    beats = int((surprises > 0).sum())
    return BeatRate(beats, total, "yfinance")


# ---------------------------------------------------------------------------
# FMP-gated fetchers (only called when the probe flag is set — see run_*).
# These deliberately never raise; any failure yields an n/a result.
# ---------------------------------------------------------------------------


def _fmp_get_json(url: str, timeout: int = 20):
    import json
    import urllib.request

    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
        return json.load(resp)


def fetch_fmp_estimate_range(
    ticker: str, event_date: str, api_key: str
) -> EstimateRange:
    """Analyst estimate count + low/high EPS range for the reporting quarter.

    UNAVAILABLE on the current FMP Starter plan and therefore always n/a:
    - /stable/analyst-estimates with period=quarter -> HTTP 402 ("this value
      set for 'period' is not available under your current subscription")
    - the v3 /api/v3/analyst-estimates endpoint is retired -> HTTP 403
      ("Legacy Endpoint")
    (Both verified 2026-07-12.) We short-circuit rather than fire a request
    that can only burn a gated 402/403; the caller renders the honest
    "not available on FMP Starter plan" reason. Kept as a documented seam so a
    future plan upgrade can restore quarterly estimate ranges here.
    """
    return EstimateRange(None, None, None)


def fetch_fmp_rev_beat_history(
    ticker: str, api_key: str, max_q: int = 20
) -> BeatRate:
    """Revenue beat rate over up to `max_q` most-recent COMPLETED quarters.

    Endpoint: FMP /stable/earnings (fields revenueActual / revenueEstimated).
    This is the Starter-plan-available successor to the retired v3
    /historical/earning_calendar endpoint (v3 -> HTTP 403 "Legacy Endpoint";
    /stable/earnings -> HTTP 200, verified 2026-07-12). Returns n/a on any
    error or unexpected shape — never a partial/fabricated number.
    """
    if not api_key:
        return BeatRate(None, None, "fmp")
    # /stable/earnings returns the upcoming (actual=None) quarter plus history;
    # over-request so `max_q` COMPLETED quarters survive the null-actual skip.
    limit = max(max_q * 2, 40)
    url = (
        f"https://financialmodelingprep.com/stable/earnings"
        f"?symbol={ticker}&limit={limit}&apikey={api_key}"
    )
    try:
        data = _fmp_get_json(url)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"FMP revenue-beat fetch failed for {ticker}: {exc}")
        return BeatRate(None, None, "fmp")
    if not isinstance(data, list) or not data:
        return BeatRate(None, None, "fmp")

    # Newest-first so the `max_q` cap keeps the MOST RECENT completed quarters,
    # regardless of the API's return order.
    data = sorted(data, key=lambda r: str(r.get("date", "")), reverse=True)

    beats = 0
    total = 0
    for row in data:
        actual = row.get("revenueActual")
        est = row.get("revenueEstimated")
        if actual is None or est is None:
            continue
        try:
            actual_f, est_f = float(actual), float(est)
        except (TypeError, ValueError):
            continue
        if est_f == 0:
            continue
        total += 1
        if actual_f > est_f:
            beats += 1
        if total >= max_q:
            break
    if total == 0:
        return BeatRate(None, None, "fmp")
    return BeatRate(beats, total, "fmp")


# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------


def assemble_preview_rows(
    conn: sqlite3.Connection,
    reporters: list[Reporter],
    coverage_map: dict[str, TickerInfo],
    *,
    fmp_key: Optional[str] = None,
    fmp_ok: bool = False,
) -> list[PreviewRow]:
    """Assemble one PreviewRow per reporter.

    QTD is batched across every reporter + SPY + sector ETFs in a single
    yfinance download. Each per-field fetch is wrapped so a single failure
    degrades that field to n/a rather than losing the row; a catastrophic
    per-ticker error still yields a skeleton row flagged in na_fields.
    """
    today = date.today()
    since = _prior_quarter_end(today)

    # Batch the QTD panel: reporters + SPY + every mapped sector ETF.
    qtd_universe: set[str] = {"SPY"}
    for rep in reporters:
        qtd_universe.add(rep.ticker.upper())
        etf = sector_etf_for(rep.sector, rep.subsector)
        if etf:
            qtd_universe.add(etf[1].upper())
    qtd_map = fetch_quarter_performance(sorted(qtd_universe), since)

    rows: list[PreviewRow] = []
    for rep in reporters:
        try:
            rows.append(
                _build_row(conn, rep, since, qtd_map, fmp_key=fmp_key, fmp_ok=fmp_ok)
            )
        except Exception as exc:  # noqa: BLE001 - never drop a row
            logger.warning(f"Preview assembly failed for {rep.ticker}: {exc}")
            row = PreviewRow(
                ticker=rep.ticker,
                company_name=rep.company_name,
                tier=rep.tier,
                event_date=rep.event_date,
                event_hour=rep.event_hour,
                call_datetime_utc=rep.call_datetime_utc,
                eps_mean=rep.eps_estimate,
                rev_mean=rep.rev_estimate,
            )
            row.mark_na("row", f"assembly failed ({exc.__class__.__name__})")
            rows.append(row)
    return rows


def _build_row(
    conn: sqlite3.Connection,
    rep: Reporter,
    since: date,
    qtd_map: dict[str, Optional[float]],
    *,
    fmp_key: Optional[str],
    fmp_ok: bool,
) -> PreviewRow:
    row = PreviewRow(
        ticker=rep.ticker,
        company_name=rep.company_name,
        tier=rep.tier,
        event_date=rep.event_date,
        event_hour=rep.event_hour,
        call_datetime_utc=rep.call_datetime_utc,
        eps_mean=rep.eps_estimate,
        rev_mean=rep.rev_estimate,
        qtd_since=since.isoformat(),
    )

    if rep.eps_estimate is None:
        row.mark_na("consensus.eps.mean", "no EPS estimate in DB")
    if rep.rev_estimate is None:
        row.mark_na("consensus.revenue.mean", "no revenue estimate in DB")

    # --- price snapshot ---
    try:
        last, day_change = fetch_price_snapshot(rep.ticker)
    except Exception as exc:  # noqa: BLE001
        last, day_change = None, None
        logger.debug(f"price snapshot error {rep.ticker}: {exc}")
    row.last_price = last
    row.day_change_pct = day_change
    if last is None:
        row.mark_na("last_price", "yfinance price unavailable")

    # --- QTD ---
    row.qtd_ticker_pct = qtd_map.get(rep.ticker.upper())
    row.qtd_spx_pct = qtd_map.get("SPY")
    if row.qtd_ticker_pct is None:
        row.mark_na("qtd.ticker_pct", "yfinance QTD unavailable")
    if row.qtd_spx_pct is None:
        row.mark_na("qtd.spx_pct", "yfinance QTD unavailable for SPY")
    etf = sector_etf_for(rep.sector, rep.subsector)
    if etf is None:
        row.mark_na(
            "qtd.sector_etf",
            f"no mapped ETF for sector '{rep.sector or 'unknown'}'",
        )
    else:
        label, etf_ticker = etf
        row.sector_etf = etf_ticker
        row.sector_etf_label = label
        row.sector_etf_pct = qtd_map.get(etf_ticker.upper())
        if row.sector_etf_pct is None:
            row.mark_na("qtd.sector_etf_pct", f"yfinance QTD unavailable for {etf_ticker}")

    # --- implied move ---
    try:
        row.implied_move = compute_implied_move(
            rep.ticker, rep.event_date, rep.event_hour
        )
    except Exception as exc:  # noqa: BLE001 - defensive; compute_* shouldn't raise
        row.implied_move = ImpliedMove(
            None, None, "ATM straddle",
            [f"n/a (unexpected error: {exc.__class__.__name__})"],
        )
    if not row.implied_move.ok:
        reason = row.implied_move.notes[0] if row.implied_move.notes else "n/a"
        row.mark_na("implied_move", reason)

    # --- post-earnings moves ---
    try:
        row.post_moves = fetch_recent_post_moves(conn, rep.ticker, n=4)
    except Exception as exc:  # noqa: BLE001
        row.post_moves = []
        logger.debug(f"post-move error {rep.ticker}: {exc}")
    if not row.post_moves:
        row.mark_na("post_earnings_moves", "no computable prior reactions")

    # --- EPS beat rate ---
    try:
        row.eps_beat = fetch_eps_beat_history(rep.ticker, max_q=20)
    except Exception as exc:  # noqa: BLE001
        row.eps_beat = BeatRate(None, None, "yfinance")
        logger.debug(f"eps beat error {rep.ticker}: {exc}")
    if not row.eps_beat.ok:
        row.mark_na("beat_rates.eps", "no yfinance surprise history")

    # --- FMP B-fields (independently gated) ---
    # Estimate count/range: permanently unavailable on the FMP Starter plan
    # (quarterly analyst-estimates -> 402; verified 2026-07-12) -> always n/a.
    row.mark_na(
        "consensus.eps.range",
        "estimate range not available on FMP Starter plan",
    )
    # Revenue beat rate: reachable via /stable/earnings whenever a key is set.
    if fmp_ok and fmp_key:
        row.rev_beat = fetch_fmp_rev_beat_history(rep.ticker, fmp_key, max_q=20)
        if not row.rev_beat.ok:
            row.mark_na("beat_rates.revenue", "FMP revenue-beat history unavailable")
    else:
        row.mark_na("beat_rates.revenue", "no FMP_API_KEY — revenue beat n/a")

    return row


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def _fmt_short_date(iso: Optional[str]) -> str:
    d = _parse_iso_date(iso or "")
    if d is None:
        return "n/a"
    return f"{d.strftime('%a %b')} {d.day}"


def _fmt_expiry(iso: Optional[str]) -> str:
    d = _parse_iso_date(iso or "")
    if d is None:
        return "n/a"
    return f"{d.strftime('%b')} {d.day}"


def _timing_badge(hour: Optional[str]) -> str:
    if not hour:
        return "TBD"
    h = hour.lower()
    return {"bmo": "BMO", "amc": "AMC", "dmh": "DMH"}.get(h, hour.upper())


def _fmt_pct(v: Optional[float], *, signed: bool = True, na: str = "n/a") -> str:
    if v is None:
        return na
    if signed:
        return f"{v:+.1f}%"
    return f"{v:.1f}%"


def _fmt_price(v: Optional[float]) -> str:
    if v is None:
        return "n/a"
    return f"${v:,.2f}"


def _fmt_eps(v: Optional[float]) -> str:
    if v is None:
        return "n/a"
    return f"${v:.2f}"


def _fmt_rev(v: Optional[float]) -> str:
    if v is None:
        return "n/a"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    return f"${v:,.0f}"


def _fmt_call_et(call_iso: Optional[str], event_date: Optional[str]) -> str:
    """Conference-call time in ET, noting same-day vs next-day."""
    if not call_iso:
        return "n/a"
    try:
        raw = call_iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return "n/a"
    if ZoneInfo is None:
        return dt.strftime("%b %d %H:%M UTC")
    try:
        et = dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:  # noqa: BLE001
        return dt.strftime("%b %d %H:%M UTC")
    hour12 = et.strftime("%I:%M %p").lstrip("0")
    ev = _parse_iso_date(event_date or "")
    if ev is not None and et.date() == ev:
        return f"{hour12} ET (same day)"
    return f"{et.strftime('%a %b')} {et.day} {hour12} ET"


def _render_implied_move(im: ImpliedMove) -> str:
    if not im.ok:
        reason = im.notes[0] if im.notes else "n/a"
        return f"Options-implied move: {reason}"
    line = f"Options imply ~{im.pct:.1f}% move ({_fmt_expiry(im.expiry)} exp straddle)"
    annotations = [n for n in im.notes if n]
    if annotations:
        line += " — " + ", ".join(annotations)
    return line


def _render_post_moves(moves: list[QuarterMove]) -> str:
    if not moves:
        return "Last post-earnings moves: n/a (no computable prior reactions)"
    parts = [f"{_fmt_expiry(m.event_date)} {m.move_pct:+.1f}%" for m in moves]
    return f"Last {len(moves)} post-earnings moves: " + ", ".join(parts)


def _render_beat_rate(br: BeatRate, label: str) -> str:
    if not br.ok:
        return f"{label}: n/a (FMP endpoint unavailable)" if br.source == "fmp" \
            else f"{label}: n/a"
    return f"{label}: {br.beats}/{br.total} ({br.source})"


def _render_estimate_range(er: EstimateRange) -> str:
    if not er.ok:
        return "est. range n/a (FMP endpoint unavailable)"
    lo = _fmt_eps(er.low)
    hi = _fmt_eps(er.high)
    return f"est. from {er.count} analysts ({lo}-{hi})"


def _render_row_lines(row: PreviewRow) -> list[str]:
    """The full set of spec §-scope lines for one reporter (fallback text)."""
    badge = _timing_badge(row.event_hour)
    name = row.company_name or row.ticker
    day = f" ({_fmt_pct(row.day_change_pct)} day)" if row.day_change_pct is not None else ""

    header = (
        f"`{row.ticker}` {name} — {_fmt_short_date(row.event_date)} · {badge} · "
        f"{_fmt_price(row.last_price)}{day}"
    )

    consensus = (
        f"Consensus: EPS {_fmt_eps(row.eps_mean)} · Rev {_fmt_rev(row.rev_mean)} · "
        f"{_render_estimate_range(row.eps_range)}"
    )

    qtd_bits = [f"{row.ticker} {_fmt_pct(row.qtd_ticker_pct)}",
                f"S&P 500 (SPY) {_fmt_pct(row.qtd_spx_pct)}"]
    if row.sector_etf:
        qtd_bits.append(
            f"{row.sector_etf_label} ({row.sector_etf}) {_fmt_pct(row.sector_etf_pct)}"
        )
    else:
        qtd_bits.append("sector ETF n/a (no mapped ETF)")
    qtd = f"QTD since {_fmt_short_date(row.qtd_since)}: " + " · ".join(qtd_bits)

    implied = _render_implied_move(row.implied_move)
    post = _render_post_moves(row.post_moves)
    beats = (
        f"{_render_beat_rate(row.eps_beat, 'EPS beat rate')} · "
        f"{_render_beat_rate(row.rev_beat, 'Rev beat rate')}"
    )
    call = f"Conference call: {_fmt_call_et(row.call_datetime_utc, row.event_date)}"

    return [header, consensus, qtd, implied, post, beats, call]


# ---------------------------------------------------------------------------
# Slack builders
# ---------------------------------------------------------------------------


SLACK_MAX_BLOCKS = 48


def build_preview_fallback(rows: list[PreviewRow], as_of: date) -> str:
    lines = [
        f"Consensus previews — {len(rows)} name(s) (as of {_fmt_short_date(as_of.isoformat())})",
        "",
    ]
    if not rows:
        lines.append("(no upcoming reporters in window)")
        return "\n".join(lines)
    for row in rows:
        lines.extend(_render_row_lines(row))
        lines.append("")
    return "\n".join(lines).rstrip()


def build_preview_blocks(
    rows: list[PreviewRow], as_of: date
) -> tuple[list[dict], int]:
    """Build the Block Kit payload and return (blocks, n_included).

    Rows are added WHOLE (section + optional degraded-context) until the next
    row would push the payload past SLACK_MAX_BLOCKS; the loop then stops. It
    never truncates mid-row. `n_included` (rows actually in the payload, in
    order) lets the caller mark ONLY posted rows for dedup and warn about the
    overflow — instead of the old truncate-then-mark-all, which silently
    dropped overflow rows yet recorded them as posted.
    """
    body: list[dict] = []
    n_included = 0
    # Reserve the 3 lead blocks (header + context + divider).
    body_budget = SLACK_MAX_BLOCKS - 3
    for row in rows:
        header, *rest = _render_row_lines(row)
        row_blocks: list[dict] = [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{header}\n{chr(10).join(rest)}"[:2900]},
        }]
        if row.na_notes:
            row_blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Degraded: " + "; ".join(row.na_notes)[:1900],
                    }
                ],
            })
        if len(body) + len(row_blocks) > body_budget:
            break
        body.extend(row_blocks)
        n_included += 1

    count_label = (
        f"{len(rows)} name(s)" if n_included == len(rows)
        else f"{n_included} of {len(rows)} name(s)"
    )
    lead: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Consensus previews — {count_label}"[:150],
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"As of {_fmt_short_date(as_of.isoformat())} · "
                        "consensus + options-implied moves · free data "
                        "(yfinance + SQLite)"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]
    return lead + body, n_included


# ---------------------------------------------------------------------------
# JSON export (mirrors scripts/export_upcoming_events.py contract)
# ---------------------------------------------------------------------------


def _beat_rate_json(br: BeatRate) -> dict:
    return {"beats": br.beats, "total": br.total, "source": br.source}


def _row_to_json(row: PreviewRow) -> dict:
    return {
        "ticker": row.ticker,
        "company_name": row.company_name,
        "tier": row.tier,
        "event_date": row.event_date,
        "event_hour": (row.event_hour or "").lower() or None,
        "call_datetime_utc": row.call_datetime_utc,
        "last_price": row.last_price,
        "day_change_pct": row.day_change_pct,
        "consensus": {
            "eps": {
                "mean": row.eps_mean,
                "count": row.eps_range.count,
                "low": row.eps_range.low,
                "high": row.eps_range.high,
            },
            "revenue": {
                "mean": row.rev_mean,
                "count": None,
                "low": None,
                "high": None,
            },
        },
        "qtd": {
            "since": row.qtd_since,
            "ticker_pct": row.qtd_ticker_pct,
            "spx_pct": row.qtd_spx_pct,
            "sector_etf": row.sector_etf,
            "sector_etf_pct": row.sector_etf_pct,
        },
        "implied_move": {
            "pct": row.implied_move.pct,
            "expiry": row.implied_move.expiry,
            "method": row.implied_move.method,
            "notes": row.implied_move.notes,
        },
        "post_earnings_moves": [
            {
                "event_date": m.event_date,
                "move_pct": m.move_pct,
                "window_label": m.window_label,
            }
            for m in row.post_moves
        ],
        "beat_rates": {
            "eps": _beat_rate_json(row.eps_beat),
            "revenue": _beat_rate_json(row.rev_beat),
        },
        "na_fields": row.na_fields,
    }


def write_preview_export(
    rows: list[PreviewRow], out_path: Path, window: dict
) -> int:
    """Write the versioned preview JSON. Returns the count written.

    Mirrors the upcoming_events.json contract: schema_version, source,
    generated_at (ISO-Z), window{start,end}, counts{previews}, previews[].
    """
    import json

    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "earnings-agent",
        "generated_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "window": window,
        "counts": {"previews": len(rows)},
        "previews": [_row_to_json(r) for r in rows],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return len(rows)
