"""
Market data helpers — wraps yfinance for price performance queries.

Currently provides YTD percent change. Best-effort: individual tickers that
yfinance cannot resolve (e.g., some foreign listings) return None so the
caller can render "–" instead of failing the whole digest.
"""

from __future__ import annotations

import io
import logging
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - py<3.9 fallback
    ZoneInfo = None

import yfinance as yf

logger = logging.getLogger("earnings_agent")


@dataclass
class PostEarningsMove:
    move_pct: float
    # Short human-readable label describing what the move represents, e.g.
    # "close vs prior close" or "next-day close vs earnings-day close".
    window_label: str


def fetch_ytd_performance(tickers: list[str]) -> dict[str, float | None]:
    """
    Return {ticker: ytd_pct_change} for the given tickers. A missing or
    failed ticker maps to None instead of raising.

    Uses a single batched yfinance download to keep network overhead low.
    """
    if not tickers:
        return {}

    unique = sorted(set(t.upper() for t in tickers))
    result: dict[str, float | None] = {t: None for t in unique}

    # yfinance is chatty on stdout/stderr even with progress=False; silence both.
    buf_out, buf_err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(buf_out), redirect_stderr(buf_err):
            data = yf.download(
                tickers=unique,
                period="ytd",
                progress=False,
                auto_adjust=True,
                threads=True,
                group_by="ticker",
            )
    except Exception as exc:
        logger.warning(f"yfinance batch download failed: {exc}")
        return result

    if data is None or data.empty:
        logger.warning("yfinance returned empty YTD dataset")
        return result

    # When multiple tickers are requested, columns are a MultiIndex:
    #   top level = ticker, second level = field (Open/High/Low/Close/Volume).
    # When only one ticker is requested, columns are flat.
    for ticker in unique:
        try:
            if len(unique) == 1:
                closes = data["Close"].dropna()
            else:
                closes = data[ticker]["Close"].dropna()
            if closes.empty:
                continue
            first = float(closes.iloc[0])
            last = float(closes.iloc[-1])
            if first == 0:
                continue
            result[ticker] = (last - first) / first * 100
        except (KeyError, IndexError, ValueError) as exc:
            logger.debug(f"YTD unavailable for {ticker}: {exc}")

    missing = [t for t, v in result.items() if v is None]
    if missing:
        logger.info(f"YTD missing for {len(missing)} tickers: {', '.join(missing[:8])}"
                    + ("..." if len(missing) > 8 else ""))
    return result


def fetch_post_earnings_move(
    ticker: str, earnings_date: str, timing: str | None
) -> PostEarningsMove | None:
    """
    Compute the post-earnings stock reaction for a ticker.

    BMO / DMH / unknown: move = close(earnings_date) / close(prior trading day) - 1
        → captures the day-of reaction to a pre-market or intraday announcement.
    AMC: move = close(next trading day) / close(earnings_date) - 1
        → AMC reports land after close; the reaction is the next day.

    Returns None if we don't yet have the closing prices needed (e.g. running
    the check the same evening as an AMC report — next-day data doesn't exist
    yet).
    """
    try:
        ed = date.fromisoformat(earnings_date)
    except ValueError:
        return None

    # Pull a window wide enough to cover weekends/holidays either side.
    start = ed - timedelta(days=10)
    end = ed + timedelta(days=10)

    buf_out, buf_err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(buf_out), redirect_stderr(buf_err):
            hist = yf.Ticker(ticker).history(
                start=start.isoformat(),
                end=end.isoformat(),
                auto_adjust=True,
            )
    except Exception as exc:
        logger.debug(f"post-earnings move fetch failed for {ticker}: {exc}")
        return None

    if hist is None or hist.empty:
        return None

    closes = hist["Close"].dropna()
    if closes.empty:
        return None

    # Normalize index to date objects for comparison.
    idx_dates = [d.date() for d in closes.index.to_pydatetime()]

    def _find_on_or_before(target: date) -> tuple[date, float] | None:
        for d, px in sorted(zip(idx_dates, closes.values), reverse=True):
            if d <= target:
                return d, float(px)
        return None

    def _find_on_or_after(target: date) -> tuple[date, float] | None:
        for d, px in zip(idx_dates, closes.values):
            if d >= target:
                return d, float(px)
        return None

    timing_l = (timing or "").lower()

    if timing_l == "amc":
        ref = _find_on_or_before(ed)
        post = _find_on_or_after(ed + timedelta(days=1))
        if not ref or not post or post[0] == ref[0]:
            return None
        ref_px, post_px = ref[1], post[1]
        window_label = "next-day close vs earnings-day close"
    else:
        # BMO, DMH, or unknown timing
        post = _find_on_or_after(ed)
        if not post:
            return None
        ref = _find_on_or_before(post[0] - timedelta(days=1))
        if not ref:
            return None
        ref_px, post_px = ref[1], post[1]
        window_label = "earnings-day close vs prior close"

    if ref_px == 0:
        return None
    move_pct = (post_px - ref_px) / ref_px * 100
    return PostEarningsMove(move_pct=move_pct, window_label=window_label)


def fetch_yfinance_earnings_date(ticker: str) -> list[date] | None:
    """
    Return yfinance's current earnings date(s) for a ticker as a list of
    date objects. May be a single date or a [start, end] range when
    yfinance has a window rather than a specific day. Returns None if
    yfinance has no data or the call fails.

    Used as a cheap sanity check on Finnhub's date. yfinance itself
    scrapes Yahoo Finance and is imperfect — treat disagreements as
    "please verify manually", not as an automatic override.
    """
    buf_out, buf_err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(buf_out), redirect_stderr(buf_err):
            cal = yf.Ticker(ticker).calendar
    except Exception as exc:
        logger.debug(f"yfinance calendar fetch failed for {ticker}: {exc}")
        return None

    if not cal:
        return None

    # yfinance typically returns a dict; handle DataFrame just in case
    if isinstance(cal, dict):
        raw = cal.get("Earnings Date")
    elif hasattr(cal, "loc") and hasattr(cal, "index"):
        if "Earnings Date" not in cal.index:
            return None
        try:
            raw = cal.loc["Earnings Date"].values.tolist()
        except Exception:
            return None
    else:
        return None

    if raw is None:
        return None
    if not isinstance(raw, (list, tuple)):
        raw = [raw]

    result: list[date] = []
    for d in raw:
        if isinstance(d, date):
            result.append(d)
        elif hasattr(d, "date"):
            result.append(d.date())
        else:
            try:
                result.append(date.fromisoformat(str(d)[:10]))
            except (ValueError, TypeError):
                continue
    return result or None


def fetch_yfinance_earnings_datetime(ticker: str) -> list[datetime] | None:
    """
    Return a list of UTC tz-aware datetimes for yfinance's known earnings
    timestamps for `ticker`, with time-of-day preserved so callers can
    infer pre-market vs post-market timing when Finnhub didn't populate
    its `hour` field.

    Reads from `Ticker.info`, NOT `Ticker.calendar`. The high-level
    `Ticker.calendar` and `Ticker.get_earnings_dates()` accessors
    normalize to plain dates (their wrapper code strips time-of-day),
    making them unusable for BMO/AMC inference. The Yahoo quoteSummary
    fields exposed via `info` retain Unix-second precision.

    Fields read (each a Unix timestamp in seconds):
      * `earningsTimestamp`       — most recent release (past or imminent)
      * `earningsTimestampStart`  — next upcoming release (often estimated)
      * `earningsTimestampEnd`    — usually equal to Start

    For tickers reporting imminently, all three coincide. For tickers in
    a between-quarters state, `earningsTimestamp` is the past quarter
    and Start/End is the next-quarter estimate. Callers do per-date
    matching to find the relevant timestamp.
    """
    buf_out, buf_err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(buf_out), redirect_stderr(buf_err):
            info = yf.Ticker(ticker).info
    except Exception as exc:
        logger.debug(f"yfinance info fetch failed for {ticker}: {exc}")
        return None

    if not info:
        return None

    candidates: list[datetime] = []
    seen: set[int] = set()
    for key in ("earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd"):
        ts = info.get(key)
        # Yahoo uses Unix seconds; sanity-bound to roughly 2001..2128 to
        # reject garbage (-1, 0, ms-precision values, strings, etc.).
        if not isinstance(ts, (int, float)):
            continue
        ts_int = int(ts)
        if ts_int < 1_000_000_000 or ts_int > 5_000_000_000:
            continue
        if ts_int in seen:
            continue
        seen.add(ts_int)
        try:
            candidates.append(datetime.fromtimestamp(ts_int, tz=timezone.utc))
        except (ValueError, OSError):
            continue

    return candidates or None


def infer_hour_from_datetime(dt: datetime) -> str | None:
    """Classify a yfinance earnings datetime into 'bmo' / 'amc' / None
    based on US market session boundaries.

    Rule:
      < 09:30 America/New_York  -> 'bmo'  (pre-market release)
      >= 16:00 America/New_York -> 'amc'  (post-close release)
      otherwise                  -> None  (mid-session, ambiguous)

    The 09:30 / 16:00 boundaries are NYSE/NASDAQ regular hours. A common
    pattern is press releases at 06:30 ET (BMO) or 16:05 ET (AMC); both
    classify cleanly. The 8:30 ET pre-open release pattern that the
    earlier <9 / >16 heuristic would have classified as None is now
    correctly captured as 'bmo'.
    """
    if dt is None:
        return None
    if ZoneInfo is None:
        return None
    try:
        et = dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        return None
    open_t = time(9, 30)
    close_t = time(16, 0)
    if et.time() < open_t:
        return "bmo"
    if et.time() >= close_t:
        return "amc"
    return None


def fetch_yfinance_hour_for_date(ticker: str, earnings_date: str) -> str | None:
    """Convenience wrapper: query yfinance, find a timestamp whose local
    America/New_York date matches `earnings_date` (ISO YYYY-MM-DD), and
    infer 'bmo'/'amc' from its time-of-day.

    Date matching is done in America/New_York so a Yahoo timestamp like
    2026-05-04T20:00 UTC (= 16:00 ET) correctly matches earnings_date
    "2026-05-04" — not the UTC date, which on a midnight-UTC boundary
    could be a different calendar day.

    Returns None if yfinance has nothing for this ticker, no candidate
    matches the target date, or the matching candidate's time falls
    inside market hours (mid-session releases are ambiguous). Safe to
    call from daily sync — failures are logged at debug level only.
    """
    dts = fetch_yfinance_earnings_datetime(ticker)
    if not dts:
        return None
    if ZoneInfo is None:
        return None
    tz = ZoneInfo("America/New_York")
    for dt in dts:
        try:
            local = dt.astimezone(tz)
        except Exception:
            continue
        if local.date().isoformat() == earnings_date:
            return infer_hour_from_datetime(dt)
    return None
