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


@dataclass
class YfinanceEarningsTimestamps:
    """yfinance earnings-timing fields, distinguished by event type.

    The press release and the conference call are distinct events that
    sometimes happen on different calendar days (e.g. UFPT: AMC release
    on Monday, BMO call on Tuesday). The calendar tracks the press
    release; the call is descriptive context. Keep them separate so
    callers don't conflate them.
    """
    release_candidates: list[datetime]   # earningsTimestamp / Start / End
    call_candidates: list[datetime]      # earningsCallTimestampStart / End
    is_estimate: bool | None             # isEarningsDateEstimate; None = absent


def _coerce_unix_seconds_to_utc(ts) -> datetime | None:
    """Convert a Yahoo Unix-seconds value to a UTC datetime, or None if junk."""
    if not isinstance(ts, (int, float)):
        return None
    ts_int = int(ts)
    # sanity-bound to roughly 2001..2128
    if ts_int < 1_000_000_000 or ts_int > 5_000_000_000:
        return None
    try:
        return datetime.fromtimestamp(ts_int, tz=timezone.utc)
    except (ValueError, OSError):
        return None


def fetch_yfinance_earnings_timestamps(ticker: str) -> YfinanceEarningsTimestamps | None:
    """
    Read yfinance Ticker.info and return distinct release vs call timestamps
    plus the isEarningsDateEstimate flag. Returns None if the ticker has
    no usable info (network failure, empty dict, etc.).

    Reads from `Ticker.info`, NOT `Ticker.calendar`. The high-level
    `Ticker.calendar` and `Ticker.get_earnings_dates()` accessors
    normalize to plain dates (their wrapper code strips time-of-day).
    The Yahoo quoteSummary fields exposed via `info` retain Unix-second
    precision.

    Fields read (each a Unix timestamp in seconds):
      Release timestamps:
        * earningsTimestamp       — most recent release (past or imminent)
        * earningsTimestampStart  — next upcoming release (often estimated)
        * earningsTimestampEnd    — usually equal to Start
      Call timestamps:
        * earningsCallTimestampStart — conference call start
        * earningsCallTimestampEnd   — conference call end
      Estimate flag:
        * isEarningsDateEstimate   — Yahoo's confidence in the upcoming date
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

    def _collect(keys: tuple[str, ...]) -> list[datetime]:
        out: list[datetime] = []
        seen: set[int] = set()
        for k in keys:
            dt = _coerce_unix_seconds_to_utc(info.get(k))
            if dt is None:
                continue
            ts_key = int(dt.timestamp())
            if ts_key in seen:
                continue
            seen.add(ts_key)
            out.append(dt)
        return out

    release = _collect((
        "earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd",
    ))
    call = _collect((
        "earningsCallTimestampStart", "earningsCallTimestampEnd",
    ))
    is_est = info.get("isEarningsDateEstimate")
    if not isinstance(is_est, bool):
        is_est = None

    if not release and not call and is_est is None:
        return None

    return YfinanceEarningsTimestamps(
        release_candidates=release,
        call_candidates=call,
        is_estimate=is_est,
    )


def fetch_yfinance_earnings_datetime(ticker: str) -> list[datetime] | None:
    """Backward-compat wrapper returning only release-timestamp candidates.

    Kept so the existing fetch_yfinance_hour_for_date implementation and
    any external callers of this name keep working without change.
    Internally just unwraps the release_candidates from the new
    fetch_yfinance_earnings_timestamps shape.
    """
    bundle = fetch_yfinance_earnings_timestamps(ticker)
    if bundle is None:
        return None
    return bundle.release_candidates or None


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
    """Convenience wrapper: match a release-timestamp candidate to the
    target press-release date and infer 'bmo'/'amc' from its time-of-day.

    Reads release candidates only (earningsTimestamp / Start / End). Call
    timestamps go through fetch_yfinance_call_for_date.

    Returns None if yfinance has nothing for this ticker, no release
    candidate matches the target date, or the matching candidate's time
    falls inside market hours (mid-session releases are ambiguous).
    """
    bundle = fetch_yfinance_earnings_timestamps(ticker)
    if bundle is None or not bundle.release_candidates:
        return None
    if ZoneInfo is None:
        return None
    tz = ZoneInfo("America/New_York")
    for dt in bundle.release_candidates:
        try:
            local = dt.astimezone(tz)
        except Exception:
            continue
        if local.date().isoformat() == earnings_date:
            return infer_hour_from_datetime(dt)
    return None


def fetch_yfinance_call_for_date(ticker: str, earnings_date: str) -> datetime | None:
    """Match a call-timestamp candidate to the target press-release date.

    A "match" here is loose because the call commonly happens the
    business day AFTER an AMC release (UFPT pattern: release Mon AMC,
    call Tue BMO). We accept any call candidate whose local America/
    New_York date is in {earnings_date, earnings_date + 1 business day}.

    Returns the UTC tz-aware datetime of the matching call candidate,
    or None if no plausible match exists. Callers render the result
    via the description's "Conference call: ..." line.
    """
    bundle = fetch_yfinance_earnings_timestamps(ticker)
    if bundle is None or not bundle.call_candidates:
        return None
    if ZoneInfo is None:
        return None
    tz = ZoneInfo("America/New_York")
    try:
        target = date.fromisoformat(earnings_date)
    except ValueError:
        return None

    # Allow today, today+1 (next-day call), and today+next-business-day
    # (skip weekend if release is Friday). Conservative window — ±1
    # business day from the release.
    next_day = target + timedelta(days=1)
    skip_weekend = target + timedelta(days=3 if target.weekday() == 4 else 1)
    valid_dates = {target.isoformat(), next_day.isoformat(), skip_weekend.isoformat()}

    for dt in bundle.call_candidates:
        try:
            local_date = dt.astimezone(tz).date().isoformat()
        except Exception:
            continue
        if local_date in valid_dates:
            return dt
    return None
