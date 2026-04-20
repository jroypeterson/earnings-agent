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
from datetime import date, timedelta

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
