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

import yfinance as yf

logger = logging.getLogger("earnings_agent")


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
