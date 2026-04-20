"""
SEC EDGAR cross-check for earnings dates.

Uses the free data.sec.gov JSON API (no auth, no MCP, works in CI) to
fetch a company's recent 8-K filings. Earnings releases are filed as
8-Ks with Item 2.02 "Results of Operations and Financial Condition";
we use the filing date of the prior-year same-quarter 2.02 as a
cadence benchmark against which Finnhub and yfinance can be compared.

SEC fair-access policy requires a descriptive User-Agent containing
contact info. Configure via SEC_EDGAR_USER_AGENT env var; the default
is derived from config.py.
"""

from __future__ import annotations

import json
import time
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests

from config import SEC_EDGAR_USER_AGENT

logger = logging.getLogger("earnings_agent")


class EdgarError(Exception):
    """Raised when EDGAR returns a non-retryable error or malformed data."""
    pass


_SEC_HEADERS = {"User-Agent": SEC_EDGAR_USER_AGENT, "Accept": "application/json"}

# Earnings-related 8-K items.
# 2.02 = Results of Operations and Financial Condition (THE earnings release).
# 7.01 / 8.01 occasionally carry pre-announcements of the release date but are
# noisy; we use 2.02 as the primary signal.
_EARNINGS_RELEASE_ITEM = "2.02"

# SEC fair-access: max 10 requests/second. We add a small delay between calls.
_SEC_MIN_INTERVAL_SEC = 0.12
_last_request_ts = 0.0

_CIK_CACHE_PATH = Path(__file__).parent / ".ticker_cik_cache.json"
_CIK_CACHE_MAX_AGE_DAYS = 30


def _sleep_for_rate_limit():
    """Minimal self-throttle so we don't exceed SEC's 10 req/s ceiling."""
    global _last_request_ts
    elapsed = time.monotonic() - _last_request_ts
    if elapsed < _SEC_MIN_INTERVAL_SEC:
        time.sleep(_SEC_MIN_INTERVAL_SEC - elapsed)
    _last_request_ts = time.monotonic()


def _get_json(url: str) -> dict | None:
    _sleep_for_rate_limit()
    try:
        r = requests.get(url, headers=_SEC_HEADERS, timeout=30)
    except requests.RequestException as exc:
        logger.debug(f"EDGAR GET failed {url}: {exc}")
        return None
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        logger.debug(f"EDGAR GET {url}: HTTP {r.status_code}")
        return None
    try:
        return r.json()
    except ValueError:
        logger.debug(f"EDGAR GET {url}: non-JSON response")
        return None


@dataclass
class Filing8K:
    form: str
    filing_date: str  # ISO date
    accession: str
    primary_doc_title: str
    items: tuple[str, ...]


# ---------------------------------------------------------------------------
# Ticker -> CIK (cached)
# ---------------------------------------------------------------------------


def _load_cik_map() -> dict[str, str]:
    if _CIK_CACHE_PATH.exists():
        mtime = _CIK_CACHE_PATH.stat().st_mtime
        age_days = (time.time() - mtime) / 86400
        if age_days < _CIK_CACHE_MAX_AGE_DAYS:
            try:
                return json.loads(_CIK_CACHE_PATH.read_text())
            except (OSError, ValueError):
                pass

    raw = _get_json("https://www.sec.gov/files/company_tickers.json")
    if not raw:
        return {}
    mapping: dict[str, str] = {}
    for entry in raw.values():
        ticker = str(entry.get("ticker", "")).upper()
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            mapping[ticker] = str(cik).zfill(10)
    try:
        _CIK_CACHE_PATH.write_text(json.dumps(mapping))
    except OSError as exc:
        logger.debug(f"Could not write CIK cache: {exc}")
    return mapping


def get_cik(ticker: str) -> str | None:
    """Return the 10-digit zero-padded CIK for a US-listed ticker, or None."""
    return _load_cik_map().get(ticker.upper())


# ---------------------------------------------------------------------------
# 8-K fetch
# ---------------------------------------------------------------------------


def fetch_8k_filings(
    ticker: str, days_back: int = 400, earnings_only: bool = True
) -> list[Filing8K]:
    """
    Return recent 8-K filings for `ticker` going back `days_back` days
    (default ~13 months, so prior-year same-quarter comparisons always
    have at least one reference point).
    """
    cik = get_cik(ticker)
    if not cik:
        return []

    data = _get_json(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if not data:
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accs = recent.get("accessionNumber", [])
    descs = recent.get("primaryDocDescription", [])
    items_col = recent.get("items", [])

    cutoff = date.today() - timedelta(days=days_back)
    out: list[Filing8K] = []
    for i, form in enumerate(forms):
        if form != "8-K":
            continue
        try:
            fd = date.fromisoformat(dates[i])
        except (ValueError, IndexError):
            continue
        if fd < cutoff:
            continue
        raw_items = items_col[i] if i < len(items_col) else ""
        items = tuple(x.strip() for x in raw_items.split(",") if x.strip())
        if earnings_only and _EARNINGS_RELEASE_ITEM not in items:
            continue
        out.append(Filing8K(
            form=form,
            filing_date=dates[i],
            accession=accs[i] if i < len(accs) else "",
            primary_doc_title=descs[i] if i < len(descs) else "",
            items=items,
        ))

    out.sort(key=lambda f: f.filing_date, reverse=True)
    return out


# ---------------------------------------------------------------------------
# Cadence inference: find the prior-year same-quarter 2.02 filing
# ---------------------------------------------------------------------------


@dataclass
class EdgarCadenceSignal:
    """
    Prior-year same-quarter earnings release date for a ticker, used as
    a cadence benchmark against the current Finnhub/yfinance dates.
    """
    ticker: str
    reference_date: str       # prior-year same-quarter 2.02 filing date
    target_date: str          # the upcoming date we're comparing against
    days_from_ref: int        # signed: target - reference (mod +/-365)
    commentary: str           # human-readable summary


def _signed_days_from_anniversary(target: date, reference: date) -> int:
    """
    Compute (target - reference's one-year-later) in days. Positive means
    target is AFTER the anniversary of reference; negative means before.
    """
    anniversary = date(target.year, reference.month, reference.day) \
        if (reference.month, reference.day) != (2, 29) \
        else date(target.year, 2, 28)
    return (target - anniversary).days


def infer_cadence_signal(
    ticker: str, target_date: str, window_days: int = 45
) -> EdgarCadenceSignal | None:
    """
    Look for an Item 2.02 8-K filed roughly one year before `target_date`
    (± window_days). Return a signal describing how far `target_date` is
    from that anniversary.

    Returns None if EDGAR has no CIK for the ticker, or no same-quarter
    2.02 filing was found in the expected window.
    """
    try:
        target = date.fromisoformat(target_date)
    except ValueError:
        return None

    filings = fetch_8k_filings(ticker, days_back=400, earnings_only=True)
    if not filings:
        return None

    # Anniversary of target - 1 year
    try:
        anniversary = date(target.year - 1, target.month, target.day)
    except ValueError:  # Feb 29 edge case
        anniversary = date(target.year - 1, target.month, 28)

    best: Filing8K | None = None
    best_offset = window_days + 1
    for f in filings:
        try:
            fd = date.fromisoformat(f.filing_date)
        except ValueError:
            continue
        offset = abs((fd - anniversary).days)
        if offset <= window_days and offset < best_offset:
            best = f
            best_offset = offset

    if not best:
        return None

    ref_dt = date.fromisoformat(best.filing_date)
    days_from_ref = _signed_days_from_anniversary(target, ref_dt)
    sign = "+" if days_from_ref >= 0 else "-"
    commentary = (
        f"Prior-year Q release: {best.filing_date} "
        f"(target is {sign}{abs(days_from_ref)}d from anniversary)"
    )
    return EdgarCadenceSignal(
        ticker=ticker,
        reference_date=best.filing_date,
        target_date=target_date,
        days_from_ref=days_from_ref,
        commentary=commentary,
    )
