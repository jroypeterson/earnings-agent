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
import re
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


# Request-health counters so callers can detect SYSTEMIC EDGAR degradation
# (network down, SEC blocking/rate-limiting, malformed responses) vs a
# legitimate 404 (no such filing/CIK). Without this, a total SEC outage looks
# identical to "no filings found" — which would make the missed-results
# backstop silently useless. Callers reset before a batch and read after.
_edgar_requests = 0
_edgar_failures = 0


def reset_request_stats() -> None:
    global _edgar_requests, _edgar_failures
    _edgar_requests = 0
    _edgar_failures = 0


def get_request_stats() -> tuple[int, int]:
    """Return (requests_made, hard_failures) since the last reset. A 404 is NOT
    counted as a failure — it's a valid 'not found'."""
    return _edgar_requests, _edgar_failures


def _get_json(url: str) -> dict | None:
    global _edgar_requests, _edgar_failures
    _edgar_requests += 1
    _sleep_for_rate_limit()
    try:
        r = requests.get(url, headers=_SEC_HEADERS, timeout=30)
    except requests.RequestException as exc:
        logger.debug(f"EDGAR GET failed {url}: {exc}")
        _edgar_failures += 1
        return None
    if r.status_code == 404:
        return None  # legitimate "not found" — not a degradation
    if r.status_code != 200:
        logger.debug(f"EDGAR GET {url}: HTTP {r.status_code}")
        _edgar_failures += 1
        return None
    try:
        return r.json()
    except ValueError:
        logger.debug(f"EDGAR GET {url}: non-JSON response")
        _edgar_failures += 1
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


def find_earnings_release_filing(
    ticker: str, window_start: date, window_end: date
) -> Filing8K | None:
    """Search SEC EDGAR for an 8-K Item 2.02 ('Results of Operations and
    Financial Condition') filed in the inclusive window [window_start,
    window_end]. Returns the most recent matching filing, or None.

    8-K Item 2.02 IS the earnings release by SEC definition — when a
    filing exists in the window, its filing date is ground truth for the
    earnings event date. Used by the cross-check tiebreaker as an
    authoritative signal beyond Finnhub vs yfinance.
    """
    if window_end < window_start:
        return None

    # Cap days_back so we always have enough history to cover the window
    # plus a small buffer (filings are sometimes filed a few days late).
    today = date.today()
    days_back = max(7, (today - window_start).days + 7)
    filings = fetch_8k_filings(ticker, days_back=days_back, earnings_only=True)
    for f in filings:
        try:
            fd = date.fromisoformat(f.filing_date)
        except ValueError:
            continue
        if window_start <= fd <= window_end:
            return f
    return None


# ---------------------------------------------------------------------------
# 6-K fetch (foreign private issuers — no 8-K, no Item 2.02 taxonomy)
# ---------------------------------------------------------------------------

# Foreign private issuers (ICLR, etc.) furnish results on a 6-K, which carries
# NO `items` codes and a generic "6-K" primaryDocDescription — so the 8-K
# Item 2.02 path is blind to them, leaving phantom/flapping Finnhub dates
# uncorrected. The one usable metadata signal is the primaryDocument FILENAME,
# which issuers tend to name after the content (e.g. ICON: `iconplc6kq425.htm`,
# `iconearningscall…htm`). These heuristics classify a 6-K's filename as
# likely-earnings; a hit is treated as a *signal*, never proof — it flows
# through the SAME yfinance ±1d corroboration gate as the 8-K path before any
# auto-lock, and is surfaced (not locked) when uncorroborated.
_RESULTS_6K_POSITIVE = re.compile(
    r"(earnings|results|quarterly|interim[-_ ]?report|"
    r"6kq[1-4]|q[1-4][-_ ]?\d{2}|[1-4]q[-_ ]?\d{2}|\bh[12][-_ ]?\d{2}\b|fy\d{2})",
    re.IGNORECASE,
)
_RESULTS_6K_EXCLUDE = re.compile(
    r"(non[-_ ]?reliance|restat|prospectus|annual[-_ ]?general|\bagm\b|dividend|"
    r"conference|fireside|particip|loan|offering|registration|notes[-_ ]?due|"
    r"buyback|repurchase|appoint|director|schedule|provides[-_ ]?upd|announceme?n?t?s?$)",
    re.IGNORECASE,
)


def is_likely_earnings_6k_doc(primary_document: str, primary_doc_description: str = "") -> bool:
    """Heuristic: does this 6-K's filename/description look like an earnings release?

    Requires a positive earnings token AND no disqualifying token (a conference,
    dividend, non-reliance notice, etc.). Pure + side-effect-free so it is unit
    tested against real issuer filenames. Deliberately conservative: a miss just
    means no auto-correct for that filing; a false hit is caught by the
    downstream corroboration gate before anything is locked."""
    blob = f"{primary_document or ''} {primary_doc_description or ''}"
    if _RESULTS_6K_EXCLUDE.search(blob):
        return False
    return bool(_RESULTS_6K_POSITIVE.search(blob))


def fetch_6k_filings(ticker: str, days_back: int = 400) -> list[Filing8K]:
    """Return recent 6-K filings whose filename looks like an earnings release.

    Mirrors `fetch_8k_filings` but for foreign private issuers: 6-Ks have no
    `items`, so the earnings filter is the `is_likely_earnings_6k_doc` filename
    heuristic instead of an Item 2.02 match. Returns Filing8K records (form
    "6-K"), most-recent-first."""
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
    docs = recent.get("primaryDocument", [])

    cutoff = date.today() - timedelta(days=days_back)
    out: list[Filing8K] = []
    for i, form in enumerate(forms):
        if form != "6-K":
            continue
        try:
            fd = date.fromisoformat(dates[i])
        except (ValueError, IndexError):
            continue
        if fd < cutoff:
            continue
        doc = docs[i] if i < len(docs) else ""
        desc = descs[i] if i < len(descs) else ""
        if not is_likely_earnings_6k_doc(doc, desc):
            continue
        out.append(Filing8K(
            form=form,
            filing_date=dates[i],
            accession=accs[i] if i < len(accs) else "",
            primary_doc_title=doc or desc,
            items=(),
        ))

    out.sort(key=lambda f: f.filing_date, reverse=True)
    return out


def find_results_6k(
    ticker: str, window_start: date, window_end: date
) -> Filing8K | None:
    """Foreign-filer analog of `find_earnings_release_filing`: the most recent
    earnings-looking 6-K filed in [window_start, window_end], or None.

    The 6-K date is a weaker signal than an 8-K Item 2.02 (no SEC item code
    proves it's the results), so callers MUST keep it behind the yfinance
    corroboration gate — never auto-lock on a 6-K alone."""
    if window_end < window_start:
        return None
    today = date.today()
    days_back = max(7, (today - window_start).days + 7)
    for f in fetch_6k_filings(ticker, days_back=days_back):
        try:
            fd = date.fromisoformat(f.filing_date)
        except ValueError:
            continue
        if window_start <= fd <= window_end:
            return f
    return None


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
