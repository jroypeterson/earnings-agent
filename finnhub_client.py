"""
Finnhub API client — earnings calendar queries with chunking,
retry with exponential backoff, and specific exception handling.
"""

import time
import logging
from datetime import date, timedelta

import finnhub
from requests.exceptions import RequestException, Timeout, ConnectionError as ReqConnectionError

from config import (
    FINNHUB_API_KEY,
    CHUNK_DAYS,
    CHUNK_SLEEP,
    FINNHUB_MAX_RESULTS,
    RETRY_MAX_ATTEMPTS,
    RETRY_BASE_DELAY,
)

logger = logging.getLogger("earnings_agent")


class FinnhubError(Exception):
    """Raised when Finnhub API returns an error we can't retry."""
    pass


def _retry(func, *args, **kwargs):
    """
    Retry a function with exponential backoff.
    Retries on transient network errors and rate limits.
    """
    last_exc = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return func(*args, **kwargs)
        except (Timeout, ReqConnectionError) as exc:
            last_exc = exc
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"Transient error (attempt {attempt}/{RETRY_MAX_ATTEMPTS}): {exc}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
        except finnhub.FinnhubAPIException as exc:
            # Rate limit (429) is retryable; other API errors are not
            if "429" in str(exc) or "rate limit" in str(exc).lower():
                last_exc = exc
                if attempt < RETRY_MAX_ATTEMPTS:
                    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        f"Rate limited (attempt {attempt}/{RETRY_MAX_ATTEMPTS}). "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
            else:
                raise FinnhubError(f"Finnhub API error: {exc}") from exc
        except RequestException as exc:
            last_exc = exc
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"Request error (attempt {attempt}/{RETRY_MAX_ATTEMPTS}): {exc}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

    raise FinnhubError(f"Failed after {RETRY_MAX_ATTEMPTS} attempts: {last_exc}") from last_exc


def get_client() -> finnhub.Client:
    """Create a Finnhub API client."""
    if not FINNHUB_API_KEY:
        raise FinnhubError("FINNHUB_API_KEY not configured")
    return finnhub.Client(api_key=FINNHUB_API_KEY)


def _fetch_chunk(
    client: finnhub.Client,
    start: date,
    end: date,
    ticker_set: set[str],
    depth: int = 0,
) -> tuple[list[dict], int]:
    """
    Fetch a single date-range chunk, adaptively splitting on cap-hit.

    Returns (matched_events, total_events_fetched_in_subtree).
    Raises FinnhubError if the cap is still hit at 1-day granularity,
    or if retries are exhausted inside _retry — both are run-aborting
    conditions because continuing would yield silently-incomplete data.
    """
    span_days = (end - start).days
    chunk_from = start.isoformat()
    chunk_to = end.isoformat()

    # _retry raises FinnhubError on exhausted retries. We let it propagate
    # — swallowing here is what caused silent data loss.
    result = _retry(
        client.earnings_calendar,
        _from=chunk_from,
        to=chunk_to,
        symbol="",
        international=False,
    )
    all_earnings = result.get("earningsCalendar", [])
    chunk_matches = [
        e for e in all_earnings
        if e.get("symbol", "").upper() in ticker_set
    ]
    indent = "  " * (depth + 1)
    logger.info(
        f"{indent}{chunk_from} -> {chunk_to} (span={span_days}d): "
        f"{len(all_earnings)} total, {len(chunk_matches)} matched"
    )

    if len(all_earnings) < FINNHUB_MAX_RESULTS:
        return chunk_matches, len(all_earnings)

    # Cap hit. Finnhub silently truncated — we must re-fetch at smaller
    # granularity or abort. A 1-day chunk at the cap means >= 1500 earnings
    # reports in a single day, which shouldn't happen in our universe; if it
    # does, we have no way to paginate and cannot trust completeness.
    if span_days <= 1:
        raise FinnhubError(
            f"Finnhub cap ({FINNHUB_MAX_RESULTS}) hit at 1-day granularity "
            f"for {chunk_from}. Cannot guarantee complete data — aborting."
        )

    mid = start + timedelta(days=max(span_days // 2, 1))
    logger.warning(
        f"{indent}Cap hit at {span_days}d span. Splitting at {mid.isoformat()}."
    )
    left_matched, left_total = _fetch_chunk(client, start, mid, ticker_set, depth + 1)
    time.sleep(CHUNK_SLEEP)
    right_matched, right_total = _fetch_chunk(client, mid, end, ticker_set, depth + 1)
    return left_matched + right_matched, left_total + right_total


def fetch_earnings(
    client: finnhub.Client,
    tickers: list[str],
    from_date: str,
    to_date: str,
) -> list[dict]:
    """
    Query Finnhub earnings calendar in date-range chunks, filtering
    client-side for our watchlist.

    Chunks that hit Finnhub's 1500-result cap are adaptively split until
    they clear or bottom out at 1-day granularity. If a 1-day chunk still
    hits the cap, or any chunk's retries are exhausted, this function
    raises FinnhubError — silent incomplete data is worse than a loud
    failed run that fires the on-failure Slack alert.
    """
    logger.info(f"Querying Finnhub earnings calendar: {from_date} -> {to_date}")

    ticker_set = {t.upper() for t in tickers}
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    matched: list[dict] = []
    total_fetched = 0

    while start < end:
        chunk_end = min(start + timedelta(days=CHUNK_DAYS), end)
        chunk_matched, chunk_total = _fetch_chunk(
            client, start, chunk_end, ticker_set
        )
        matched.extend(chunk_matched)
        total_fetched += chunk_total
        start = chunk_end
        time.sleep(CHUNK_SLEEP)

    logger.info(
        f"Scanned {total_fetched} total earnings across all chunks. "
        f"Matched {len(matched)} events for {len(tickers)} tickers."
    )
    return matched


# Per-symbol probes allowed per run for events the bulk calendar missed. The
# bulk scan normally leaves 0-1 Tier 1/2 events unseen, so this is a ceiling on
# a pathological run, not a working budget (free tier: 60 calls/min).
UNSEEN_PROBE_MAX = 10


def probe_foreign_listing_dates(
    client: finnhub.Client,
    pairs: list[tuple[str, str]],
    max_probes: int = UNSEEN_PROBE_MAX,
) -> set[tuple[str, str]]:
    """Confirm bulk-unseen (ticker, event_date) pairs by asking Finnhub by symbol.

    Why (board #450, 2026-10-08): Finnhub re-homed argenx's event from the ADR
    `ARGX` to its Euronext Brussels primary, `ARGX.BR`. The bulk calendar is
    US-only (`international=False`; the international feed is premium and
    401s), so the event vanished from it and B2 alerted "missing" for 29
    straight runs on a date argenx itself publishes. A query BY SYMBOL
    (`symbol="ARGX"`) still answers on the free tier, returning the row under
    `ARGX.BR`.

    Returns the subset of `pairs` the vendor confirms on the SAME date under
    the queried symbol or a dotted listing of it (`ARGX` -> `ARGX.BR`). Only
    the date is used: a foreign listing's estimates/actuals are in the home
    currency (ARGX.BR revenue estimate EUR 1.69B vs the ADR's USD 1.86B), so
    they must never be merged into an event. A different date is NOT a
    confirmation - that is exactly the date move the unseen alert exists for.

    Best-effort: any per-probe failure is logged and leaves that pair unseen,
    which is today's behaviour.
    """
    confirmed: set[tuple[str, str]] = set()
    for i, (ticker, event_date) in enumerate(pairs):
        if i >= max_probes:
            logger.warning(
                f"B2 probe: cap of {max_probes} reached; {len(pairs) - i} "
                f"unseen event(s) not probed this run"
            )
            break
        want = ticker.upper()
        try:
            res = _retry(
                client.earnings_calendar,
                _from=event_date,
                to=event_date,
                symbol=want,
                international=False,
            )
        except Exception as exc:  # best-effort; never abort the sync
            logger.warning(f"B2 probe: {want} {event_date} failed: {exc}")
            continue
        rows = (res or {}).get("earningsCalendar") or []
        for e in rows:
            sym = (e.get("symbol") or "").upper()
            if sym != want and sym.split(".", 1)[0] != want:
                continue
            if e.get("date") == event_date:
                confirmed.add((ticker, event_date))
                logger.info(
                    f"B2 probe: {want} {event_date} confirmed by Finnhub "
                    f"under {sym} (absent from the US bulk calendar)"
                )
                break
        else:
            logger.info(
                f"B2 probe: {want} {event_date} not confirmed by symbol query "
                f"({len(rows)} row(s): "
                f"{[(r.get('symbol'), r.get('date')) for r in rows][:3]})"
            )
        if i + 1 < min(len(pairs), max_probes):
            time.sleep(CHUNK_SLEEP)
    return confirmed
