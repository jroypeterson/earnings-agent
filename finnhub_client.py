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
