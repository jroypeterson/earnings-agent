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


def fetch_earnings(
    client: finnhub.Client,
    tickers: list[str],
    from_date: str,
    to_date: str,
) -> list[dict]:
    """
    Query Finnhub earnings calendar in date-range chunks to stay under
    the 1500-result cap, then filter client-side for our watchlist.
    """
    logger.info(f"Querying Finnhub earnings calendar: {from_date} -> {to_date}")

    ticker_set = {t.upper() for t in tickers}
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    matched = []
    total_fetched = 0

    while start < end:
        chunk_end = min(start + timedelta(days=CHUNK_DAYS), end)
        chunk_from = start.isoformat()
        chunk_to = chunk_end.isoformat()

        try:
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
            total_fetched += len(all_earnings)
            matched.extend(chunk_matches)

            logger.info(
                f"  {chunk_from} -> {chunk_to}: "
                f"{len(all_earnings)} total, {len(chunk_matches)} matched"
            )

            if len(all_earnings) >= FINNHUB_MAX_RESULTS:
                logger.warning(
                    f"  Chunk returned {FINNHUB_MAX_RESULTS} results (cap hit). "
                    f"Consider reducing CHUNK_DAYS below {CHUNK_DAYS}."
                )

        except FinnhubError as exc:
            logger.error(f"  Chunk {chunk_from} -> {chunk_to} failed: {exc}")

        start = chunk_end
        time.sleep(CHUNK_SLEEP)

    logger.info(
        f"Scanned {total_fetched} total earnings across all chunks. "
        f"Matched {len(matched)} events for {len(tickers)} tickers."
    )
    return matched
