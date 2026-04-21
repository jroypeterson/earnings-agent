"""
Google Calendar operations — create, update, delete, find, and cleanup
earnings events with retry logic and pagination.
"""

import re
import time
import logging
from datetime import date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    GOOGLE_CREDENTIALS_PATH,
    GOOGLE_CALENDAR_ID,
    CALENDAR_SCOPES,
    CALENDAR_PAGE_SIZE,
    TIMEZONE,
    TIMING_LABELS,
    RETRY_MAX_ATTEMPTS,
    RETRY_BASE_DELAY,
)

logger = logging.getLogger("earnings_agent")


class CalendarError(Exception):
    """Raised when a Calendar API operation fails after retries."""
    pass


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


def _retry_calendar(func, *args, **kwargs):
    """Retry Calendar API calls with exponential backoff on transient errors."""
    last_exc = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return func(*args, **kwargs)
        except HttpError as exc:
            status = exc.resp.status if exc.resp else 0
            # Retry on 429 (rate limit), 500, 502, 503, 504
            if status in (429, 500, 502, 503, 504):
                last_exc = exc
                if attempt < RETRY_MAX_ATTEMPTS:
                    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        f"Calendar API error {status} (attempt {attempt}/{RETRY_MAX_ATTEMPTS}). "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                continue
            # Non-retryable HTTP errors
            raise CalendarError(f"Calendar API error (HTTP {status}): {exc}") from exc
        except (TimeoutError, ConnectionError) as exc:
            last_exc = exc
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"Connection error (attempt {attempt}/{RETRY_MAX_ATTEMPTS}): {exc}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

    raise CalendarError(
        f"Calendar API failed after {RETRY_MAX_ATTEMPTS} attempts: {last_exc}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Service initialization
# ---------------------------------------------------------------------------


def get_calendar_service():
    """Build an authenticated Google Calendar API service."""
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=CALENDAR_SCOPES
    )
    return build("calendar", "v3", credentials=creds)


# ---------------------------------------------------------------------------
# Event description builder
# ---------------------------------------------------------------------------


def _is_confirmed_hour(hour: str | None) -> bool:
    """Finnhub populates bmo/amc/dmh only when timing is announced."""
    return (hour or "").lower() in ("bmo", "amc", "dmh")


def build_description(
    ticker: str,
    hour: str | None,
    eps_estimate: float | None,
    eps_actual: float | None,
    revenue_estimate: float | None,
    revenue_actual: float | None,
) -> str:
    """Build the calendar event description, including actuals if available."""
    timing_str = TIMING_LABELS.get(hour, "Time TBD")

    lines = [
        f"Ticker: {ticker}",
        f"Timing: {timing_str}",
    ]

    has_actuals = eps_actual is not None or revenue_actual is not None
    if not has_actuals:
        status = "Confirmed" if _is_confirmed_hour(hour) else "Estimated (Finnhub has no timing)"
        lines.append(f"Status: {status}")

    # --- EPS section ---
    if eps_actual is not None and eps_estimate is not None:
        diff = eps_actual - eps_estimate
        pct = (diff / abs(eps_estimate) * 100) if eps_estimate != 0 else 0
        if diff > 0:
            verdict = f"BEAT by ${abs(diff):.2f} ({abs(pct):.1f}%)"
        elif diff < 0:
            verdict = f"MISS by ${abs(diff):.2f} ({abs(pct):.1f}%)"
        else:
            verdict = "IN LINE"
        lines.append(f"\nEPS: ${eps_actual:.2f} actual vs ${eps_estimate:.2f} est — {verdict}")
    elif eps_actual is not None:
        lines.append(f"\nEPS Actual: ${eps_actual:.2f}")
    elif eps_estimate is not None:
        lines.append(f"EPS Estimate: ${eps_estimate:.2f}")

    # --- Revenue section ---
    if revenue_actual is not None and revenue_estimate is not None:
        act_b = revenue_actual / 1_000_000_000
        est_b = revenue_estimate / 1_000_000_000
        diff = revenue_actual - revenue_estimate
        pct = (diff / abs(revenue_estimate) * 100) if revenue_estimate != 0 else 0
        if diff > 0:
            verdict = f"BEAT by {abs(pct):.1f}%"
        elif diff < 0:
            verdict = f"MISS by {abs(pct):.1f}%"
        else:
            verdict = "IN LINE"
        lines.append(f"Revenue: ${act_b:.2f}B actual vs ${est_b:.2f}B est — {verdict}")
    elif revenue_actual is not None:
        rev_b = revenue_actual / 1_000_000_000
        lines.append(f"Revenue Actual: ${rev_b:.2f}B")
    elif revenue_estimate is not None:
        rev_b = revenue_estimate / 1_000_000_000
        lines.append(f"Revenue Estimate: ${rev_b:.2f}B")

    if has_actuals:
        lines.append("\nREPORTED")

    lines.append(f"\nSource: Finnhub | Auto-generated by Earnings Agent")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Find events (with pagination)
# ---------------------------------------------------------------------------


def find_calendar_event(
    service,
    calendar_id: str,
    ticker: str,
    earnings_date: str,
    source_fingerprint: str | None = None,
):
    """
    Search Google Calendar for an existing earnings event.

    Uses extendedProperties to filter by ticker, then matches by
    source_fingerprint or date proximity. Paginates to avoid missing events.

    Returns the event dict if found, None otherwise.
    """
    dt = date.fromisoformat(earnings_date)
    time_min = (dt - timedelta(days=90)).isoformat() + "T00:00:00Z"
    time_max = (dt + timedelta(days=90)).isoformat() + "T00:00:00Z"

    try:
        page_token = None
        while True:
            request = service.events().list(
                calendarId=calendar_id,
                privateExtendedProperty=f"ticker={ticker}",
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                maxResults=CALENDAR_PAGE_SIZE,
                pageToken=page_token,
            )
            result = _retry_calendar(request.execute)

            for event in result.get("items", []):
                props = event.get("extendedProperties", {}).get("private", {})

                # Match by source_fingerprint if available
                if source_fingerprint and props.get("source_fingerprint") == source_fingerprint:
                    logger.info(f"Found existing calendar event for {ticker} on {earnings_date} via fingerprint")
                    return event

                # Match by date proximity (within 7 days)
                event_start = event.get("start", {})
                event_date_str = event_start.get("date") or (event_start.get("dateTime", "")[:10])
                if event_date_str:
                    try:
                        event_dt = date.fromisoformat(event_date_str)
                        if abs((event_dt - dt).days) <= 7:
                            logger.info(f"Found existing calendar event for {ticker} near {earnings_date} via date match")
                            return event
                    except ValueError:
                        pass

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    except CalendarError as exc:
        logger.warning(f"Calendar API lookup failed for {ticker} on {earnings_date}: {exc}")

    return None


# ---------------------------------------------------------------------------
# Create, update, delete events
# ---------------------------------------------------------------------------


def create_calendar_event(
    service,
    calendar_id: str,
    ticker: str,
    earnings_date: str,
    hour: str | None,
    *,
    quarter: str | None = None,
    eps_estimate: float | None = None,
    eps_actual: float | None = None,
    revenue_estimate: float | None = None,
    revenue_actual: float | None = None,
    tier: int = 3,
    source_fingerprint: str | None = None,
) -> str | None:
    """
    Create an earnings event on Google Calendar.
    Returns the created event's Google Calendar ID.
    """
    has_actuals = eps_actual is not None or revenue_actual is not None
    est_marker = "" if (has_actuals or _is_confirmed_hour(hour)) else " (est.)"
    summary = (
        f"{'[REPORTED]' if has_actuals else ''} {ticker} Earnings Release{est_marker}"
    ).strip()
    description = build_description(
        ticker, hour, eps_estimate, eps_actual, revenue_estimate, revenue_actual
    )

    if source_fingerprint is None:
        source_fingerprint = f"{ticker}:{earnings_date}"

    extended_props = {
        "private": {
            "earningsAgent": "true",
            "ticker": ticker,
            "source_fingerprint": source_fingerprint,
            "tier": str(tier),
        }
    }
    if quarter:
        extended_props["private"]["quarter"] = quarter

    if hour == "bmo":
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": f"{earnings_date}T07:00:00", "timeZone": TIMEZONE},
            "end": {"dateTime": f"{earnings_date}T07:15:00", "timeZone": TIMEZONE},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 60}]},
            "extendedProperties": extended_props,
        }
    elif hour == "amc":
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": f"{earnings_date}T16:30:00", "timeZone": TIMEZONE},
            "end": {"dateTime": f"{earnings_date}T16:45:00", "timeZone": TIMEZONE},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 60}]},
            "extendedProperties": extended_props,
        }
    else:
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"date": earnings_date},
            "end": {"date": earnings_date},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 720}]},
            "extendedProperties": extended_props,
        }

    request = service.events().insert(calendarId=calendar_id, body=event_body)
    created = _retry_calendar(request.execute)
    logger.info(f"Created calendar event: {summary} on {earnings_date}")
    return created.get("id")


def update_calendar_event_description(
    service,
    calendar_id: str,
    gcal_id: str,
    new_summary: str,
    new_description: str,
    *,
    ticker: str | None = None,
    quarter: str | None = None,
    source_fingerprint: str | None = None,
    tier: int | None = None,
):
    """Update an existing calendar event's summary, description, and extended properties."""
    get_request = service.events().get(calendarId=calendar_id, eventId=gcal_id)
    event = _retry_calendar(get_request.execute)
    event["summary"] = new_summary
    event["description"] = new_description

    # Ensure extendedProperties are present (backfill for pre-existing events)
    if ticker:
        props = event.setdefault("extendedProperties", {}).setdefault("private", {})
        props["earningsAgent"] = "true"
        props["ticker"] = ticker
        if quarter:
            props["quarter"] = quarter
        if source_fingerprint:
            props["source_fingerprint"] = source_fingerprint
        if tier is not None:
            props["tier"] = str(tier)

    update_request = service.events().update(
        calendarId=calendar_id, eventId=gcal_id, body=event
    )
    _retry_calendar(update_request.execute)


def delete_calendar_event(service, calendar_id: str, gcal_id: str):
    """Delete an event from Google Calendar."""
    request = service.events().delete(calendarId=calendar_id, eventId=gcal_id)
    _retry_calendar(request.execute)


# ---------------------------------------------------------------------------
# Summary parsing helper
# ---------------------------------------------------------------------------


def parse_ticker_from_summary(summary: str) -> str | None:
    """Extract ticker from event summaries like '[REPORTED] AAPL Earnings Release'."""
    m = re.match(r"^(?:\[REPORTED\]\s*)?(?:[^\w]*)?(\w+)\s+Earnings\s+Release", summary)
    return m.group(1).upper() if m else None


# ---------------------------------------------------------------------------
# Cleanup duplicates
# ---------------------------------------------------------------------------


def cleanup_duplicates(conn, dry_run: bool = False):
    """
    Scan Google Calendar for duplicate earnings events and delete extras.

    Fetches both tagged events (extendedProperties earningsAgent=true) and
    legacy events (matched by summary pattern), merges them into a single
    ticker|date grouping, and dedupes. The keeper prefers a tagged event over
    a legacy one; ties break on newest creation time.
    """
    from storage import upsert_event, date_to_quarter

    if not GOOGLE_CALENDAR_ID:
        logger.error("Missing GOOGLE_CALENDAR_ID")
        return

    cal_service = get_calendar_service()

    today = date.today()
    time_min = (today - timedelta(days=365)).isoformat() + "T00:00:00Z"
    time_max = (today + timedelta(days=365)).isoformat() + "T00:00:00Z"

    # -- Fetch tagged events --
    logger.info("Fetching events with extendedProperties...")
    tagged_events = []
    page_token = None
    while True:
        request = cal_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            privateExtendedProperty="earningsAgent=true",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=CALENDAR_PAGE_SIZE,
            pageToken=page_token,
        )
        result = _retry_calendar(request.execute)
        tagged_events.extend(result.get("items", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    tagged_ids = {e["id"] for e in tagged_events}
    logger.info(f"  Found {len(tagged_events)} tagged events")

    # -- Fetch legacy events (summary search, minus tagged) --
    logger.info("Fetching legacy events by summary pattern...")
    legacy_events = []
    page_token = None
    while True:
        request = cal_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            q="Earnings Release",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=CALENDAR_PAGE_SIZE,
            pageToken=page_token,
        )
        result = _retry_calendar(request.execute)
        legacy_events.extend(result.get("items", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    legacy_events = [e for e in legacy_events if e["id"] not in tagged_ids]
    logger.info(f"  Found {len(legacy_events)} legacy events (without extendedProperties)")

    # -- Build unified groups keyed by ticker|date --
    groups: dict[str, list[dict]] = {}

    for event in tagged_events:
        props = event.get("extendedProperties", {}).get("private", {})
        ticker = props.get("ticker")
        if not ticker:
            continue
        start = event.get("start", {})
        event_date = start.get("date") or start.get("dateTime", "")[:10]
        if not event_date:
            continue
        groups.setdefault(f"{ticker}|{event_date}", []).append(event)

    for event in legacy_events:
        ticker = parse_ticker_from_summary(event.get("summary", ""))
        if not ticker:
            continue
        start = event.get("start", {})
        event_date = start.get("date") or start.get("dateTime", "")[:10]
        if not event_date:
            continue
        groups.setdefault(f"{ticker}|{event_date}", []).append(event)

    deleted_count = 0
    for key, events in groups.items():
        deleted_count += _dedup_group(
            events, key.replace("|", " "), cal_service, conn, dry_run
        )

    logger.info("=" * 50)
    if dry_run:
        logger.info(f"Cleanup dry run: would delete {deleted_count} duplicate(s)")
    else:
        logger.info(f"Cleanup complete: deleted {deleted_count} duplicate(s)")
    logger.info("=" * 50)


def _dedup_group(
    events: list[dict],
    label: str,
    cal_service,
    conn,
    dry_run: bool,
) -> int:
    """Keep the newest event in a group, delete the rest."""
    from storage import upsert_event, date_to_quarter

    if len(events) <= 1:
        return 0

    def _is_tagged(e: dict) -> bool:
        return bool(e.get("extendedProperties", {}).get("private", {}).get("earningsAgent"))

    events.sort(key=lambda e: (_is_tagged(e), e.get("created", "")), reverse=True)
    keep = events[0]
    dupes = events[1:]

    logger.info(
        f"  {label}: {len(events)} events found, "
        f"keeping {keep['id']}, deleting {len(dupes)} duplicate(s)"
    )

    deleted = 0
    for dupe in dupes:
        if dry_run:
            logger.info(f"    [dry-run] Would delete {dupe['id']} ({dupe.get('summary', '')})")
        else:
            try:
                delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, dupe["id"])
                logger.info(f"    Deleted {dupe['id']}")
            except CalendarError as exc:
                logger.error(f"    Failed to delete {dupe['id']}: {exc}")
                continue
        deleted += 1

    # Sync local DB to the kept event
    if not dry_run:
        start = keep.get("start", {})
        event_date = start.get("date") or start.get("dateTime", "")[:10]
        if event_date:
            props = keep.get("extendedProperties", {}).get("private", {})
            ticker = props.get("ticker") or parse_ticker_from_summary(keep.get("summary", ""))
            if ticker:
                quarter = props.get("quarter") or date_to_quarter(event_date)
                upsert_event(
                    conn, ticker, event_date, None, keep["id"],
                    quarter=quarter,
                )

    return deleted
