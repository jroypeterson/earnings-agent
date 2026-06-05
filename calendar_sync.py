"""
Google Calendar operations — create, update, delete, find, and cleanup
earnings events with retry logic and pagination.
"""

import re
import time
import logging
from datetime import date, datetime, timedelta

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


def _today_et() -> date:
    """Today's calendar date in America/New_York (the market's timezone)."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).date()


def _date_has_passed(earnings_date: str | None) -> bool:
    """True when ``earnings_date`` (YYYY-MM-DD) is strictly before today (ET).

    An estimate qualifier only makes sense for a future release date. Once the
    expected date has passed, Finnhub's projected-vs-confirmed distinction is
    moot, so the title's "(est.)" marker and the description's "Estimated"
    status are stale and misleading — callers drop them in favour of a neutral
    "date passed, results pending" presentation. Today is NOT treated as past
    (an estimate for today is still live until the day ends). Unparseable or
    missing dates are treated as not-past (safe: keeps prior behaviour).
    """
    if not earnings_date:
        return False
    try:
        return date.fromisoformat(earnings_date) < _today_et()
    except (ValueError, TypeError):
        return False


def expected_calendar_state(
    ticker: str,
    hour: str | None,
    eps_estimate: float | None,
    eps_actual: float | None,
    revenue_estimate: float | None,
    revenue_actual: float | None,
    *,
    quarter: str | None,
    tier: int,
    source_fingerprint: str | None = None,
    hour_yf: str | None = None,
    earnings_date: str | None = None,
    call_datetime_utc: str | None = None,
) -> tuple[str, str, dict]:
    """Render the canonical (summary, description, private-props) for an event.

    Single source of truth for how we present an event on Calendar. All
    create/update paths should derive their payload from this so drift between
    the title (which encodes [REPORTED]/(est.)) and the description (which
    carries Timing/EPS/Revenue) is impossible.

    `hour` is the Finnhub-canonical timing. `hour_yf` is the yfinance-inferred
    fallback used only when Finnhub's hour is empty. The chosen effective hour
    drives both the time block (in create_calendar_event) and the (est.) /
    confirmed marker. The description's `Status:` line distinguishes
    "Confirmed" (Finnhub) from "Confirmed (yfinance)" so provenance is visible.

    `date_confirmed` semantics in the DB are NOT affected by hour_yf — that
    flag continues to derive from `hour` only, so cross-check verdict
    messaging keeps its company-confirmed meaning.
    """
    effective_hour = hour or hour_yf or ""
    used_yf = bool(hour_yf and not hour)

    has_actuals = eps_actual is not None or revenue_actual is not None
    # "(est.)" flags a Finnhub-projected (not company-confirmed) future date.
    # Drop it once we have actuals, once the timing is confirmed, OR once the
    # date has passed — a stale "(est.)" lingering on a past date is misleading
    # since an estimate only makes sense for a date still ahead of us.
    est_marker = (
        ""
        if (has_actuals or _is_confirmed_hour(effective_hour) or _date_has_passed(earnings_date))
        else " (est.)"
    )
    if has_actuals:
        # Compact title once results are in — the calendar event is now
        # historical context, no need for a verbose "[REPORTED] ... Release".
        summary = f"{ticker} Rpt'd Earnings"
    else:
        summary = f"{ticker} Earnings Release{est_marker}"
    description = build_description(
        ticker, effective_hour, eps_estimate, eps_actual,
        revenue_estimate, revenue_actual,
        hour_source=("yfinance" if used_yf else "finnhub"),
        earnings_date=earnings_date,
        call_datetime_utc=call_datetime_utc,
    )
    if source_fingerprint is None:
        source_fingerprint = f"{ticker}:?"
    props = {
        "earningsAgent": "true",
        "ticker": ticker,
        "source_fingerprint": source_fingerprint,
        "tier": str(tier),
    }
    if quarter:
        props["quarter"] = quarter
    if used_yf:
        # Stamp provenance on the calendar event so it's visible to anyone
        # inspecting the raw API payload (and to drift detection).
        props["hour_source"] = "yfinance"
    return summary, description, props


def _wall_clock_et(dt_str: str) -> str:
    """HH:MM wall-clock of an ISO-8601 calendar dateTime in America/New_York,
    regardless of the timezone the Calendar API returned it in.

    The API normalizes start.dateTime to the *calendar's* default timezone, so
    a 07:00 ET event on a UTC-default calendar comes back as '...T11:00:00Z'.
    A naive string slice would read "11:00" and report perpetual "shape" drift
    against the expected "07:00"/"16:30", causing an every-run delete/recreate
    churn. Converting to ET first makes the comparison correct for any calendar
    timezone (the floridabusinessman calendars default to UTC, the legacy one
    was ET).
    """
    if not dt_str:
        return ""
    try:
        from zoneinfo import ZoneInfo
        normalized = dt_str.replace("Z", "+00:00") if dt_str.endswith("Z") else dt_str
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.strftime("%H:%M")  # no offset: assume already ET wall-clock
        return dt.astimezone(ZoneInfo("America/New_York")).strftime("%H:%M")
    except (ValueError, TypeError):
        return dt_str[11:16]


def calendar_event_drift_kind(
    cal_event: dict,
    expected_summary: str,
    expected_description: str,
    expected_props: dict,
    expected_hour: str | None,
) -> str:
    """Classify drift between cal_event and the expected state.

    Returns one of:
      'fresh' — calendar event matches expected state
      'text'  — only summary/description/props differ; safe to patch in place
      'shape' — timed-vs-all-day or bmo-vs-amc differ; must delete + recreate

    `expected_hour` should be the EFFECTIVE hour (Finnhub if set, else the
    yfinance fallback). The drift detector doesn't care which source —
    only whether the calendar's current time block matches what we'd render
    now. Callers pass `hour or hour_yf or ""`.

    The shape distinction matters because update_calendar_event_description()
    only patches summary/description/extendedProperties — it leaves start, end,
    and reminders untouched. A TBD->amc transition needs the event to flip
    from all-day to a 4:30 PM timed event; only delete+recreate does that.
    """
    cal_start = cal_event.get("start", {})
    has_datetime = bool(cal_start.get("dateTime"))
    expected_timed = (expected_hour or "").lower() in ("bmo", "amc")

    # Timed vs all-day mismatch
    if expected_timed != has_datetime:
        return "shape"

    # Both timed: check the wall-clock time portion in ET.
    if expected_timed:
        existing_hm = _wall_clock_et(cal_start.get("dateTime") or "")
        expected_hm = "07:00" if expected_hour == "bmo" else "16:30"
        if existing_hm != expected_hm:
            return "shape"

    # Shape OK — text/props
    if (cal_event.get("summary") or "") != expected_summary:
        return "text"
    if (cal_event.get("description") or "") != expected_description:
        return "text"
    actual = cal_event.get("extendedProperties", {}).get("private", {})
    for k, v in expected_props.items():
        if actual.get(k) != v:
            return "text"
    return "fresh"


def _render_call_line(
    earnings_date: str | None,
    call_dt_iso: str | None,
) -> str | None:
    """Format the 'Conference call: ...' line for the calendar description.

    `earnings_date` is the press-release date (the calendar event's anchor).
    `call_dt_iso` is the call's ISO-8601 datetime with offset, or None.

    Returns:
      * None when call_dt_iso is missing (caller omits the line entirely)
      * "<weekday> <Mon DD> <H:MM AM/PM> ET" when call is on a different
        calendar day than the press release (split-day pattern; spell out
        the weekday so the date is unambiguous)
      * "<H:MM AM/PM> ET (same day)" when call is the same day as release
    """
    if not call_dt_iso:
        return None
    try:
        # fromisoformat handles "+00:00" but not "Z" prior to py3.11
        normalized = call_dt_iso.replace("Z", "+00:00") if call_dt_iso.endswith("Z") else call_dt_iso
        call_dt = datetime.fromisoformat(normalized)
    except (ValueError, TypeError):
        return None

    try:
        from zoneinfo import ZoneInfo
        local = call_dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        return None

    # Manual time formatting — strftime("%-I"/"%-d") doesn't work on Windows.
    hour12 = local.hour % 12 or 12
    ampm = "AM" if local.hour < 12 else "PM"
    time_str = f"{hour12}:{local.strftime('%M')} {ampm} ET"

    if earnings_date and local.date().isoformat() == earnings_date:
        return f"Conference call: {time_str} (same day)"

    weekday = local.strftime("%a")           # e.g. "Tue"
    month = local.strftime("%b")             # e.g. "May"
    return f"Conference call: {weekday} {month} {local.day} {time_str}"


def build_description(
    ticker: str,
    hour: str | None,
    eps_estimate: float | None,
    eps_actual: float | None,
    revenue_estimate: float | None,
    revenue_actual: float | None,
    hour_source: str = "finnhub",
    earnings_date: str | None = None,
    call_datetime_utc: str | None = None,
    source: str = "Finnhub + FMP merge",
) -> str:
    """Build the calendar event description, including actuals if available.

    `hour_source` annotates provenance in the Status line: "Confirmed" when
    Finnhub provided the timing, "Confirmed (yfinance)" when we fell back to
    yfinance's earnings datetime, "Estimated (Finnhub has no timing)" when
    neither source gave us anything.

    `earnings_date` and `call_datetime_utc` together drive the
    "Press release:" / "Conference call:" pair of lines. The calendar
    event itself anchors to the press-release date; the call line is
    descriptive context. Both are optional — when call_datetime_utc is
    None the conference-call line is omitted entirely.
    """
    timing_str = TIMING_LABELS.get(hour, "Time TBD")

    lines = [f"Ticker: {ticker}"]

    # Press release / Conference call lines. When we have call info,
    # label the timing as "Press release:" so the reader knows which
    # event the calendar tracks. Otherwise use the legacy "Timing:" label.
    call_line = _render_call_line(earnings_date, call_datetime_utc)
    if call_line is not None:
        lines.append(f"Press release: {timing_str}")
        lines.append(call_line)
    else:
        lines.append(f"Timing: {timing_str}")

    has_actuals = eps_actual is not None or revenue_actual is not None
    if not has_actuals:
        if _date_has_passed(earnings_date):
            # Expected date came and went without actuals captured yet — it is
            # no longer an estimate, so don't keep calling it one.
            status = "Date passed (results pending)"
        elif _is_confirmed_hour(hour):
            status = "Confirmed (yfinance)" if hour_source == "yfinance" else "Confirmed"
        else:
            status = "Estimated (Finnhub has no timing)"
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

    # Provenance reflects the calendar's actual data pipeline. Earnings data is
    # the Finnhub+FMP merge (per-event source isn't persisted in the DB, so the
    # label names the pipeline, not the single winning provider for this row).
    lines.append(f"\nSource: {source} | Auto-generated by ClaudeFin")
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
    hour_yf: str | None = None,
    call_datetime_utc: str | None = None,
) -> str | None:
    """
    Create an earnings event on Google Calendar.
    Returns the created event's Google Calendar ID.

    `hour_yf` provides a yfinance-inferred fallback used only when `hour`
    is empty. It drives the time block (BMO 7am, AMC 4:30pm) so the event
    is timed instead of all-day, and stamps `hour_source: yfinance` in
    extendedProperties for provenance.
    """
    if source_fingerprint is None:
        source_fingerprint = f"{ticker}:{earnings_date}"

    summary, description, props = expected_calendar_state(
        ticker, hour, eps_estimate, eps_actual, revenue_estimate, revenue_actual,
        quarter=quarter, tier=tier, source_fingerprint=source_fingerprint,
        hour_yf=hour_yf,
        earnings_date=earnings_date,
        call_datetime_utc=call_datetime_utc,
    )
    extended_props = {"private": props}
    effective_hour = hour or hour_yf or ""

    if effective_hour == "bmo":
        event_body = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": f"{earnings_date}T07:00:00", "timeZone": TIMEZONE},
            "end": {"dateTime": f"{earnings_date}T07:15:00", "timeZone": TIMEZONE},
            "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 60}]},
            "extendedProperties": extended_props,
        }
    elif effective_hour == "amc":
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
    """Extract ticker from event summaries.

    Matches three title shapes the agent has produced:
      * "AAPL Earnings Release"            (legacy, pre-actuals)
      * "AAPL Earnings Release (est.)"     (legacy, estimated)
      * "[REPORTED] AAPL Earnings Release" (legacy, post-actuals)
      * "AAPL Rpt'd Earnings"              (current, post-actuals)
    """
    # Reported (current short form): "TICKER Rpt'd Earnings"
    m = re.match(r"^(\w+)\s+Rpt'd\s+Earnings", summary)
    if m:
        return m.group(1).upper()
    # Legacy / estimated forms: "[REPORTED]? <symbols>? TICKER Earnings Release..."
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
