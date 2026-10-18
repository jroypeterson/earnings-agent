"""
Mirror earnings events for Position-list names onto a second Google Calendar.

JP 2026-10-18: a dedicated calendar for Portfolio / Researching / Ready to Buy /
Ready to Short ("Ernings- Port, Rsching-Ready to Buy/Sell"), separate from the
main earnings calendar (since renamed "Ernings - Core (ex-Port,Rsch,RTB/S)").

This is the interim, duplicate-on-both step. It is STATELESS and one-way:

  * source of truth = events the agent already wrote to GOOGLE_CALENDAR_ID
    (tagged `earningsAgent=true`), so every create/update/delete path in
    main.py is picked up without touching any of them;
  * a mirror event carries `mirrorOf=<source event id>` and is upserted by
    that key — the DB never learns the mirror exists;
  * a mirror whose source is gone, or whose ticker left the Position lists,
    is deleted.

One exception to "copy the source calendar": a Researching name without
Core=Y is Tier 3, and the agent never writes Tier 3 to Calendar (10 of 35
Researching names on 2026-10-18: ARXS KLRA MAIR MDA MICC MNKD MWH TRUP UBER
XE). For those the mirror renders events straight from the events DB with the
agent's own `build_event_body`, keyed `mirrorOf=db:TICKER:QUARTER` so a date
move updates in place. The DB is a CI artifact and a local copy is stale by
construction, so this arm runs only when the DB's sync heartbeat is fresh;
when it is skipped, existing `db:` mirrors are left untouched, never deleted.

The end state (events ONLY on the position calendar, not duplicated on Core)
is a separate board row: it means routing at create time in main.py and moving
the existing events, which this module deliberately does not attempt.

Guards, because a mirror's failure mode is "wipe the calendar":
  * empty Position lists (CM checkout missing/broken) -> abort, no writes;
  * empty source listing -> abort, no writes;
  * deletes above MAX_DELETE_FRACTION of existing mirrors -> refuse deletes
    (creates/updates still apply) unless --allow-mass-delete.

Usage:
    python position_calendar_mirror.py [--dry-run] [--allow-mass-delete]
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import sqlite3

from calendar_sync import _retry_calendar, build_event_body, get_calendar_service
from config import CALENDAR_PAGE_SIZE, COVERAGE_MANAGER_PATH, DB_PATH, GOOGLE_CALENDAR_ID
from coverage import _read_position_lists
from storage import get_last_sync_completed

logger = logging.getLogger("earnings_agent")

POSITION_CALENDAR_ID = os.getenv("GOOGLE_POSITION_CALENDAR_ID")

# Position lists mirrored. "Ready to Short" is CM's name for JP's "ready to sell".
# Following for Interest is deliberately excluded. No Core gate on Researching:
# the calendar is for names JP is actively working, Core or not.
MIRRORED_POSITIONS = ("Portfolio", "Researching", "Ready to Buy", "Ready to Short")

# Window of source events considered. Mirrors dated before the window are left
# alone (never deleted), so history accumulates rather than scrolling off.
LOOKBACK_DAYS = 100  # one full quarter back, so last season's prints show
LOOKAHEAD_DAYS = 400

_ET = ZoneInfo("America/New_York")

# Same bound as the TickTick reconcile's staleness guard.
DB_MAX_AGE_DAYS = 4
DB_KEY_PREFIX = "db:"

MAX_DELETE_FRACTION = 0.5
MAX_DELETE_FLOOR = 10  # a small calendar can always lose this many

# Fields copied verbatim from the source event.
_COPIED_FIELDS = ("summary", "description", "start", "end", "reminders")


class MirrorAbort(Exception):
    """Raised when inputs look broken; nothing is written."""


def position_tickers(exports_path: Path) -> dict[str, str]:
    """{TICKER: position label} for the mirrored Position lists."""
    lists = _read_position_lists(exports_path)
    out: dict[str, str] = {}
    for label in MIRRORED_POSITIONS:
        for ticker in lists.get(label, {}):
            out.setdefault(ticker, label)
    return out


def _list_events(service, calendar_id: str, prop: str, time_min: str, time_max: str) -> list[dict]:
    events, page_token = [], None
    while True:
        request = service.events().list(
            calendarId=calendar_id,
            privateExtendedProperty=prop,
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=CALENDAR_PAGE_SIZE,
            pageToken=page_token,
        )
        result = _retry_calendar(request.execute)
        events.extend(result.get("items", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            return events


def _private(event: dict) -> dict:
    return event.get("extendedProperties", {}).get("private", {})


def _as_mirror(event: dict, key: str, ticker: str, position: str) -> dict:
    body = {k: event[k] for k in _COPIED_FIELDS if k in event}
    body["extendedProperties"] = {"private": {
        "earningsMirror": "true",
        "mirrorOf": key,
        "ticker": ticker,
        "position": position,
    }}
    return body


def mirror_body(source: dict, position: str) -> dict:
    """The mirror event body for a source event."""
    return _as_mirror(source, source["id"], _private(source).get("ticker", ""), position)


def db_bodies(
    conn: sqlite3.Connection, tickers: dict[str, str], start: str, end: str
) -> dict[str, dict]:
    """{mirrorOf key: body} for Tier-3 Position-name events the agent never calendars.

    Tier <= 2 rows are excluded: those reach the source calendar, and rendering
    them here as well would mirror one print twice.
    """
    rows = conn.execute(
        "SELECT ticker, quarter, event_date, event_hour, event_hour_yf, eps_estimate, "
        "eps_actual, rev_estimate, rev_actual, tier, source_fingerprint, call_datetime_utc "
        "FROM events WHERE closed_reason IS NULL AND tier > 2 "
        "AND event_date >= ? AND event_date < ? ORDER BY updated_at",
        (start, end),
    ).fetchall()
    out: dict[str, dict] = {}
    for (ticker, quarter, event_date, hour, hour_yf, eps_est, eps_act,
         rev_est, rev_act, tier, fingerprint, call_utc) in rows:
        ticker = (ticker or "").upper()
        if ticker not in tickers:
            continue
        body = build_event_body(
            ticker, event_date, hour,
            quarter=quarter, eps_estimate=eps_est, eps_actual=eps_act,
            revenue_estimate=rev_est, revenue_actual=rev_act, tier=tier or 3,
            source_fingerprint=fingerprint, hour_yf=hour_yf, call_datetime_utc=call_utc,
        )
        key = f"{DB_KEY_PREFIX}{ticker}:{quarter or event_date}"
        # ORDER BY updated_at: a same-quarter duplicate resolves to the newest row.
        out[key] = _as_mirror(body, key, ticker, tickers[ticker])
    return out


def _db_is_fresh(conn: sqlite3.Connection, today: date) -> bool:
    last = get_last_sync_completed(conn)
    if not last:
        return False
    return (today - date.fromisoformat(last[:10])).days <= DB_MAX_AGE_DAYS


def _instant(when: dict | None):
    """Comparable form of an event start/end.

    The Calendar API normalises a returned `dateTime` to the CALENDAR's default
    timezone, and the two calendars differ (source UTC, position calendar
    America/New_York) — so a string compare would read every timed event as
    drifted and rewrite it on every run. Same trap as `_wall_clock_et`.
    """
    when = when or {}
    if "dateTime" in when:
        return ("dt", datetime.fromisoformat(when["dateTime"].replace("Z", "+00:00")).timestamp())
    return ("d", when.get("date"))


def _et_day(when: dict | None) -> str:
    """Calendar day (America/New_York) an event start falls on."""
    when = when or {}
    if "dateTime" in when:
        dt = datetime.fromisoformat(when["dateTime"].replace("Z", "+00:00"))
        return dt.astimezone(_ET).date().isoformat()
    return when.get("date", "")


def _differs(mirror: dict, desired: dict) -> bool:
    for k in _COPIED_FIELDS:
        if k in ("start", "end"):
            if _instant(mirror.get(k)) != _instant(desired.get(k)):
                return True
        elif mirror.get(k) != desired.get(k):
            return True
    return _private(mirror) != desired["extendedProperties"]["private"]


def plan_mirror(
    sources: list[dict], mirrors: list[dict], tickers: dict[str, str],
    db_desired: dict[str, dict] | None = None,
) -> tuple[list[dict], list[tuple[str, dict]], list[dict]]:
    """Pure diff: (creates, updates as (mirror_id, body), deletes).

    Duplicate mirrors of one source (e.g. a run killed between insert and the
    next listing) collapse to the first; the rest are deleted.

    `db_desired=None` means the DB arm did not run: `db:` mirrors are then
    neither created, updated nor deleted.
    """
    # The source calendar can hold duplicates of one ticker+day (measured
    # 2026-10-18: 4x CPRT on 2026-09-09) — `--cleanup` is the source-side remedy.
    # Mirror one per ticker+day, the most recently updated.
    chosen: dict[tuple[str, str], dict] = {}
    for src in sources:
        if src.get("status") == "cancelled":
            continue
        ticker = _private(src).get("ticker", "").upper()
        if ticker not in tickers:
            continue
        key = (ticker, _et_day(src.get("start")))
        if key not in chosen or src.get("updated", "") > chosen[key].get("updated", ""):
            chosen[key] = src
    desired: dict[str, dict] = {
        src["id"]: mirror_body(src, tickers[ticker]) for (ticker, _), src in chosen.items()
    }
    if db_desired:
        desired.update(db_desired)

    existing: dict[str, dict] = {}
    deletes: list[dict] = []
    for m in mirrors:
        if m.get("status") == "cancelled":
            continue
        key = _private(m).get("mirrorOf") or ""
        if db_desired is None and key.startswith(DB_KEY_PREFIX):
            continue
        if key in desired and key not in existing:
            existing[key] = m
        else:
            deletes.append(m)

    creates = [body for key, body in desired.items() if key not in existing]
    updates = [
        (existing[key]["id"], body)
        for key, body in desired.items()
        if key in existing and _differs(existing[key], body)
    ]
    return creates, updates, deletes


def run_mirror(dry_run: bool = False, allow_mass_delete: bool = False) -> dict:
    if not GOOGLE_CALENDAR_ID or not POSITION_CALENDAR_ID:
        raise MirrorAbort("GOOGLE_CALENDAR_ID and GOOGLE_POSITION_CALENDAR_ID are both required")
    if GOOGLE_CALENDAR_ID == POSITION_CALENDAR_ID:
        raise MirrorAbort("position calendar is the source calendar; refusing to mirror onto itself")

    tickers = position_tickers(Path(COVERAGE_MANAGER_PATH) / "exports")
    if not tickers:
        raise MirrorAbort(
            f"no Position-list tickers under {COVERAGE_MANAGER_PATH}/exports — "
            "refusing to run (would delete every mirror)"
        )

    today = date.today()
    time_min = f"{(today - timedelta(days=LOOKBACK_DAYS)).isoformat()}T00:00:00Z"
    time_max = f"{(today + timedelta(days=LOOKAHEAD_DAYS)).isoformat()}T00:00:00Z"

    service = get_calendar_service()
    sources = _list_events(service, GOOGLE_CALENDAR_ID, "earningsAgent=true", time_min, time_max)
    if not sources:
        raise MirrorAbort("source calendar returned no earningsAgent events in window — refusing to run")
    mirrors = _list_events(service, POSITION_CALENDAR_ID, "earningsMirror=true", time_min, time_max)

    db_desired = None
    if Path(DB_PATH).exists():
        conn = sqlite3.connect(f"file:{Path(DB_PATH).as_posix()}?mode=ro", uri=True)
        try:
            if _db_is_fresh(conn, today):
                db_desired = db_bodies(conn, tickers, time_min[:10], time_max[:10])
            else:
                logger.warning(
                    f"Mirror: events DB sync heartbeat is {get_last_sync_completed(conn)} "
                    f"(older than {DB_MAX_AGE_DAYS}d, or missing) - skipping Tier-3 Position "
                    "names; existing db: mirrors left untouched"
                )
        finally:
            conn.close()
    else:
        logger.warning(f"Mirror: no events DB at {DB_PATH} - skipping Tier-3 Position names")

    creates, updates, deletes = plan_mirror(sources, mirrors, tickers, db_desired)

    delete_cap = max(MAX_DELETE_FLOOR, int(len(mirrors) * MAX_DELETE_FRACTION))
    deletes_blocked = len(deletes) > delete_cap and not allow_mass_delete
    if deletes_blocked:
        logger.error(
            f"Mirror: {len(deletes)} deletes exceeds cap {delete_cap} of {len(mirrors)} mirrors — "
            "skipping deletes (rerun with --allow-mass-delete if intended). Would delete: "
            + ", ".join(sorted({_private(m).get('ticker', '?') for m in deletes}))
        )

    summary = {
        "position_tickers": len(tickers),
        "db_arm": "skipped" if db_desired is None else len(db_desired),
        "source_events": len(sources),
        "mirror_events": len(mirrors),
        "create": len(creates),
        "update": len(updates),
        "delete": 0 if deletes_blocked else len(deletes),
        "deletes_blocked": len(deletes) if deletes_blocked else 0,
    }
    logger.info(f"Mirror plan ({'dry run' if dry_run else 'live'}): {summary}")
    for body in creates:
        logger.info(f"  + {body['summary']} {body['start']}")
    for _, body in updates:
        logger.info(f"  ~ {body['summary']} {body['start']}")
    if not deletes_blocked:
        for m in deletes:
            logger.info(f"  - {m.get('summary')} {m.get('start')}")
    if dry_run:
        return summary

    events = service.events()
    for body in creates:
        _retry_calendar(events.insert(calendarId=POSITION_CALENDAR_ID, body=body).execute)
    for mirror_id, body in updates:
        _retry_calendar(events.update(calendarId=POSITION_CALENDAR_ID, eventId=mirror_id, body=body).execute)
    if not deletes_blocked:
        for m in deletes:
            _retry_calendar(events.delete(calendarId=POSITION_CALENDAR_ID, eventId=m["id"]).execute)

    if deletes_blocked:
        # Creates/updates landed; fail the step so the blocked deletes are seen.
        raise MirrorAbort(f"{len(deletes)} mirror deletes blocked by the mass-delete cap")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-mass-delete", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        run_mirror(dry_run=args.dry_run, allow_mass_delete=args.allow_mass_delete)
    except MirrorAbort as exc:
        logger.error(f"Mirror aborted: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
