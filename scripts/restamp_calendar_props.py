"""Repair step for the 2026-05-28 calendar migration.

Google Calendar's events.move() (used by migrate_calendar.py) preserves the
event ID but STRIPS extendedProperties. earnings_agent queries its events via
privateExtendedProperty=earningsAgent=true, and analyst-days uses
analyst_days_event_id for DB-rebuild recovery, so the stripped props must be
restored.

Restores, via events.patch (touches only extendedProperties — summary/time
untouched):

  Earnings cal:  earningsAgent=true, ticker (parsed from summary),
                 source_fingerprint=ticker:<start-date>
                 (tier/quarter intentionally omitted — earnings_agent's
                  update path re-stamps them on the next sync; their absence
                  doesn't affect the privateExtendedProperty queries.)

  Other Investing cal: analyst_days_event_id / ticker / event_type, read back
                 from analyst-days' events.db by matching calendar_event_id.

    python scripts/restamp_calendar_props.py            # dry-run
    python scripts/restamp_calendar_props.py --apply
"""

import os
import re
import sqlite3
import sys
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

EARN = "9e8a2c3d76d6096b77307a4ae2b9e3c4c6e847789559f35d12694d669970bd00@group.calendar.google.com"
OTHER = "cb8bbf73e077041aef26b0d5203878ea5a215338d0bdaf3673a9665e48d880fa@group.calendar.google.com"
SCOPES = ["https://www.googleapis.com/auth/calendar"]
AD_DB = os.path.join(os.path.dirname(__file__), "..", "..", "analyst-days", "data", "events.db")

# First all-caps/dot/digit token (after any emoji/space prefix) is the ticker:
#   "✅ CLOV Earnings Release" -> CLOV ;  "AAPL Rpt'd Earnings" -> AAPL
TICKER_RE = re.compile(r"([A-Z][A-Z0-9.]*)")


def _service():
    creds = service_account.Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _patch(svc, cal_id, event_id, private):
    body = {"extendedProperties": {"private": private}}
    for attempt in range(1, 6):
        try:
            svc.events().patch(calendarId=cal_id, eventId=event_id, body=body).execute()
            return True, None
        except HttpError as exc:
            status = exc.resp.status if exc.resp else 0
            if status in (403, 429, 500, 502, 503, 504) and attempt < 5:
                time.sleep(2 ** (attempt - 1))
                continue
            return False, f"HTTP {status}: {exc}"
        except Exception as exc:  # noqa: BLE001
            if attempt < 5:
                time.sleep(2 ** (attempt - 1))
                continue
            return False, str(exc)
    return False, "exhausted retries"


def _ticker_from_summary(summary):
    m = TICKER_RE.search(summary or "")
    return m.group(1) if m else None


def _start_date(event):
    s = event.get("start", {})
    return (s.get("date") or s.get("dateTime", "") or "")[:10]


def _list_all(svc, cal_id):
    out, tok = [], None
    while True:
        r = svc.events().list(calendarId=cal_id, maxResults=2500, pageToken=tok).execute()
        out.extend(r.get("items", []))
        tok = r.get("nextPageToken")
        if not tok:
            break
    return out


def main():
    apply = "--apply" in sys.argv
    svc = _service()

    # --- Earnings calendar ---
    earn_events = _list_all(svc, EARN)
    earn_plan = []
    skipped = []
    for e in earn_events:
        ticker = _ticker_from_summary(e.get("summary", ""))
        if not ticker:
            skipped.append(e.get("summary"))
            continue
        date = _start_date(e)
        private = {"earningsAgent": "true", "ticker": ticker, "source_fingerprint": f"{ticker}:{date}"}
        earn_plan.append((e["id"], private, e.get("summary")))

    # --- Other Investing calendar (analyst-days) ---
    other_events = _list_all(svc, OTHER)
    ad_rows = {}
    if os.path.exists(AD_DB):
        conn = sqlite3.connect(AD_DB)
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            "SELECT id, ticker, event_type, calendar_event_id FROM events "
            "WHERE calendar_event_id IS NOT NULL"
        ):
            ad_rows[row["calendar_event_id"]] = row
    other_plan = []
    other_skipped = []
    for e in other_events:
        row = ad_rows.get(e["id"])
        if not row:
            other_skipped.append(e.get("summary"))
            continue
        private = {
            "analyst_days_event_id": str(row["id"]),
            "ticker": row["ticker"],
            "event_type": row["event_type"],
        }
        other_plan.append((e["id"], private, e.get("summary")))

    print(f"Earnings cal: {len(earn_plan)} to re-stamp, {len(skipped)} skipped (no ticker parsed)")
    for s in skipped:
        print(f"  skip: {s!r}")
    print(f"Other Investing cal: {len(other_plan)} to re-stamp, {len(other_skipped)} skipped (not in analyst-days DB)")
    for s in other_skipped:
        print(f"  skip: {s!r}")

    if not apply:
        print("\nDRY-RUN. Re-run with --apply.")
        return

    ok = fail = 0
    fails = []
    for cal_id, plan in ((EARN, earn_plan), (OTHER, other_plan)):
        for event_id, private, summary in plan:
            good, err = _patch(svc, cal_id, event_id, private)
            if good:
                ok += 1
            else:
                fail += 1
                fails.append((summary, err))
    print(f"\nRe-stamped {ok} events. Failures: {fail}")
    for summary, err in fails:
        print(f"  FAIL {summary!r}: {err}")


if __name__ == "__main__":
    main()
