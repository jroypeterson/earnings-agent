"""One-off calendar migration (2026-05-28).

Splits the legacy shared "Public Investing" calendar into three dedicated
floridabusinessman@gmail.com calendars:

  - earningsAgent-tagged + legacy "... Earnings Release" events  -> Earnings cal
  - analyst-days events (extendedProperties.analyst_days_event_id) -> Other Investing cal
  - everything else (manually-created personal events)            -> LEFT IN PLACE

Uses the Calendar API events.move() operation, which relocates an event
WITHOUT changing its event ID. That keeps both earnings_agent's DB
(events.calendar_event_id-equivalent) and analyst-days' events.calendar_event_id
pointers valid, and avoids any create/delete duplication.

Run from the earnings_agent dir (uses its credentials.json / service account).

    python scripts/migrate_calendar.py            # dry-run (default): categorize + report
    python scripts/migrate_calendar.py --apply     # actually move events
"""

import sys
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

OLD_CAL = "4pun76831lhmuumu9e65r7c4s4@group.calendar.google.com"
NEW_EARNINGS = "9e8a2c3d76d6096b77307a4ae2b9e3c4c6e847789559f35d12694d669970bd00@group.calendar.google.com"
NEW_OTHER = "cb8bbf73e077041aef26b0d5203878ea5a215338d0bdaf3673a9665e48d880fa@group.calendar.google.com"

SCOPES = ["https://www.googleapis.com/auth/calendar"]


def _service():
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json", scopes=SCOPES
    )
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _categorize(event):
    """Return (destination_calendar_id_or_None, group_label)."""
    props = event.get("extendedProperties", {}).get("private", {})
    summary = event.get("summary", "") or ""
    if props.get("analyst_days_event_id"):
        return NEW_OTHER, "analyst_days"
    if props.get("earningsAgent"):
        return NEW_EARNINGS, "earnings_tagged"
    low = summary.lower()
    if "earnings release" in low or "rpt'd earnings" in low or "earnings" in low:
        return NEW_EARNINGS, "earnings_legacy"
    return None, "skip_personal"


def _list_all_master_events(svc):
    """List events WITHOUT singleEvents expansion so each returned item has a
    real, movable event ID (recurring instances cannot be moved individually)."""
    events = []
    token = None
    while True:
        resp = svc.events().list(
            calendarId=OLD_CAL, maxResults=2500, pageToken=token, showDeleted=False
        ).execute()
        events.extend(resp.get("items", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return events


def _move(svc, event_id, dest):
    for attempt in range(1, 6):
        try:
            svc.events().move(
                calendarId=OLD_CAL, eventId=event_id, destination=dest
            ).execute()
            return True, None
        except HttpError as exc:
            status = exc.resp.status if exc.resp else 0
            if status in (403, 429, 500, 502, 503, 504) and attempt < 5:
                time.sleep(2 ** (attempt - 1))
                continue
            return False, f"HTTP {status}: {exc}"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
    return False, "exhausted retries"


def main():
    apply = "--apply" in sys.argv
    svc = _service()
    events = _list_all_master_events(svc)

    buckets = {"earnings_tagged": [], "earnings_legacy": [], "analyst_days": [], "skip_personal": []}
    for e in events:
        dest, group = _categorize(e)
        buckets[group].append((e["id"], dest, e.get("summary", "")))

    print(f"Listed {len(events)} master events on OLD calendar.\n")
    for group, items in buckets.items():
        print(f"  {group:16s} {len(items):4d}")
    print()
    print("skip_personal (LEFT IN PLACE):")
    for _id, _dest, summary in buckets["skip_personal"]:
        print(f"  - {summary!r}")
    print()

    if not apply:
        print("DRY-RUN. Re-run with --apply to move earnings_* -> Earnings cal "
              "and analyst_days -> Other Investing cal.")
        return

    moved = 0
    failed = []
    for group in ("earnings_tagged", "earnings_legacy", "analyst_days"):
        for event_id, dest, summary in buckets[group]:
            ok, err = _move(svc, event_id, dest)
            if ok:
                moved += 1
            else:
                failed.append((summary, err))
        print(f"  {group}: done")
    print(f"\nMoved {moved} events. Failures: {len(failed)}")
    for summary, err in failed:
        print(f"  FAIL {summary!r}: {err}")


if __name__ == "__main__":
    main()
