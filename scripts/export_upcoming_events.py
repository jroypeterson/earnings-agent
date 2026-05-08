"""Publish a portable JSON of upcoming earnings events to exports/upcoming_events.json.

Consumer: sa-monitor (Phase 2 'Note:' context enrichment). Schema matches
sa-monitor/src/calendars.py:EarningsCalendar — see that file for the canonical
contract.

Window: events with event_date in [today - 2 days, today + 14 days] AND
reported = 0 (i.e. earnings not yet reported). The 2-day lookback handles
late-night halts on prior-day AMC earnings.

Run from the earnings-agent repo root:
    python scripts/export_upcoming_events.py

The resulting JSON file is small (~10-50KB typical) and intended to be
committed to the repo so sa-monitor's CI can fetch it via raw.githubusercontent.com.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "earnings_events.db"
DEFAULT_OUT = REPO_ROOT / "exports" / "upcoming_events.json"

SCHEMA_VERSION = 1
WINDOW_LOOKBACK_DAYS = 2
WINDOW_LOOKAHEAD_DAYS = 14


def export(db_path: Path, out_path: Path,
           *, lookback_days: int = WINDOW_LOOKBACK_DAYS,
           lookahead_days: int = WINDOW_LOOKAHEAD_DAYS) -> int:
    """Write the upcoming-events JSON. Returns the count of events written."""
    if not db_path.exists():
        print(f"ERROR: DB not found at {db_path}", file=sys.stderr)
        return -1

    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=lookback_days)).isoformat()
    end = (today + timedelta(days=lookahead_days)).isoformat()

    con = sqlite3.connect(db_path)
    rows = con.execute(
        """
        SELECT ticker, event_date, event_hour, tier, date_confirmed,
               call_datetime_utc, company_name
        FROM events
        WHERE event_date BETWEEN ? AND ?
          AND COALESCE(reported, 0) = 0
        ORDER BY event_date, ticker
        """,
        (start, end),
    ).fetchall()
    con.close()

    events = []
    for ticker, event_date, event_hour, tier, date_confirmed, call_dt_utc, name in rows:
        events.append({
            "ticker": (ticker or "").upper(),
            "event_date": event_date,
            "event_hour": (event_hour or "").lower(),
            "tier": int(tier or 0),
            "date_confirmed": bool(date_confirmed),
            "call_datetime_utc": call_dt_utc,
            "company_name": name or "",
        })

    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "earnings-agent",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "window": {"start": start, "end": end},
        "counts": {"events": len(events)},
        "events": events,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return len(events)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export upcoming earnings events to JSON")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help=f"Path to earnings_events.db (default: {DEFAULT_DB})")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help=f"Output JSON path (default: {DEFAULT_OUT})")
    parser.add_argument("--lookback-days", type=int, default=WINDOW_LOOKBACK_DAYS)
    parser.add_argument("--lookahead-days", type=int, default=WINDOW_LOOKAHEAD_DAYS)
    args = parser.parse_args(argv)

    n = export(args.db, args.out,
               lookback_days=args.lookback_days,
               lookahead_days=args.lookahead_days)
    if n < 0:
        return 1
    print(f"wrote {n} events to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
