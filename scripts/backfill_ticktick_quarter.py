"""One-shot: mint TickTick tasks for named tickers whose call ALREADY happened.

`sync_ticktick_tasks` is fed by a query bounded `event_date >= today`
(`main.py`), and that bound is correct — a task for a call that has already
happened is not a to-do, it is a record, and creating them by default would
mint hundreds on any coverage change. So there is deliberately no automatic
path to this, and there should not be one.

What it exists for: a name that was misclassified for a whole season gets no
task, the season ends, and the forward-only query means it never will. On
2026-08-12 that was LLY plus five other held names — Tier 1 all along, but
Coverage Manager carried them at `Core=''` in `portfolio.json`, so they sat at
Tier 3 below every `tier <= 2` gate. See CLAUDE.md, "Why the Portfolio gate
went". JP asked for those names in the 2Q list after the fact; this is that.

    python scripts/backfill_ticktick_quarter.py --tickers LLY,BE --dry-run
    python scripts/backfill_ticktick_quarter.py --tickers LLY,BE

**Requires an explicit --tickers list. There is no "all" mode**, because the
difference between 6 tasks and 39 was a judgement call JP made, not a default
worth encoding — and an accidental run over a whole quarter is tedious to undo
by hand.

**Reads the DB read-only and never writes `ticktick_task_id`.** The live DB is
a CI artifact, so a local write would be discarded at the next run anyway
(`season_progress.py` documents the same constraint). That leaves the created
task with no DB pointer, which is safe here and only here: the rows are past
and reported, `sync_ticktick_tasks`' forward-only query cannot re-create them,
and `reconcile_ticktick_tasks` selects `tier <= 2` while these rows are the
tier-3 strays that caused the problem — so nothing will touch, duplicate or
strand them. The task is created already titled `[REPORTED]`; it needs no
further lifecycle.
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

import coverage as cov  # noqa: E402
import ticktick as tt  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Creates pace at 0.75s: TickTick enforces BOTH 100/min AND 300 per 5 minutes,
# and the second binds at ~60/min sustained. Past the cap a read comes back
# EMPTY and the write fails as "task not found in project" — which reads like
# missing data rather than a rate limit. See CLAUDE.md, ir_ticktick section.
_CREATE_PACE_S = 0.75


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tickers", required=True,
                    help="Comma-separated. Required on purpose — there is no 'all' mode.")
    ap.add_argument("--since", default="2026-07-01",
                    help="Earliest event_date to consider (default: start of the 2Q26 window).")
    ap.add_argument("--until", default="2026-09-30",
                    help="Latest event_date to consider.")
    ap.add_argument("--db", default=None,
                    help="Path to earnings_events.db. The repo copy is a STALE "
                         "snapshot of a CI artifact — pull a fresh one with "
                         "`gh run download <run-id> -n earnings-db` and pass it here.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        logger.error("--tickers resolved to nothing")
        return 2

    token = os.environ.get("TICKTICK_ACCESS_TOKEN")
    if not token:
        logger.error("TICKTICK_ACCESS_TOKEN not set")
        return 2

    info_by_ticker = {t.ticker: t for t in cov.load_coverage()}

    db = Path(args.db) if args.db else (
        Path(__file__).resolve().parent.parent / "earnings_events.db")
    if not db.exists():
        logger.error("No DB at %s", db)
        return 2
    logger.info("Reading %s", db)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT ticker, event_date, event_hour, eps_estimate, eps_actual, "
        "rev_estimate, rev_actual, company_name, ticktick_task_id "
        "FROM events WHERE ticker IN (%s) "
        "AND event_date BETWEEN ? AND ? AND closed_reason IS NULL "
        "ORDER BY event_date" % ",".join("?" * len(tickers)),
        (*tickers, args.since, args.until),
    ).fetchall()
    conn.close()

    if not rows:
        logger.warning("No event rows matched — nothing to backfill.")
        return 1

    missing = sorted(set(tickers) - {r["ticker"] for r in rows})
    if missing:
        # Never silent: a ticker with no row in the window is a different
        # problem from a ticker that already has a task.
        logger.warning("No event row in window for: %s", ", ".join(missing))

    created = skipped = errors = 0
    for r in rows:
        ticker, event_date = r["ticker"], r["event_date"]
        info = info_by_ticker.get(ticker)
        tier = info.tier if info else 3
        position = info.position if info else ""
        sector = info.sector if info else ""

        list_name = tt._quarter_list_name(event_date, tier, position=position)

        # Cross-list dedup, same rule sync_ticktick_tasks uses: a task for this
        # ticker anywhere under "<RQ> Earnings" means it already exists, whether
        # or not the DB knows about it.
        rq = tt._reporting_quarter(event_date)
        already = None
        for proj in tt._list_all_projects(token):
            if not (proj.get("name") or "").startswith(f"{rq} Earnings"):
                continue
            hit = tt.find_existing_task_by_ticker(
                tt.list_tasks_in_project(token, proj["id"]), ticker)
            if hit:
                already = (proj.get("name"), hit.get("title"))
                break
        if already:
            logger.info("SKIP %s — already in %r as %r", ticker, *already)
            skipped += 1
            continue

        # Created already-reported: the call happened, so the date is real and
        # the title carries it. Reported-ness rides the tag, not a title prefix
        # (JP 2026-08-12) — mirrors mark_task_reported exactly.
        title = tt.build_task_title(ticker, event_date, r["event_hour"])
        content = tt.build_task_content(
            ticker, r["event_hour"],
            eps_estimate=r["eps_estimate"], revenue_estimate=r["rev_estimate"],
            eps_actual=r["eps_actual"], revenue_actual=r["rev_actual"],
            tier=tier, company_name=r["company_name"],
        )
        tags = [t for t in (tt.sector_tag(sector), tt.position_tag(position),
                            tt.REPORTED_TAG) if t]

        if args.dry_run:
            logger.info("WOULD CREATE in %r: %s  (tags=%s)", list_name, title, tags)
            created += 1
            continue

        list_id = tt.find_or_create_list(token, list_name)
        if not list_id:
            logger.error("Could not resolve list %r for %s", list_name, ticker)
            errors += 1
            continue

        task_id = tt.create_task(token, list_id, title, content,
                                 due_date=event_date, tags=tags)
        if task_id:
            logger.info("CREATED %s in %r", title, list_name)
            created += 1
        else:
            errors += 1
        time.sleep(_CREATE_PACE_S)

    logger.info("Backfill: %d created, %d skipped, %d errors%s",
                created, skipped, errors, " (DRY RUN)" if args.dry_run else "")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
