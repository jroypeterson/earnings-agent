"""One-shot: move reported-ness onto a tag, and tag every task by Position.

JP 2026-08-12: *"going forward lets tag entries as portfolio or researching so
I can sort by that"* and *"Instead of having [Reported] at the front as a text
lets just add it as a tag that the stock has reported"*.

`reconcile_ticktick_tasks` applies both rules going forward, but it is bounded
(14 days back, 45 forward) and only sees rows that are `tier <= 2` and present
in the DB. Measured 2026-08-12: **224** live tasks still carried the legacy
`[REPORTED] ` prefix and **0 of 1209** carried any tag. Waiting for the window
to drift over them would leave a half-tagged list, and a half-tagged list is
worse than an untagged one — a sort silently splits the same category in two.

    python scripts/migrate_ticktick_tags.py --dry-run
    python scripts/migrate_ticktick_tags.py

**Default scope is the current + forward quarters (2Q26, 3Q26).** Older lists
carry no `[REPORTED] ` prefix at all — the mark-reported lane only started
converging this season — so they are consistent as they stand, and rewriting a
year of archive to add a sort key nobody sorts by is churn. Widen with
`--quarters`.

**One POST per task, not `update_task_content`.** That helper re-reads the
project before every write, costing two calls per task; TickTick allows 300 per
5 minutes, so the read-then-write lane paces at ~30 tasks/min against ~60 for a
direct write. The full task object is already in hand from the batch read, so
the re-read buys nothing here. Past the cap the READ returns EMPTY and the
write fails as "task not found in project" — which reads like missing data
rather than a rate limit, so pacing is not optional.

**Never touches a completed task** (`status == 2`) — the same invariant
`update_task_content` enforces for every other writer. A tick is JP's record.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

import coverage as cov  # noqa: E402
import ticktick as tt  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# A pipeline-managed title: "[REPORTED] ]?TICKER Q<n> <year> Earnings[ (...)]".
# Guarding on this is what keeps the sweep off JP's own hand-written tasks,
# which share these lists — the "Earnings 2Q25" list is full of titles like
# "Cooper Cos Inc/The COO US" that would otherwise be parsed for a ticker and
# rewritten. Absent a match we skip and say so; we never guess.
_MANAGED = re.compile(
    r"^(?:\[REPORTED\]\s+)?(?P<ticker>[A-Z0-9.\-]{1,12})\s+Q[1-4]\s+\d{4}\s+Earnings\b"
)

_PACE_S = 1.05  # ~57/min, just under the 300-per-5-minutes ceiling


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quarters", default="2Q26,3Q26",
                    help="Comma-separated reporting quarters whose lists to sweep.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    quarters = [q.strip() for q in args.quarters.split(",") if q.strip()]
    token = os.environ.get("TICKTICK_ACCESS_TOKEN")
    if not token:
        logger.error("TICKTICK_ACCESS_TOKEN not set")
        return 2

    info = {t.ticker: t for t in cov.load_coverage()}
    headers = tt._headers(token)

    changed = skipped = unmanaged = errors = 0
    for proj in sorted(tt._list_all_projects(token), key=lambda p: p.get("name") or ""):
        name = proj.get("name") or ""
        if not any(name.startswith(f"{q} Earnings") for q in quarters):
            continue
        tasks = tt.list_tasks_in_project(token, proj["id"])
        logger.info("--- %s (%d tasks)", name, len(tasks))

        for task in tasks:
            title = task.get("title") or ""
            if task.get("status") == 2:
                skipped += 1
                continue
            m = _MANAGED.match(title)
            if not m:
                logger.info("   skip (not a managed title): %r", title[:70])
                unmanaged += 1
                continue

            ticker = m.group("ticker")
            ci = info.get(ticker)
            reported = tt.task_is_reported(task)

            want = [t for t in (
                tt.sector_tag(ci.sector if ci else None),
                tt.position_tag(ci.position if ci else None),
                tt.REPORTED_TAG if reported else None,
            ) if t]
            merged = tt._merge_tags(task.get("tags"), *want)
            new_title = title.replace(f"{tt._LEGACY_REPORTED_PREFIX} ", "", 1) \
                if title.startswith(tt._LEGACY_REPORTED_PREFIX) else None

            if merged is None and new_title is None:
                skipped += 1
                continue

            if args.dry_run:
                logger.info("   WOULD WRITE %s: title=%r tags=%s",
                            ticker, new_title or title, merged or task.get("tags"))
                changed += 1
                continue

            # Send the object back whole. A POST to /task/{id} REPLACES the
            # items array, so a partial body would wipe JP's ticked checklist.
            body = dict(task)
            if new_title is not None:
                body["title"] = new_title
            if merged is not None:
                body["tags"] = merged
            body["id"] = task["id"]
            body["projectId"] = proj["id"]
            try:
                r = requests.post(f"{tt.TICKTICK_API_BASE}/task/{task['id']}",
                                  headers=headers, json=body, timeout=15)
            except requests.RequestException as exc:
                logger.error("   %s: %s", ticker, exc)
                errors += 1
                continue
            if r.status_code == 401:
                logger.error("TickTick token expired — stopping")
                return 2
            if r.status_code != 200:
                logger.error("   %s: HTTP %s %s", ticker, r.status_code, r.text[:120])
                errors += 1
                continue
            logger.info("   %s -> title=%r tags=%s", ticker, body["title"], body["tags"])
            changed += 1
            time.sleep(_PACE_S)

    logger.info("Migration: %d changed, %d already correct, %d unmanaged titles "
                "skipped, %d errors%s",
                changed, skipped, unmanaged, errors,
                " (DRY RUN)" if args.dry_run else "")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
