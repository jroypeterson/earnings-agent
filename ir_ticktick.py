"""Push the IR signup candidates into TickTick — two lists (JP 2026-08-05).

    python ir_ticktick.py --dry-run      # show what would be created, write nothing
    python ir_ticktick.py                # create/refresh both lists

WHY TWO LISTS. 239 tasks in one list is not a to-do list, it is a wall. The 28 names JP
actually HOLDS are the ones worth signing up for first, so they get their own list and
the long coverage tail gets a second. JP chose this split over one combined list.

WHAT A TASK MEANS. "No IR mail from this company in the last 365 days." JP's call
(2026-08-05): at a full year of silence he is *likely* not subscribed, so these are
actionable rather than merely unknown. That is a judgement about a signup workflow, not
a claim about the mailbox — the underlying report still keeps "received" and "silent"
as separate facts, because only the first is directly evidenced.

IDEMPOTENT BY TICKER. Re-running never duplicates: every existing task in the target
list is read first and matched on the leading ticker, the same convention the earnings
lists use. A task JP has already ticked off (status 2) is left completely alone — it is
his record that he signed up, and recreating it would erase that.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date

import ir_coverage
import ir_links
import ticktick

# 100 req/min is the documented cap; 0.75s between writes leaves headroom for the
# list-read each push does and for TickTick counting other calls in the same window.
_MIN_INTERVAL_S = 0.75
_last_call = [0.0]


def _throttle() -> None:
    wait = _MIN_INTERVAL_S - (time.monotonic() - _last_call[0])
    if wait > 0:
        time.sleep(wait)
    _last_call[0] = time.monotonic()


PORTFOLIO_LIST = "IR signups - Portfolio"
COVERAGE_LIST = "IR signups - Coverage"

# How the IR link was obtained, rendered so a bad link is debuggable from the task alone.
_PROVENANCE = {
    "probed": "",                       # fetched and verified; the normal case, no note
    "curated": " (hand-verified)",
    "unverified": " (unverified - site blocks automated checks)",
    "homepage": " (company homepage - no IR page found)",
}


def build_rows(days: int = 365) -> tuple[list[dict], list[dict], dict]:
    """(portfolio rows, coverage rows, report). Rows are the signup candidates."""
    rep = ir_coverage.build(days=days)
    links = ir_links.load_cache().get("links", {})
    scopes = rep.get("scopes", {})
    names = rep.get("core_rows", {})

    port, cov = [], []
    for tk in rep["missing"]:
        rec = links.get(tk) or {}
        row = {
            "ticker": tk,
            "name": names.get(tk) or tk,
            "url": rec.get("url"),
            "how": rec.get("how", "none"),
            "signup_url": rec.get("signup_url"),
        }
        (port if scopes.get(tk) == "portfolio" else cov).append(row)
    return port, cov, rep


_SEP = " - "


def task_title(row: dict) -> str:
    """`LLY - Eli Lilly and Company`."""
    return f"{row['ticker']}{_SEP}{row['name']}" if row["name"] != row["ticker"] else row["ticker"]


def find_existing(tasks: list[dict], ticker: str) -> dict | None:
    """Match on OUR title prefix, not ticktick.find_existing_task_by_ticker.

    That helper splits the title on the first space, which is correct for the earnings
    lists (US tickers, never spaced) and WRONG here: this universe carries exchange-
    suffixed tickers like `AFX DE` and `LONN CH`, for which it returns `AFX` / `LONN`
    and therefore never matches. The dedup lookup silently missed on all 11 such names
    and the second run recreated every one of them (live, 2026-08-05).
    """
    want = f"{ticker}{_SEP}"
    for t in tasks:
        title = (t.get("title") or "").strip()
        if title == ticker or title.startswith(want):
            return t
    return None


def task_content(row: dict, rep: dict) -> str:
    lines = []
    # The DIRECT signup page leads when we have one. JP is doing these by hand, and the
    # IR landing page costs a hunt on every name -- the form is usually several clicks
    # deep (Encompass: /overview -> Resources -> Investor Email Alerts). Resolved for
    # 95 of 267; the rest fall back to the IR page, which is still the right start.
    if row.get("signup_url"):
        lines.append(f"SIGN UP HERE: {row['signup_url']}")
        if row["url"]:
            lines.append(f"IR page: {row['url']}{_PROVENANCE.get(row['how'], '')}")
    elif row["url"]:
        lines.append(f"IR page: {row['url']}{_PROVENANCE.get(row['how'], '')}")
        lines.append("(no direct signup page found - look for 'Email Alerts' under Resources)")
    else:
        lines.append("IR page: not found - search the company name + 'investor relations'")
    lines.append("")
    lines.append(f"Sign up with: {ir_coverage.IR_ALIAS}")
    lines.append("If the form rejects a Gmail address, use: research@jasonpeterson.nyc")
    lines.append("")
    lines.append(f"No IR email received in the last {rep['window_days']} days "
                 f"(checked {rep['generated']}).")
    return "\n".join(lines)


def push(rows: list[dict], list_name: str, rep: dict, *, token: str,
         dry_run: bool = False) -> dict:
    """Create one task per candidate. Returns counters. Never duplicates."""
    stats = {"list": list_name, "candidates": len(rows), "created": 0,
             "existing": 0, "errors": 0}
    if dry_run:
        return stats
    list_id = ticktick.find_or_create_list(token, list_name)
    if not list_id:
        stats["errors"] = len(rows)
        return stats
    existing = ticktick.list_tasks_in_project(token, list_id)
    for row in rows:
        if find_existing(existing, row["ticker"]):
            stats["existing"] += 1
            continue
        # TickTick caps at 100 requests/minute and answers HTTP 500 `exceed_query_limit`
        # past it -- which create_task reports as a plain failure, so an unpaced run
        # looks like 28 mystery errors (seen live 2026-08-05 on the 231-name coverage
        # push). Pace under the cap instead of retrying into it.
        _throttle()
        tid = ticktick.create_task(
            token, list_id, task_title(row), task_content(row, rep), due_date=None)
        if tid:
            stats["created"] += 1
        else:
            stats["errors"] += 1
    return stats


def complete_task(token: str, list_id: str, task_id: str) -> bool:
    """Mark a task done. TickTick has a dedicated /complete endpoint for this."""
    import requests
    try:
        r = requests.post(
            f"https://api.ticktick.com/open/v1/project/{list_id}/task/{task_id}/complete",
            headers={"Authorization": f"Bearer {token}"}, timeout=20)
        return r.status_code in (200, 204)
    except Exception:                                   # noqa: BLE001 - reported by caller
        return False


def reconcile(rep: dict, *, token: str, dry_run: bool = False) -> dict:
    """Tick off any task whose company has STARTED sending IR mail.

    Without this the lists rot from day one: JP signs up, the mail starts arriving, and
    the task sits open forever with nothing to close it. This is the loop that makes the
    audit self-maintaining rather than a one-off snapshot.

    COMPLETE, never delete. A ticked task is the durable record that he subscribed and
    when; deleting it would throw that away and let a quiet quarter re-create the task as
    if he had never signed up.
    """
    stats = {"checked": 0, "completed": 0, "already": 0, "errors": 0, "names": []}
    covered = set(rep["covered"])
    for list_name in (PORTFOLIO_LIST, COVERAGE_LIST):
        list_id = ticktick.find_or_create_list(token, list_name)
        if not list_id:
            continue
        for t in ticktick.list_tasks_in_project(token, list_id):
            title = (t.get("title") or "").strip()
            tk = title.split(_SEP)[0] if _SEP in title else title
            if tk not in covered:
                continue
            stats["checked"] += 1
            if t.get("status") == 2:
                stats["already"] += 1
                continue
            if dry_run:
                stats["names"].append(tk)
                continue
            _throttle()
            if complete_task(token, list_id, t["id"]):
                stats["completed"] += 1
                stats["names"].append(tk)
            else:
                stats["errors"] += 1
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--portfolio-only", action="store_true",
                    help="skip the long coverage tail")
    ap.add_argument("--update-links", action="store_true",
                    help="rewrite task bodies with the latest IR / signup links")
    ap.add_argument("--reconcile", action="store_true",
                    help="tick off tasks whose company now sends IR mail (no creates)")
    a = ap.parse_args(argv)

    port, cov, rep = build_rows(a.days)
    if rep.get("error"):
        print(f"INCONCLUSIVE - could not read Gmail: {rep['error']}", file=sys.stderr)
        return 1

    print(f"audited {rep['core_total']} · receiving {len(rep['covered'])} · "
          f"signup candidates {len(rep['missing'])} "
          f"(portfolio {len(port)} · coverage {len(cov)})")
    nolink = [r["ticker"] for r in port + cov if not r["url"]]
    if nolink:
        print(f"no IR link ({len(nolink)}): {', '.join(sorted(nolink))}")

    if a.dry_run:
        for label, rows in (("PORTFOLIO", port), ("COVERAGE", cov[:10])):
            print(f"\n-- {label} --")
            for r in rows:
                print(f"   {task_title(r)}\n      {r['url'] or '(no link)'}"
                      f"{_PROVENANCE.get(r['how'], '')}")
        if len(cov) > 10:
            print(f"   ... and {len(cov) - 10} more coverage names")
        return 0

    cfg = ticktick.get_ticktick_config()
    if not cfg:
        print("TICKTICK_ACCESS_TOKEN not set", file=sys.stderr)
        return 1
    token = cfg["token"]

    if a.update_links:
        # update_task_content costs TWO calls (it re-reads the project, then writes), so
        # the create-path pace of 0.75s puts this at ~160/min -- over TickTick's 100/min
        # cap. Past it the READ comes back empty and the write fails as "task not found
        # in project", which reads like a missing task rather than a rate limit (76 such
        # errors, live 2026-08-05). Halve the rate for this mode.
        # TickTick enforces BOTH 100/min and 300 per 5 min -- the second is the binding
        # one at ~60/min sustained. Two calls per update therefore needs ~2s spacing;
        # 1.5s cleared the per-minute cap and still tripped the 5-minute budget.
        global _MIN_INTERVAL_S
        _MIN_INTERVAL_S = 2.2
        st = {"updated": 0, "skipped": 0, "errors": 0}
        rows = {r["ticker"]: r for r in port + cov}
        for list_name in (PORTFOLIO_LIST, COVERAGE_LIST):
            lid = ticktick.find_or_create_list(token, list_name)
            if not lid:
                continue
            for t in ticktick.list_tasks_in_project(token, lid):
                title = (t.get("title") or "").strip()
                tk = title.split(_SEP)[0] if _SEP in title else title
                row = rows.get(tk)
                if not row:
                    st["skipped"] += 1
                    continue
                body = task_content(row, rep)
                if (t.get("content") or "").strip() == body.strip():
                    st["skipped"] += 1
                    continue
                _throttle()
                ok = ticktick.update_task_content(token, lid, t["id"], new_content=body)
                st["updated" if ok else "errors"] += 1
        print(f"link refresh: {st['updated']} updated · {st['skipped']} unchanged "
              f"· {st['errors']} errors")
        return 1 if st["errors"] else 0

    if a.reconcile:
        st = reconcile(rep, token=token)
        print(f"reconcile: {st['completed']} ticked off · {st['already']} already done "
              f"· {st['errors']} errors"
              + (f"  ({', '.join(sorted(st['names']))})" if st["names"] else ""))
        return 1 if st["errors"] else 0

    todo = [(port, PORTFOLIO_LIST)]
    if not a.portfolio_only:
        todo.append((cov, COVERAGE_LIST))
    rc = 0
    for rows, name in todo:
        st = push(rows, name, rep, token=token)
        print(f"{st['list']}: {st['created']} created · {st['existing']} already there "
              f"· {st['errors']} errors")
        if st["errors"]:
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
