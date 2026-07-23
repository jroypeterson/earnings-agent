"""
TickTick integration — the execution surface for earnings review work.

Creates quarterly task lists and per-company review tasks for Tier 1+2 names.
Calendar answers "when is this happening?" — TickTick answers "what do I need
to do about it?"

Uses the TickTick Open API v1:
  - POST /open/v1/project  — create a list
  - GET  /open/v1/project  — list all projects
  - POST /open/v1/task     — create a task
  - GET  /open/v1/task/{projectId}/{taskId} — get task details
"""

import os
import logging
from datetime import datetime, timezone

import requests

from config import TIMEZONE, TIMING_LABELS
from storage import date_to_quarter

logger = logging.getLogger("earnings_agent")

TICKTICK_API_BASE = "https://api.ticktick.com/open/v1"


class TickTickError(Exception):
    """Raised when TickTick API operations fail."""
    pass


class TickTickTokenExpired(TickTickError):
    """Raised when the TickTick access token has expired (401)."""
    pass


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def get_ticktick_config() -> dict | None:
    """
    Load TickTick configuration from environment.
    Returns dict with token and list_id, or None if not configured.
    """
    token = os.environ.get("TICKTICK_ACCESS_TOKEN")
    if not token:
        return None

    return {
        "token": token,
        "list_id": os.environ.get("TICKTICK_LIST_EARNINGS"),
    }


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# List (project) management
# ---------------------------------------------------------------------------


TIER_LIST_LABELS = {
    1: "Core Watchlist - Positions/Researching",  # all Tier 1 consolidated here
    2: "HC Svcs & MedTech",
}


def _reporting_quarter(event_date: str) -> str:
    """
    Derive the reporting quarter from an earnings release date.

    Earnings released in Jan-Mar report Q4 of prior year,
    Apr-Jun report Q1, Jul-Sep report Q2, Oct-Dec report Q3.

    Returns e.g. "1Q26" for April 2026 releases.
    """
    from datetime import date as date_type
    dt = date_type.fromisoformat(event_date)
    month = dt.month

    if month <= 3:
        q = 4
        year = dt.year - 1
    elif month <= 6:
        q = 1
        year = dt.year
    elif month <= 9:
        q = 2
        year = dt.year
    else:
        q = 3
        year = dt.year

    return f"{q}Q{year % 100}"


def _quarter_list_name(event_date: str, tier: int = 2, position: str = "") -> str:
    """
    Generate the quarterly list name from an event date and tier.

    Uses the reporting quarter (what period results cover), not the
    calendar quarter of the release date.

    `position` is accepted for backwards compatibility with callers but is
    no longer used — all Tier 1 names land in the same "Core Watchlist"
    list regardless of Portfolio/Researching designation.

    Examples:
      tier=1 -> "1Q26 Earnings - Core Watchlist"
      tier=2 -> "1Q26 Earnings - HC Svcs & MedTech"
      tier=3 -> "1Q26 Earnings"
    """
    rq = _reporting_quarter(event_date)
    tier_label = TIER_LIST_LABELS.get(tier)
    if tier_label:
        return f"{rq} Earnings - {tier_label}"
    return f"{rq} Earnings"


def find_or_create_list(token: str, list_name: str) -> str | None:
    """
    Find an existing TickTick list by name, or create it.
    Returns the list/project ID, or None on failure.
    """
    # First, try to find existing list
    try:
        resp = requests.get(
            f"{TICKTICK_API_BASE}/project",
            headers=_headers(token),
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            projects = resp.json()
            for p in projects:
                if p.get("name") == list_name:
                    logger.info(f"Found existing TickTick list: {list_name} (id={p['id']})")
                    return p["id"]
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"Failed to list TickTick projects: {exc}")

    # List not found — try to create it
    # Use the same groupId as existing earnings lists
    earnings_group_id = os.environ.get(
        "TICKTICK_EARNINGS_GROUP_ID", "6887c72473800767fff51d51"
    )
    try:
        payload = {"name": list_name}
        if earnings_group_id:
            payload["groupId"] = earnings_group_id
        resp = requests.post(
            f"{TICKTICK_API_BASE}/project",
            headers=_headers(token),
            json=payload,
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            project = resp.json()
            project_id = project.get("id")
            logger.info(f"Created TickTick list: {list_name} (id={project_id})")
            return project_id
        else:
            logger.warning(
                f"Failed to create TickTick list '{list_name}': "
                f"HTTP {resp.status_code} — {resp.text}"
            )
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"Failed to create TickTick list: {exc}")

    return None


# ---------------------------------------------------------------------------
# Task content builders
# ---------------------------------------------------------------------------


def build_task_title(
    ticker: str,
    event_date: str,
    hour: str | None,
) -> str:
    """Build the TickTick task title. e.g. 'UNH Q1 2026 Earnings (Apr 21 BMO)'"""
    quarter = date_to_quarter(event_date)
    # Format quarter for display: "2025Q4" -> "Q4 2025"
    q_label = f"Q{quarter[-1]} {quarter[:4]}"

    from datetime import date as date_type
    dt = date_type.fromisoformat(event_date)
    date_str = dt.strftime("%b %d")  # e.g. "Apr 21"

    timing = ""
    if hour == "bmo":
        timing = " BMO"
    elif hour == "amc":
        timing = " AMC"

    return f"{ticker} {q_label} Earnings ({date_str}{timing})"


def build_task_content(
    ticker: str,
    hour: str | None,
    eps_estimate: float | None = None,
    revenue_estimate: float | None = None,
    eps_actual: float | None = None,
    revenue_actual: float | None = None,
    company_name: str | None = None,
    tier: int = 3,
) -> str:
    """Build the TickTick task body with consensus estimates and a review checklist."""
    lines = []

    if company_name:
        lines.append(f"**{company_name}** ({ticker})")
    else:
        lines.append(f"**{ticker}**")

    timing_str = TIMING_LABELS.get(hour, "Time TBD")
    lines.append(f"Timing: {timing_str}")
    lines.append("")

    # Consensus estimates
    est_parts = []
    if eps_estimate is not None:
        est_parts.append(f"EPS ${eps_estimate:.2f}")
    if revenue_estimate is not None:
        rev_b = revenue_estimate / 1_000_000_000
        est_parts.append(f"Rev ${rev_b:.2f}B")
    if est_parts:
        lines.append(f"Consensus: {' | '.join(est_parts)}")
        lines.append("")

    # Review checklist
    lines.append("**Review checklist:**")
    lines.append("- [ ] Read transcript")
    lines.append("- [ ] Review company documents / IR materials")
    lines.append("- [ ] Read sell-side take")
    if tier == 1:
        lines.append("- [ ] Update model if relevant")
        lines.append("- [ ] Update thesis / investment view")
    lines.append("")

    # Results section (populated post-earnings via update)
    if eps_actual is not None or revenue_actual is not None:
        lines.append("---")
        lines.append("**Results:**")
        if eps_actual is not None and eps_estimate is not None:
            diff = eps_actual - eps_estimate
            pct = (diff / abs(eps_estimate) * 100) if eps_estimate != 0 else 0
            direction = "Beat" if diff > 0 else ("Miss" if diff < 0 else "Inline")
            lines.append(f"EPS: ${eps_actual:.2f} vs ${eps_estimate:.2f} est ({direction} {abs(pct):.1f}%)")
        if revenue_actual is not None and revenue_estimate is not None:
            act_b = revenue_actual / 1_000_000_000
            est_b = revenue_estimate / 1_000_000_000
            diff = revenue_actual - revenue_estimate
            pct = (diff / abs(revenue_estimate) * 100) if revenue_estimate != 0 else 0
            direction = "Beat" if diff > 0 else ("Miss" if diff < 0 else "Inline")
            lines.append(f"Rev: ${act_b:.2f}B vs ${est_b:.2f}B est ({direction} {abs(pct):.1f}%)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Read existing tasks
# ---------------------------------------------------------------------------


def list_tasks_in_project(token: str, project_id: str) -> list[dict]:
    """
    Fetch all tasks in a TickTick project/list.
    Returns list of task dicts with id, title, status, dueDate, etc.
    """
    try:
        resp = requests.get(
            f"{TICKTICK_API_BASE}/project/{project_id}/data",
            headers=_headers(token),
            timeout=30,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            data = resp.json()
            # The /data endpoint returns {"tasks": [...], ...}
            return data.get("tasks", [])
        else:
            logger.warning(f"Failed to list tasks in project {project_id}: HTTP {resp.status_code}")
            return []
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"Failed to list tasks in project {project_id}: {exc}")
        return []


def _list_tasks_strict(token: str, project_id: str) -> list[dict]:
    """Like list_tasks_in_project but RAISES TickTickError on a read failure
    instead of returning [] — so callers can tell "empty list" from "read
    failed". Used by the reconcile gather, where a swallowed failure would look
    like a deleted task and could drive a wrong-sibling adoption."""
    try:
        resp = requests.get(
            f"{TICKTICK_API_BASE}/project/{project_id}/data",
            headers=_headers(token),
            timeout=30,
        )
    except requests.RequestException as exc:
        raise TickTickError(f"read of project {project_id} failed: {exc}") from exc
    if resp.status_code == 401:
        raise TickTickTokenExpired("TickTick access token expired")
    if resp.status_code == 200:
        return resp.json().get("tasks", [])
    raise TickTickError(f"read of project {project_id} failed: HTTP {resp.status_code}")


def find_existing_task_by_ticker(tasks: list[dict], ticker: str) -> dict | None:
    """
    Search a list of TickTick tasks for one matching a ticker.
    Matches on the task title starting with the ticker symbol.
    """
    for task in tasks:
        title = task.get("title", "")
        # Task titles look like "UNH Q1 2026 Earnings (Apr 21 BMO)"
        # or "[REPORTED] UNH Q1 2026 Earnings (Apr 21 BMO)"
        if _ticker_from_task_title(title) == ticker:
            return task
    return None


def _ticker_from_task_title(title: str) -> str | None:
    """
    Extract the ticker symbol from a TickTick task title.
    Handles both plain titles ('UNH Q1 ...') and reported-prefix titles
    ('[REPORTED] UNH Q1 ...').
    """
    if not title:
        return None
    parts = title.split(" ", 2)
    if not parts:
        return None
    first = parts[0]
    if first.startswith("[") and len(parts) >= 2:
        return parts[1] or None
    return first or None


def _list_all_projects(token: str) -> list[dict]:
    """Fetch all TickTick projects once. Returns [] on failure."""
    try:
        resp = requests.get(
            f"{TICKTICK_API_BASE}/project",
            headers=_headers(token),
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"Failed to list TickTick projects: HTTP {resp.status_code}")
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"Failed to list TickTick projects: {exc}")
    return []


def _gather_quarter_existing_tasks(
    token: str,
    projects: list[dict],
    quarters: set[str],
) -> dict[str, dict[str, tuple[str, str, str]]]:
    """
    Build a per-quarter ticker -> (project_id, project_name, task_id) map by
    scanning every TickTick list whose name starts with '<RQ> Earnings'.

    Used by sync_ticktick_tasks to avoid creating a duplicate task when the
    same ticker already has one in a sibling list (e.g. tier promoted from
    HC Svcs to Core Watchlist mid-quarter).
    """
    result: dict[str, dict[str, tuple[str, str, str]]] = {q: {} for q in quarters}
    for p in projects:
        name = p.get("name", "")
        for q in quarters:
            if name.startswith(f"{q} Earnings"):
                pid = p["id"]
                tasks = list_tasks_in_project(token, pid)
                for t in tasks:
                    ticker = _ticker_from_task_title(t.get("title", ""))
                    if ticker and ticker not in result[q]:
                        result[q][ticker] = (pid, name, t["id"])
                break
    return result


def _gather_quarter_tasks_full(
    token: str,
    projects: list[dict],
    quarters: set[str],
) -> tuple[dict[str, dict[str, list[tuple[str, str, dict]]]], set[str]]:
    """
    Like `_gather_quarter_existing_tasks` but keeps the FULL task object (not
    just its id) and ALL candidates per ticker, so the caller can (a) inspect
    the live title / status / date and (b) honor an existing DB pointer instead
    of arbitrarily picking one when a ticker has tasks in two sibling lists of
    the same quarter (a mid-quarter tier promotion leaves a task in the old
    list). Caller does candidate selection — see reconcile_ticktick_tasks.

    Returns `(result, degraded)` where:
      result = { quarter: { ticker: [(project_id, project_name, task_dict), ...] } }
      degraded = set of quarters where a list read FAILED. A failed read looks
        like an empty list, so those quarters' candidate sets may be incomplete —
        the caller must not adopt a NULL-pointer task there (it could pick a
        stale sibling in a still-readable list while the real task's list failed).
    """
    result: dict[str, dict[str, list[tuple[str, str, dict]]]] = {q: {} for q in quarters}
    degraded: set[str] = set()
    for p in projects:
        name = p.get("name", "")
        for q in quarters:
            if name.startswith(f"{q} Earnings"):
                pid = p["id"]
                try:
                    tasks = _list_tasks_strict(token, pid)
                except TickTickTokenExpired:
                    raise
                except TickTickError as exc:
                    logger.warning(
                        f"  TickTick: read of '{name}' failed ({exc}); "
                        f"quarter {q} marked degraded (no NULL-pointer adoption)"
                    )
                    degraded.add(q)
                    break
                for t in tasks:
                    ticker = _ticker_from_task_title(t.get("title", ""))
                    if not ticker:
                        continue
                    result[q].setdefault(ticker, []).append((pid, name, t))
                break
    return result, degraded


def _task_date_stale(task: dict, event_date: str) -> bool:
    """True if the task's startDate OR dueDate (date part) differs from
    event_date. Both matter: TickTick snaps dueDate back to startDate, so a
    task whose dueDate looks right but startDate is stale will silently revert."""
    for field in ("startDate", "dueDate"):
        v = (task.get(field) or "")[:10]
        if v and v != event_date:
            return True
    return False


def get_all_earnings_lists(token: str) -> list[dict]:
    """Find all TickTick lists that look like earnings lists."""
    try:
        resp = requests.get(
            f"{TICKTICK_API_BASE}/project",
            headers=_headers(token),
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            projects = resp.json()
            return [p for p in projects if "arning" in p.get("name", "")]
        return []
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"Failed to list TickTick projects: {exc}")
        return []


def show_ticktick_status(token: str):
    """
    Show the status of all earnings-related TickTick lists and tasks.
    Prints a summary of completed vs. pending review tasks.
    """
    earnings_lists = get_all_earnings_lists(token)
    if not earnings_lists:
        logger.info("No earnings lists found in TickTick")
        return

    for project in sorted(earnings_lists, key=lambda p: p.get("name", "")):
        name = project["name"]
        project_id = project["id"]
        tasks = list_tasks_in_project(token, project_id)

        completed = [t for t in tasks if t.get("status", 0) == 2]
        pending = [t for t in tasks if t.get("status", 0) != 2]

        print(f"\n{name} ({len(tasks)} tasks: {len(completed)} done, {len(pending)} pending)")
        print("-" * 60)

        if pending:
            # Sort by due date
            pending.sort(key=lambda t: t.get("dueDate", ""))
            for t in pending:
                title = t.get("title", "Unknown")
                due = t.get("dueDate", "")[:10] if t.get("dueDate") else "no date"
                print(f"  [ ] {title} (due {due})")

        if completed:
            completed.sort(key=lambda t: t.get("dueDate", ""))
            for t in completed[:5]:  # Show last 5 completed
                title = t.get("title", "Unknown")
                print(f"  [x] {title}")
            if len(completed) > 5:
                print(f"  ... and {len(completed) - 5} more completed")


# ---------------------------------------------------------------------------
# Task creation
# ---------------------------------------------------------------------------


def create_task(
    token: str,
    list_id: str,
    title: str,
    content: str,
    due_date: str,
    tags: list[str] | None = None,
) -> str | None:
    """
    Create a task in TickTick. Returns the task ID on success, None on failure.

    Args:
        due_date: YYYY-MM-DD format earnings date
        tags: optional workspace tags to attach (e.g. the coverage sector,
              "Healthcare Services" / "MedTech"). Case is preserved by the API.
    """
    # TickTick expects ISO 8601 with timezone. Set startDate == dueDate so a
    # later date correction (which must move BOTH) has a start to move.
    due_datetime = f"{due_date}T09:00:00.000+0000"

    payload = {
        "title": title,
        "content": content,
        "startDate": due_datetime,
        "dueDate": due_datetime,
        "projectId": list_id,
    }
    if tags:
        payload["tags"] = tags

    try:
        resp = requests.post(
            f"{TICKTICK_API_BASE}/task",
            headers=_headers(token),
            json=payload,
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            task_data = resp.json()
            task_id = task_data.get("id", "")
            logger.info(f"  Created TickTick task: {title}")
            return task_id
        else:
            logger.warning(f"  Failed to create TickTick task '{title}': HTTP {resp.status_code} — {resp.text}")
            return None
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"  Failed to create TickTick task '{title}': {exc}")
        return None


def _find_task_in_project(token: str, project_id: str, task_id: str) -> dict | None:
    """
    Fetch one task by scanning its project's `/data` payload.

    The Open API's documented single-task GET (`/task/{projectId}/{taskId}`)
    returns HTTP 404 for every task in practice — verified 2026-07-22 across
    8/8 tasks in two lists using ids taken straight from `/project/{id}/data`
    (so both ids were provably correct). `/project/{id}/data` is the only
    reliable read, so every task read routes through it.
    """
    for t in list_tasks_in_project(token, project_id):
        if t.get("id") == task_id:
            return t
    return None


def _due_iso(event_date: str) -> str:
    """Format a YYYY-MM-DD date as the TickTick ISO timestamp used on create."""
    return f"{event_date}T09:00:00.000+0000"


# Coverage sectors we surface as TickTick tags (JP's request). Restricted to the
# two Tier-2 sectors — other sectors are left untagged rather than guessed.
_SECTOR_TAGS = {"Healthcare Services", "MedTech"}


def sector_tag(sector: str | None) -> str | None:
    """Return the TickTick tag for a coverage sector, or None if not tagged."""
    return sector if sector in _SECTOR_TAGS else None


def _merge_tags(existing: list[str] | None, add: str) -> list[str] | None:
    """Add `add` to `existing` if not already present (case-insensitive).
    Returns the merged list, or None if no change is needed (tag already there)."""
    existing = existing or []
    if any((t or "").lower() == add.lower() for t in existing):
        return None
    return existing + [add]


def update_task_content(
    token: str,
    list_id: str,
    task_id: str,
    new_content: str | None = None,
    new_title: str | None = None,
    new_date: str | None = None,
    new_tags: list[str] | None = None,
    allow_completed: bool = False,
) -> bool:
    """
    Update a task's title / content / date in place. Returns success.

    Reads the task via the project `/data` endpoint (the single-task GET is
    dead — see `_find_task_in_project`), mutates ONLY the provided fields, and
    POSTs the full object back.

    `new_date` (YYYY-MM-DD) sets BOTH `startDate` AND `dueDate`. TickTick snaps
    `dueDate` back to `startDate` server-side, so writing `dueDate` alone
    silently reverts within ~a minute (verified 2026-07-22 on the UNH task:
    title moved but the date bounced back to the stale `startDate`). Both must
    move together.

    Pass `new_content=None` (the default) to leave the body untouched — this is
    what preserves the user's checklist ticks and notes during a pre-report
    date correction. Only `mark_task_reported` rewrites the body (at report
    time, when the checklist is spent).

    A COMPLETED task (status==2) is left untouched unless `allow_completed=True`:
    reopening/rewriting a task the user ticked off is the worst clobber, and this
    is the single write chokepoint, so the invariant is enforced here for every
    caller (reconcile AND the notify_results/--check-results path). Returns True
    (a no-op success) so callers don't treat the intentional skip as a failure.
    """
    task = _find_task_in_project(token, list_id, task_id)
    if task is None:
        logger.warning(f"  Task {task_id} not found in project {list_id}; cannot update")
        return False

    if task.get("status") == 2 and not allow_completed:
        logger.info(f"  Task {task_id} is completed — leaving it untouched")
        return True

    if new_content is not None:
        task["content"] = new_content
    if new_title is not None:
        task["title"] = new_title
    if new_date is not None:
        iso = _due_iso(new_date)
        task["startDate"] = iso
        task["dueDate"] = iso
    if new_tags is not None:
        task["tags"] = new_tags
    # POST /task/{taskId} requires the id + projectId in the body.
    task["id"] = task_id
    task["projectId"] = list_id

    try:
        resp = requests.post(
            f"{TICKTICK_API_BASE}/task/{task_id}",
            headers=_headers(token),
            json=task,
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            logger.info(f"  Updated TickTick task {task_id}")
            return True
        else:
            logger.warning(
                f"  Failed to update task {task_id}: HTTP {resp.status_code} — {resp.text}"
            )
            return False
    except TickTickTokenExpired:
        raise
    except requests.RequestException as exc:
        logger.warning(f"  Failed to update task {task_id}: {exc}")
        return False


def mark_task_reported(
    token: str,
    task_id: str,
    *,
    ticker: str,
    event_date: str,
    hour: str | None,
    tier: int,
    company_name: str | None,
    eps_estimate: float | None,
    eps_actual: float | None,
    revenue_estimate: float | None,
    revenue_actual: float | None,
    move_pct: float | None = None,
    position: str = "",
    move_label: str | None = None,
    list_id: str | None = None,
    tags: list[str] | None = None,
) -> bool:
    """
    Mark a TickTick task as reported: prepend "[REPORTED]" to the title, embed
    actuals (beat/miss) in the content, and correct the due date to the actual
    report `event_date` (projected dates are routinely wrong — this is the same
    write that fixes the stale date).

    `list_id` is the project the task actually lives in. When known (the caller
    located the task by scanning the quarter's lists), pass it so we fetch the
    RIGHT list. When None (legacy callers) we reconstruct the list name from
    (event_date, tier) — but that reconstruction can miss a sibling/legacy list
    and, worse, `find_or_create_list` would then CREATE a spurious empty list,
    so prefer passing `list_id`.
    """
    if list_id is None:
        list_name = _quarter_list_name(event_date, tier, position=position)
        try:
            list_id = find_or_create_list(token, list_name)
        except TickTickTokenExpired:
            raise
        if not list_id:
            logger.warning(f"  Could not resolve TickTick list '{list_name}' — task {task_id} not updated")
            return False

    new_title = "[REPORTED] " + build_task_title(ticker, event_date, hour)
    new_content = build_task_content(
        ticker=ticker,
        hour=hour,
        eps_estimate=eps_estimate,
        revenue_estimate=revenue_estimate,
        eps_actual=eps_actual,
        revenue_actual=revenue_actual,
        company_name=company_name,
        tier=tier,
    )
    if move_pct is not None:
        sign = "+" if move_pct >= 0 else ""
        suffix = f"\nStock reaction: {sign}{move_pct:.1f}%"
        if move_label:
            suffix += f" ({move_label})"
        new_content += suffix

    return update_task_content(
        token, list_id, task_id, new_content, new_title=new_title,
        new_date=event_date, new_tags=tags,
    )


# ---------------------------------------------------------------------------
# Orchestrator — called from main.py after calendar sync
# ---------------------------------------------------------------------------


def sync_ticktick_tasks(
    conn,
    events: list[dict],
    dry_run: bool = False,
) -> dict:
    """
    Create TickTick tasks for Tier 1+2 earnings events that don't already
    have a task. Returns stats dict.

    Args:
        conn: SQLite connection
        events: list of event dicts from the current run (with ticker, event_date,
                hour, eps_estimate, rev_estimate, tier, company_name, etc.)
        dry_run: if True, log what would be created but don't call API
    """
    config = get_ticktick_config()
    if not config:
        logger.info("TickTick not configured — skipping task creation")
        return {"created": 0, "skipped": 0, "errors": 0}

    token = config["token"]
    stats = {"created": 0, "skipped": 0, "errors": 0}

    # Group events by reporting quarter + tier (separate lists per tier)
    events_by_list: dict[str, list[dict]] = {}
    quarters_needed: set[str] = set()
    for event in events:
        tier = event.get("tier", 3)
        if tier > 2:
            continue  # Skip Tier 3
        if event.get("ticktick_task_id"):
            stats["skipped"] += 1
            continue  # Already has a task

        position = event.get("position", "") or ""
        list_name = _quarter_list_name(event["event_date"], tier, position=position)
        events_by_list.setdefault(list_name, []).append(event)
        quarters_needed.add(_reporting_quarter(event["event_date"]))

    if not events_by_list:
        logger.info("TickTick: No new tasks needed")
        return stats

    logger.info(f"TickTick: {sum(len(v) for v in events_by_list.values())} tasks to create across {len(events_by_list)} list(s)")

    # Cross-list dedup: scan ALL of each quarter's lists once, so a ticker
    # already tracked in any sibling list (Core Watchlist / HC Svcs / legacy
    # Portfolio / legacy Researching) blocks a duplicate write.
    quarter_ticker_map: dict[str, dict[str, tuple[str, str, str]]] = {q: {} for q in quarters_needed}
    projects: list[dict] = []
    if not dry_run:
        try:
            projects = _list_all_projects(token)
            quarter_ticker_map = _gather_quarter_existing_tasks(token, projects, quarters_needed)
            sibling_count = sum(
                1 for p in projects
                for q in quarters_needed
                if p.get("name", "").startswith(f"{q} Earnings")
            )
            total_existing = sum(len(m) for m in quarter_ticker_map.values())
            logger.info(
                f"  Quarter-wide dedup: {total_existing} existing task(s) found "
                f"across {sibling_count} sibling list(s)"
            )
        except TickTickTokenExpired:
            logger.error(
                "TickTick access token expired. Re-run the OAuth flow at "
                "developer.ticktick.com and update TICKTICK_ACCESS_TOKEN."
            )
            stats["errors"] = sum(len(v) for v in events_by_list.values())
            return stats

    # Process each quarterly list
    list_id_cache: dict[str, str] = {p.get("name", ""): p["id"] for p in projects}

    # If a default list ID is configured, use it as fallback
    default_list_id = config.get("list_id")

    for list_name, list_events in events_by_list.items():
        # Resolve list ID
        list_id = list_id_cache.get(list_name)

        if not list_id and not dry_run:
            try:
                list_id = find_or_create_list(token, list_name)
            except TickTickTokenExpired:
                logger.error(
                    "TickTick access token expired. Re-run the OAuth flow at "
                    "developer.ticktick.com and update TICKTICK_ACCESS_TOKEN."
                )
                stats["errors"] = len(list_events)
                return stats

            if list_id:
                list_id_cache[list_name] = list_id
            elif default_list_id:
                logger.warning(f"Could not find/create list '{list_name}', using default list")
                list_id = default_list_id
            else:
                logger.error(
                    f"Could not find/create TickTick list '{list_name}' and no "
                    f"TICKTICK_LIST_EARNINGS fallback configured. "
                    f"Please create the list manually in TickTick and set "
                    f"TICKTICK_LIST_EARNINGS in .env."
                )
                stats["errors"] += len(list_events)
                continue

        if dry_run:
            list_id = "dry-run"

        # Load existing tasks from TickTick to avoid duplicates
        # (handles case where DB lost its ticktick_task_id but task exists)
        existing_tasks = []
        if not dry_run and list_id:
            try:
                existing_tasks = list_tasks_in_project(token, list_id)
                if existing_tasks:
                    logger.info(f"  Found {len(existing_tasks)} existing tasks in '{list_name}'")
            except TickTickTokenExpired:
                raise

        # Create tasks for each event in this list
        for event in list_events:
            ticker = event["ticker"]
            event_date = event["event_date"]
            hour = event.get("event_hour") or event.get("hour")
            title = build_task_title(ticker, event_date, hour)
            content = build_task_content(
                ticker=ticker,
                hour=hour,
                eps_estimate=event.get("eps_estimate"),
                revenue_estimate=event.get("rev_estimate"),
                company_name=event.get("company_name"),
                tier=event.get("tier", 3),
            )

            if dry_run:
                logger.info(f"  [dry-run] Would create TickTick task: {title}")
                stats["created"] += 1
                continue

            # Cross-list dedup: did this ticker already get a task in any
            # sibling list for this quarter? (e.g. legacy Portfolio task or a
            # tier-promoted HC Svcs task)
            quarter = _reporting_quarter(event_date)
            cross_existing = quarter_ticker_map.get(quarter, {}).get(ticker)
            if cross_existing:
                src_pid, src_name, src_tid = cross_existing
                logger.info(f"  Task for {ticker} already exists in '{src_name}', backfilling DB")
                conn.execute(
                    "UPDATE events SET ticktick_task_id = ?, updated_at = datetime('now') "
                    "WHERE ticker = ? AND event_date = ?",
                    (src_tid, ticker, event_date),
                )
                conn.commit()
                stats["skipped"] += 1
                continue

            # Check if task already exists in this specific list
            # (covers cases where the cross-list scan didn't run, e.g. dry_run path)
            existing = find_existing_task_by_ticker(existing_tasks, ticker)
            if existing:
                logger.info(f"  Task already exists in TickTick for {ticker}, backfilling DB")
                conn.execute(
                    "UPDATE events SET ticktick_task_id = ?, updated_at = datetime('now') "
                    "WHERE ticker = ? AND event_date = ?",
                    (existing["id"], ticker, event_date),
                )
                conn.commit()
                stats["skipped"] += 1
                continue

            tag = sector_tag(event.get("sector"))
            try:
                task_id = create_task(
                    token, list_id, title, content, event_date,
                    tags=[tag] if tag else None,
                )
            except TickTickTokenExpired:
                logger.error(
                    "TickTick access token expired. Re-run the OAuth flow at "
                    "developer.ticktick.com and update TICKTICK_ACCESS_TOKEN."
                )
                stats["errors"] += 1
                return stats

            if task_id:
                # Store task ID in the events table
                conn.execute(
                    "UPDATE events SET ticktick_task_id = ?, updated_at = datetime('now') "
                    "WHERE ticker = ? AND event_date = ?",
                    (task_id, ticker, event_date),
                )
                conn.commit()
                # Register in cross-list map so dedup catches same-quarter twins
                quarter_ticker_map.setdefault(quarter, {})[ticker] = (list_id, list_name, task_id)
                stats["created"] += 1
            else:
                stats["errors"] += 1

    logger.info(
        f"TickTick sync: {stats['created']} created, "
        f"{stats['skipped']} already had tasks, {stats['errors']} errors"
    )
    return stats


# ---------------------------------------------------------------------------
# Reconcile — make TickTick a projection of DB truth (self-healing)
# ---------------------------------------------------------------------------


def reconcile_ticktick_tasks(
    conn,
    today,
    *,
    sector_by_ticker: dict | None = None,
    lookback_days: int = 14,
    lookahead_days: int = 45,
    max_db_staleness_days: int = 4,
    dry_run: bool = False,
) -> dict:
    """
    Idempotent desired-state pass over Tier 1/2 TickTick tasks.

    `sync_ticktick_tasks` only CREATES tasks (and only for `event_date >= today`)
    and never edits an existing one. So two things drift and never self-correct:
      1. A task's due date/title, when the projected earnings date was wrong
         (Finnhub cadence projections routinely miss by a week+). The pointer to
         the task also gets destroyed when `upsert_event` collapses the
         projected-date row into the actual-date row, so the one-shot
         reported-marking in `notify_results` can't find the task and skips it
         silently — leaving the task open, unmarked, at the stale date forever.
      2. Any Tier 1/2 name that reported while its task pointer was lost.

    This pass fixes both by treating the DB as truth and the TickTick tasks as a
    projection to be reconciled each run. It is keyed on (ticker, reporting
    quarter): task IDENTITY comes from scanning the quarter's lists live (only
    two lists per quarter, so ~2-4 API calls amortized across the whole run),
    and the DB `ticktick_task_id` is treated as a cache that gets backfilled.

    For each Tier 1/2 event in [today-lookback, today+lookahead]:
      - locate the task by scanning the quarter's lists; backfill the DB pointer;
      - NEVER touch a completed task (`status == 2`) — reopening/rewriting a task
        the user ticked off is the worst possible clobber;
      - if the DB row is reported and the task isn't `[REPORTED]` yet → mark it
        reported (rewrites title + body with actuals AND corrects the date);
      - else (pre-report) if the task's due date != the event's current date →
        correct the title + start/due date ONLY, leaving the body (checklist
        ticks / notes) intact.

    The window is quarter-scoped in effect: only quarters that the window's
    events map to are scanned, so long-closed quarters are never re-touched
    regardless of `lookback_days`.

    Set `dry_run=True` to log every change it WOULD make without writing — used
    for the first run so the bulk repair can be eyeballed before it writes.
    """
    from datetime import timedelta, date

    stats = {
        "checked": 0,
        "date_fixed": 0,
        "marked_reported": 0,
        "tag_added": 0,
        "pointer_backfilled": 0,
        "skipped_done": 0,
        "skipped_phantom": 0,
        "skipped_stale": 0,
        "skipped_ambiguous": 0,
        "no_task": 0,
        "errors": 0,
    }
    sector_by_ticker = sector_by_ticker or {}
    from storage import find_reported_event_for_quarter

    config = get_ticktick_config()
    if not config:
        logger.info("TickTick not configured — skipping reconcile")
        return stats
    token = config["token"]

    lo = (today - timedelta(days=lookback_days)).isoformat()
    hi = (today + timedelta(days=lookahead_days)).isoformat()

    # Staleness guard. Reconcile pushes DB truth ONTO TickTick, so a stale DB
    # (e.g. a frozen local copy, or a failed CI-artifact restore that fell back
    # to an old snapshot) would faithfully corrupt TickTick — reverting a correct
    # date to an old projection, or un-reporting a name. Refuse rather than act on
    # input we can't trust. Scope the freshness check to the rows reconcile
    # actually mutates by date: UNREPORTED Tier 1/2 events in-window. A live CI
    # sync stamps updated_at=now on these every run; a reported past row is
    # legitimately old (sync stops touching it) so it must NOT count toward
    # staleness, and an unrelated fresh row elsewhere must NOT mask a frozen
    # target set. NULL (no unreported targets) → nothing risky to write → proceed.
    # date_locked rows are operator overrides: intentionally pinned and
    # legitimately old, so they must NOT count toward staleness (one old lock
    # would otherwise abort the whole reconcile).
    newest = conn.execute(
        "SELECT MAX(updated_at) FROM events "
        "WHERE tier <= 2 AND reported = 0 AND date_locked = 0 "
        "AND event_date BETWEEN ? AND ?",
        (lo, hi),
    ).fetchone()[0]
    if newest:
        try:
            newest_date = date.fromisoformat(str(newest)[:10])
            age = (today - newest_date).days
            if age > max_db_staleness_days:
                logger.error(
                    "TickTick reconcile ABORTED: reconcile-target rows look stale "
                    "(newest unreported Tier 1/2 event updated %s, %d days old > %d). "
                    "Reconcile would push stale dates onto TickTick. Refusing.",
                    newest_date, age, max_db_staleness_days,
                )
                stats["errors"] += 1
                return stats
        except ValueError:
            pass  # unparseable timestamp — don't block on it
    rows = conn.execute(
        "SELECT ticker, event_date, event_hour, quarter, reported, "
        "eps_estimate, eps_actual, rev_estimate, rev_actual, tier, "
        "company_name, ticktick_task_id, updated_at, date_locked "
        "FROM events WHERE tier <= 2 AND event_date BETWEEN ? AND ? "
        "ORDER BY event_date, ticker",
        (lo, hi),
    ).fetchall()
    if not rows:
        logger.info("TickTick reconcile: no Tier 1/2 events in window")
        return stats

    # Canonicalize to ONE row per (ticker, reporting-quarter). Finnhub
    # double-lists a name that flapped dates (a reported row WITH actuals plus a
    # no-actuals phantom forward date, both in the same quarter — the ICLR
    # class). Without this, processing the phantom (reported=0) row would move
    # the reported task to the phantom date and strip its [REPORTED] title. A
    # reported row wins; then the later event_date. (row layout: event_date=1,
    # reported=4.)
    canonical: dict[tuple, tuple] = {}
    for r in rows:
        key = (r[0], _reporting_quarter(r[1]))
        cur = canonical.get(key)
        if cur is None or (int(r[4] or 0), r[1]) > (int(cur[4] or 0), cur[1]):
            canonical[key] = r
    rows = list(canonical.values())

    quarters = {_reporting_quarter(r[1]) for r in rows}
    try:
        projects = _list_all_projects(token)
        qmap, degraded_quarters = _gather_quarter_tasks_full(token, projects, quarters)
    except TickTickTokenExpired:
        logger.error(
            "TickTick access token expired — reconcile aborted. Re-run the OAuth "
            "flow at developer.ticktick.com and update TICKTICK_ACCESS_TOKEN."
        )
        stats["errors"] += 1
        return stats

    prefix = "[dry-run] " if dry_run else ""

    for r in rows:
        (ticker, event_date, event_hour, quarter, reported,
         eps_est, eps_act, rev_est, rev_act, tier, company_name, db_task_id,
         row_updated, date_locked) = r
        stats["checked"] += 1

        # Per-row staleness skip. The wholesale-frozen-DB case aborts loudly
        # above; this catches PARTIAL staleness the global MAX check can't — a
        # single unreported row that stopped being synced (an unseen/dropped
        # ticker) while its siblings stay fresh. Its projected date can't be
        # trusted, so don't push it. Reported rows are exempt (their date is the
        # real report date, correct however old the row); so are date_locked rows
        # (operator-pinned, trusted regardless of age).
        if not reported and not date_locked and row_updated:
            try:
                if (today - date.fromisoformat(str(row_updated)[:10])).days > max_db_staleness_days:
                    stats["skipped_stale"] += 1
                    continue
            except ValueError:
                pass

        # DB-wide phantom guard. An unreported row whose quarter is ALREADY
        # reported at a different date is a Finnhub phantom forward-listing —
        # acting on it would move the [REPORTED] task to the phantom date and
        # strip the prefix. This catches the case the in-window canonicalization
        # can't: the real reported row sits OUTSIDE the reconcile window. Uses the
        # stored DB quarter ("2026Q1"), scanning the whole table, not just window.
        if not reported:
            rep = find_reported_event_for_quarter(conn, ticker, quarter)
            if rep and rep["event_date"] != event_date:
                stats["skipped_phantom"] += 1
                continue

        rq = _reporting_quarter(event_date)
        candidates = qmap.get(rq, {}).get(ticker) or []
        if not candidates:
            stats["no_task"] += 1
            continue

        # Candidate selection.
        #  - A non-null DB pointer is authoritative: use the matching live task.
        #  - If it's set but matches NO candidate this run, do NOT clobber it or
        #    mutate an arbitrary sibling. A per-list read failure surfaces as an
        #    empty list (list_tasks_in_project swallows errors), indistinguishable
        #    from a real delete — either way, skip and let a healthy next run
        #    reconcile rather than repoint onto the wrong task.
        #  - Only when the pointer is NULL do we adopt a task, preferring an OPEN
        #    candidate over a completed legacy one (a done T_OLD listed first must
        #    not shadow the live T_GOOD).
        if db_task_id:
            chosen = next((c for c in candidates if c[2].get("id") == db_task_id), None)
            if chosen is None:
                logger.warning(
                    "  TickTick: %s DB pointer %s matches no live task this run "
                    "(deleted, or a transient list read failure) — skipping to avoid "
                    "clobbering onto the wrong task.",
                    ticker, db_task_id,
                )
                stats["skipped_ambiguous"] += 1
                continue
        else:
            # NULL pointer → adopt a task. But if this quarter had a list read
            # failure, the visible candidates may be incomplete (the real task's
            # list is the one that failed), so adopting/backfilling could latch
            # onto a stale sibling. Skip; a healthy next run adopts correctly.
            if rq in degraded_quarters:
                logger.warning(
                    "  TickTick: %s has no DB pointer and quarter %s had a list "
                    "read failure — skipping adoption to avoid a wrong sibling.",
                    ticker, rq,
                )
                stats["skipped_ambiguous"] += 1
                continue
            pool = [c for c in candidates if c[2].get("status") != 2] or candidates
            if len(pool) > 1:
                logger.warning(
                    "  TickTick: %s has %d open tasks in %s across %s with no DB "
                    "pointer — adopting '%s'. Dedupe manually.",
                    ticker, len(pool), rq, sorted({c[1] for c in pool}), pool[0][1],
                )
            chosen = pool[0]
        pid, pname, task = chosen
        task_id = task.get("id")

        # Backfill the DB pointer (cache) only when it's missing or dead. If it
        # already matched a live candidate, `chosen` honored it and task_id ==
        # db_task_id, so no write.
        if db_task_id != task_id:
            stats["pointer_backfilled"] += 1
            if not dry_run:
                conn.execute(
                    "UPDATE events SET ticktick_task_id = ?, updated_at = datetime('now') "
                    "WHERE ticker = ? AND event_date = ?",
                    (task_id, ticker, event_date),
                )
                conn.commit()

        # Never touch a task the user has completed.
        if task.get("status") == 2:
            stats["skipped_done"] += 1
            continue

        title = task.get("title", "") or ""
        # Sector tag to ensure (merged in without clobbering the user's tags).
        want_tag = sector_tag(sector_by_ticker.get(ticker))
        merged_tags = _merge_tags(task.get("tags"), want_tag) if want_tag else None
        # Date repair is independent of reported/title state and checks BOTH
        # date fields (dueDate alone would miss a stale startDate that then drags
        # dueDate back).
        date_stale = _task_date_stale(task, event_date)
        try:
            if reported and "[REPORTED]" not in title:
                # Transition to reported in ONE write: title + date + actuals body
                # + sector tag together. Splitting the tag into a second write
                # would re-read /project/data and, if that read lags, clobber the
                # just-posted [REPORTED] repair with a stale object.
                logger.info(
                    f"  {prefix}mark reported: {ticker} {event_date} (list='{pname}')"
                )
                if merged_tags is not None:
                    logger.info(f"  {prefix}add tag {want_tag!r}: {ticker} (list='{pname}')")
                if not dry_run:
                    ok = mark_task_reported(
                        token, task_id,
                        ticker=ticker, event_date=event_date, hour=event_hour,
                        tier=tier, company_name=company_name,
                        eps_estimate=eps_est, eps_actual=eps_act,
                        revenue_estimate=rev_est, revenue_actual=rev_act,
                        list_id=pid, tags=merged_tags,
                    )
                    if not ok:
                        stats["errors"] += 1
                        continue
                stats["marked_reported"] += 1
                if merged_tags is not None:
                    stats["tag_added"] += 1
            elif date_stale or merged_tags is not None:
                # Title/date fix + tag in ONE write, for BOTH pre-report tasks and
                # already-[REPORTED] tasks whose date is stale. Body (checklist /
                # actuals) is left untouched. On an already-reported task the
                # title keeps its "[REPORTED] " prefix.
                new_title = None
                if date_stale:
                    new_title = build_task_title(ticker, event_date, event_hour)
                    if reported and "[REPORTED]" in title:
                        new_title = "[REPORTED] " + new_title
                    logger.info(
                        f"  {prefix}fix date: {ticker} -> {event_date} (list='{pname}')"
                    )
                if merged_tags is not None:
                    logger.info(f"  {prefix}add tag {want_tag!r}: {ticker} (list='{pname}')")
                if not dry_run:
                    ok = update_task_content(
                        token, pid, task_id,
                        new_title=new_title,
                        new_date=event_date if date_stale else None,
                        new_tags=merged_tags,
                    )
                    if not ok:
                        stats["errors"] += 1
                        continue
                if date_stale:
                    stats["date_fixed"] += 1
                if merged_tags is not None:
                    stats["tag_added"] += 1
        except TickTickTokenExpired:
            logger.error("TickTick access token expired mid-reconcile — stopping")
            stats["errors"] += 1
            break
        except Exception as exc:  # best-effort per task; never abort the whole pass
            logger.warning(f"  TickTick reconcile failed for {ticker} {event_date}: {exc}")
            stats["errors"] += 1

    logger.info(
        "TickTick reconcile%s: checked=%d date_fixed=%d marked_reported=%d "
        "tag_added=%d backfilled=%d skipped_done=%d skipped_phantom=%d "
        "skipped_stale=%d skipped_ambiguous=%d no_task=%d errors=%d"
        % (" [dry-run]" if dry_run else "", stats["checked"], stats["date_fixed"],
           stats["marked_reported"], stats["tag_added"], stats["pointer_backfilled"],
           stats["skipped_done"], stats["skipped_phantom"], stats["skipped_stale"],
           stats["skipped_ambiguous"], stats["no_task"], stats["errors"])
    )
    return stats
