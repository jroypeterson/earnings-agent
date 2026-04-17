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
    1: "Core Watchlist",
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


def _quarter_list_name(event_date: str, tier: int = 2) -> str:
    """
    Generate the quarterly list name from an event date and tier.

    Uses the reporting quarter (what period results cover), not the
    calendar quarter of the release date.

    e.g. "2026-04-30", tier=1 -> "1Q26 Earnings - Core Watchlist"
         "2026-04-30", tier=2 -> "1Q26 Earnings - HC Svcs & MedTech"
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
        "TICKTICK_EARNINGS_GROUP_ID", "6887b7f873800767fff51bf5"
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


def find_existing_task_by_ticker(tasks: list[dict], ticker: str) -> dict | None:
    """
    Search a list of TickTick tasks for one matching a ticker.
    Matches on the task title starting with the ticker symbol.
    """
    for task in tasks:
        title = task.get("title", "")
        # Task titles look like "UNH Q1 2026 Earnings (Apr 21 BMO)"
        if title.startswith(f"{ticker} "):
            return task
    return None


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
) -> str | None:
    """
    Create a task in TickTick. Returns the task ID on success, None on failure.

    Args:
        due_date: YYYY-MM-DD format earnings date
    """
    # TickTick expects ISO 8601 with timezone
    due_datetime = f"{due_date}T09:00:00.000+0000"

    payload = {
        "title": title,
        "content": content,
        "dueDate": due_datetime,
        "projectId": list_id,
    }

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


def update_task_content(
    token: str,
    list_id: str,
    task_id: str,
    new_content: str,
    new_title: str | None = None,
) -> bool:
    """Update a task's content (and optionally title). Returns success."""
    try:
        # First get the existing task
        resp = requests.get(
            f"{TICKTICK_API_BASE}/task/{list_id}/{task_id}",
            headers=_headers(token),
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code != 200:
            logger.warning(f"  Failed to fetch task {task_id}: HTTP {resp.status_code}")
            return False

        task_data = resp.json()
        task_data["content"] = new_content
        if new_title is not None:
            task_data["title"] = new_title

        # Update the task
        resp = requests.post(
            f"{TICKTICK_API_BASE}/task/{task_id}",
            headers=_headers(token),
            json=task_data,
            timeout=15,
        )
        if resp.status_code == 401:
            raise TickTickTokenExpired("TickTick access token expired")
        if resp.status_code == 200:
            logger.info(f"  Updated TickTick task {task_id}")
            return True
        else:
            logger.warning(f"  Failed to update task {task_id}: HTTP {resp.status_code}")
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
    move_label: str | None = None,
) -> bool:
    """
    Mark a TickTick task as reported: prepend "[REPORTED]" to the title and
    embed actuals (beat/miss) in the content.

    The task lives in the quarterly list keyed on (event_date, tier), so we
    look up the list ID by name rather than tracking it in the DB.
    """
    list_name = _quarter_list_name(event_date, tier)
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

    return update_task_content(token, list_id, task_id, new_content, new_title=new_title)


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
    for event in events:
        tier = event.get("tier", 3)
        if tier > 2:
            continue  # Skip Tier 3
        if event.get("ticktick_task_id"):
            stats["skipped"] += 1
            continue  # Already has a task

        list_name = _quarter_list_name(event["event_date"], tier)
        events_by_list.setdefault(list_name, []).append(event)

    if not events_by_list:
        logger.info("TickTick: No new tasks needed")
        return stats

    logger.info(f"TickTick: {sum(len(v) for v in events_by_list.values())} tasks to create across {len(events_by_list)} list(s)")

    # Process each quarterly list
    list_id_cache: dict[str, str] = {}

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

            # Check if task already exists in TickTick (dedup fallback)
            existing = find_existing_task_by_ticker(existing_tasks, ticker)
            if existing:
                logger.info(f"  Task already exists in TickTick for {ticker}, backfilling DB")
                # Backfill the task ID into our DB
                conn.execute(
                    "UPDATE events SET ticktick_task_id = ?, updated_at = datetime('now') "
                    "WHERE ticker = ? AND event_date = ?",
                    (existing["id"], ticker, event_date),
                )
                conn.commit()
                stats["skipped"] += 1
                continue

            try:
                task_id = create_task(token, list_id, title, content, event_date)
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
                stats["created"] += 1
            else:
                stats["errors"] += 1

    logger.info(
        f"TickTick sync: {stats['created']} created, "
        f"{stats['skipped']} already had tasks, {stats['errors']} errors"
    )
    return stats
