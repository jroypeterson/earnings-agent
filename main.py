"""
Earnings Intelligence System — CLI entry point and orchestrator.

Usage:
    python main.py                     # Daily sync (normal mode)
    python main.py --dry-run           # Preview without creating calendar events
    python main.py --backfill          # Also look back 30 days for missed earnings
    python main.py --cleanup           # Delete duplicate events from Google Calendar
    python main.py --cleanup --dry-run # Preview which duplicates would be deleted
"""

import sys
import time
import argparse
import logging
from datetime import date, timedelta

from config import (
    GOOGLE_CALENDAR_ID,
    FINNHUB_API_KEY,
    SLACK_WEBHOOK_EARNINGS,
    DIGEST_HTML_PATH,
)
from coverage import load_coverage, get_tickers_by_tier, get_ticker_info, TickerInfo
from storage import (
    init_db,
    find_existing_event,
    find_event_for_ticker_near_date,
    upsert_event,
    record_estimate_snapshot,
    date_to_quarter,
    set_date_lock,
    list_locked_events,
    is_ticker_date_locked,
)
from finnhub_client import get_client as get_finnhub_client, fetch_earnings, FinnhubError
from calendar_sync import (
    get_calendar_service,
    find_calendar_event,
    create_calendar_event,
    update_calendar_event_description,
    delete_calendar_event,
    build_description,
    cleanup_duplicates,
    CalendarError,
)
from ticktick import (
    sync_ticktick_tasks,
    get_ticktick_config,
    show_ticktick_status,
    mark_task_reported,
    TickTickTokenExpired,
)
from digest import build_weekly_digest
from notifications import (
    build_slack_blocks,
    build_slack_fallback_text,
    build_email_html,
    build_email_text,
    build_results_slack_blocks,
    build_results_fallback_text,
    build_reconcile_blocks,
    build_reconcile_fallback,
    build_unseen_blocks,
    build_unseen_fallback,
    post_slack,
    post_heartbeat,
    NotificationError,
    ResultRow,
    DriftRow,
    UnseenRow,
)
from market_data import fetch_post_earnings_move

logger = logging.getLogger("earnings_agent")


def run(
    dry_run: bool = False,
    backfill: bool = False,
    skip_ticktick: bool = False,
    skip_heartbeat: bool = False,
):
    """Main sync: collect earnings data, sync to calendar, store estimate snapshots."""
    start_ts = time.monotonic()

    # --- Load coverage ---
    coverage = load_coverage()
    if not coverage:
        logger.error("No tickers loaded. Check Coverage Manager exports or tickers.txt.")
        sys.exit(1)

    # For Finnhub queries, include all tiers (we filter on output, not input)
    all_tickers = [t.ticker for t in coverage]
    # Build lookup for tier info
    coverage_map = {t.ticker: t for t in coverage}

    # --- Validate config ---
    missing = []
    if not FINNHUB_API_KEY:
        missing.append("FINNHUB_API_KEY")
    if not GOOGLE_CALENDAR_ID:
        missing.append("GOOGLE_CALENDAR_ID")
    if missing:
        logger.error(f"Missing required config: {', '.join(missing)}")
        logger.error("Copy .env.example to .env and fill in your values.")
        sys.exit(1)

    # --- Set up clients ---
    try:
        fh_client = get_finnhub_client()
    except FinnhubError as exc:
        logger.error(f"Failed to initialize Finnhub client: {exc}")
        sys.exit(1)

    conn = init_db()

    cal_service = None
    if not dry_run:
        try:
            cal_service = get_calendar_service()
        except Exception as exc:
            logger.error(f"Failed to initialize Google Calendar service: {exc}")
            sys.exit(1)

    # --- Determine date range ---
    today = date.today()
    if backfill:
        from_date = (today - timedelta(days=30)).isoformat()
    else:
        from_date = (today - timedelta(days=14)).isoformat()
    to_date = (today + timedelta(days=90)).isoformat()

    # --- Preflight: index tagged calendar events by gcal_id so we can
    #     detect DB/calendar date drift without an extra GET per event. ---
    cal_start_by_id: dict[str, str] = {}
    if cal_service:
        try:
            from config import CALENDAR_PAGE_SIZE
            preflight_min = (today - timedelta(days=30)).isoformat() + "T00:00:00Z"
            preflight_max = (today + timedelta(days=120)).isoformat() + "T00:00:00Z"
            page_token = None
            while True:
                r = cal_service.events().list(
                    calendarId=GOOGLE_CALENDAR_ID,
                    privateExtendedProperty="earningsAgent=true",
                    timeMin=preflight_min,
                    timeMax=preflight_max,
                    singleEvents=True,
                    maxResults=CALENDAR_PAGE_SIZE,
                    pageToken=page_token,
                ).execute()
                for e in r.get("items", []):
                    start = e.get("start", {})
                    d = start.get("date") or start.get("dateTime", "")[:10]
                    if d:
                        cal_start_by_id[e["id"]] = d
                page_token = r.get("nextPageToken")
                if not page_token:
                    break
            logger.info(f"Preflight indexed {len(cal_start_by_id)} tagged calendar events")
        except Exception as exc:
            logger.warning(f"Calendar preflight failed; drift detection disabled: {exc}")

    # --- Fetch earnings ---
    earnings = fetch_earnings(fh_client, all_tickers, from_date, to_date)

    new_count = 0
    updated_count = 0
    actuals_count = 0
    skip_count = 0
    snapshot_date = today.isoformat()
    # Actuals collected during this sync for Slack + TickTick downstream
    sync_results: list[ResultRow] = []

    for e in earnings:
        ticker = e["symbol"].upper()
        earnings_date = e["date"]
        hour = e.get("hour")
        eps_est = e.get("epsEstimate")
        eps_act = e.get("epsActual")
        rev_est = e.get("revenueEstimate")
        rev_act = e.get("revenueActual")
        quarter = date_to_quarter(earnings_date)

        # Resolve tier
        info = coverage_map.get(ticker)
        tier = info.tier if info else 3
        company_name = info.company_name if info else None
        source_fingerprint = f"{ticker}:{earnings_date}"

        has_actuals = eps_act is not None or rev_act is not None

        # Record estimate snapshot for revision tracking
        if eps_est is not None or rev_est is not None:
            record_estimate_snapshot(
                conn, ticker, earnings_date, snapshot_date, eps_est, rev_est
            )

        # Look up existing event (by exact date first, then nearby)
        existing = find_existing_event(conn, ticker, earnings_date)
        if not existing:
            existing = find_event_for_ticker_near_date(conn, ticker, earnings_date)

        if existing:
            # --- Check if actuals just came in ---
            if has_actuals and not existing["reported"]:
                logger.info(
                    f"Actuals in: {ticker} {quarter} — "
                    f"EPS: ${eps_act:.2f} vs ${eps_est:.2f} est"
                    if eps_act is not None and eps_est is not None
                    else f"Actuals in: {ticker} {quarter}"
                )

                gcal_id = existing["gcal_id"]

                if not dry_run and gcal_id and cal_service:
                    try:
                        new_summary = f"[REPORTED] {ticker} Earnings Release"
                        new_description = build_description(
                            ticker, hour, eps_est, eps_act, rev_est, rev_act
                        )
                        update_calendar_event_description(
                            cal_service, GOOGLE_CALENDAR_ID,
                            gcal_id, new_summary, new_description,
                            ticker=ticker, quarter=quarter,
                            source_fingerprint=source_fingerprint,
                            tier=tier,
                        )
                        logger.info(f"Updated calendar event with actuals for {ticker}")
                    except CalendarError as exc:
                        logger.error(f"Failed to update event for {ticker}: {exc}")

                upsert_event(
                    conn, ticker, earnings_date, hour, existing["gcal_id"],
                    quarter=quarter, eps_estimate=eps_est, eps_actual=eps_act,
                    rev_estimate=rev_est, rev_actual=rev_act, reported=True,
                    tier=tier, company_name=company_name,
                    source_fingerprint=source_fingerprint,
                )
                actuals_count += 1

                # Collect for Slack + TickTick post-loop notification
                if not dry_run:
                    move = fetch_post_earnings_move(ticker, earnings_date, hour)
                    sync_results.append(
                        ResultRow(
                            ticker=ticker,
                            company_name=company_name or "",
                            event_date=earnings_date,
                            event_hour=hour,
                            eps_actual=eps_act,
                            eps_estimate=eps_est,
                            rev_actual=rev_act,
                            rev_estimate=rev_est,
                            tier=tier,
                            move=move,
                        )
                    )
                continue

            # --- Check if date or timing changed ---
            date_changed = existing["event_date"] != earnings_date
            hour_changed = existing.get("event_hour") != hour

            # Calendar drift: DB agrees with Finnhub but the calendar event
            # itself is parked at an old date (happens when a prior run
            # backfilled DB from calendar without pushing Finnhub's update).
            calendar_stale = False
            existing_gcal_id = existing.get("gcal_id")
            if (
                tier <= 2
                and existing_gcal_id
                and existing_gcal_id in cal_start_by_id
            ):
                cal_start = cal_start_by_id[existing_gcal_id]
                if cal_start != earnings_date:
                    calendar_stale = True
                    logger.info(
                        f"Calendar drift: {ticker} {quarter} "
                        f"calendar={cal_start}, Finnhub={earnings_date}. Recreating."
                    )

            if not date_changed and not hour_changed and not calendar_stale:
                skip_count += 1
                continue

            # D2: respect human override. If the event is date-locked, do
            # not delete+recreate the calendar event even when Finnhub
            # disagrees. Log a single WARNING per drift so the user knows
            # the lock is suppressing an update.
            if existing.get("date_locked"):
                logger.warning(
                    f"Locked: {ticker} {quarter} date_locked=1 — "
                    f"NOT moving calendar. DB={existing['event_date']}, "
                    f"Finnhub={earnings_date}"
                )
                skip_count += 1
                continue

            old_date = existing["event_date"]
            old_gcal_id = existing["gcal_id"]

            if date_changed:
                logger.info(
                    f"Date changed: {ticker} {quarter} moved from "
                    f"{old_date} -> {earnings_date}"
                )
            if hour_changed:
                logger.info(
                    f"Timing changed: {ticker} {quarter} on {earnings_date} "
                    f"({existing.get('event_hour') or 'TBD'} -> {hour or 'TBD'})"
                )

            # Only manage calendar events for Tier 1 and Tier 2
            if tier <= 2:
                if not dry_run and old_gcal_id and cal_service:
                    try:
                        delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, old_gcal_id)
                        logger.info(f"Deleted old calendar event for {ticker} on {old_date}")
                    except CalendarError as exc:
                        logger.warning(f"Could not delete old event for {ticker}: {exc}")

                gcal_id = None
                if not dry_run and cal_service:
                    try:
                        gcal_id = create_calendar_event(
                            cal_service, GOOGLE_CALENDAR_ID, ticker,
                            earnings_date, hour,
                            quarter=quarter, eps_estimate=eps_est,
                            eps_actual=eps_act, revenue_estimate=rev_est,
                            revenue_actual=rev_act, tier=tier,
                            source_fingerprint=source_fingerprint,
                        )
                    except CalendarError as exc:
                        logger.error(f"Failed to create updated event for {ticker}: {exc}")
                        continue
            else:
                gcal_id = existing.get("gcal_id")

            upsert_event(
                conn, ticker, earnings_date, hour,
                gcal_id if tier <= 2 else existing.get("gcal_id"),
                quarter=quarter, eps_estimate=eps_est, eps_actual=eps_act,
                rev_estimate=rev_est, rev_actual=rev_act, reported=has_actuals,
                tier=tier, company_name=company_name,
                source_fingerprint=source_fingerprint,
            )
            updated_count += 1

        else:
            # --- Brand new event ---
            # Only create calendar events for Tier 1 and Tier 2
            gcal_id = None

            if tier <= 2:
                # Check Google Calendar API to prevent duplicates when DB is lost
                if not dry_run and cal_service:
                    cal_event = find_calendar_event(
                        cal_service, GOOGLE_CALENDAR_ID, ticker, earnings_date,
                        source_fingerprint=source_fingerprint,
                    )
                    if cal_event:
                        gcal_id = cal_event.get("id")
                        # Compare the calendar event's actual start date against
                        # Finnhub's current date. If Finnhub has moved the event,
                        # delete and recreate so calendar + DB stay in sync.
                        cal_start = cal_event.get("start", {})
                        cal_start_date = (
                            cal_start.get("date")
                            or cal_start.get("dateTime", "")[:10]
                        )
                        if cal_start_date and cal_start_date != earnings_date:
                            logger.info(
                                f"Stale calendar event for {ticker}: "
                                f"calendar={cal_start_date}, Finnhub={earnings_date}. Recreating."
                            )
                            try:
                                delete_calendar_event(
                                    cal_service, GOOGLE_CALENDAR_ID, gcal_id
                                )
                            except CalendarError as exc:
                                logger.warning(
                                    f"Could not delete stale event for {ticker}: {exc}"
                                )
                            gcal_id = None
                            try:
                                gcal_id = create_calendar_event(
                                    cal_service, GOOGLE_CALENDAR_ID, ticker,
                                    earnings_date, hour,
                                    quarter=quarter, eps_estimate=eps_est,
                                    eps_actual=eps_act, revenue_estimate=rev_est,
                                    revenue_actual=rev_act, tier=tier,
                                    source_fingerprint=source_fingerprint,
                                )
                            except CalendarError as exc:
                                logger.error(
                                    f"Failed to recreate event for {ticker}: {exc}"
                                )
                            upsert_event(
                                conn, ticker, earnings_date, hour, gcal_id,
                                quarter=quarter, eps_estimate=eps_est,
                                eps_actual=eps_act, rev_estimate=rev_est,
                                rev_actual=rev_act, reported=has_actuals,
                                tier=tier, company_name=company_name,
                                source_fingerprint=source_fingerprint,
                            )
                            updated_count += 1
                            continue

                        # Date matches — safe to backfill DB only.
                        upsert_event(
                            conn, ticker, earnings_date, hour, gcal_id,
                            quarter=quarter, eps_estimate=eps_est,
                            eps_actual=eps_act, rev_estimate=rev_est,
                            rev_actual=rev_act, reported=has_actuals,
                            tier=tier, company_name=company_name,
                            source_fingerprint=source_fingerprint,
                        )
                        logger.info(f"Backfilled DB from calendar for {ticker} {quarter}")
                        skip_count += 1
                        continue

                logger.info(f"New earnings: {ticker} {quarter} on {earnings_date} ({hour or 'time TBD'}) [Tier {tier}]")

                if not dry_run and cal_service:
                    try:
                        gcal_id = create_calendar_event(
                            cal_service, GOOGLE_CALENDAR_ID, ticker,
                            earnings_date, hour,
                            quarter=quarter, eps_estimate=eps_est,
                            eps_actual=eps_act, revenue_estimate=rev_est,
                            revenue_actual=rev_act, tier=tier,
                            source_fingerprint=source_fingerprint,
                        )
                    except CalendarError as exc:
                        logger.error(f"Failed to create calendar event for {ticker}: {exc}")
                        continue
            else:
                logger.debug(f"New earnings (Tier 3, no calendar): {ticker} {quarter} on {earnings_date}")

            upsert_event(
                conn, ticker, earnings_date, hour, gcal_id,
                quarter=quarter, eps_estimate=eps_est, eps_actual=eps_act,
                rev_estimate=rev_est, rev_actual=rev_act, reported=has_actuals,
                tier=tier, company_name=company_name,
                source_fingerprint=source_fingerprint,
            )
            new_count += 1

    # --- Summary ---
    logger.info("=" * 50)
    logger.info(
        f"Done! {new_count} new, {updated_count} updated, "
        f"{actuals_count} actuals added, {skip_count} unchanged."
    )
    if dry_run:
        logger.info("(Dry run — no calendar events were actually created, updated, or deleted)")
    logger.info("=" * 50)

    # --- Notify on any newly-reported actuals detected during this sync ---
    if sync_results and not dry_run:
        logger.info(f"Notifying on {len(sync_results)} actuals detected during sync")
        notify_results(conn, sync_results, today)

    # --- B2: unseen-ticker detection ---
    # For every Tier 1/2 upcoming-30d event in DB, check whether Finnhub
    # surfaced it this run. Increment a per-event counter and alert when it
    # hits 2 consecutive daily syncs. Tickers not in current coverage are
    # skipped to avoid noise from Coverage Manager drops.
    if not dry_run:
        seen_pairs = {
            (e.get("symbol", "").upper(), e.get("date"))
            for e in earnings
        }
        horizon_iso = (today + timedelta(days=30)).isoformat()
        cur = conn.execute(
            "SELECT ticker, event_date, company_name, tier, unseen_run_count "
            "FROM events "
            "WHERE tier <= 2 AND event_date >= ? AND event_date <= ? "
            "AND reported = 0",
            (today.isoformat(), horizon_iso),
        )
        persistent_unseen: list[UnseenRow] = []
        for row in cur.fetchall():
            ticker, event_date, company_name, tier_val, prev_count = row
            if ticker not in coverage_map:
                continue
            if (ticker, event_date) in seen_pairs:
                if prev_count and prev_count > 0:
                    conn.execute(
                        "UPDATE events SET unseen_run_count = 0 "
                        "WHERE ticker = ? AND event_date = ?",
                        (ticker, event_date),
                    )
                continue
            new_count = (prev_count or 0) + 1
            conn.execute(
                "UPDATE events SET unseen_run_count = ? "
                "WHERE ticker = ? AND event_date = ?",
                (new_count, ticker, event_date),
            )
            if new_count >= 2:
                persistent_unseen.append(UnseenRow(
                    ticker=ticker,
                    company_name=company_name or "",
                    event_date=event_date,
                    tier=tier_val,
                    miss_count=new_count,
                ))
        conn.commit()

        if persistent_unseen:
            logger.warning(
                f"B2: {len(persistent_unseen)} Tier 1/2 event(s) missing from "
                f"Finnhub for 2+ consecutive runs"
            )
            if SLACK_WEBHOOK_EARNINGS:
                try:
                    post_slack(
                        SLACK_WEBHOOK_EARNINGS,
                        build_unseen_blocks(persistent_unseen, today),
                        build_unseen_fallback(persistent_unseen, today),
                    )
                except NotificationError as exc:
                    logger.error(f"Unseen-ticker Slack post failed: {exc}")

    # --- TickTick task sync (Tier 1 + Tier 2 only) ---
    tt_stats: dict | None = None
    if not skip_ticktick:
        # Gather all Tier 1+2 events that need TickTick tasks
        cur = conn.execute(
            "SELECT ticker, event_date, event_hour, eps_estimate, rev_estimate, "
            "tier, company_name, ticktick_task_id "
            "FROM events WHERE tier <= 2 AND event_date >= ? "
            "ORDER BY event_date, ticker",
            (today.isoformat(),)
        )
        ticktick_events = []
        for row in cur.fetchall():
            ticktick_events.append({
                "ticker": row[0],
                "event_date": row[1],
                "event_hour": row[2],
                "eps_estimate": row[3],
                "rev_estimate": row[4],
                "tier": row[5],
                "company_name": row[6],
                "ticktick_task_id": row[7],
            })

        if ticktick_events:
            tt_stats = sync_ticktick_tasks(conn, ticktick_events, dry_run=dry_run)
        else:
            logger.info("No Tier 1/2 future events for TickTick")
    else:
        logger.info("TickTick sync skipped (--no-ticktick)")

    conn.close()

    # --- Heartbeat ---
    if not skip_heartbeat and not dry_run and SLACK_WEBHOOK_EARNINGS:
        stats: dict[str, object] = {
            "events": len(earnings),
            "new": new_count,
            "updated": updated_count,
            "actuals": actuals_count,
            "unchanged": skip_count,
        }
        if tt_stats is not None:
            stats["tt"] = (
                f"{tt_stats.get('created', 0)} new / "
                f"{tt_stats.get('errors', 0)} err"
            )
        post_heartbeat(
            SLACK_WEBHOOK_EARNINGS,
            "Daily sync",
            stats,
            duration_sec=time.monotonic() - start_ts,
        )


# ---------------------------------------------------------------------------
# Weekly digest
# ---------------------------------------------------------------------------


def run_weekly_digest(dry_run: bool = False):
    """Assemble the weekly digest, post to Slack, and write HTML for email drafting."""
    coverage = load_coverage()
    if not coverage:
        logger.error("No tickers loaded. Cannot build digest.")
        sys.exit(1)

    conn = init_db()
    digest = build_weekly_digest(conn, coverage)
    conn.close()

    blocks = build_slack_blocks(digest)
    fallback = build_slack_fallback_text(digest)
    html_body = build_email_html(digest)
    text_body = build_email_text(digest)

    # Write HTML + plaintext for Gmail MCP drafting
    DIGEST_HTML_PATH.write_text(html_body, encoding="utf-8")
    text_path = DIGEST_HTML_PATH.with_suffix(".txt")
    text_path.write_text(text_body, encoding="utf-8")
    logger.info(f"Digest HTML written to {DIGEST_HTML_PATH}")
    logger.info(f"Digest plaintext written to {text_path}")

    if dry_run:
        logger.info("=" * 50)
        logger.info("DRY RUN — Slack payload preview:")
        logger.info(fallback)
        for block in blocks:
            if block.get("type") == "section":
                logger.info(f"  [section] {block['text']['text'][:200]}")
            elif block.get("type") == "header":
                logger.info(f"  [header] {block['text']['text']}")
            elif block.get("type") == "context":
                for el in block.get("elements", []):
                    logger.info(f"  [context] {el.get('text', '')[:200]}")
            elif block.get("type") == "divider":
                logger.info("  [divider]")
        logger.info("=" * 50)
        logger.info("(Dry run — no Slack post, no email draft created)")
        return

    if not SLACK_WEBHOOK_EARNINGS:
        logger.warning("SLACK_WEBHOOK_EARNINGS not set — skipping Slack post.")
    else:
        try:
            post_slack(SLACK_WEBHOOK_EARNINGS, blocks, fallback)
        except NotificationError as exc:
            logger.error(f"Slack digest post failed: {exc}")

    logger.info("Weekly digest complete. Run Gmail MCP draft creation "
                f"using content from {DIGEST_HTML_PATH}")


# ---------------------------------------------------------------------------
# Shared results notification (Slack + TickTick)
# ---------------------------------------------------------------------------


def notify_results(
    conn, results: list[ResultRow], as_of: date
) -> bool:
    """
    Post results to Slack and update TickTick tasks for Tier 1+2 rows.
    Returns True if Slack posted successfully (or no webhook configured).

    The caller is responsible for DB updates (reported=1). This function
    only handles the outbound notification surfaces.
    """
    if not results:
        return True

    blocks = build_results_slack_blocks(results, as_of)
    fallback = build_results_fallback_text(results, as_of)

    posted = False
    if SLACK_WEBHOOK_EARNINGS:
        try:
            post_slack(SLACK_WEBHOOK_EARNINGS, blocks, fallback)
            posted = True
        except NotificationError as exc:
            logger.error(f"Slack results post failed: {exc}")
    else:
        logger.warning("SLACK_WEBHOOK_EARNINGS not set — skipping Slack post.")
        posted = True  # treat as "handled" — no retry target

    # TickTick task updates (best-effort, don't block on failures)
    tt_config = get_ticktick_config()
    if tt_config:
        tt_token: str | None = tt_config["token"]
        for r in results:
            if r.tier > 2:
                continue
            existing = find_existing_event(conn, r.ticker, r.event_date)
            task_id = existing.get("ticktick_task_id") if existing else None
            if not task_id:
                continue
            try:
                ok = mark_task_reported(
                    tt_token,
                    task_id,
                    ticker=r.ticker,
                    event_date=r.event_date,
                    hour=r.event_hour,
                    tier=r.tier,
                    company_name=r.company_name,
                    eps_estimate=r.eps_estimate,
                    eps_actual=r.eps_actual,
                    revenue_estimate=r.rev_estimate,
                    revenue_actual=r.rev_actual,
                    move_pct=r.move.move_pct if r.move else None,
                    move_label=r.move.window_label if r.move else None,
                )
                if ok:
                    logger.info(f"  TickTick task marked reported for {r.ticker}")
            except TickTickTokenExpired:
                logger.error(
                    "TickTick access token expired. Re-run the OAuth flow at "
                    "developer.ticktick.com and update TICKTICK_ACCESS_TOKEN."
                )
                tt_token = None
                break
            except Exception as exc:
                logger.warning(f"  TickTick update failed for {r.ticker}: {exc}")

    return posted


# ---------------------------------------------------------------------------
# Post-earnings results check
# ---------------------------------------------------------------------------


def run_check_results(
    target_date: str | None = None,
    dry_run: bool = False,
    skip_heartbeat: bool = False,
):
    """
    Detect newly-reported earnings for a specific date and alert to Slack.

    Queries Finnhub for the target date, compares to DB state, and for any
    events with actuals that aren't yet marked reported=1, computes beat/miss,
    fetches the post-earnings stock move, posts a consolidated Slack message,
    updates the Calendar event description (Tier 1+2), and marks DB reported=1.
    """
    start_ts = time.monotonic()
    target = date.fromisoformat(target_date) if target_date else date.today()
    target_iso = target.isoformat()

    coverage = load_coverage()
    if not coverage:
        logger.error("No tickers loaded. Cannot check results.")
        sys.exit(1)
    coverage_map = {t.ticker: t for t in coverage}
    all_tickers = [t.ticker for t in coverage]

    if not FINNHUB_API_KEY:
        logger.error("FINNHUB_API_KEY missing.")
        sys.exit(1)

    try:
        fh_client = get_finnhub_client()
    except FinnhubError as exc:
        logger.error(f"Failed to initialize Finnhub client: {exc}")
        sys.exit(1)

    conn = init_db()

    cal_service = None
    if not dry_run and GOOGLE_CALENDAR_ID:
        try:
            cal_service = get_calendar_service()
        except Exception as exc:
            logger.warning(f"Calendar service unavailable, skipping calendar updates: {exc}")

    # fetch_earnings iterates `while start < end`, so a single-day range returns nothing.
    # Query one day beyond the target and filter client-side.
    to_iso = (target + timedelta(days=1)).isoformat()
    earnings = [
        e for e in fetch_earnings(fh_client, all_tickers, target_iso, to_iso)
        if e.get("date") == target_iso
    ]
    logger.info(f"Finnhub returned {len(earnings)} earnings entries for {target_iso}")

    new_results: list[ResultRow] = []
    skipped_no_actuals = 0
    skipped_already_reported = 0

    for e in earnings:
        ticker = e["symbol"].upper()
        eps_act = e.get("epsActual")
        rev_act = e.get("revenueActual")
        has_actuals = eps_act is not None or rev_act is not None
        if not has_actuals:
            skipped_no_actuals += 1
            continue

        event_date = e["date"]
        existing = find_existing_event(conn, ticker, event_date)
        if existing and existing.get("reported"):
            skipped_already_reported += 1
            continue

        info = coverage_map.get(ticker)
        tier = info.tier if info else (existing.get("tier", 3) if existing else 3)
        company_name = (info.company_name if info else "") or (
            existing.get("company_name") if existing else ""
        )
        hour = (existing.get("event_hour") if existing else None) or e.get("hour")
        eps_est = e.get("epsEstimate")
        if eps_est is None and existing:
            eps_est = existing.get("eps_estimate")
        rev_est = e.get("revenueEstimate")
        if rev_est is None and existing:
            rev_est = existing.get("rev_estimate")

        move = fetch_post_earnings_move(ticker, event_date, hour)

        result = ResultRow(
            ticker=ticker,
            company_name=company_name or "",
            event_date=event_date,
            event_hour=hour,
            eps_actual=eps_act,
            eps_estimate=eps_est,
            rev_actual=rev_act,
            rev_estimate=rev_est,
            tier=tier,
            move=move,
        )
        new_results.append(result)

        # Update Calendar description for Tier 1/2 events with an existing calendar event
        gcal_id = existing.get("gcal_id") if existing else None
        if tier <= 2 and gcal_id and cal_service:
            try:
                new_summary = f"[REPORTED] {ticker} Earnings Release"
                new_description = build_description(
                    ticker, hour, eps_est, eps_act, rev_est, rev_act
                )
                update_calendar_event_description(
                    cal_service, GOOGLE_CALENDAR_ID, gcal_id,
                    new_summary, new_description,
                    ticker=ticker,
                    quarter=(existing.get("quarter") if existing else None)
                            or date_to_quarter(event_date),
                    source_fingerprint=f"{ticker}:{event_date}",
                    tier=tier,
                )
                logger.info(f"Calendar updated with actuals for {ticker}")
            except CalendarError as exc:
                logger.error(f"Failed to update calendar for {ticker}: {exc}")

    logger.info(
        f"Results scan complete: {len(new_results)} new, "
        f"{skipped_already_reported} already reported, "
        f"{skipped_no_actuals} pending actuals"
    )

    def _emit_heartbeat():
        if skip_heartbeat or dry_run or not SLACK_WEBHOOK_EARNINGS:
            return
        post_heartbeat(
            SLACK_WEBHOOK_EARNINGS,
            "Results check",
            {
                "target": target_iso,
                "new": len(new_results),
                "already_reported": skipped_already_reported,
                "pending": skipped_no_actuals,
            },
            duration_sec=time.monotonic() - start_ts,
        )

    if not new_results:
        logger.info(f"No new reported results for {target_iso}")
        conn.close()
        _emit_heartbeat()
        return

    # Build + post Slack
    blocks = build_results_slack_blocks(new_results, target)
    fallback = build_results_fallback_text(new_results, target)

    if dry_run:
        logger.info("=" * 50)
        logger.info("DRY RUN — Results Slack preview:")
        logger.info(fallback)
        for block in blocks:
            btype = block.get("type")
            if btype == "section":
                logger.info(f"  [section] {block['text']['text'][:400]}")
            elif btype == "header":
                logger.info(f"  [header] {block['text']['text']}")
            elif btype == "context":
                for el in block.get("elements", []):
                    logger.info(f"  [context] {el.get('text', '')}")
            elif btype == "divider":
                logger.info("  [divider]")
        logger.info("=" * 50)
        logger.info("(Dry run — no Slack post, no DB updates)")
        conn.close()
        return

    posted = notify_results(conn, new_results, target)

    # Only mark reported=True after Slack has been handled. If the post failed
    # and we have a webhook configured, leave records unmarked so the next
    # run retries.
    if not posted:
        conn.close()
        return

    for r in new_results:
        existing = find_existing_event(conn, r.ticker, r.event_date)
        upsert_event(
            conn,
            r.ticker,
            r.event_date,
            r.event_hour,
            existing.get("gcal_id") if existing else None,
            quarter=existing.get("quarter") if existing else date_to_quarter(r.event_date),
            eps_estimate=r.eps_estimate,
            eps_actual=r.eps_actual,
            rev_estimate=r.rev_estimate,
            rev_actual=r.rev_actual,
            reported=True,
            tier=r.tier,
            company_name=r.company_name,
        )

    logger.info(f"Marked {len(new_results)} results as reported in DB")
    conn.close()
    _emit_heartbeat()


# ---------------------------------------------------------------------------
# Calendar reconcile (lightweight drift detection + auto-repair)
# ---------------------------------------------------------------------------


def run_reconcile_calendar(dry_run: bool = False):
    """
    Compare tagged Calendar events against Finnhub's current view and
    auto-fix any date drift. Only touches existing Tier 1/2 events —
    new-event creation is the daily sync's job.

    Posts to Slack only when drift was detected (silent no-op otherwise),
    so this job can run every few hours during market hours without
    spamming the channel.
    """
    from config import CALENDAR_PAGE_SIZE

    coverage = load_coverage()
    if not coverage:
        logger.error("No tickers loaded. Cannot reconcile.")
        sys.exit(1)
    coverage_map = {t.ticker: t for t in coverage}
    all_tickers = [t.ticker for t in coverage]

    if not FINNHUB_API_KEY or not GOOGLE_CALENDAR_ID:
        logger.error("FINNHUB_API_KEY and GOOGLE_CALENDAR_ID are required.")
        sys.exit(1)

    try:
        fh_client = get_finnhub_client()
    except FinnhubError as exc:
        logger.error(f"Failed to initialize Finnhub client: {exc}")
        sys.exit(1)

    conn = init_db()

    try:
        cal_service = get_calendar_service()
    except Exception as exc:
        logger.error(f"Failed to initialize Calendar service: {exc}")
        sys.exit(1)

    today = date.today()
    from_date = today.isoformat()
    to_date = (today + timedelta(days=45)).isoformat()

    # Finnhub's current view of the next 45 days
    earnings = fetch_earnings(fh_client, all_tickers, from_date, to_date)
    finnhub_map: dict[str, dict] = {}
    for e in earnings:
        t = e.get("symbol", "").upper()
        if t:
            finnhub_map[t] = e

    # Tagged calendar events in the same window
    cal_events: dict[str, tuple[str, str]] = {}  # ticker -> (gcal_id, start_date)
    time_min = today.isoformat() + "T00:00:00Z"
    time_max = (today + timedelta(days=45)).isoformat() + "T00:00:00Z"
    page_token = None
    while True:
        r = cal_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            privateExtendedProperty="earningsAgent=true",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            maxResults=CALENDAR_PAGE_SIZE,
            pageToken=page_token,
        ).execute()
        for ev in r.get("items", []):
            props = ev.get("extendedProperties", {}).get("private", {})
            ticker = props.get("ticker")
            if not ticker:
                continue
            start = ev.get("start", {})
            d = start.get("date") or start.get("dateTime", "")[:10]
            if d:
                cal_events[ticker] = (ev["id"], d)
        page_token = r.get("nextPageToken")
        if not page_token:
            break
    logger.info(f"Reconcile: indexed {len(cal_events)} tagged calendar events")

    # Detect drift. Locked events are skipped but reported as "respected"
    # so we never silently move a date the user has overridden.
    drift: list[tuple[str, str, dict, str]] = []  # (ticker, old_date, fh_event, gcal_id)
    locked_drift: list[tuple[str, str, str]] = []  # (ticker, cal_date, fh_date)
    for ticker, (gcal_id, cal_date) in cal_events.items():
        fh = finnhub_map.get(ticker)
        if not fh:
            # Finnhub dropped this ticker from the window. Unseen-ticker
            # alerting is phase B2; here we skip to stay narrow.
            continue
        fh_date = fh.get("date")
        if cal_date == fh_date:
            continue
        # Ticker-wide lock check handles the case where DB and calendar
        # have drifted and an exact-date lookup would miss the locked row.
        if is_ticker_date_locked(conn, ticker, cal_date):
            locked_drift.append((ticker, cal_date, fh_date))
            continue
        drift.append((ticker, cal_date, fh, gcal_id))

    for ticker, cal_date, fh_date in locked_drift:
        logger.warning(
            f"Reconcile: {ticker} locked at {cal_date}, Finnhub says {fh_date}. "
            f"Skipping auto-fix."
        )

    if not drift:
        if locked_drift:
            logger.info(
                f"Reconcile: no actionable drift ({len(locked_drift)} locked event(s) "
                f"disagree with Finnhub, but overrides are in place)"
            )
        else:
            logger.info("Reconcile: no drift detected — calendar matches Finnhub")
        conn.close()
        return

    logger.info(f"Reconcile: {len(drift)} drifted event(s) to fix")

    fixed: list[DriftRow] = []
    for ticker, old_date, fh, old_gcal_id in drift:
        new_date = fh["date"]
        hour = fh.get("hour")
        eps_est = fh.get("epsEstimate")
        eps_act = fh.get("epsActual")
        rev_est = fh.get("revenueEstimate")
        rev_act = fh.get("revenueActual")
        has_actuals = eps_act is not None or rev_act is not None
        quarter = date_to_quarter(new_date)
        info = coverage_map.get(ticker)
        tier = info.tier if info else 3
        company_name = info.company_name if info else None
        source_fingerprint = f"{ticker}:{new_date}"

        if dry_run:
            logger.info(
                f"  [dry-run] {ticker} (T{tier}): {old_date} -> {new_date} ({hour or 'TBD'})"
            )
            fixed.append(DriftRow(
                ticker=ticker, old_date=old_date, new_date=new_date,
                hour=hour, tier=tier,
            ))
            continue

        try:
            delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, old_gcal_id)
        except CalendarError as exc:
            logger.warning(f"  Could not delete stale event for {ticker}: {exc}")

        new_gcal_id = None
        try:
            new_gcal_id = create_calendar_event(
                cal_service, GOOGLE_CALENDAR_ID, ticker,
                new_date, hour,
                quarter=quarter,
                eps_estimate=eps_est, eps_actual=eps_act,
                revenue_estimate=rev_est, revenue_actual=rev_act,
                tier=tier,
                source_fingerprint=source_fingerprint,
            )
        except CalendarError as exc:
            logger.error(f"  Failed to recreate event for {ticker}: {exc}")
            continue

        upsert_event(
            conn, ticker, new_date, hour, new_gcal_id,
            quarter=quarter,
            eps_estimate=eps_est, eps_actual=eps_act,
            rev_estimate=rev_est, rev_actual=rev_act,
            reported=has_actuals, tier=tier,
            company_name=company_name,
            source_fingerprint=source_fingerprint,
        )
        logger.info(f"  Fixed {ticker} (T{tier}): {old_date} -> {new_date}")
        fixed.append(DriftRow(
            ticker=ticker, old_date=old_date, new_date=new_date,
            hour=hour, tier=tier,
        ))

    conn.close()

    # Slack summary — only when we actually did something
    if fixed and not dry_run and SLACK_WEBHOOK_EARNINGS:
        blocks = build_reconcile_blocks(fixed, today)
        fallback = build_reconcile_fallback(fixed, today)
        try:
            post_slack(SLACK_WEBHOOK_EARNINGS, blocks, fallback)
        except NotificationError as exc:
            logger.error(f"Reconcile Slack post failed: {exc}")


# ---------------------------------------------------------------------------
# Date-lock management (D2)
# ---------------------------------------------------------------------------


def _parse_lock_arg(value: str) -> tuple[str, str]:
    """Parse a TICKER:YYYY-MM-DD argument. Raises ValueError on bad input."""
    if ":" not in value:
        raise ValueError(f"Expected TICKER:YYYY-MM-DD, got {value!r}")
    ticker, event_date = value.split(":", 1)
    ticker = ticker.strip().upper()
    event_date = event_date.strip()
    # Validate date
    date.fromisoformat(event_date)
    if not ticker:
        raise ValueError("Ticker cannot be empty")
    return ticker, event_date


def run_set_lock(target: str, locked: bool):
    """Set or clear the date-lock on an event. target format: TICKER:YYYY-MM-DD."""
    try:
        ticker, event_date = _parse_lock_arg(target)
    except ValueError as exc:
        logger.error(f"Invalid lock argument: {exc}")
        sys.exit(1)

    conn = init_db()
    ok = set_date_lock(conn, ticker, event_date, locked)
    conn.close()

    if not ok:
        logger.error(
            f"No event found for {ticker} on {event_date}. "
            f"Nothing to {'lock' if locked else 'unlock'}."
        )
        sys.exit(1)

    verb = "Locked" if locked else "Unlocked"
    logger.info(f"{verb} {ticker} {event_date}")


def run_list_locks():
    """Print currently-locked events."""
    conn = init_db()
    locks = list_locked_events(conn)
    conn.close()

    if not locks:
        print("No events are currently date-locked.")
        return
    print(f"{len(locks)} event(s) currently date-locked:")
    for lock in locks:
        ticker = lock["ticker"]
        ev_date = lock["event_date"]
        hour = lock["event_hour"] or "TBD"
        tier = lock["tier"]
        co = f" — {lock['company_name']}" if lock["company_name"] else ""
        print(f"  {ticker} (T{tier}) {ev_date} {hour}{co}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Earnings Intelligence System — earnings calendar sync and workflow"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview earnings without creating calendar events",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Also check the past 30 days for any missed earnings",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Scan calendar for duplicate events and delete extras",
    )
    parser.add_argument(
        "--no-ticktick",
        action="store_true",
        help="Skip TickTick task creation",
    )
    parser.add_argument(
        "--ticktick-status",
        action="store_true",
        help="Show TickTick earnings review queue status",
    )
    parser.add_argument(
        "--weekly-digest",
        action="store_true",
        help="Build and send the weekly earnings digest (Slack + email HTML for MCP draft)",
    )
    parser.add_argument(
        "--check-results",
        action="store_true",
        help="Check for newly-reported earnings on --date (default: today); post results to Slack",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD) for --check-results. Defaults to today.",
    )
    parser.add_argument(
        "--no-heartbeat",
        action="store_true",
        help="Skip the Slack success heartbeat at the end of the run",
    )
    parser.add_argument(
        "--reconcile-calendar",
        action="store_true",
        help="Detect and fix calendar/Finnhub date drift; silent no-op if in sync",
    )
    parser.add_argument(
        "--lock",
        metavar="TICKER:YYYY-MM-DD",
        help="Lock an event's date so sync/reconcile won't overwrite it with Finnhub",
    )
    parser.add_argument(
        "--unlock",
        metavar="TICKER:YYYY-MM-DD",
        help="Remove a date lock",
    )
    parser.add_argument(
        "--list-locks",
        action="store_true",
        help="Show all currently date-locked events",
    )
    args = parser.parse_args()

    if args.list_locks:
        run_list_locks()
    elif args.lock:
        run_set_lock(args.lock, locked=True)
    elif args.unlock:
        run_set_lock(args.unlock, locked=False)
    elif args.ticktick_status:
        config = get_ticktick_config()
        if not config:
            logger.error("TickTick not configured. Set TICKTICK_ACCESS_TOKEN in .env")
            sys.exit(1)
        show_ticktick_status(config["token"])
    elif args.weekly_digest:
        run_weekly_digest(dry_run=args.dry_run)
    elif args.check_results:
        run_check_results(
            target_date=args.date,
            dry_run=args.dry_run,
            skip_heartbeat=args.no_heartbeat,
        )
    elif args.reconcile_calendar:
        run_reconcile_calendar(dry_run=args.dry_run)
    elif args.cleanup:
        conn = init_db()
        cleanup_duplicates(conn, dry_run=args.dry_run)
        conn.close()
    else:
        run(
            dry_run=args.dry_run,
            backfill=args.backfill,
            skip_ticktick=args.no_ticktick,
            skip_heartbeat=args.no_heartbeat,
        )


if __name__ == "__main__":
    main()
