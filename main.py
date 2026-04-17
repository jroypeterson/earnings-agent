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
from ticktick import sync_ticktick_tasks, get_ticktick_config, show_ticktick_status
from digest import build_weekly_digest
from notifications import (
    build_slack_blocks,
    build_slack_fallback_text,
    build_email_html,
    build_email_text,
    build_results_slack_blocks,
    build_results_fallback_text,
    post_slack,
    NotificationError,
    ResultRow,
)
from market_data import fetch_post_earnings_move

logger = logging.getLogger("earnings_agent")


def run(dry_run: bool = False, backfill: bool = False, skip_ticktick: bool = False):
    """Main sync: collect earnings data, sync to calendar, store estimate snapshots."""

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

    # --- Fetch earnings ---
    earnings = fetch_earnings(fh_client, all_tickers, from_date, to_date)

    new_count = 0
    updated_count = 0
    actuals_count = 0
    skip_count = 0
    snapshot_date = today.isoformat()

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
                continue

            # --- Check if date or timing changed ---
            date_changed = existing["event_date"] != earnings_date
            hour_changed = existing.get("event_hour") != hour

            if not date_changed and not hour_changed:
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

    # --- TickTick task sync (Tier 1 + Tier 2 only) ---
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
# Post-earnings results check
# ---------------------------------------------------------------------------


def run_check_results(target_date: str | None = None, dry_run: bool = False):
    """
    Detect newly-reported earnings for a specific date and alert to Slack.

    Queries Finnhub for the target date, compares to DB state, and for any
    events with actuals that aren't yet marked reported=1, computes beat/miss,
    fetches the post-earnings stock move, posts a consolidated Slack message,
    updates the Calendar event description (Tier 1+2), and marks DB reported=1.
    """
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

    if not new_results:
        logger.info(f"No new reported results for {target_iso}")
        conn.close()
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

    posted = False
    if SLACK_WEBHOOK_EARNINGS:
        try:
            post_slack(SLACK_WEBHOOK_EARNINGS, blocks, fallback)
            posted = True
        except NotificationError as exc:
            logger.error(f"Slack results post failed: {exc}")
    else:
        logger.warning("SLACK_WEBHOOK_EARNINGS not set — skipping Slack post.")

    # Only mark reported=True after Slack has been handled (or skipped explicitly
    # because the webhook isn't configured). If the post raises, we keep the
    # records unmarked so the next run retries.
    if posted or not SLACK_WEBHOOK_EARNINGS:
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
    args = parser.parse_args()

    if args.ticktick_status:
        config = get_ticktick_config()
        if not config:
            logger.error("TickTick not configured. Set TICKTICK_ACCESS_TOKEN in .env")
            sys.exit(1)
        show_ticktick_status(config["token"])
    elif args.weekly_digest:
        run_weekly_digest(dry_run=args.dry_run)
    elif args.check_results:
        run_check_results(target_date=args.date, dry_run=args.dry_run)
    elif args.cleanup:
        conn = init_db()
        cleanup_duplicates(conn, dry_run=args.dry_run)
        conn.close()
    else:
        run(dry_run=args.dry_run, backfill=args.backfill, skip_ticktick=args.no_ticktick)


if __name__ == "__main__":
    main()
