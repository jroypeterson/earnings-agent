"""
Earnings Intelligence System — CLI entry point and orchestrator.

Usage:
    python main.py                     # Daily sync (normal mode)
    python main.py --dry-run           # Preview without creating calendar events
    python main.py --backfill          # Also look back 30 days for missed earnings
    python main.py --cleanup           # Delete duplicate events from Google Calendar
    python main.py --cleanup --dry-run # Preview which duplicates would be deleted
"""

import re
import sys
import time
import argparse
import logging
from datetime import date, timedelta

from config import (
    GOOGLE_CALENDAR_ID,
    FINNHUB_API_KEY,
    SLACK_WEBHOOK_EARNINGS,
    SLACK_WEBHOOK_STATUS,
    SLACK_BOT_TOKEN,
    SLACK_CHANNEL_ID,
    SLACK_STATUS_CHANNEL_ID,
    DIGEST_HTML_PATH,
)
from coverage import (
    load_coverage,
    get_tickers_by_tier,
    get_ticker_info,
    TickerInfo,
    compute_coverage_freshness,
    CoverageHealth,
    COVERAGE_STALENESS_DAYS,
)
from storage import (
    init_db,
    find_existing_event,
    find_event_for_ticker_near_date,
    find_reported_event_for_quarter,
    upsert_event,
    record_estimate_snapshot,
    date_to_quarter,
    set_date_lock,
    list_locked_events,
    is_ticker_date_locked,
    open_question,
    update_question_state,
    advance_reply_watermark,
    list_open_questions,
    kv_get,
    kv_set,
)
from finnhub_client import get_client as get_finnhub_client, fetch_earnings, FinnhubError
from fmp_client import fetch_fmp_earnings, merge_earnings
from config import FMP_API_KEY
from calendar_sync import (
    get_calendar_service,
    find_calendar_event,
    create_calendar_event,
    update_calendar_event_description,
    delete_calendar_event,
    build_description,
    cleanup_duplicates,
    _is_confirmed_hour,
    expected_calendar_state,
    calendar_event_drift_kind,
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
    build_move_unavailable_blocks,
    build_move_unavailable_fallback,
    build_reconcile_blocks,
    build_reconcile_fallback,
    build_unseen_blocks,
    build_unseen_fallback,
    build_unseen_summary_blocks,
    build_unseen_summary_fallback,
    build_unseen_thread_blocks,
    build_unseen_thread_fallback,
    build_crosscheck_blocks,
    build_crosscheck_fallback,
    build_crosscheck_summary_blocks,
    build_crosscheck_summary_fallback,
    build_crosscheck_thread_blocks,
    build_crosscheck_thread_fallback,
    build_urgent_move_blocks,
    build_urgent_move_fallback,
    build_urgent_move_summary_blocks,
    build_urgent_move_summary_fallback,
    build_urgent_move_thread_blocks,
    build_urgent_move_thread_fallback,
    post_slack,
    post_heartbeat,
    urlopen_with_retry,
    _short_company_name,
    NotificationError,
    ResultRow,
    DriftRow,
    UnseenRow,
    DisagreementRow,
    UrgentMoveRow,
)
from slack_api import (
    post_message as slack_post_message,
    fetch_thread_replies,
    SlackAPIError,
)
from slack_replies import (
    parse_reply,
    format_help,
    format_status,
    ReplyContext,
    ParsedAction,
    ACT_LOCK,
    ACT_CONFIRM_FH,
    ACT_WAIT,
    ACT_SNOOZE,
    ACT_IGNORE,
    ACT_REPORTED,
    ACT_IR,
    ACT_NOTE,
    ACT_HELP,
    ACT_STATUS,
    ACT_UNKNOWN,
)
from market_data import (
    fetch_post_earnings_move,
    fetch_yfinance_earnings_date,
    fetch_yfinance_hour_for_date,
    fetch_yfinance_call_for_date,
)
from edgar_client import infer_cadence_signal, find_earnings_release_filing, get_cik

logger = logging.getLogger("earnings_agent")


# A3: "within 5 business days" threshold for Tier 1 urgent alerts.
A3_URGENT_BIZ_DAYS = 5


def _business_days_until(target_iso: str, as_of: date) -> int:
    """
    Count business days (Mon-Fri) from as_of to target, inclusive of target
    but exclusive of as_of. Returns -1 for past targets.
    """
    target = date.fromisoformat(target_iso)
    if target < as_of:
        return -1
    if target == as_of:
        return 0
    days = 0
    d = as_of
    while d < target:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            days += 1
    return days


# Max age (calendar days) before we give up waiting for the post-earnings
# stock move and post anyway. Friday-AMC events need close(Mon) = X+3
# calendar days, so the cap must be >= 3 to give the X+3 sweep a chance.
_POST_DEFER_MAX_AGE_DAYS = 3


def _alert_move_unavailable(rows, as_of: date) -> None:
    """Post a #status-reports alert for results that gave up on the stock-move
    calc after the deferral window expired. Failures should be visible, not
    silent — see feedback memory `feedback_no_silent_fails.md`.
    """
    if not rows:
        return
    webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    if not webhook:
        logger.warning(
            f"Move-unavailable alert suppressed (no webhook): "
            f"{', '.join(r.ticker for r in rows)}"
        )
        return
    try:
        post_slack(
            webhook,
            build_move_unavailable_blocks(rows, as_of),
            build_move_unavailable_fallback(rows, as_of),
        )
        logger.info(
            f"Posted move-unavailable alert for {len(rows)} event(s): "
            f"{', '.join(r.ticker for r in rows)}"
        )
    except NotificationError as exc:
        logger.error(f"Move-unavailable Slack post failed: {exc}")


def _alert_fmp_degraded(message: str, dry_run: bool) -> None:
    """Surface an FMP degradation (full or partial) to #status-reports so a
    quietly-shrunk merge doesn't pass for a healthy run. Best-effort; suppressed
    in dry-run/populate so the DB-seeding steps don't post."""
    if dry_run:
        return
    webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    if not webhook:
        return
    try:
        post_slack(
            webhook,
            [{"type": "section", "text": {"type": "mrkdwn",
              "text": f":warning: *Earnings source degraded* — {message}"}}],
            f"Earnings source degraded: {message}",
        )
    except NotificationError as exc:
        logger.error(f"FMP-degraded alert failed: {exc}")


def _fetch_earnings_source(
    fh_client, tickers, from_iso: str, to_iso: str, *, dry_run: bool = False
) -> list[dict]:
    """Earnings calendar = Finnhub merged with FMP (co-primary).

    Finnhub keeps date authority on shared names; FMP adds the names Finnhub
    misses and the actuals Finnhub hasn't ingested yet (see fmp_client). When
    FMP_API_KEY is unset, or an FMP fetch fails (fully or partially), we say so
    loudly (log + #status-reports) and merge whatever we got — never a silent
    coverage drop. Provider counts are logged every run for observability.
    """
    fh = fetch_earnings(fh_client, tickers, from_iso, to_iso)
    if not FMP_API_KEY:
        logger.warning(
            "FMP_API_KEY not set — earnings source is Finnhub-only "
            "(reduced breadth + slower actuals; set the secret to enable FMP merge)"
        )
        return fh
    try:
        res = fetch_fmp_earnings(tickers, from_iso, to_iso)
    except Exception as exc:
        logger.error(
            f"FMP earnings fetch failed ({exc}) — degrading to Finnhub-only this run"
        )
        _alert_fmp_degraded(
            f"FMP fetch failed entirely ({exc}); merge ran Finnhub-only", dry_run
        )
        return fh
    if res.failed_chunks:
        # Partial outage: some chunks dropped, so the merge silently lost
        # coverage for those date ranges. Alarm, don't paper over it.
        msg = (
            f"FMP partial degradation: {res.failed_chunks}/{res.total_chunks} "
            f"calendar chunks failed — merge coverage reduced this run"
        )
        logger.warning(msg)
        _alert_fmp_degraded(msg, dry_run)
    merged = merge_earnings(fh, res.events)
    logger.info(
        f"Earnings sources merged: Finnhub={len(fh)} FMP={len(res.events)} "
        f"-> {len(merged)} events"
    )
    return merged


def _run_safeguard(label: str, fn):
    """Run a verify-layer safeguard (cross-check, missed-results backstop) so
    its failures are LOUD, never silent.

    The cross-check (Finnhub-vs-yfinance + EDGAR auto-correction) and the
    EDGAR missed-results backstop are exactly the "don't miss / verify dates"
    layer. Their workflow steps used to be `continue-on-error`, which made a
    broken safeguard look identical to a clean run. Now: on any exception we
    post a context-rich degraded-health alert to #status-reports, then
    re-raise so the workflow step also fails and its `if: failure()` Slack
    alert fires. The steps no longer set continue-on-error. See
    `feedback_no_silent_failures.md`.
    """
    try:
        return fn()
    except SystemExit:
        # Hard config failure (missing key/coverage). Let it propagate — the
        # workflow fails and the failure-Slack fires; no "degraded" framing.
        raise
    except Exception:
        import traceback
        last = traceback.format_exc().strip().splitlines()[-1][:300]
        webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
        if webhook:
            try:
                post_slack(
                    webhook,
                    [{"type": "section", "text": {"type": "mrkdwn", "text": (
                        f":warning: *Safeguard degraded* — `{label}` raised an error "
                        f"and did not complete. Date-correctness / missed-results "
                        f"coverage may be incomplete this run.\n```{last}```"
                    )}}],
                    f"Safeguard degraded: {label} — {last}",
                )
            except Exception as alert_exc:
                logger.error(f"Failed to post degraded-safeguard alert: {alert_exc}")
        raise


def _should_defer_post(
    move, event_date_iso: str, today: date,
    max_age_days: int = _POST_DEFER_MAX_AGE_DAYS,
) -> bool:
    """Whether to hold a results post back until the next sweep.

    fetch_post_earnings_move returns None when the comparison close hasn't
    posted yet — for AMC events, that's the next-day close, which only
    becomes available at the post_earnings_check sweep on day X+1 (6:37 PM
    ET). Defer the Slack post + reported flag so the next sweep retries.
    Cap at max_age_days so a delisted/unfetchable ticker doesn't loop.
    """
    if move is not None:
        return False
    try:
        ed = date.fromisoformat(event_date_iso)
    except ValueError:
        return False
    return (today - ed).days <= max_age_days


def _alert_coverage_stale_if_needed(conn, health: CoverageHealth) -> None:
    """Post a Slack alert when Coverage Manager exports are stale, dedup'd
    once per UTC day via kv_store. Always called after init_db so kv_store
    is available.

    Routes to #status-reports (falls back to #earnings). Swallows its own
    errors — coverage staleness is informational, not blocking.
    """
    if not health.stale:
        return

    today_iso = date.today().isoformat()
    dedup_key = f"coverage_stale_alerted:{today_iso}"
    if kv_get(conn, dedup_key):
        logger.debug(f"Coverage staleness already alerted today ({today_iso}); skipping")
        return

    logger.warning(f"Coverage Manager exports stale: {health.message}")

    webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    if not webhook:
        logger.info("No Slack webhook configured; skipping coverage staleness alert")
        # Mark dedup anyway so we don't re-log this every hour today
        kv_set(conn, dedup_key, "no-webhook")
        return

    if health.source == "missing":
        text = (
            f":rotating_light: *Coverage Manager exports missing* — "
            f"{health.message}. The earnings agent is running on the legacy "
            f"fallback (or no tickers at all). Investigate Coverage Manager CI."
        )
    else:
        age_str = f"{health.age_days:.1f} days" if health.age_days is not None else "unknown"
        text = (
            f":warning: *Coverage Manager exports stale* — last published "
            f"{age_str} ago (threshold {COVERAGE_STALENESS_DAYS} days, source={health.source}). "
            f"The earnings agent is using stale tier classifications. "
            f"Check Coverage Manager's weekly publish job."
        )

    try:
        import urllib.request
        import json as _json
        req = urllib.request.Request(
            webhook,
            data=_json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        # Retry transient network blips; preserves swallow-on-error below.
        urlopen_with_retry(req, timeout=10, label="Coverage staleness alert").read()
        kv_set(conn, dedup_key, "alerted")
    except Exception as exc:
        logger.error(f"Coverage staleness Slack post failed: {exc}")


def _alert_coverage_changes_if_needed(conn, coverage) -> None:
    """Compare current coverage to the prior snapshot in kv_store, post a
    Tier 1/2 diff to #status-reports, and persist the new snapshot.

    Detects: added (Tier 1/2), removed (Tier 1/2), tier_changed (involving
    Tier 1/2 on either side), and position_changed (Portfolio <-> Researching
    within Tier 1). Tier 3 churn is suppressed since it's universe-wide noise.

    First run: seeds the snapshot silently — no alert without a baseline.
    Swallows its own errors — informational, not blocking.
    """
    import json as _json

    KEY = "coverage_snapshot"

    current = {
        t.ticker: {
            "tier": t.tier,
            "position": t.position or "",
            "name": t.company_name or "",
        }
        for t in coverage
    }
    current_json = _json.dumps(current, sort_keys=True)

    prior_raw = kv_get(conn, KEY)
    if not prior_raw:
        kv_set(conn, KEY, current_json)
        logger.info(f"Coverage snapshot seeded ({len(current)} tickers); no diff alert")
        return

    try:
        prior = _json.loads(prior_raw)
    except (ValueError, TypeError):
        logger.warning("Prior coverage snapshot unparseable; reseeding")
        kv_set(conn, KEY, current_json)
        return

    if prior == current:
        return  # no change

    added: list[tuple[str, dict]] = []
    removed: list[tuple[str, dict]] = []
    tier_changed: list[tuple[str, dict, dict]] = []
    position_changed: list[tuple[str, dict, dict]] = []

    all_tickers = set(prior) | set(current)
    for tk in sorted(all_tickers):
        old = prior.get(tk)
        new = current.get(tk)
        if old and new:
            if old.get("tier") != new.get("tier"):
                if old.get("tier", 3) <= 2 or new.get("tier", 3) <= 2:
                    tier_changed.append((tk, old, new))
            elif old.get("position", "") != new.get("position", "") and new.get("tier") == 1:
                position_changed.append((tk, old, new))
        elif new and not old:
            if new.get("tier", 3) <= 2:
                added.append((tk, new))
        elif old and not new:
            if old.get("tier", 3) <= 2:
                removed.append((tk, old))

    # Always persist the latest snapshot, even if the diff was Tier 3-only
    kv_set(conn, KEY, current_json)

    if not (added or removed or tier_changed or position_changed):
        return

    logger.info(
        f"Coverage diff: +{len(added)} added, -{len(removed)} removed, "
        f"{len(tier_changed)} tier changes, {len(position_changed)} position changes"
    )

    webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    if not webhook:
        logger.info("No Slack webhook configured; skipping coverage diff alert")
        return

    def _fmt_tier(t: int) -> str:
        return f"T{t}"

    def _fmt_pos(p: str) -> str:
        return f"/{p}" if p else ""

    lines: list[str] = []
    lines.append(":compass: *Coverage Manager diff*")

    if added:
        lines.append(f"*Added* ({len(added)}):")
        for tk, info in added:
            tag = f"{_fmt_tier(info['tier'])}{_fmt_pos(info.get('position',''))}"
            name = info.get("name") or ""
            suffix = f" — {name}" if name else ""
            lines.append(f"  • `{tk}` ({tag}){suffix}")

    if removed:
        lines.append(f"*Removed* ({len(removed)}):")
        for tk, info in removed:
            tag = f"was {_fmt_tier(info['tier'])}{_fmt_pos(info.get('position',''))}"
            name = info.get("name") or ""
            suffix = f" — {name}" if name else ""
            lines.append(f"  • `{tk}` ({tag}){suffix}")

    if tier_changed:
        lines.append(f"*Tier changed* ({len(tier_changed)}):")
        for tk, old, new in tier_changed:
            old_tag = f"{_fmt_tier(old['tier'])}{_fmt_pos(old.get('position',''))}"
            new_tag = f"{_fmt_tier(new['tier'])}{_fmt_pos(new.get('position',''))}"
            lines.append(f"  • `{tk}`: {old_tag} → {new_tag}")

    if position_changed:
        lines.append(f"*Position changed* ({len(position_changed)}):")
        for tk, old, new in position_changed:
            lines.append(
                f"  • `{tk}`: {old.get('position') or '(none)'} → "
                f"{new.get('position') or '(none)'}"
            )

    lines.append(
        "_Note: tier-changed tickers may have stale TickTick tasks in the old list "
        "(cross-list dedup blocks auto-recreation). Move manually if needed._"
    )
    text = "\n".join(lines)

    try:
        import urllib.request
        import json as __json
        req = urllib.request.Request(
            webhook,
            data=__json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        # Retry transient network blips; preserves swallow-on-error below.
        urlopen_with_retry(req, timeout=10, label="Coverage diff alert").read()
    except Exception as exc:
        logger.error(f"Coverage diff Slack post failed: {exc}")


def _post_urgent_alert(rows: list[UrgentMoveRow], as_of: date, conn=None):
    """
    Post the A3 Tier 1 urgent Slack alert. Swallows its own errors.

    With SLACK_BOT_TOKEN+SLACK_CHANNEL_ID set, posts a summary header +
    one threaded parent per row and persists thread_ts via open_question
    so --check-replies can drive resolution. Falls back to webhook batched
    post otherwise. `conn` is required for the bot-token path.
    """
    if not rows:
        return
    bot_path = bool(SLACK_BOT_TOKEN and SLACK_CHANNEL_ID and conn is not None)
    if bot_path:
        try:
            slack_post_message(
                SLACK_BOT_TOKEN,
                SLACK_CHANNEL_ID,
                blocks=build_urgent_move_summary_blocks(rows, as_of),
                text=build_urgent_move_summary_fallback(rows),
            )
            for r in rows:
                ts = slack_post_message(
                    SLACK_BOT_TOKEN,
                    SLACK_CHANNEL_ID,
                    blocks=build_urgent_move_thread_blocks(r, as_of),
                    text=build_urgent_move_thread_fallback(r),
                )
                open_question(
                    conn,
                    r.ticker,
                    r.new_date,
                    thread_ts=ts,
                    kind="urgent",
                    first_seen_iso=as_of.isoformat(),
                )
        except SlackAPIError as exc:
            logger.error(f"Urgent T1 alert Slack post failed (bot path): {exc}")
        return
    if not SLACK_WEBHOOK_EARNINGS:
        return
    try:
        post_slack(
            SLACK_WEBHOOK_EARNINGS,
            build_urgent_move_blocks(rows, as_of),
            build_urgent_move_fallback(rows, as_of),
        )
    except NotificationError as exc:
        logger.error(f"Urgent T1 alert Slack post failed: {exc}")


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
    _alert_coverage_stale_if_needed(conn, compute_coverage_freshness())
    _alert_coverage_changes_if_needed(conn, coverage)

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

    # --- Fetch earnings (Finnhub + FMP merged) ---
    earnings = _fetch_earnings_source(
        fh_client, all_tickers, from_date, to_date, dry_run=dry_run
    )

    new_count = 0
    updated_count = 0
    actuals_count = 0
    skip_count = 0
    snapshot_date = today.isoformat()
    # Actuals collected during this sync for Slack + TickTick downstream
    sync_results: list[ResultRow] = []
    # A3: Tier 1 date moves that land within 5 business days
    urgent_moves: list[UrgentMoveRow] = []

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

        # Phantom / duplicate-listing guard. Once this ticker's reporting
        # quarter has been recorded with actuals and posted (reported=1), any
        # *other* Finnhub entry for the same quarter at a different date is
        # noise: a date-flapping forward listing (Finnhub double-lists names
        # like ICLR — real 2026-05-27 actuals + a phantom 2026-06-xx with no
        # actuals) or a same-quarter duplicate. Processing it re-posts the
        # actuals every run and churns the calendar. Skip it outright so the
        # reported row stays authoritative.
        reported_quarter = find_reported_event_for_quarter(conn, ticker, quarter)
        if reported_quarter and reported_quarter["event_date"] != earnings_date:
            logger.info(
                f"Skipping {'phantom' if not has_actuals else 'duplicate'} "
                f"Finnhub entry {ticker} {earnings_date} ({quarter}) — already "
                f"reported on {reported_quarter['event_date']}"
            )
            continue

        # Record estimate snapshot for revision tracking
        if eps_est is not None or rev_est is not None:
            record_estimate_snapshot(
                conn, ticker, earnings_date, snapshot_date, eps_est, rev_est
            )

        # Look up existing event (by exact date first, then nearby)
        existing = find_existing_event(conn, ticker, earnings_date)
        if not existing:
            existing = find_event_for_ticker_near_date(conn, ticker, earnings_date)

        # yfinance hour fallback: when Finnhub didn't publish timing for an
        # upcoming Tier 1/2 event, try to infer it from yfinance's earnings
        # datetime time-of-day. Only run for upcoming events (Finnhub often
        # has empty hour for past events too, but those don't need calendar
        # corrections). Reuse a cached value from DB to avoid hammering
        # yfinance on every sync. Does NOT touch event_hour or date_confirmed
        # — keeps Finnhub-canonical semantics intact.
        hour_yf: str | None = None
        call_dt_iso: str | None = None
        call_source_val: str | None = None
        is_upcoming = (not has_actuals) and earnings_date >= today.isoformat()
        if not hour and is_upcoming and tier <= 2:
            cached_yf = (existing or {}).get("event_hour_yf") if existing else None
            if cached_yf:
                hour_yf = cached_yf
            else:
                try:
                    inferred = fetch_yfinance_hour_for_date(ticker, earnings_date)
                    if inferred:
                        hour_yf = inferred
                        logger.info(
                            f"yfinance fallback timing for {ticker} {earnings_date}: {hour_yf}"
                        )
                except Exception as exc:
                    logger.debug(f"yfinance hour fallback failed for {ticker}: {exc}")

        # Conference call timestamp — descriptive context for the calendar
        # event description. Anchored to the press-release earnings_date;
        # the call may be the same day (common) or the next business day
        # (UFPT pattern: AMC release Mon, BMO call Tue). Fetched for
        # upcoming Tier 1/2 events only. Cached in DB to avoid re-querying.
        if is_upcoming and tier <= 2:
            cached_call = (existing or {}).get("call_datetime_utc") if existing else None
            if cached_call:
                call_dt_iso = cached_call
                call_source_val = (existing or {}).get("call_source")
            else:
                try:
                    call_dt = fetch_yfinance_call_for_date(ticker, earnings_date)
                    if call_dt is not None:
                        call_dt_iso = call_dt.isoformat()
                        call_source_val = "yfinance"
                        logger.info(
                            f"yfinance call timestamp for {ticker} "
                            f"(release {earnings_date}): {call_dt_iso}"
                        )
                except Exception as exc:
                    logger.debug(f"yfinance call fetch failed for {ticker}: {exc}")

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
                        new_summary, new_description, _ = expected_calendar_state(
                            ticker, hour, eps_est, eps_act, rev_est, rev_act,
                            quarter=quarter, tier=tier,
                            source_fingerprint=source_fingerprint,
                            earnings_date=earnings_date,
                            call_datetime_utc=call_dt_iso,
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

                # Fetch the post-earnings move first so we can decide whether
                # to mark this row reported (and post to Slack) now, or defer
                # until the next sweep when the comparison close will exist.
                # Skip the network call entirely in dry-run mode.
                move = None
                if not dry_run:
                    move = fetch_post_earnings_move(ticker, earnings_date, hour)
                defer_post = _should_defer_post(move, earnings_date, today)
                # In dry-run we never persist `reported` (it's a preview).
                # Otherwise, mark reported only when we're actually going
                # to post Slack this run (i.e. not deferring).
                mark_reported = (not dry_run) and (not defer_post)

                upsert_event(
                    conn, ticker, earnings_date, hour, existing["gcal_id"],
                    quarter=quarter, eps_estimate=eps_est, eps_actual=eps_act,
                    rev_estimate=rev_est, rev_actual=rev_act,
                    reported=mark_reported,
                    tier=tier, company_name=company_name,
                    source_fingerprint=source_fingerprint,
                    event_hour_yf=hour_yf,
                    call_datetime_utc=call_dt_iso,
                    call_source=call_source_val,
                )
                actuals_count += 1

                if defer_post:
                    logger.info(
                        f"Deferring Slack post for {ticker} {earnings_date}: "
                        f"stock-move data not yet available, will retry next sweep"
                    )
                    continue

                # Collect for Slack + TickTick post-loop notification
                if not dry_run:
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
                            sector=info.sector if info else "",
                            subsector=info.subsector if info else "",
                            position=info.position if info else "",
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
                if tier == 1:
                    biz_days = _business_days_until(earnings_date, today)
                    if 0 <= biz_days <= A3_URGENT_BIZ_DAYS:
                        urgent_moves.append(UrgentMoveRow(
                            ticker=ticker,
                            company_name=company_name or "",
                            old_date=old_date,
                            new_date=earnings_date,
                            hour=hour,
                            biz_days_until=biz_days,
                            source="sync",
                        ))
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
                            hour_yf=hour_yf,
                            call_datetime_utc=call_dt_iso,
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
                event_hour_yf=hour_yf,
                call_datetime_utc=call_dt_iso,
                call_source=call_source_val,
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
                                    hour_yf=hour_yf,
                                    call_datetime_utc=call_dt_iso,
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
                                event_hour_yf=hour_yf,
                                call_datetime_utc=call_dt_iso,
                                call_source=call_source_val,
                            )
                            updated_count += 1
                            continue

                        # Date matches — but the calendar event itself may
                        # be stale relative to current Finnhub state (hour
                        # just got populated, actuals just came in, tier
                        # reclassified, fingerprint missing). Decide whether
                        # to patch in place or recreate based on the kind
                        # of drift: 'shape' drift (all-day<->timed, or
                        # bmo<->amc) requires delete+recreate because
                        # update_calendar_event_description doesn't touch
                        # start/end/reminders.
                        exp_summary, exp_description, exp_props = expected_calendar_state(
                            ticker, hour, eps_est, eps_act, rev_est, rev_act,
                            quarter=quarter, tier=tier,
                            source_fingerprint=source_fingerprint,
                            hour_yf=hour_yf,
                            earnings_date=earnings_date,
                            call_datetime_utc=call_dt_iso,
                        )
                        effective_hour = hour or hour_yf or ""
                        drift = calendar_event_drift_kind(
                            cal_event, exp_summary, exp_description, exp_props,
                            effective_hour,
                        )
                        if drift == "shape":
                            if not dry_run:
                                try:
                                    delete_calendar_event(
                                        cal_service, GOOGLE_CALENDAR_ID, gcal_id
                                    )
                                    gcal_id = create_calendar_event(
                                        cal_service, GOOGLE_CALENDAR_ID, ticker,
                                        earnings_date, hour,
                                        quarter=quarter, eps_estimate=eps_est,
                                        eps_actual=eps_act, revenue_estimate=rev_est,
                                        revenue_actual=rev_act, tier=tier,
                                        source_fingerprint=source_fingerprint,
                                        hour_yf=hour_yf,
                                        call_datetime_utc=call_dt_iso,
                                    )
                                    logger.info(
                                        f"Recreated calendar event for {ticker} "
                                        f"{quarter} (shape drift): "
                                        f"{cal_event.get('summary')!r} -> {exp_summary!r}"
                                    )
                                except CalendarError as exc:
                                    logger.warning(
                                        f"Could not recreate stale event for {ticker}: {exc}"
                                    )
                            else:
                                logger.info(
                                    f"  [dry-run] Would recreate {ticker} {quarter} "
                                    f"(shape drift): {cal_event.get('summary')!r} -> {exp_summary!r}"
                                )
                        elif drift == "text":
                            if not dry_run:
                                try:
                                    update_calendar_event_description(
                                        cal_service, GOOGLE_CALENDAR_ID, gcal_id,
                                        exp_summary, exp_description,
                                        ticker=ticker, quarter=quarter,
                                        source_fingerprint=source_fingerprint,
                                        tier=tier,
                                    )
                                    logger.info(
                                        f"Patched calendar event text for {ticker} "
                                        f"{quarter}: "
                                        f"{cal_event.get('summary')!r} -> {exp_summary!r}"
                                    )
                                except CalendarError as exc:
                                    logger.warning(
                                        f"Could not patch stale text for {ticker}: {exc}"
                                    )
                            else:
                                logger.info(
                                    f"  [dry-run] Would patch text for {ticker} {quarter}: "
                                    f"{cal_event.get('summary')!r} -> {exp_summary!r}"
                                )

                        upsert_event(
                            conn, ticker, earnings_date, hour, gcal_id,
                            quarter=quarter, eps_estimate=eps_est,
                            eps_actual=eps_act, rev_estimate=rev_est,
                            rev_actual=rev_act, reported=has_actuals,
                            tier=tier, company_name=company_name,
                            source_fingerprint=source_fingerprint,
                            event_hour_yf=hour_yf,
                            call_datetime_utc=call_dt_iso,
                            call_source=call_source_val,
                        )
                        logger.info(f"Backfilled DB from calendar for {ticker} {quarter}")
                        skip_count += 1
                        continue

                logger.info(f"New earnings: {ticker} {quarter} on {earnings_date} ({hour or hour_yf or 'time TBD'}) [Tier {tier}]")

                if not dry_run and cal_service:
                    try:
                        gcal_id = create_calendar_event(
                            cal_service, GOOGLE_CALENDAR_ID, ticker,
                            earnings_date, hour,
                            quarter=quarter, eps_estimate=eps_est,
                            eps_actual=eps_act, revenue_estimate=rev_est,
                            revenue_actual=rev_act, tier=tier,
                            source_fingerprint=source_fingerprint,
                            hour_yf=hour_yf,
                            call_datetime_utc=call_dt_iso,
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
                event_hour_yf=hour_yf,
                call_datetime_utc=call_dt_iso,
                call_source=call_source_val,
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
        # Move-give-up alert: rows whose deferral window expired but yfinance
        # still produced no usable move. These are in sync_results because
        # _should_defer_post returned False (event too old to keep waiting).
        gave_up = [r for r in sync_results if r.move is None]
        if gave_up:
            _alert_move_unavailable(gave_up, today)

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
            # Unseen-ticker alerts route to the status-reports channel.
            unseen_channel = SLACK_STATUS_CHANNEL_ID or SLACK_CHANNEL_ID
            unseen_webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
            bot_path = bool(SLACK_BOT_TOKEN and unseen_channel)
            if bot_path:
                try:
                    slack_post_message(
                        SLACK_BOT_TOKEN,
                        unseen_channel,
                        blocks=build_unseen_summary_blocks(persistent_unseen, today),
                        text=build_unseen_summary_fallback(persistent_unseen),
                    )
                    for u in persistent_unseen:
                        ts = slack_post_message(
                            SLACK_BOT_TOKEN,
                            unseen_channel,
                            blocks=build_unseen_thread_blocks(u, today),
                            text=build_unseen_thread_fallback(u),
                        )
                        open_question(
                            conn,
                            u.ticker,
                            u.event_date,
                            thread_ts=ts,
                            kind="unseen",
                            first_seen_iso=today.isoformat(),
                            channel_id=unseen_channel,
                        )
                except SlackAPIError as exc:
                    logger.error(f"Unseen-ticker Slack post failed (bot path): {exc}")
            elif unseen_webhook:
                try:
                    post_slack(
                        unseen_webhook,
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
            ticker = row[0]
            # Look up position from coverage so Tier 1 events can be split
            # into Portfolio vs Researching TickTick lists.
            info = coverage_map.get(ticker)
            position = info.position if info else ""
            ticktick_events.append({
                "ticker": ticker,
                "event_date": row[1],
                "event_hour": row[2],
                "eps_estimate": row[3],
                "rev_estimate": row[4],
                "tier": row[5],
                "company_name": row[6],
                "ticktick_task_id": row[7],
                "position": position,
            })

        if ticktick_events:
            tt_stats = sync_ticktick_tasks(conn, ticktick_events, dry_run=dry_run)
        else:
            logger.info("No Tier 1/2 future events for TickTick")
    else:
        logger.info("TickTick sync skipped (--no-ticktick)")

    conn.close()

    # --- A3: post urgent Tier 1 alert (if any) ---
    if urgent_moves:
        logger.warning(
            f"A3: {len(urgent_moves)} Tier 1 date move(s) within "
            f"{A3_URGENT_BIZ_DAYS} business days"
        )
        if not dry_run:
            _post_urgent_alert(urgent_moves, today, conn=conn)

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

    # Build a fresh coverage_map locally — historically this referenced
    # a closure variable from run(), which broke when the function was
    # called from run_check_results() (different caller, different scope).
    coverage = load_coverage()
    coverage_map = {t.ticker: t for t in coverage}

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
            # Position is needed so Tier 1 reports go into the correct
            # Portfolio vs Researching TickTick list.
            info = coverage_map.get(r.ticker)
            r_position = info.position if info else ""
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
                    position=r_position,
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
    # Query one day beyond the target and filter client-side. Merged source
    # (Finnhub + FMP) so FMP's timelier actuals are caught here too.
    to_iso = (target + timedelta(days=1)).isoformat()
    earnings = [
        e for e in _fetch_earnings_source(
            fh_client, all_tickers, target_iso, to_iso, dry_run=dry_run
        )
        if e.get("date") == target_iso
    ]
    logger.info(f"Earnings entries for {target_iso}: {len(earnings)}")

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

        # Same phantom/duplicate guard as the daily sync: if this quarter is
        # already reported at a different date, this is a duplicate Finnhub
        # listing — don't re-post.
        reported_quarter = find_reported_event_for_quarter(
            conn, ticker, date_to_quarter(event_date)
        )
        if reported_quarter and reported_quarter["event_date"] != event_date:
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
            sector=info.sector if info else "",
            subsector=info.subsector if info else "",
            position=info.position if info else "",
        )
        new_results.append(result)

        # Update Calendar description for Tier 1/2 events with an existing calendar event
        gcal_id = existing.get("gcal_id") if existing else None
        if tier <= 2 and gcal_id and cal_service:
            try:
                quarter_for_event = (
                    (existing.get("quarter") if existing else None)
                    or date_to_quarter(event_date)
                )
                source_fingerprint = f"{ticker}:{event_date}"
                cached_call = (existing.get("call_datetime_utc") if existing else None)
                new_summary, new_description, _ = expected_calendar_state(
                    ticker, hour, eps_est, eps_act, rev_est, rev_act,
                    quarter=quarter_for_event, tier=tier,
                    source_fingerprint=source_fingerprint,
                    earnings_date=event_date,
                    call_datetime_utc=cached_call,
                )
                update_calendar_event_description(
                    cal_service, GOOGLE_CALENDAR_ID, gcal_id,
                    new_summary, new_description,
                    ticker=ticker,
                    quarter=quarter_for_event,
                    source_fingerprint=source_fingerprint,
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

    # Split into rows ready to post (have a stock move, or too old to keep
    # waiting) and rows to defer until the next sweep. AMC events on day X
    # need close X+1, which only becomes available at the post_earnings_check
    # 22:37 UTC sweep on day X+1.
    ready: list[ResultRow] = []
    deferred: list[ResultRow] = []
    for r in new_results:
        if _should_defer_post(r.move, r.event_date, target):
            deferred.append(r)
        else:
            ready.append(r)

    if deferred:
        logger.info(
            f"Deferring {len(deferred)} result(s) until move data available: "
            + ", ".join(r.ticker for r in deferred)
        )

    # Among ready rows, separate those whose move calc gave up. They still
    # post (with "Stock data unavailable" inline) but also trigger a separate
    # #status-reports alert so the failure is loud, not silent.
    gave_up = [r for r in ready if r.move is None]
    if gave_up:
        _alert_move_unavailable(gave_up, target)

    if not ready:
        logger.info(
            f"No ready results for {target_iso} "
            f"(deferred: {len(deferred)}, already reported: {skipped_already_reported}, "
            f"pending actuals: {skipped_no_actuals})"
        )
        conn.close()
        _emit_heartbeat()
        return

    # Build + post Slack — only ready rows go out.
    blocks = build_results_slack_blocks(ready, target)
    fallback = build_results_fallback_text(ready, target)

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

    posted = notify_results(conn, ready, target)

    # Only mark reported=True after Slack has been handled. If the post failed
    # and we have a webhook configured, leave records unmarked so the next
    # run retries. Deferred rows are intentionally left as reported=False so
    # the next sweep picks them up again with fresh move data.
    if not posted:
        conn.close()
        return

    for r in ready:
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

    logger.info(f"Marked {len(ready)} results as reported in DB")
    conn.close()
    _emit_heartbeat()


# ---------------------------------------------------------------------------
# EDGAR results fallback — catch Tier 1/2 names that reported but Finnhub
# hasn't produced actuals for (the FIVE-class silent miss).
# ---------------------------------------------------------------------------

# How many days past the expected date we keep probing EDGAR for a missing
# Tier 1/2 result before giving up (covers a Fri report not seen until Mon +
# typical Finnhub backfill lag).
_MISSED_RESULTS_LOOKBACK_DAYS = 10

# Window for the DB-independent Tier-1 blind sweep: probe every Tier-1 name's
# 8-K Item 2.02 filings over the last N days, regardless of whether Finnhub
# ever created a DB row. Catches the worst miss — a name Finnhub never lists.
_TIER1_SWEEP_DAYS = 6


def run_edgar_results_fallback(dry_run: bool = False, skip_heartbeat: bool = False):
    """Detect Tier 1/2 earnings that have happened (confirmed by an SEC 8-K
    Item 2.02 filing) but for which Finnhub still has no actuals, and alert
    loudly to #earnings.

    Finnhub is the only source feeding `run_check_results`, so when it lags or
    parks an event on the wrong date, a name reports and the agent says
    nothing (e.g. FIVE: reported 2026-06-03, Finnhub stuck at 2026-06-02 with
    no actuals). SEC EDGAR is authoritative and free — an Item 2.02 filing IS
    the earnings release. We use it as a backstop so important names can't be
    silently missed (see `feedback_no_silent_failures.md`).

    Alert-only by design: EDGAR confirms *that* a company reported and *when*,
    but the beat/miss figures still come from Finnhub on a later sweep (the
    daily sync's 14-day look-back posts them once Finnhub backfills). Dedup'd
    per ticker+quarter via kv_store so it alerts once, not every run.
    """
    start_ts = time.monotonic()
    today = date.today()

    coverage = load_coverage()
    if not coverage:
        logger.error("No tickers loaded. Cannot run EDGAR results fallback.")
        sys.exit(1)
    coverage_map = {t.ticker: t for t in coverage}

    conn = init_db()

    # Tier 1/2 events that are overdue (date has passed, within the lookback
    # window), not yet reported, and have no actuals from Finnhub.
    lookback_iso = (today - timedelta(days=_MISSED_RESULTS_LOOKBACK_DAYS)).isoformat()
    cur = conn.execute(
        "SELECT ticker, quarter, event_date, tier, company_name "
        "FROM events "
        "WHERE tier <= 2 AND reported = 0 "
        "AND eps_actual IS NULL AND rev_actual IS NULL "
        "AND event_date <= ? AND event_date >= ? "
        "ORDER BY event_date",
        (today.isoformat(), lookback_iso),
    )
    candidates = cur.fetchall()
    logger.info(
        f"EDGAR results fallback: {len(candidates)} overdue Tier 1/2 "
        f"event(s) with no Finnhub actuals to probe"
    )

    confirmed_missed: list[dict] = []
    db_probed: set[str] = set()  # tickers EDGAR-probed in pass 1 (skip in pass 2)

    # --- Pass 1: DB candidates (Finnhub listed the event but no actuals). ---
    for ticker, quarter, event_date, tier, company_name in candidates:
        # Only probe names still in coverage (avoid noise from dropped tickers).
        if ticker not in coverage_map:
            continue
        # Dedup: alert once per ticker+quarter.
        dedup_key = f"missed_results_alerted:{ticker}:{quarter}"
        if kv_get(conn, dedup_key):
            continue
        try:
            ed = date.fromisoformat(event_date)
        except ValueError:
            continue
        # Search a small window around the expected date: a few days early
        # (companies sometimes pre-announce) through today.
        db_probed.add(ticker)
        filing = find_earnings_release_filing(
            ticker, ed - timedelta(days=3), today
        )
        if not filing:
            continue  # Genuinely hasn't reported yet — estimated date slipped.

        confirmed_missed.append({
            "ticker": ticker,
            "company_name": company_name or (
                coverage_map[ticker].company_name if ticker in coverage_map else ""
            ),
            "quarter": quarter,
            "event_date": event_date,
            "tier": tier,
            "filing_date": filing.filing_date,
            "filing_url": _edgar_filing_url(ticker, filing.accession),
            "dedup_key": dedup_key,
            "blind_sweep": False,
        })

    # --- Pass 2: DB-independent Tier-1 blind sweep. ---
    # Pass 1 can only see names Finnhub already wrote to SQLite. A Tier-1 name
    # that Finnhub never lists (or parks on a future/wrong date) has no
    # candidate row and would slip through entirely. Probe EVERY Tier-1
    # ticker's recent 8-K Item 2.02 filings directly; if one exists and no
    # reported event covers that quarter, the company reported and we have NO
    # record — the worst silent miss the "never miss" goal must catch.
    tier1_tickers = sorted(t.ticker for t in coverage if t.tier == 1)
    sweep_start = today - timedelta(days=_TIER1_SWEEP_DAYS)
    swept = 0
    for ticker in tier1_tickers:
        if ticker in db_probed:
            continue  # already handled in pass 1
        swept += 1
        try:
            filing = find_earnings_release_filing(ticker, sweep_start, today)
        except Exception as exc:
            logger.debug(f"Tier-1 sweep EDGAR probe failed for {ticker}: {exc}")
            continue
        if not filing:
            continue
        quarter = date_to_quarter(filing.filing_date)
        # Already recorded as reported for that quarter ⇒ handled elsewhere.
        if find_reported_event_for_quarter(conn, ticker, quarter):
            continue
        dedup_key = f"missed_results_alerted:{ticker}:{quarter}"
        if kv_get(conn, dedup_key):
            continue
        info = coverage_map.get(ticker)
        confirmed_missed.append({
            "ticker": ticker,
            "company_name": info.company_name if info else "",
            "quarter": quarter,
            "event_date": filing.filing_date,
            "tier": 1,
            "filing_date": filing.filing_date,
            "filing_url": _edgar_filing_url(ticker, filing.accession),
            "dedup_key": dedup_key,
            "blind_sweep": True,
        })
    logger.info(
        f"EDGAR results fallback: Tier-1 blind sweep probed {swept} name(s) "
        f"not already covered by DB candidates"
    )

    def _emit_heartbeat():
        if skip_heartbeat or dry_run or not (SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS):
            return
        post_heartbeat(
            SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS,
            "Missed-results check",
            {
                "db_probed": len(candidates),
                "t1_swept": swept,
                "confirmed_missed": len(confirmed_missed),
            },
            duration_sec=time.monotonic() - start_ts,
        )

    if not confirmed_missed:
        logger.info("EDGAR results fallback: no silently-missed results found")
        conn.close()
        _emit_heartbeat()
        return

    logger.warning(
        f"EDGAR results fallback: {len(confirmed_missed)} Tier 1/2 name(s) "
        f"reported (per SEC) but Finnhub has no actuals: "
        + ", ".join(r["ticker"] for r in confirmed_missed)
    )

    if dry_run:
        for r in confirmed_missed:
            logger.info(
                f"  [dry-run] would alert: {r['ticker']} {r['quarter']} — "
                f"8-K 2.02 filed {r['filing_date']}, Finnhub actuals missing "
                f"({r['filing_url']})"
            )
        conn.close()
        return

    webhook = SLACK_WEBHOOK_EARNINGS or SLACK_WEBHOOK_STATUS
    if not webhook:
        logger.warning("Missed-results alert suppressed (no webhook configured)")
        conn.close()
        return

    try:
        post_slack(
            webhook,
            _build_missed_results_blocks(confirmed_missed, today),
            _build_missed_results_fallback(confirmed_missed),
        )
        # Only set dedup keys after a successful post so a failed post retries.
        for r in confirmed_missed:
            kv_set(conn, r["dedup_key"], f"alerted:{today.isoformat()}")
        logger.info(
            f"Posted missed-results alert for {len(confirmed_missed)} name(s)"
        )
    except NotificationError as exc:
        logger.error(f"Missed-results Slack post failed: {exc}")

    conn.close()
    _emit_heartbeat()


def _edgar_filing_url(ticker: str, accession: str) -> str:
    """Build a clickable SEC filing-index URL from a ticker + accession."""
    cik = get_cik(ticker)
    acc_nodash = accession.replace("-", "")
    if cik:
        cik_int = cik.lstrip("0") or "0"
        return (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
            f"&CIK={cik_int}&type=8-K"
        ) if not acc_nodash else (
            f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/"
            f"{accession}-index.htm"
        )
    return "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K"


def _build_missed_results_blocks(rows: list[dict], as_of: date) -> list[dict]:
    """Block Kit for the missed-results alert (#earnings)."""
    # Cross-platform "Wed Jun 4" (no %-d / %#d portability headache).
    header = (
        f":rotating_light: *Possible missed results* — "
        f"{as_of.strftime('%a %b ')}{as_of.day}"
    )
    lines = []
    for r in rows:
        tier_tag = "T1" if r["tier"] == 1 else "T2"
        # Blind-sweep hits are worse than DB-candidate lags: Finnhub never
        # even tracked the event, so flag them explicitly.
        gap = (
            "Finnhub never listed this event"
            if r.get("blind_sweep")
            else "Finnhub actuals still missing"
        )
        lines.append(
            f"{tier_tag} `{r['ticker']}` {_short_company_name(r['company_name'])} "
            f"({r['quarter']}) — SEC 8-K 2.02 filed *{r['filing_date']}*, "
            f"{gap} · <{r['filing_url']}|filing>"
        )
    body = (
        "These names filed an earnings release with the SEC but Finnhub hasn't "
        "published actuals, so the beat/miss post is delayed. Verify manually; "
        "the figures will auto-post once Finnhub backfills.\n\n" + "\n".join(lines)
    )
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": body}},
    ]


def _build_missed_results_fallback(rows: list[dict]) -> str:
    return "Possible missed results: " + ", ".join(
        f"{r['ticker']} ({r['quarter']}, 8-K {r['filing_date']})" for r in rows
    )


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
    _alert_coverage_stale_if_needed(conn, compute_coverage_freshness())

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
    urgent_moves: list[UrgentMoveRow] = []
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

        # yfinance hour fallback for upcoming events whose Finnhub hour is empty.
        hour_yf = None
        call_dt_iso: str | None = None
        call_source_val: str | None = None
        is_upcoming = (not has_actuals) and new_date >= today.isoformat()
        if not hour and is_upcoming and tier <= 2:
            try:
                inferred = fetch_yfinance_hour_for_date(ticker, new_date)
                if inferred:
                    hour_yf = inferred
                    logger.info(
                        f"yfinance fallback timing for {ticker} {new_date}: {hour_yf}"
                    )
            except Exception as exc:
                logger.debug(f"yfinance hour fallback failed for {ticker}: {exc}")
        if is_upcoming and tier <= 2:
            try:
                call_dt = fetch_yfinance_call_for_date(ticker, new_date)
                if call_dt is not None:
                    call_dt_iso = call_dt.isoformat()
                    call_source_val = "yfinance"
            except Exception as exc:
                logger.debug(f"yfinance call fetch failed for {ticker}: {exc}")

        if dry_run:
            logger.info(
                f"  [dry-run] {ticker} (T{tier}): {old_date} -> {new_date} ({hour or hour_yf or 'TBD'})"
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
                hour_yf=hour_yf,
                call_datetime_utc=call_dt_iso,
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
            event_hour_yf=hour_yf,
            call_datetime_utc=call_dt_iso,
            call_source=call_source_val,
        )
        logger.info(f"  Fixed {ticker} (T{tier}): {old_date} -> {new_date}")
        fixed.append(DriftRow(
            ticker=ticker, old_date=old_date, new_date=new_date,
            hour=hour, tier=tier,
        ))

        # A3: flag Tier 1 moves within 5 business days for the urgent alert
        if tier == 1:
            biz_days = _business_days_until(new_date, today)
            if 0 <= biz_days <= A3_URGENT_BIZ_DAYS:
                info = coverage_map.get(ticker)
                urgent_moves.append(UrgentMoveRow(
                    ticker=ticker,
                    company_name=(info.company_name if info else "") or "",
                    old_date=old_date,
                    new_date=new_date,
                    hour=hour,
                    biz_days_until=biz_days,
                    source="reconcile",
                ))

    conn.close()

    # Slack summary — only when we actually did something. Routes to the
    # status-reports channel; falls back to the earnings webhook if unset.
    reconcile_webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    if fixed and not dry_run and reconcile_webhook:
        blocks = build_reconcile_blocks(fixed, today)
        fallback = build_reconcile_fallback(fixed, today)
        try:
            post_slack(reconcile_webhook, blocks, fallback)
        except NotificationError as exc:
            logger.error(f"Reconcile Slack post failed: {exc}")

    # A3: separate urgent alert for T1 moves inside the 5-biz-day window
    if urgent_moves:
        logger.warning(
            f"A3: {len(urgent_moves)} Tier 1 date move(s) within "
            f"{A3_URGENT_BIZ_DAYS} business days"
        )
        if not dry_run:
            _post_urgent_alert(urgent_moves, today, conn=conn)


# ---------------------------------------------------------------------------
# Refresh calendar summaries + descriptions (one-shot backfill)
# ---------------------------------------------------------------------------


def run_refresh_descriptions(dry_run: bool = False, days_ahead: int = 90, days_back: int = 30):
    """
    Rewrite title + description for every tagged Calendar event in the window
    using current DB state. Useful after adding a field (like date_confirmed)
    so existing events pick up the new rendering without waiting for a natural
    date change.

    The window reaches ``days_back`` days into the past (default 30) as well as
    ``days_ahead`` forward, so recently-passed events drop a stale "(est.)"
    marker once their expected date has elapsed (an estimate only makes sense
    for a future date). The daily ``run()`` sync already re-renders events back
    ~14 days; this look-back lets the manual/daily refresh also clean events
    that passed without actuals ever being captured (e.g. delisted names).
    """
    from config import CALENDAR_PAGE_SIZE

    if not GOOGLE_CALENDAR_ID:
        logger.error("GOOGLE_CALENDAR_ID required")
        sys.exit(1)

    conn = init_db()
    try:
        cal_service = get_calendar_service()
    except Exception as exc:
        logger.error(f"Failed to init Calendar: {exc}")
        sys.exit(1)

    today = date.today()
    time_min = (today - timedelta(days=days_back)).isoformat() + "T00:00:00Z"
    time_max = (today + timedelta(days=days_ahead)).isoformat() + "T00:00:00Z"

    events = []
    page_token = None
    while True:
        r = cal_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            privateExtendedProperty="earningsAgent=true",
            timeMin=time_min, timeMax=time_max,
            singleEvents=True, maxResults=CALENDAR_PAGE_SIZE,
            pageToken=page_token,
        ).execute()
        events.extend(r.get("items", []))
        page_token = r.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"Refresh: {len(events)} tagged calendar events in window")

    updated = 0
    skipped_no_db = 0
    failed = 0
    for ev in events:
        props = ev.get("extendedProperties", {}).get("private", {})
        ticker = props.get("ticker")
        if not ticker:
            continue
        start = ev.get("start", {})
        event_date = start.get("date") or start.get("dateTime", "")[:10]
        if not event_date:
            continue

        db_row = find_existing_event(conn, ticker, event_date)
        if not db_row:
            skipped_no_db += 1
            continue

        hour = db_row.get("event_hour")
        hour_yf = db_row.get("event_hour_yf")
        call_dt_iso = db_row.get("call_datetime_utc")
        eps_est = db_row.get("eps_estimate")
        eps_act = db_row.get("eps_actual")
        rev_est = db_row.get("rev_estimate")
        rev_act = db_row.get("rev_actual")
        quarter_for_event = db_row.get("quarter")
        tier_for_event = db_row.get("tier") or 3
        source_fingerprint = f"{ticker}:{event_date}"

        new_summary, new_description, exp_props = expected_calendar_state(
            ticker, hour, eps_est, eps_act, rev_est, rev_act,
            quarter=quarter_for_event, tier=tier_for_event,
            source_fingerprint=source_fingerprint,
            hour_yf=hour_yf,
            earnings_date=event_date,
            call_datetime_utc=call_dt_iso,
        )
        drift = calendar_event_drift_kind(
            ev, new_summary, new_description, exp_props,
            hour or hour_yf or "",
        )
        if drift == "fresh":
            continue

        if dry_run:
            verb = "recreate" if drift == "shape" else "patch"
            logger.info(
                f"  [dry-run] Would {verb} {ticker} {event_date} ({drift}): "
                f"'{ev.get('summary')}' -> '{new_summary}'"
            )
            updated += 1
            continue

        try:
            if drift == "shape":
                # update_calendar_event_description doesn't touch start/end,
                # so a TBD->amc/bmo or all-day<->timed transition needs a
                # full recreate to get the right time block.
                delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, ev["id"])
                new_gcal_id = create_calendar_event(
                    cal_service, GOOGLE_CALENDAR_ID, ticker,
                    event_date, hour,
                    quarter=quarter_for_event,
                    eps_estimate=eps_est, eps_actual=eps_act,
                    revenue_estimate=rev_est, revenue_actual=rev_act,
                    tier=tier_for_event,
                    source_fingerprint=source_fingerprint,
                    hour_yf=hour_yf,
                    call_datetime_utc=call_dt_iso,
                )
                # Persist the recreated event's id so the DB stops pointing at
                # the just-deleted event. Without this, the DB keeps the stale
                # gcal_id, the next daily sync can't find it and recreates the
                # event, and the following refresh sees "shape" drift again —
                # an every-run delete/recreate churn (observed: ~140 recreates
                # per run) that also multiplies duplicates for date-flapping
                # tickers like ICLR. Mirrors the main-sync recreate path.
                conn.execute(
                    "UPDATE events SET gcal_id = ? WHERE id = ?",
                    (new_gcal_id, db_row["id"]),
                )
                conn.commit()
            else:  # 'text'
                update_calendar_event_description(
                    cal_service, GOOGLE_CALENDAR_ID, ev["id"],
                    new_summary, new_description,
                    ticker=ticker,
                    quarter=quarter_for_event,
                    source_fingerprint=source_fingerprint,
                    tier=tier_for_event,
                )
            updated += 1
        except CalendarError as exc:
            logger.warning(f"  Failed to update {ticker} {event_date}: {exc}")
            failed += 1

    conn.close()

    logger.info(
        f"Refresh complete: {updated} updated, {skipped_no_db} skipped "
        f"(no DB row), {failed} failed"
    )


# ---------------------------------------------------------------------------
# Announcement check: scan configured IR RSS feeds (B1 bonus)
# ---------------------------------------------------------------------------


def run_check_ir_emails(
    dry_run: bool = False,
    days_ahead: int = 30,
    lookback_days: int = 14,
    max_messages: int = 200,
):
    """
    Scan Gmail for IR press-release emails that pre-announce upcoming
    earnings dates for Tier 1/2 estimated events.

    Query is bounded to known IR distribution platforms (Notified, Q4,
    GlobeNewswire, BusinessWire, PR Newswire) plus mail to the +ir
    alias if present. For each matching message, runs the same
    announcement-detection regex used by --check-announcements
    (rss_client.detect_announcement). On match, sets date_confirmed=1
    and stores the Gmail thread URL as announcement_url so future
    runs don't re-process and the user can click through to the
    original email.

    No-ops cleanly when gmail_token.json isn't present (e.g., the
    integration hasn't been configured for this environment yet).
    """
    try:
        from gmail_client import (
            get_gmail_service, list_message_ids, get_message,
            detect_earnings_announcement, extract_sender_email,
            GmailError,
        )
    except ImportError as exc:
        logger.error(f"gmail_client unavailable: {exc}")
        return

    try:
        svc = get_gmail_service()
    except GmailError as exc:
        logger.info(f"Gmail integration not configured (skipping): {exc}")
        return

    conn = init_db()
    today = date.today()
    horizon = (today + timedelta(days=days_ahead)).isoformat()

    # Pull Tier 1/2 estimated events that haven't been confirmed yet
    # AND haven't already been linked to an announcement.
    cur = conn.execute(
        "SELECT ticker, event_date, company_name, tier "
        "FROM events "
        "WHERE tier <= 2 AND reported = 0 AND date_confirmed = 0 "
        "AND announcement_url IS NULL "
        "AND event_date >= ? AND event_date <= ? "
        "ORDER BY event_date, ticker",
        (today.isoformat(), horizon),
    )
    candidates = cur.fetchall()
    if not candidates:
        logger.info("Gmail IR scan: no Tier 1/2 estimated events in window")
        conn.close()
        return

    logger.info(
        f"Gmail IR scan: {len(candidates)} Tier 1/2 estimated event(s) to check "
        f"against the last {lookback_days} days of mail"
    )

    # One broad Gmail query — IR alert platforms + the +ir alias.
    # Then we iterate per-event and match by ticker or company name in
    # subject/body. Cheaper than per-ticker queries when there are 100+
    # estimated events to check.
    query = (
        f"(from:(notified.com OR q4inc.com OR globenewswire.com OR "
        f"businesswire.com OR prnewswire.com OR investorroom.com) "
        f"OR to:floridabusinessman+ir@gmail.com) "
        f"newer_than:{lookback_days}d"
    )
    try:
        message_ids = list_message_ids(svc, query, max_results=max_messages)
    except GmailError as exc:
        logger.error(f"Gmail list failed: {exc}")
        conn.close()
        return

    logger.info(f"Gmail IR scan: {len(message_ids)} candidate message(s)")
    if not message_ids:
        conn.close()
        return

    # Pre-fetch all message bodies once. Each ticker iteration is a
    # cheap in-memory scan over this list.
    messages = []
    for mid in message_ids:
        try:
            msg = get_message(svc, mid)
            messages.append(msg)
        except GmailError as exc:
            logger.debug(f"Failed to fetch {mid}: {exc}")

    newly_confirmed: list[dict] = []
    for ticker, event_date, company_name, tier in candidates:
        ticker_pat = ticker.upper()
        # Match if subject OR body contains the ticker (whole word) or
        # the company name (case-insensitive). Company name match is a
        # fallback for emails that don't ticker-tag in the subject.
        company_lower = (company_name or "").lower()
        relevant = []
        for msg in messages:
            text = f"{msg.subject} {msg.body}"
            if re.search(rf"\b{re.escape(ticker_pat)}\b", text):
                relevant.append(msg)
            elif company_lower and len(company_lower) >= 5 and company_lower in text.lower():
                relevant.append(msg)

        if not relevant:
            continue

        evt_dt = date.fromisoformat(event_date)
        # Newest first — if multiple matching emails exist, use the most
        # recent one (a re-announcement supersedes the earlier one).
        relevant.sort(key=lambda m: m.received_date, reverse=True)
        for msg in relevant:
            announced, matched = detect_earnings_announcement(msg, evt_dt)
            if not matched:
                continue
            sender_addr = extract_sender_email(msg.sender)
            gmail_url = f"https://mail.google.com/mail/u/0/#inbox/{msg.thread_id}"
            logger.info(
                f"  {ticker}: IR email match [{msg.received_date}] from "
                f"{sender_addr} — {msg.subject[:80]}"
            )
            newly_confirmed.append({
                "ticker": ticker,
                "event_date": event_date,
                "company_name": company_name or "",
                "subject": msg.subject,
                "sender": sender_addr,
                "received_date": msg.received_date.isoformat(),
                "gmail_url": gmail_url,
                "announced_date": (
                    announced.isoformat() if announced else None
                ),
            })
            if not dry_run:
                conn.execute(
                    "UPDATE events SET date_confirmed = 1, announcement_url = ? "
                    "WHERE ticker = ? AND event_date = ?",
                    (gmail_url, ticker, event_date),
                )
            break  # one match per ticker is enough

    if not dry_run:
        conn.commit()
    conn.close()

    if not newly_confirmed:
        logger.info("Gmail IR scan: no new announcements detected")
        return

    logger.info(
        f"Gmail IR scan: confirmed {len(newly_confirmed)} event(s) via IR email"
    )

    if not dry_run and SLACK_WEBHOOK_EARNINGS:
        lines = []
        for r in newly_confirmed:
            co = f" — {r['company_name']}" if r["company_name"] else ""
            lines.append(
                f"• `{r['ticker']}`{co}  →  *confirmed* for {r['event_date']}"
                f"\n  <{r['gmail_url']}|{r['subject'][:90]}>"
            )
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": (
                        f":email: {len(newly_confirmed)} event(s) newly confirmed via IR email"
                    ),
                },
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        ]
        try:
            post_slack(
                SLACK_WEBHOOK_EARNINGS, blocks,
                f"{len(newly_confirmed)} event(s) confirmed via IR email",
            )
        except NotificationError as exc:
            logger.warning(f"IR email Slack post failed: {exc}")


def run_check_announcements(dry_run: bool = False, days_ahead: int = 30):
    """
    For each upcoming Tier 1 estimated event that has an IR RSS URL
    configured in ir_feeds.json, scan the feed for a pre-announcement
    press release. When found, upgrade date_confirmed=1 and record the
    announcement URL.

    Tickers without an IR feed configured are silently skipped —
    aggregator RSS (Seeking Alpha, Nasdaq, Business Wire) don't
    reliably carry company earnings press releases, so we don't pretend
    to cover them.
    """
    from rss_client import (
        fetch_ticker_feed, detect_announcement, _load_ir_feeds,
    )

    ir_map = _load_ir_feeds()
    if not ir_map:
        logger.warning(
            "No IR feeds configured in ir_feeds.json — nothing to check. "
            "Populate the file with {TICKER: rss_url} entries."
        )
        return

    conn = init_db()
    today = date.today()
    horizon = (today + timedelta(days=days_ahead)).isoformat()

    cur = conn.execute(
        "SELECT ticker, event_date, company_name, tier "
        "FROM events "
        "WHERE tier = 1 AND reported = 0 AND date_confirmed = 0 "
        "AND announcement_url IS NULL "
        "AND event_date >= ? AND event_date <= ? "
        "ORDER BY event_date, ticker",
        (today.isoformat(), horizon),
    )
    candidates = cur.fetchall()
    if not candidates:
        logger.info("No Tier 1 estimated events in window")
        conn.close()
        return

    configured = [c for c in candidates if c[0].upper() in ir_map]
    skipped = len(candidates) - len(configured)
    logger.info(
        f"Checking {len(configured)} Tier 1 estimated event(s) with IR feeds "
        f"({skipped} without IR feed configured)"
    )

    newly_confirmed = []
    for ticker, event_date, company_name, tier in configured:
        try:
            result = fetch_ticker_feed(ticker)
        except Exception as exc:
            logger.warning(f"  {ticker}: feed fetch failed: {exc}")
            continue
        if not result:
            continue
        items, src = result
        evt_dt = date.fromisoformat(event_date)
        match = detect_announcement(items, evt_dt, source=src)
        if not match:
            logger.info(f"  {ticker}: no announcement found in {len(items)} IR items")
            continue
        logger.info(
            f"  {ticker}: announcement detected [{match.feed_item.pub_date}] "
            f"{match.feed_item.title[:100]}"
        )
        newly_confirmed.append({
            "ticker": ticker,
            "event_date": event_date,
            "company_name": company_name or "",
            "title": match.feed_item.title,
            "link": match.feed_item.link,
            "pub_date": (
                match.feed_item.pub_date.isoformat()
                if match.feed_item.pub_date else ""
            ),
            "announced_date": (
                match.announced_date.isoformat() if match.announced_date else None
            ),
        })

        if not dry_run:
            conn.execute(
                "UPDATE events SET date_confirmed = 1, announcement_url = ? "
                "WHERE ticker = ? AND event_date = ?",
                (match.feed_item.link, ticker, event_date),
            )

    if not dry_run:
        conn.commit()
    conn.close()

    if not newly_confirmed:
        logger.info("No new announcements detected")
        return

    logger.info(
        f"Upgraded {len(newly_confirmed)} Tier 1 event(s) from estimated to confirmed"
    )

    if not dry_run and SLACK_WEBHOOK_EARNINGS:
        lines = []
        for r in newly_confirmed:
            co = f" — {r['company_name']}" if r["company_name"] else ""
            lines.append(
                f"• `{r['ticker']}`{co}  →  *confirmed* for {r['event_date']}"
                + (f"\n  <{r['link']}|{r['title'][:90]}>" if r["link"] else "")
            )
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":newspaper: {len(newly_confirmed)} Tier 1 event(s) newly confirmed via IR RSS",
                },
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        ]
        try:
            post_slack(
                SLACK_WEBHOOK_EARNINGS,
                blocks,
                f"{len(newly_confirmed)} IR-announced earnings dates confirmed",
            )
        except NotificationError as exc:
            logger.error(f"Slack post failed: {exc}")


# ---------------------------------------------------------------------------
# Cross-check: Finnhub vs yfinance (B1)
# ---------------------------------------------------------------------------


def _yfinance_agrees(finnhub_date: str, yf_dates: list[date], tolerance_days: int = 1) -> bool:
    """True if Finnhub's date falls within yfinance's date/range ± tolerance."""
    fh_d = date.fromisoformat(finnhub_date)
    if not yf_dates:
        return True  # yfinance unknown — don't call it a disagreement
    lo = min(yf_dates) - timedelta(days=tolerance_days)
    hi = max(yf_dates) + timedelta(days=tolerance_days)
    return lo <= fh_d <= hi


def _yf_dates_signature(yf_dates: list[date]) -> str:
    """Stable string key for a sorted list of yfinance dates."""
    return ",".join(d.isoformat() for d in sorted(yf_dates))


def _edgar_date_corroborated(
    edgar_date: str, yf_dates: list[date], tolerance_days: int = 1
) -> bool:
    """Whether an EDGAR 8-K Item 2.02 filing date is corroborated as the true
    press-release date by an independent source (yfinance).

    An Item 2.02 is almost always filed the same day as the release, so the
    filing date is a strong signal — but it is NOT guaranteed (a company can
    file the 8-K a day or more after the actual release). Before we LOCK the
    calendar to the EDGAR date (overriding Finnhub for good), require that an
    independent source — a yfinance earnings date within ±tolerance — agrees.
    When EDGAR is a third, uncorroborated date, we still surface it for manual
    review but do not auto-lock a possibly-wrong date.
    """
    try:
        ed = date.fromisoformat(edgar_date)
    except ValueError:
        return False
    return any(abs((ed - d).days) <= tolerance_days for d in (yf_dates or []))


def _alert_uncorroborated_edgar(
    conn, ticker: str, company_name: str | None,
    stored_date: str, edgar_date: str, yf_dates: list[date],
) -> bool:
    """Surface (to #status-reports) an EDGAR 2.02 whose date disagrees with the
    stored date and isn't corroborated by yfinance — i.e. the company reported
    but the exact press-release date is unverified, so we deliberately did NOT
    auto-lock it. Deduped per ticker+EDGAR-date via kv_store. Returns True when
    a fresh alert was emitted, False when suppressed as a duplicate.
    Best-effort: swallows its own post errors.
    """
    dedup_key = f"edgar_uncorroborated_alerted:{ticker}:{edgar_date}"
    if kv_get(conn, dedup_key):
        return False
    yf_str = ", ".join(d.isoformat() for d in yf_dates) or "none"
    name = _short_company_name(company_name or "")
    text = (
        f":mag: *EDGAR date needs review* — `{ticker}` {name}\n"
        f"SEC 8-K Item 2.02 filed *{edgar_date}*, but the stored date is "
        f"{stored_date} and yfinance ({yf_str}) doesn't corroborate it. The "
        f"company has reported; the press-release date is unverified, so it was "
        f"*not auto-locked*. If {edgar_date} is right, pin it with "
        f"`python main.py --lock {ticker}:{edgar_date}`."
    )
    webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    if webhook:
        try:
            post_slack(
                webhook,
                [{"type": "section", "text": {"type": "mrkdwn", "text": text}}],
                f"EDGAR date needs review: {ticker} 2.02 {edgar_date} vs stored {stored_date}",
            )
        except NotificationError as exc:
            logger.error(f"Uncorroborated-EDGAR alert failed for {ticker}: {exc}")
            return False
    kv_set(conn, dedup_key, f"alerted:{date.today().isoformat()}")
    return True


def _apply_edgar_auto_correction(
    conn,
    cal_service,
    ticker: str,
    old_event_date: str,
    new_event_date: str,
) -> bool:
    """Move an event from old_event_date to new_event_date and lock it.

    Used when SEC EDGAR Item 2.02 confirms a press-release date that
    differs from what's stored. Atomically:
      - inserts a new DB row at the EDGAR date, copying forward fields,
        with date_locked=1 to prevent Finnhub from overriding next run
      - deletes the old DB row
      - delete+recreates the calendar event at the new date

    Idempotent: if old_event_date == new_event_date, no-op.
    Returns True if a correction was applied.
    """
    if old_event_date == new_event_date:
        return False
    existing = find_existing_event(conn, ticker, old_event_date)
    if not existing:
        return False

    # Re-query yfinance for the new date so the new row carries fresh
    # hour and call signals.
    new_hour_yf = None
    new_call_iso = None
    new_call_source = None
    try:
        new_hour_yf = fetch_yfinance_hour_for_date(ticker, new_event_date)
    except Exception:
        pass
    try:
        call_dt = fetch_yfinance_call_for_date(ticker, new_event_date)
        if call_dt is not None:
            new_call_iso = call_dt.isoformat()
            new_call_source = "yfinance"
    except Exception:
        pass

    # Insert new row at the EDGAR date, then delete old row.
    upsert_event(
        conn, ticker, new_event_date, existing.get("event_hour"), gcal_id=None,
        quarter=existing.get("quarter"),
        eps_estimate=existing.get("eps_estimate"),
        eps_actual=existing.get("eps_actual"),
        rev_estimate=existing.get("rev_estimate"),
        rev_actual=existing.get("rev_actual"),
        reported=existing.get("reported", False),
        tier=existing.get("tier", 3),
        company_name=existing.get("company_name"),
        source_fingerprint=f"{ticker}:{new_event_date}",
        event_hour_yf=new_hour_yf,
        call_datetime_utc=new_call_iso,
        call_source=new_call_source,
    )
    set_date_lock(conn, ticker, new_event_date, locked=True)
    conn.execute(
        "DELETE FROM events WHERE ticker = ? AND event_date = ?",
        (ticker, old_event_date),
    )
    conn.commit()

    # Move the calendar event. Best-effort — DB has already been
    # corrected, so a calendar API hiccup leaves us in a recoverable
    # state (next reconcile will re-create from DB).
    old_gcal_id = existing.get("gcal_id")
    if old_gcal_id and cal_service:
        try:
            delete_calendar_event(cal_service, GOOGLE_CALENDAR_ID, old_gcal_id)
        except CalendarError as exc:
            logger.warning(f"Could not delete old calendar event for {ticker}: {exc}")
        try:
            new_gcal_id = create_calendar_event(
                cal_service, GOOGLE_CALENDAR_ID, ticker,
                new_event_date, existing.get("event_hour"),
                quarter=existing.get("quarter"),
                eps_estimate=existing.get("eps_estimate"),
                eps_actual=existing.get("eps_actual"),
                revenue_estimate=existing.get("rev_estimate"),
                revenue_actual=existing.get("rev_actual"),
                tier=existing.get("tier", 3),
                source_fingerprint=f"{ticker}:{new_event_date}",
                hour_yf=new_hour_yf,
                call_datetime_utc=new_call_iso,
            )
            conn.execute(
                "UPDATE events SET gcal_id = ? WHERE ticker = ? AND event_date = ?",
                (new_gcal_id, ticker, new_event_date),
            )
            conn.commit()
        except CalendarError as exc:
            logger.error(f"Could not create new calendar event for {ticker}: {exc}")

    logger.info(
        f"EDGAR auto-correction: {ticker} moved {old_event_date} -> {new_event_date} "
        f"(date_locked=1)"
    )
    return True


def run_cross_check(dry_run: bool = False, days_ahead: int = 14):
    """
    Compare Finnhub's date against yfinance for upcoming Tier 1/2 events.

    Alert-only: Finnhub has already been applied by the time this runs.
    The goal is to surface "two sources disagree, verify manually".
    Date-locked events are skipped — the user has already decided to
    override auto-updates for those.

    Dedup: only Slacks when the set of yfinance dates has changed since
    the last alert (or this is the first alert). Prevents daily spam
    when yfinance is persistently stale.

    yfinance is scrapy and sometimes stale; a disagreement is a hint to
    check the IR page, not an automatic override.
    """
    conn = init_db()
    today = date.today()
    horizon_iso = (today + timedelta(days=days_ahead)).isoformat()

    cur = conn.execute(
        "SELECT ticker, event_date, tier, company_name, last_xcheck_yf_dates, "
        "date_confirmed "
        "FROM events "
        "WHERE tier <= 2 AND reported = 0 AND date_locked = 0 "
        "AND event_date >= ? AND event_date <= ? "
        "ORDER BY event_date, ticker",
        (today.isoformat(), horizon_iso),
    )
    upcoming = cur.fetchall()

    if not upcoming:
        logger.info("Cross-check: no upcoming Tier 1/2 events in window")
        conn.close()
        return

    logger.info(
        f"Cross-check: verifying {len(upcoming)} Tier 1/2 event(s) in the "
        f"next {days_ahead} day(s) against yfinance"
    )

    # Set up calendar service once for the auto-correction path. Best-
    # effort — auto-correction still updates DB if calendar is down.
    cal_service = None
    if not dry_run and GOOGLE_CALENDAR_ID:
        try:
            cal_service = get_calendar_service()
        except Exception as exc:
            logger.warning(f"Calendar service unavailable for cross-check: {exc}")

    new_disagreements = []  # list[tuple[DisagreementRow, signature_str]]
    suppressed_count = 0
    yf_missing = 0
    edgar_corrections = 0
    edgar_uncorroborated = 0
    for ticker, event_date, tier, company_name, last_sig, date_confirmed in upcoming:
        yf_dates = fetch_yfinance_earnings_date(ticker)
        if yf_dates is None:
            yf_missing += 1
            continue

        # EDGAR 8-K Item 2.02 tiebreaker runs FIRST — before
        # _yfinance_agrees and before the suppression filter — so
        # authoritative SEC evidence overrides every heuristic. This
        # catches UFPT-class cases where Finnhub picked the call day
        # and yfinance picked the release day (1-day diff falls within
        # _yfinance_agrees' tolerance, so it'd otherwise be silently
        # treated as agreement).
        edgar_release: str | None = None
        try:
            fh_d = date.fromisoformat(event_date)
            yf_d_min = min(yf_dates) if yf_dates else fh_d
            yf_d_max = max(yf_dates) if yf_dates else fh_d
            window_start = min(fh_d, yf_d_min) - timedelta(days=1)
            window_end = max(fh_d, yf_d_max) + timedelta(days=1)
            if window_start <= today:
                # Cap upper bound at today — EDGAR can't have a filing
                # for a future date.
                edgar_window_end = min(window_end, today)
                filing = find_earnings_release_filing(
                    ticker, window_start, edgar_window_end
                )
                if filing is not None:
                    edgar_release = filing.filing_date
                    logger.info(
                        f"EDGAR Item 2.02 for {ticker} filed {filing.filing_date} "
                        f"(accession {filing.accession})"
                    )
        except Exception as exc:
            logger.debug(f"EDGAR tiebreaker failed for {ticker}: {exc}")

        # When EDGAR contradicts what's stored, decide whether to act on it.
        # EDGAR 8-K Item 2.02 is authoritative that the company REPORTED, but
        # the filing date is not a guaranteed proxy for the press-release date
        # (an 8-K can be filed late). So we only auto-move+LOCK the calendar
        # when the EDGAR date is corroborated by an independent source
        # (yfinance ±1d). An uncorroborated third date is surfaced for manual
        # review instead of silently locking a possibly-wrong date.
        if edgar_release and edgar_release != event_date and not dry_run:
            if _edgar_date_corroborated(edgar_release, yf_dates):
                try:
                    applied = _apply_edgar_auto_correction(
                        conn, cal_service, ticker, event_date, edgar_release
                    )
                    if applied:
                        edgar_corrections += 1
                        continue
                except Exception as exc:
                    logger.error(f"EDGAR auto-correction failed for {ticker}: {exc}")
            else:
                # EDGAR shows a 2.02 on a date neither corroborated by
                # yfinance — report happened, but don't auto-lock. Alert once.
                logger.warning(
                    f"EDGAR 2.02 for {ticker} filed {edgar_release} differs from "
                    f"stored {event_date} and is NOT corroborated by yfinance "
                    f"{[d.isoformat() for d in yf_dates]} — surfacing, not locking"
                )
                if _alert_uncorroborated_edgar(
                    conn, ticker, company_name, event_date, edgar_release, yf_dates
                ):
                    edgar_uncorroborated += 1
                # fall through: still run the normal disagreement path below

        if _yfinance_agrees(event_date, yf_dates):
            # Agreement restored — clear any stale alert state
            if last_sig:
                conn.execute(
                    "UPDATE events SET last_xcheck_yf_dates = NULL "
                    "WHERE ticker = ? AND event_date = ?",
                    (ticker, event_date),
                )
            continue

        current_sig = _yf_dates_signature(yf_dates)
        if last_sig == current_sig and not edgar_release:
            # Already alerted with these yf dates AND no new EDGAR signal.
            suppressed_count += 1
            continue

        # Enrich with EDGAR cadence signal. Fail silently if EDGAR has no
        # data — it's a bonus hint, not a blocker.
        edgar_ref: str | None = None
        edgar_fh_offset: int | None = None
        edgar_yf_offset: int | None = None
        try:
            sig = infer_cadence_signal(ticker, event_date)
            if sig:
                edgar_ref = sig.reference_date
                edgar_fh_offset = sig.days_from_ref
                # Re-use same reference by recomputing offset for yfinance
                yf_primary = min(yf_dates)  # earliest yf candidate
                sig_yf = infer_cadence_signal(ticker, yf_primary.isoformat())
                if sig_yf and sig_yf.reference_date == sig.reference_date:
                    edgar_yf_offset = sig_yf.days_from_ref
        except Exception as exc:
            logger.debug(f"EDGAR cadence lookup failed for {ticker}: {exc}")

        # Split-day detection: if yfinance's CALL timestamp lands on
        # Finnhub's date (while the release timestamp lands on a
        # different day), this is a release/call split-day pattern, not
        # a true source conflict. UFPT pattern: AMC release Mon (yf
        # release date), BMO call Tue (yf call date == Finnhub date).
        split_day_call: str | None = None
        try:
            from zoneinfo import ZoneInfo
            call_dt = fetch_yfinance_call_for_date(ticker, event_date)
            if call_dt is not None:
                local_call_date = call_dt.astimezone(ZoneInfo("America/New_York")).date().isoformat()
                if local_call_date == event_date:
                    split_day_call = local_call_date
        except Exception as exc:
            logger.debug(f"split-day detection failed for {ticker}: {exc}")

        new_disagreements.append((DisagreementRow(
            ticker=ticker,
            company_name=company_name or "",
            finnhub_date=event_date,
            yf_dates=yf_dates,
            tier=tier,
            finnhub_confirmed=bool(date_confirmed),
            edgar_ref_date=edgar_ref,
            edgar_finnhub_offset=edgar_fh_offset,
            edgar_yf_offset=edgar_yf_offset,
            split_day_call_date=split_day_call,
            edgar_release_date=edgar_release,
        ), current_sig))

    logger.info(
        f"Cross-check: {len(new_disagreements)} new disagreement(s), "
        f"{edgar_corrections} EDGAR auto-correction(s), "
        f"{edgar_uncorroborated} uncorroborated-EDGAR alert(s), "
        f"{suppressed_count} suppressed (already alerted), "
        f"{yf_missing} tickers with no yfinance data"
    )

    if not new_disagreements:
        conn.close()
        return

    # Always log the disagreement detail so it shows up in CI artifacts
    for r, _ in new_disagreements:
        logger.warning(
            f"  T{r.tier} {r.ticker}: Finnhub={r.finnhub_date} "
            f"yfinance={[d.isoformat() for d in r.yf_dates]}"
        )

    disagreement_rows = [r for r, _ in new_disagreements]
    posted = True
    # Cross-check disagreements route to the status-reports channel so
    # the earnings channel stays focused on actual earnings updates.
    # Falls back to the earnings channel if status secrets are unset.
    target_channel = SLACK_STATUS_CHANNEL_ID or SLACK_CHANNEL_ID
    target_webhook = SLACK_WEBHOOK_STATUS or SLACK_WEBHOOK_EARNINGS
    bot_path = bool(SLACK_BOT_TOKEN and target_channel)
    if not dry_run and bot_path:
        # v9 per-thread path: summary header + one thread parent per row.
        # Persist each thread_ts back to the event row so --check-replies
        # can later poll for resolution.
        try:
            slack_post_message(
                SLACK_BOT_TOKEN,
                target_channel,
                blocks=build_crosscheck_summary_blocks(disagreement_rows, today),
                text=build_crosscheck_summary_fallback(disagreement_rows),
            )
            for r in disagreement_rows:
                ts = slack_post_message(
                    SLACK_BOT_TOKEN,
                    target_channel,
                    blocks=build_crosscheck_thread_blocks(r, today),
                    text=build_crosscheck_thread_fallback(r),
                )
                open_question(
                    conn,
                    r.ticker,
                    r.finnhub_date,
                    thread_ts=ts,
                    kind="xcheck",
                    first_seen_iso=today.isoformat(),
                    channel_id=target_channel,
                )
        except SlackAPIError as exc:
            logger.error(f"Cross-check Slack post failed (bot path): {exc}")
            posted = False
    elif not dry_run and target_webhook:
        # Legacy webhook fallback: single batched message, no replies.
        try:
            post_slack(
                target_webhook,
                build_crosscheck_blocks(disagreement_rows, today),
                build_crosscheck_fallback(disagreement_rows, today),
            )
        except NotificationError as exc:
            logger.error(f"Cross-check Slack post failed: {exc}")
            posted = False

    # Only update the dedup state if the alert actually went out (or if
    # there's no webhook configured — in which case we'd suppress forever
    # anyway). On Slack failure, leave the state untouched so the next
    # run retries.
    if not dry_run and posted:
        for r, sig in new_disagreements:
            conn.execute(
                "UPDATE events SET last_xcheck_yf_dates = ? "
                "WHERE ticker = ? AND event_date = ?",
                (sig, r.ticker, r.finnhub_date),
            )
        conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Slack reply polling (v9): drive resolution from in-thread commands
# ---------------------------------------------------------------------------


def _yf_dates_from_signature(sig: str | None) -> list[str]:
    """Recover the list of yfinance ISO dates from a stored signature."""
    if not sig:
        return []
    return [s for s in sig.split(",") if s]


def _ack_in_thread(thread_ts: str, text: str, channel_id: str | None = None) -> None:
    """Post a short text-only ack in the thread. Logs but does not raise."""
    if not SLACK_BOT_TOKEN:
        return
    target = channel_id or SLACK_CHANNEL_ID
    if not target:
        return
    try:
        slack_post_message(
            SLACK_BOT_TOKEN,
            target,
            text=text,
            thread_ts=thread_ts,
        )
    except SlackAPIError as exc:
        logger.error(f"Ack post failed: {exc}")


def _apply_action(
    conn,
    q: dict,
    action: ParsedAction,
    today: date,
) -> None:
    """Apply a ParsedAction to DB state. No-ops on UNKNOWN/HELP."""
    from storage import kv_set  # local import to keep the top of file tidy
    ticker = q["ticker"]
    event_date = q["event_date"]

    if action.action == ACT_LOCK:
        new_date = action.payload["date"]
        if new_date != event_date:
            # Lock applies to the new date; ensure the row's event_date matches
            # before flipping the lock. Move the row's event_date if needed.
            conn.execute(
                "UPDATE events SET event_date = ?, updated_at = datetime('now') "
                "WHERE ticker = ? AND event_date = ?",
                (new_date, ticker, event_date),
            )
            conn.commit()
        set_date_lock(conn, ticker, new_date, True)
        update_question_state(conn, ticker, new_date, "resolved")
        return

    if action.action == ACT_CONFIRM_FH:
        conn.execute(
            "UPDATE events SET date_confirmed = 1, updated_at = datetime('now') "
            "WHERE ticker = ? AND event_date = ?",
            (ticker, event_date),
        )
        conn.commit()
        update_question_state(conn, ticker, event_date, "resolved")
        return

    if action.action == ACT_WAIT:
        update_question_state(conn, ticker, event_date, "monitoring")
        return

    if action.action == ACT_SNOOZE:
        days = action.payload["days"]
        until = (today + timedelta(days=days)).isoformat()
        update_question_state(
            conn, ticker, event_date, "snoozed", snooze_until_iso=until
        )
        return

    if action.action == ACT_IGNORE:
        update_question_state(conn, ticker, event_date, "dismissed")
        return

    if action.action == ACT_REPORTED:
        conn.execute(
            "UPDATE events SET reported = 1, unseen_run_count = 0, "
            "updated_at = datetime('now') "
            "WHERE ticker = ? AND event_date = ?",
            (ticker, event_date),
        )
        conn.commit()
        update_question_state(conn, ticker, event_date, "resolved")
        return

    if action.action == ACT_IR:
        kv_set(conn, f"ir_feed:{ticker}", action.payload["url"])
        update_question_state(conn, ticker, event_date, "resolved")
        return

    if action.action == ACT_NOTE:
        # Store as a numbered note under note:TICKER:DATE:N. Keeps history.
        from storage import kv_list_prefix
        existing = kv_list_prefix(conn, f"note:{ticker}:{event_date}:")
        next_n = len(existing) + 1
        kv_set(
            conn,
            f"note:{ticker}:{event_date}:{next_n}",
            f"{today.isoformat()} {action.payload['text']}",
        )
        return

    # HELP/STATUS/UNKNOWN have no DB side effects — caller already handled ack.


def run_check_replies(dry_run: bool = False, days_lookback: int = 14):
    """
    Poll Slack threads for replies on open questions and apply them.

    For each event with slack_thread_ts set and a non-terminal state,
    this fetches replies posted after the watermark, parses each, applies
    the resulting action to DB state, and posts a short ack in-thread.

    Snoozed questions whose snooze window has expired transition back to
    `open` so the next disagreement detection can re-alert.
    """
    if not SLACK_BOT_TOKEN or not (SLACK_CHANNEL_ID or SLACK_STATUS_CHANNEL_ID):
        logger.info(
            "--check-replies: SLACK_BOT_TOKEN and at least one of "
            "SLACK_CHANNEL_ID/SLACK_STATUS_CHANNEL_ID must be set; skipping"
        )
        return

    conn = init_db()
    today = date.today()
    questions = list_open_questions(conn)
    if not questions:
        logger.info("--check-replies: no open questions")
        conn.close()
        return

    processed = 0
    acked = 0
    snoozed_reopened = 0
    for q in questions:
        # Snooze expiry → reopen so next detection fires fresh
        if q["question_state"] == "snoozed":
            until = q["question_snooze_until"]
            if until and until <= today.isoformat():
                update_question_state(conn, q["ticker"], q["event_date"], "open")
                snoozed_reopened += 1
                q["question_state"] = "open"

        thread_ts = q["slack_thread_ts"]
        oldest = q["slack_last_reply_ts"]
        # Per-thread channel (v10). NULL on legacy rows posted before the
        # status-reports split — fall back to SLACK_CHANNEL_ID for those.
        channel_id = q.get("slack_channel_id") or SLACK_CHANNEL_ID
        if not channel_id:
            continue
        try:
            replies = fetch_thread_replies(
                SLACK_BOT_TOKEN, channel_id, thread_ts, oldest=oldest
            )
        except SlackAPIError as exc:
            logger.error(
                f"Reply fetch failed for {q['ticker']}@{q['event_date']}: {exc}"
            )
            continue
        if not replies:
            continue

        kind = q["slack_question_kind"] or "xcheck"
        ctx = ReplyContext(
            ticker=q["ticker"],
            event_date=q["event_date"],
            kind=kind,
            finnhub_date=q["event_date"] if kind == "xcheck" else None,
            yf_dates=_yf_dates_from_signature(q["last_xcheck_yf_dates"]),
        )

        latest_ts = oldest
        for reply in replies:
            if reply.is_bot:
                latest_ts = reply.ts
                continue
            action = parse_reply(reply.text, ctx)

            if action.action == ACT_HELP:
                if not dry_run:
                    _ack_in_thread(thread_ts, format_help(kind), channel_id)
            elif action.action == ACT_STATUS:
                if not dry_run:
                    _ack_in_thread(thread_ts, format_status(q, today), channel_id)
            elif action.action == ACT_UNKNOWN:
                if not dry_run:
                    _ack_in_thread(
                        thread_ts,
                        f":x: {action.error}",
                        channel_id,
                    )
            else:
                if not dry_run:
                    _apply_action(conn, q, action, today)
                    if action.ack:
                        _ack_in_thread(thread_ts, action.ack, channel_id)

            latest_ts = reply.ts
            processed += 1
            if action.ack and action.action != ACT_UNKNOWN:
                acked += 1

        if not dry_run and latest_ts and latest_ts != oldest:
            advance_reply_watermark(conn, q["ticker"], q["event_date"], latest_ts)

    logger.info(
        f"--check-replies: {processed} reply(ies) processed, "
        f"{acked} action(s) applied, {snoozed_reopened} snoozed reopened"
    )
    conn.close()


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
        help="Preview external writes (Calendar/TickTick/Slack) and skip the "
             "reported flag. NOT fully side-effect-free: still upserts events + "
             "estimate snapshots into SQLite.",
    )
    parser.add_argument(
        "--populate-db-only",
        action="store_true",
        help="Alias for --dry-run, named for the CI use of seeding the SQLite "
             "DB with events/estimates without any external writes.",
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
        "--check-missed-results",
        action="store_true",
        help="EDGAR backstop: alert when a Tier 1/2 name reported (SEC 8-K "
             "Item 2.02) but Finnhub has no actuals yet (the FIVE-class miss)",
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
    parser.add_argument(
        "--cross-check",
        action="store_true",
        help="Compare Finnhub's upcoming Tier 1/2 dates against yfinance; alert on disagreement",
    )
    parser.add_argument(
        "--refresh-descriptions",
        action="store_true",
        help="Rewrite title + description for all tagged upcoming calendar events from current DB state",
    )
    parser.add_argument(
        "--check-announcements",
        action="store_true",
        help="Scan configured IR RSS feeds (ir_feeds.json) for earnings-date announcements; upgrade estimated Tier 1 events to confirmed when found",
    )
    parser.add_argument(
        "--check-ir-emails",
        action="store_true",
        help="Scan Gmail (via gmail_token.json) for IR-alert emails on Tier 1/2 estimated events; upgrade to confirmed and store the Gmail thread URL",
    )
    parser.add_argument(
        "--check-replies",
        action="store_true",
        help="Poll Slack threads for replies on open questions; apply commands (lock/wait/snooze/ignore/etc) to DB state",
    )
    args = parser.parse_args()

    # --populate-db-only is a self-documenting alias for --dry-run (DB writes
    # only, no external side effects). Fold it in so all downstream checks of
    # args.dry_run see it.
    if getattr(args, "populate_db_only", False):
        args.dry_run = True

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
    elif args.check_missed_results:
        _run_safeguard(
            "EDGAR missed-results backstop",
            lambda: run_edgar_results_fallback(
                dry_run=args.dry_run,
                skip_heartbeat=args.no_heartbeat,
            ),
        )
    elif args.reconcile_calendar:
        run_reconcile_calendar(dry_run=args.dry_run)
    elif args.cross_check:
        _run_safeguard(
            "cross-check (Finnhub vs yfinance + EDGAR)",
            lambda: run_cross_check(dry_run=args.dry_run),
        )
    elif args.refresh_descriptions:
        run_refresh_descriptions(dry_run=args.dry_run)
    elif args.check_announcements:
        run_check_announcements(dry_run=args.dry_run)
    elif args.check_ir_emails:
        run_check_ir_emails(dry_run=args.dry_run)
    elif args.check_replies:
        run_check_replies(dry_run=args.dry_run)
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
