# earnings_agent — Claude notes

## Three systems of record

- **Coverage Manager** = universe + tier assignment (source of truth for *which* names to track).
  Consumed via sparse-checkout of `jroypeterson/Coverage-Manager/exports/` in CI, or a local Dropbox path via `COVERAGE_MANAGER_PATH`.
- **Google Calendar** = published event state (source of truth for *when* it's happening).
- **SQLite (`earnings_events.db`)** = workflow state + historical memory (reported flag, TickTick task IDs, estimate snapshots).

## CLI modes

```
python main.py                     # Daily sync: fetch Finnhub, upsert DB, create/update Calendar + TickTick
python main.py --dry-run           # Preview; no Calendar/TickTick writes (DB still populated)
python main.py --backfill          # Widen past window to 30 days
python main.py --cleanup           # Remove duplicate Calendar events
python main.py --reconcile-calendar # Detect + auto-fix calendar/Finnhub date drift (silent no-op if synced)
python main.py --cross-check       # Compare Finnhub vs yfinance for Tier 1/2 upcoming; alert on disagreement (includes EDGAR cadence)
python main.py --check-announcements # Scan configured IR RSS feeds; upgrade estimated Tier 1 events to confirmed when found
python main.py --check-ir-emails   # Scan Gmail (via gmail_token.json) for IR-alert pre-announcements on Tier 1/2 estimated events; auto-confirm + record Gmail thread URL
python main.py --check-replies     # Poll Slack threads for replies on open questions; apply commands (lock/wait/snooze/ignore/etc) to DB
python main.py --refresh-descriptions # One-shot: rewrite title + description for all tagged upcoming calendar events
python main.py --lock TICKER:YYYY-MM-DD     # Pin a date so sync/reconcile won't move it
python main.py --unlock TICKER:YYYY-MM-DD   # Remove a lock
python main.py --list-locks        # Show currently-locked events
python main.py --weekly-digest     # Post weekly digest to Slack + write last_digest.html/.txt
python main.py --check-results [--date YYYY-MM-DD]  # Detect newly-reported actuals, post to Slack
python main.py --check-missed-results # EDGAR backstop: alert when a Tier 1/2 name filed an 8-K Item 2.02 but Finnhub has no actuals yet (FIVE-class silent miss). Two passes: DB candidates + a DB-independent Tier-1 blind sweep.
python main.py --populate-db-only   # Alias for --dry-run, named for the CI use of seeding the SQLite DB (events + estimates) with no external writes
python main.py --ticktick-status   # Show TickTick review queue
python main.py --no-ticktick       # Skip TickTick during --sync
python main.py --no-heartbeat      # Skip Slack success heartbeat at end of run
```

## Tier semantics

- **Tier 1** (~43) — Coverage Manager Position lists, promoted as follows:
  - `Portfolio` ∩ `Core=Y` and `Researching` ∩ `Core=Y` (legacy rule — held + active thesis)
  - `Ready to Buy` and `Ready to Short` — any (Core filter dropped 2026-05-11; trigger-ready ⇒ user committed)
  - Gets Calendar events, TickTick tasks, full digest detail.
- **Tier 2** (~209) — universe in `Healthcare Services` or `MedTech` sectors, excluding Tier 1. Gets Calendar + TickTick.
- **Tier 3** (~842) — everything else. No Calendar, no TickTick. Shows in digest with YTD + timing.

`Following for Interest` names are NOT auto-promoted — they keep their sector-derived tier (T2 if HC, else T3). But every ticker in any of the five Position lists has its `TickerInfo.position` set so the digest renders it under its Position-named subgroup (Portfolio / Researching / Ready to Buy / Ready to Short / Following for Interest), not its sector subgroup. Same is true for Portfolio / Researching tickers that fail the Core=Y filter — they fall to T2/T3 but still render under their Position label in the digest.

## Date-correctness safeguards

Getting the earnings date right is the primary goal. The stack of safeguards, from most-leverage down:

- **A1 — `fetch_earnings` hard-fails on data loss.** On a Finnhub cap hit (≥1500 rows in a chunk), it adaptively halves the chunk span until it clears or bottoms out at 1 day. A 1-day cap hit, or a chunk's exhausted retries, raises `FinnhubError` — better a failed run + `if: failure()` Slack alert than a "successful" run on silently-truncated data. Never restore the old swallow-and-continue behavior at `finnhub_client.py:131`.
- **A2 — Reconcile calendar auto-repair** (`reconcile_calendar.yml`, weekday 14/17/20 UTC). Compares tagged calendar events to Finnhub's current view (next 45d) and delete+recreates on mismatch. Skips any event where `date_locked=1`. Silent no-op when in sync.
- **Preflight drift detection** in `run()` at `main.py:125`: bulk-fetches tagged calendar events into `cal_start_by_id`, then forces the delete+recreate path when DB agrees with Finnhub but the calendar event's start date differs (covers the CI-artifact-loss → DB-repopulated-from-calendar case).
- **B1 — yfinance cross-check** (`--cross-check`, runs in daily CI after main sync). Alert-only: Finnhub still wins. Dedup via `last_xcheck_yf_dates` column — only re-alerts when yfinance's set of dates changes (no daily repeat spam).
- **B1 enrichment — EDGAR cadence signal.** For each Finnhub/yfinance disagreement, `edgar_client.infer_cadence_signal()` pulls the company's prior-year same-quarter Item 2.02 8-K and shows which candidate date is closer to the anniversary. Free SEC API (data.sec.gov, no auth). Requires `SEC_EDGAR_USER_AGENT` env var with contact info or uses the hardcoded default.
- **B2 — Unseen-ticker counter.** `unseen_run_count` column bumps each daily sync where Finnhub didn't return a Tier 1/2 event that's in DB (upcoming 30d, not reported). Alerts at 2 consecutive runs. Resets on re-appearance. Tickers not in `coverage_map` are skipped.
- **D2 — `date_locked` override.** User runs `--lock TICKER:DATE` when Finnhub is wrong and IR-page verified. Both `run()` (date-change branch) and `run_reconcile_calendar()` (drift loop) respect the lock. Reconcile uses ticker-wide `is_ticker_date_locked(ticker, cal_date, window=30)` as a safety net when DB/calendar have drifted.
- **A3 — Urgent Slack** for Tier 1 date moves within 5 business days. Posted in addition to the normal summary; kept simple (no ack, no repeat logic). Business-days helper is `_business_days_until()` in `main.py`.
- **Confirmed/estimated flag (`date_confirmed`).** Derived from Finnhub's `hour` field: `bmo`/`amc`/`dmh` = confirmed timing → the company has announced the date; empty hour = Finnhub projecting from historical cadence. Surfaced in calendar title (" (est.)" suffix), calendar description ("Status: Confirmed/Estimated" line), and cross-check Slack alerts. Use `--refresh-descriptions` after schema changes to backfill existing events.
- **IR RSS announcement scanner (`--check-announcements`).** Only scans tickers configured in `ir_feeds.json`. Aggregator RSS (Seeking Alpha, Nasdaq, Business Wire, PR Newswire) was tested and **empirically does not carry company IR press releases** — it surfaces analyst commentary and post-release transcripts. So this is Tier 1-only and opt-in per company. When a match is found, upgrades `date_confirmed=1` and stores `announcement_url`. Regex is tight: verb + same-quarter marker, excluding "earnings preview / gears up / what to expect / transcript" noise.
- **Gmail IR-alert scanner (`--check-ir-emails`).** When `gmail_token.json` is present (locally) or `GMAIL_TOKEN_JSON`+`GMAIL_CLIENT_CREDENTIALS_JSON` secrets are set (CI), polls Gmail for IR-alert emails from known platforms (Notified, Q4, GlobeNewswire, BusinessWire, PR Newswire, InvestorRoom) plus mail to `floridabusinessman+ir@gmail.com`. Reuses the same announcement-detection regex as `--check-announcements` so noise filtering is consistent. On match, sets `date_confirmed=1` and stores the Gmail thread URL as `announcement_url`. Authed against `floridabusinessman@gmail.com` (separate from primary `jroypeterson@gmail.com` to keep IR alerts isolated). OAuth client reused from daily-reads project. See `gmail_client.py` and `scripts/authorize_gmail.py`.
- **EDGAR 8-K Item 2.02 auto-correction (in `--cross-check`).** When SEC EDGAR shows a 2.02 ("Results of Operations") filing in the candidate window for a Tier 1/2 disagreement, the filing date IS ground truth. `_apply_edgar_auto_correction()` atomically: inserts new DB row at EDGAR date with `date_locked=1`, deletes old DB row, delete+recreates calendar event at EDGAR date, re-queries yfinance for hour/call info. Runs BEFORE `_yfinance_agrees` and BEFORE the suppression filter — SEC evidence overrides every heuristic, including the ±1-day tolerance that masks UFPT-class split-day patterns. Verified live: UFPT (Finnhub said May 5, EDGAR said May 4) auto-corrected.
- **yfinance hour fallback via `Ticker.info`.** When Finnhub's `hour` is empty for an upcoming Tier 1/2 event, infer from yfinance's `earningsTimestamp`/`Start`/`End` (Unix-seconds, time-of-day preserved). Reads via `Ticker.info`, NOT `Ticker.calendar` or `get_earnings_dates()` — those wrappers strip time-of-day. Market-session boundaries: `< 09:30 ET → bmo`, `>= 16:00 ET → amc`, mid-session → None (catches 8:30 ET pre-open releases the original `<9 / >16` heuristic missed). Hour stored separately in `event_hour_yf` (v11 column) so `event_hour` and `date_confirmed` keep Finnhub-canonical semantics.
- **Conference-call timestamp tracking (v12 schema).** Reads `earningsCallTimestampStart`/`End` from yfinance and stores in `call_datetime_utc` (with `call_source='yfinance'`). Calendar event description renders `Press release: <timing>` + `Conference call: <weekday Mon DD H:MM ET>` (split-day) or `Conference call: H:MM ET (same day)`. Calendar event itself anchors to the press-release date; the call line is descriptive context. Cross-check classifies "release/call split-day pattern" (yfinance call date matches Finnhub date but release date doesn't) as informational, not a true conflict. Calendar title for reported events: `<TICKER> Rpt'd Earnings` (compact form vs old `[REPORTED] X Earnings Release`).
- **Slack-reply resolution (`--check-replies`, v9).** When `SLACK_BOT_TOKEN`+`SLACK_CHANNEL_ID` are set, cross-check / unseen / urgent alerts post a summary header + one threaded parent message per row instead of one batched webhook message. Each thread parent advertises a small command grammar (`lock fh|yf|YYYY-MM-DD`, `confirm fh`, `wait`, `snooze Nd|Nw`, `ignore`, `reported`, `ir <url>`, `note <text>`, `help`, `status`); replying in a thread drives DB state via the `--check-replies` poller. State is tracked on the `events` row (`slack_thread_ts`, `slack_question_kind`, `slack_last_reply_ts`, `question_state`, `question_snooze_until`, `question_first_seen`). Webhook batched-message path stays as fallback when bot token is unset, but reply commands then have no effect. Parser: `slack_replies.py`. Web client: `slack_api.py`.

## Earnings calendar source: Finnhub + FMP merged (2026-06-04)

The earnings calendar is a **merge** of Finnhub + FMP (`fmp_client.py`,
orchestrated by `main._fetch_earnings_source`, used in `run()` and
`run_check_results`; `run_reconcile_calendar` stays Finnhub-only on purpose so
date arbitration is unchanged). Decided by a universe-wide bake-off
(`scripts/compare_providers.py`): FMP (already paid via Coverage Manager's
Starter plan) covers far more names per window (~+14 T1 / +70 T2 reporters,
nearly all WITH actuals) and is never behind on actuals (had actuals for 30
names Finnhub lacked incl. FIVE; reverse = 0). Date accuracy is a wash and FMP
carries MORE phantoms, so the policy is a merge, not a replacement. Per
(ticker, reporting-quarter): Finnhub-has-actuals → keep Finnhub, fill gaps from
FMP; Finnhub-no-actuals but FMP-has-actuals → use FMP's row (fixes the FIVE
lag); both-upcoming → keep Finnhub's date (date authority; cross-check
arbitrates conflicts); only-one-source → take it (breadth, mostly FMP). The
same-quarter phantom guard still collapses FMP's extra phantoms. `FMP_API_KEY`
unset or an FMP fetch failure → degrade to Finnhub-only, logged loudly (never
silent). Provider counts logged every run: `Earnings sources merged: Finnhub=X
FMP=Y -> Z`. Output normalized to Finnhub's event shape (+`source` tag).

## Source priority hierarchy (date verification)

When sources disagree on the press-release date, the agent's tiebreaker order:

1. **EDGAR 8-K Item 2.02 (post-hoc)** — authoritative SEC filing. Auto-corrects, and **locks only when corroborated by yfinance ±1d** (else surfaces for manual `--lock`); blind to foreign filers (6-K, e.g. ICLR).
2. **IR email alert (forward-looking, `--check-ir-emails`)** — company's own pre-announcement email. Auto-confirms + records Gmail URL.
3. **IR RSS feed (forward-looking, `--check-announcements`)** — company's own RSS. Auto-confirms + records URL.
4. **Finnhub + FMP merged calendar** — co-primary for upcoming events; Finnhub holds the date on shared names, FMP adds breadth + actuals.
5. **yfinance** — release-date validation + hour/call inference (fallback) + the EDGAR auto-lock corroborator.
6. **Manual `--lock`** — user override of last resort.

## Gotchas

- **`fetch_earnings` skips single-day ranges** (`while start < end` — if `from == to`, loop never runs). `run_check_results` works around this by passing `target → target+1` and filtering client-side.
- **Slack has no native underline.** `notifications._underline()` uses U+0332 combining low line. Works in most clients; if it breaks, fall back to `*━━ Day ━━*`.
- **Gmail send is MCP-only.** CI cannot create Gmail drafts — `--weekly-digest` in CI just uploads `last_digest.html` as an artifact; email draft happens when a human Claude session runs the MCP `create_draft` tool.
- **`Other` sector catches uncategorized tickers** the user follows but hasn't assigned a formal sector to. These fall to Tier 3. (Previous "PA" code was retired in Coverage Manager schema v2 on 2026-04-17 — collapsed into "Other".)
- **TickTick list naming uses *reporting quarter*, not release-date quarter.** April releases land in `1Q26 Earnings - *`, not `2Q26`. `_reporting_quarter()` in `ticktick.py`.
- **TickTick lists consolidated to two per quarter (2026-05-07).** Tier 1 → `<RQ> Earnings - Core Watchlist - Positions/Researching` (single list, was previously split into Portfolio + Researching + legacy Core Watchlist). Tier 2 → `<RQ> Earnings - HC Svcs & MedTech`. `position` argument on `_quarter_list_name()` is accepted for back-compat but ignored. New lists auto-create under the **Work - Investing** TickTick folder (`groupId=6887c72473800767fff51d51`, hardcoded default in `find_or_create_list`; override via `TICKTICK_EARNINGS_GROUP_ID`).
- **Cross-list dedup in `sync_ticktick_tasks`.** Before creating a task, scans every TickTick list whose name starts with `<RQ> Earnings` for an existing task with that ticker. If found anywhere, skips the create and backfills `events.ticktick_task_id` to the canonical task. Prevents same-ticker duplicates across the Tier 1 and Tier 2 lists. **Side-effect / known limitation:** when a ticker's tier changes mid-quarter (e.g. Tier 2 → Tier 1), the existing task stays in the old list and dedup blocks auto-recreation in the new tier's list. Acceptable: the next reporting quarter's sync will place it correctly. Slack diff alert (below) surfaces tier changes so the user can move tasks manually if needed mid-quarter.
- **Coverage-change diff alerting.** Daily sync (`run()` only — not the 3x/day reconcile) snapshots ticker→tier→position to `kv_store.coverage_snapshot` and diffs against the prior snapshot. Posts Tier 1/2 changes (added, removed, tier_changed, position_changed) to `#status-reports` via `_alert_coverage_changes_if_needed()` in `main.py`. Tier 3 churn is suppressed (universe-wide noise). First-run seeds the snapshot silently — no alert without a baseline. Snapshot was seeded 2026-05-07 with 1094 tickers (T1=50, T2=210, T3=834).
- **TickTick Open API does NOT support `groupId` updates on projects or `projectId` updates on tasks.** Both fields are silently stripped from POST bodies despite returning HTTP 200. Workarounds: folder moves must happen in the TickTick UI manually; task moves require delete + recreate (which mints a new task ID, so the DB pointer must be repointed).
- **Idempotent result detection.** `run_check_results` skips events already marked `reported=1`. DB update happens *after* Slack post succeeds, so a Slack failure leaves records unmarked for the next run to retry.
- **Phantom/duplicate-listing clobber (fixed 2026-06-04).** Finnhub double-lists date-flapping names (e.g. ICLR after its accounting-delay): a real reported event WITH actuals (`2026-05-27 amc`) plus a phantom forward listing with NO actuals whose date flaps (`2026-06-06`→`2026-06-02`), both mapping to reporting-quarter `2026Q1`. The same-quarter cleanup `DELETE` in `upsert_event` was wiping the just-marked `reported=1` row when the phantom was (re)inserted, so the actuals re-posted on EVERY sync (ICON spam). Two guards: (1) `upsert_event`'s `DELETE ... WHERE quarter=? AND event_date != ?` now also requires `reported = 0` — a reported event's date doesn't move, so only no-actuals phantoms collide; (2) `run()` and `run_check_results` call `find_reported_event_for_quarter()` and skip any Finnhub entry whose quarter is already reported at a *different* date (kills phantom-reset and same-quarter dup double-posts; the latter also caused the TTAN/COO/DOCU "Actuals in" logged twice per run). Regression: `test_reported_row_survives_same_quarter_phantom_upsert`.
- **EDGAR results backstop (`--check-missed-results`, added 2026-06-04; hardened per code review same day).** Results detection is Finnhub-only, so when Finnhub lags or parks an event on the wrong date a Tier 1/2 name reports and the agent stays silent (FIVE: reported 2026-06-03, Finnhub stuck at 2026-06-02 with no actuals). `run_edgar_results_fallback()` runs **two passes**: (1) **DB candidates** — Tier 1/2 events overdue (`event_date <= today`, within `_MISSED_RESULTS_LOOKBACK_DAYS=10`), `reported=0`, no actuals → probe `find_earnings_release_filing` for an 8-K 2.02; (2) **DB-independent Tier-1 blind sweep** — probe EVERY Tier-1 ticker's 8-K 2.02s over the last `_TIER1_SWEEP_DAYS=6` regardless of whether Finnhub created a DB row, flag any filing with no `find_reported_event_for_quarter` hit (catches names Finnhub never lists — the worst miss). Blind-sweep hits say "Finnhub never listed this event". Alert-only (figures still come from Finnhub later); dedup'd per ticker+quarter in `kv_store` (`missed_results_alerted:TICKER:QUARTER`). Wired into `post_earnings_check.yml` after the Finnhub sweeps. See `feedback_no_silent_failures.md`.
- **Safeguards must fail LOUD, not `continue-on-error` (2026-06-04 review fix).** The cross-check and missed-results backstop steps no longer use `continue-on-error` — that masked failures in exactly the "verify dates / don't miss" layer. `main._run_safeguard(label, fn)` wraps both dispatch calls: on exception it posts a context-rich `:warning: Safeguard degraded` alert to #status-reports (best-effort), then **re-raises** so the workflow step also fails and its `if: failure()` Slack fires. `SystemExit` (missing key/coverage) propagates without the degraded framing. Per-ticker yfinance flakiness is still swallowed inside `run_cross_check` (yf_missing counter), so only a systemic failure crashes.
- **EDGAR date corroboration gate before auto-LOCK (2026-06-04 review fix).** `_apply_edgar_auto_correction` is now only called when `_edgar_date_corroborated(edgar_date, yf_dates)` is True (EDGAR filing date within ±1d of a yfinance date). An 8-K 2.02 proves the company *reported*, but the filing date isn't a guaranteed proxy for the press-release date (8-Ks can be filed late), so an uncorroborated third date is surfaced via `_alert_uncorroborated_edgar` (deduped `edgar_uncorroborated_alerted:TICKER:DATE`, suggests `--lock TICKER:DATE`) instead of silently locking a possibly-wrong date. The UFPT case still auto-locks (EDGAR==yfinance release day).
- **`--dry-run` is NOT side-effect-free (clarified 2026-06-04).** It skips Calendar/TickTick/Slack writes and the `reported` flag, but DOES upsert events + estimate snapshots into SQLite — the daily/weekly CI jobs depend on this seeding. `--populate-db-only` is a self-documenting alias (folds to `dry_run=True` right after `parse_args`); the `weekly_digest.yml` + `post_earnings_check.yml` populate steps use it. `pip install -r requirements-dev.txt` for pytest.
- **`run()` shares `notify_results()` with `run_check_results`** — the 6 AM daily sync also posts Slack alerts when it detects overnight AMC actuals. Don't re-post from a separate path.
- **Stock-move deferral (added 2026-05-07).** `fetch_post_earnings_move` returns `None` when the comparison close hasn't posted yet — for AMC events that's close X+1, which only exists by the post_earnings_check 22:37 UTC sweep on day X+1 (NOT the same-day sweep at X 22:37 UTC, NOR the next-morning daily sync at X+1 11:13 UTC). `_should_defer_post()` (in `main.py`) holds the Slack post + `reported=1` flag back when move is None and event is ≤3 calendar days old; cap at 3 days covers Fri-AMC → Mon close. Both `run()` and `run_check_results` consult the helper. Crucially `run()` no longer marks `reported=True` in `--dry-run` mode (the populate step in `post_earnings_check.yml` was previously poisoning the well by silently flipping `reported=1` without posting). When deferral expires without ever computing a move (delisted ticker, yfinance broken), `_alert_move_unavailable()` posts a `:warning:` alert to `#status-reports` AND the inline line shows "Stock data unavailable ⚠️" — failures are visible, not silent. See `feedback_no_silent_fails.md` memory.
- **Results Slack layout (refactored 2026-05-07; subgroups expanded 2026-05-11; markers moved to line prefix 2026-05-18; ticker chip switched 2026-05-27).** Mirrors the sigma-alert `#stock-price-alerts` pattern: top-level **tier headers** (Tier 1 / Tier 2 / Tier 3 with emoji + count), **mutually-exclusive subgroup** sub-headers within each tier (priority: Portfolio > Researching > Ready to Buy > Ready to Short > Following for Interest > Healthcare Services > MedTech > Large Pharma > Other), and a **compact one-line per ticker** with three per-metric color markers (EPS · Rev · Stock) at the **start** of the line so they align in a fixed column: `` 🟩 🟩 🟥  `AAPL` Apple · EPS $X/$Y +Z% · Rev $A/$B +C% · Stock -M% (1d) ``. Ticker is rendered as a backtick-monospace chip — matches the weekly digest convention (`_row_line`) and the cross-project standard (sigma-alert switched at the same time). Markers: `🟩` beat/up, `🟥` miss/down, `⬜` N/A (EPS/Rev), `⚠️` stock-data unavailable. Marker helpers in `notifications.py`: `_beat_marker()` for EPS/Rev, `_move_marker()` for stock. Subgroups are mutually exclusive (vs sigma-alert's duplicated). `ResultRow` carries `sector`, `subsector`, `position` (populated at construction sites in `main.py` from `coverage_map`). The five Position-derived subgroups all rank above sector-derived ones, so a Following-for-Interest ticker in Tech still renders under its Position label rather than "Other". `_short_company_name()` strips corporate suffixes for compactness. S&P 500 subgroup intentionally skipped — earnings_agent doesn't load the S&P universe.
- **DB artifact is shared across three workflows.** `daily_earnings_check`, `reconcile_calendar`, `post_earnings_check` all restore/upload the `earnings-db` artifact. They share the `concurrency: group: earnings-db-writer` setting so they serialize and don't clobber each other. `weekly_digest` doesn't persist the DB so it's not in the group.
- **Schema is at v12.** `storage.py CURRENT_SCHEMA_VERSION=12`. Migrations are non-destructive. Fresh-DB `CREATE TABLE` duplicates v2–v12 columns; when adding a column, update both the migration and the fresh-DB statement. The `test_fresh_db_schema_matches_migration_path` test in `test_dedup.py` enforces this invariant. Column history: v3=`unseen_run_count`, v4=`date_locked`, v5=`last_xcheck_yf_dates`, v6=`date_confirmed`, v7=backfill for v6, v8=`announcement_url`, v9=`slack_thread_ts` + `slack_question_kind` + `slack_last_reply_ts` + `question_state` + `question_snooze_until` + `question_first_seen` + `kv_store` table, v10=`slack_channel_id`, v11=`event_hour_yf` (yfinance-inferred hour), v12=`call_datetime_utc`+`call_source` (conference-call timestamp).
- **SEC EDGAR requires contact info in User-Agent.** Default in `config.py` is `"earnings-agent (jroypeterson@gmail.com)"`. Override via `SEC_EDGAR_USER_AGENT` env var. SEC returns HTTP 403/malformed responses to generic User-Agents. Rate-limited self-throttle (~8 req/s) to stay under the 10 req/s ceiling.
- **`.ticker_cik_cache.json`** is a local 30-day cache of SEC's ticker→CIK mapping (800KB blob). Gitignored.
- **`ir_feeds.json` is empty by default.** Aggregator RSS testing showed Seeking Alpha / Nasdaq / Business Wire / PR Newswire do NOT carry company IR press releases (only analyst commentary). So `--check-announcements` silently skips any ticker without an explicit per-company IR RSS URL. To use: populate `ir_feeds.json` with `{"TICKER": "https://ir.example.com/rss"}` entries, or reply `ir <url>` in a question thread (writes to `kv_store.ir_feed:TICKER` which `_load_ir_feeds()` overlays on top of the JSON).
- **Calendar artifact persistence + branch scoping.** `dawidd6/action-download-artifact@v6` (pinned to commit SHA) used in all three uploading workflows with `search_artifacts: true`, `workflow_search: true`, and `branch: main`. The default `actions/download-artifact@v4` only sees artifacts from the same run — that bug silently produced fresh-DB runs every time and erased locks/Slack-state/dedup. Upload guarded with `if: github.ref == 'refs/heads/main'` so feature-branch runs cannot pollute the persistent artifact. `EA_DB_BOOTSTRAPPED=true` repo variable is set: missing artifact post-bootstrap = Slack alert + workflow failure (no more silent state loss).
- **Calendar lives in floridabusinessman@gmail.com now (migrated 2026-05-28).** `GOOGLE_CALENDAR_ID` points at the dedicated "Earnings Calendar" (`9e8a2c3d...@group.calendar.google.com`), split off the legacy shared `4pun7683...` calendar (which earnings_agent + analyst-days both used). Auth is unchanged (service account `earnings-agent@earnings-agent-486621.iam.gserviceaccount.com`, shared into the calendar). The old `4pun7683` calendar retains only the user's manual personal events. Updated in `.env` AND the GH Actions `GOOGLE_CALENDAR_ID` secret. See `scripts/migrate_calendar.py` (one-off `events.move` migration; note `events.move` PRESERVES event id but STRIPS extendedProperties) + `scripts/restamp_calendar_props.py` (re-stamps the props the move dropped).
- **Calendar timezone matters for drift detection.** The new calendars default to **UTC**; the legacy one was `America/New_York`. The Calendar API normalizes returned `start.dateTime` to the calendar's default tz, so a 07:00-ET (bmo) event on a UTC calendar comes back as `...T11:00:00Z`. `calendar_event_drift_kind` previously sliced `dateTime[11:16]` assuming ET → read "11:00" ≠ "07:00" → perpetual false "shape" drift → `--refresh-descriptions` delete+recreated ~90 timed events EVERY run (churn observed 140→64→96/run). Fixed via `_wall_clock_et()` in `calendar_sync.py` (converts to ET before comparing) — now TZ-agnostic. The service account is only a writer ("make changes to events"), so it CANNOT change a calendar's timezone (`calendars().patch` → 403); only the calendar owner (floridabusinessman, in the UI) can. Don't rely on being able to set it programmatically.
- **`run_refresh_descriptions` must persist the recreated event id.** On "shape" drift it delete+recreates; it now repoints `events.gcal_id` to the new event (mirrors the main-sync recreate path). Earlier it discarded the id, leaving the DB pointing at the deleted event → next sync recreated it → next refresh re-drifted: a self-sustaining churn that multiplied dupes for date-flapping tickers (ICLR). `python main.py --cleanup` is the dedup remedy (keeps newest-tagged per ticker|date).
- **Watchdog workflow** (`.github/workflows/watchdog.yml`) runs 3x/day at off-peak minutes (13:37/17:37/21:37 UTC). Queries last-success per sibling workflow via `gh run list`, alerts to `#status-reports` when stale beyond per-workflow thresholds (27h daily, 70h reconcile, 75h post-earnings, 192h weekly), and auto-dispatches a recovery run via `gh workflow run`. Per-workflow recent-failure window guards against dispatch-loops on permanently-failing workflows.
- **Cron offsets.** All crons moved off `:00` (worst window for GitHub Actions delays/skips). Daily: 11:13/19:23 UTC. Reconcile: 14:09/17:09/20:09 UTC. Post-earnings: 22:37 UTC. Weekly digest: 16:43 UTC Sun. Watchdog: 13:37/17:37/21:37 UTC.

## Slack channel routing

- **#earnings** (`SLACK_WEBHOOK_EARNINGS`, `SLACK_CHANNEL_ID`): heartbeat, weekly digest, results beat/miss alerts, urgent Tier 1 date moves within 5 biz days. The "primary feed" — actual earnings updates.
- **#status-reports** (`SLACK_WEBHOOK_STATUS`, `SLACK_STATUS_CHANNEL_ID`): date-disagreement notices — cross-check (Finnhub vs yfinance), unseen-ticker, reconcile auto-fix. Routed off the earnings channel so it stays focused. Status secrets fall back to earnings ones when unset (back-compat for setups that haven't created the second channel).

## Scheduled workflows (GitHub Actions)

| Workflow | Cron (UTC) | Local ET (EDT) | Purpose |
|---|---|---|---|
| `daily_earnings_check.yml` | `13 11 * * *` | ~7:13 AM | Full `main.py` daily sync + `--refresh-descriptions` + `--cross-check` (with EDGAR auto-correction) + `--check-ir-emails` + `--check-replies` |
| `daily_earnings_check.yml` | `23 19 * * 1-5` | ~3:23 PM (weekdays) | Afternoon redundancy — catches mid-day Finnhub updates |
| `reconcile_calendar.yml` | `9 14,17,20 * * 1-5` | ~10:09 AM / 1:09 PM / 4:09 PM | Lightweight drift auto-repair (silent unless drift found) + `--check-replies` poll |
| `weekly_digest.yml` | `43 16 * * 0` | Sunday ~12:43 PM | Weekly digest to Slack |
| `post_earnings_check.yml` | `37 22 * * 1-5` | Weekday ~6:37 PM | Results sweep (today + yesterday for AMC overnight catch-up) |
| `watchdog.yml` | `37 13,17,21 * * *` | ~9:37 AM / 1:37 PM / 5:37 PM | Detect stale sibling workflows + auto-dispatch recovery |

All crons offset off `:00` to avoid GitHub Actions top-of-hour delays/skips.

All workflows sparse-checkout `jroypeterson/Coverage-Manager/exports/` (the repo is public). All DB-writing workflows share `concurrency: group: earnings-db-writer`. Every workflow has an `if: failure()` step that Slacks the run URL on non-zero exit.

## Required secrets (GitHub Actions)

`FINNHUB_API_KEY`, `GOOGLE_CALENDAR_ID`, `GOOGLE_CREDENTIALS_JSON`, `TICKTICK_ACCESS_TOKEN`, `SLACK_WEBHOOK_EARNINGS`.

Strongly recommended: `FMP_API_KEY` (set 2026-06-04) — enables the FMP co-primary earnings merge (breadth + actuals timeliness). Same key as Coverage Manager's Starter plan. When unset the agent degrades to Finnhub-only (logged loudly). Wired into the daily-sync, post-earnings populate/check, and weekly-digest populate steps.

Optional for Slack-reply flow: `SLACK_BOT_TOKEN` (xoxb-...) + `SLACK_CHANNEL_ID` (Cxxx) for the earnings channel. Bot needs `chat:write` and `channels:history` (or `groups:history` for private channels) scopes. When unset, the agent falls back to webhook-only batched messages with no reply support.

Optional for status-reports routing: `SLACK_WEBHOOK_STATUS` + `SLACK_STATUS_CHANNEL_ID`. Cross-check / unseen / reconcile alerts post here instead of #earnings. Bot must be a member of #status-reports for threaded replies to work. Falls back to the earnings webhook/channel when unset.

Optional for Gmail IR-alert scanning: `GMAIL_TOKEN_JSON` + `GMAIL_CLIENT_CREDENTIALS_JSON` (both set as of 2026-05-05). Token authed against `floridabusinessman@gmail.com` with `gmail.readonly` scope. OAuth client reused from the daily-reads project (same Google Cloud project). When unset, `--check-ir-emails` step prints "GMAIL_TOKEN_JSON secret not set — skipping IR-email scan" and exits 0. Generated locally via `scripts/authorize_gmail.py`. See `gmail_client.py`.

## Required repo variables

- `EA_DB_BOOTSTRAPPED=true` (set 2026-05-04). Activates the post-bootstrap fail-loud check on the `earnings-db` artifact restore in all three uploading workflows. Missing artifact = Slack alert + workflow failure rather than a silent fresh-DB run that loses locks/Slack-state/dedup.

## Local `.env`

Same keys as above (minus the JSON-blob form of Google creds — local uses the `credentials.json` file path) plus `COVERAGE_MANAGER_PATH`, `EMAIL_TO`, and optionally `SEC_EDGAR_USER_AGENT` (overrides the hardcoded default contact info).

## Module map

- `main.py` — CLI entry + all top-level flows (`run`, `run_reconcile_calendar`, `run_check_results`, `run_cross_check`, `run_check_announcements`, `run_check_ir_emails`, `run_refresh_descriptions`, `_apply_edgar_auto_correction`, lock management).
- `finnhub_client.py` — Finnhub earnings calendar with adaptive chunk splitting + fail-fast.
- `storage.py` — SQLite schema (v12) + non-destructive migrations + upsert/lock helpers.
- `calendar_sync.py` — Google Calendar CRUD, deduplication, description rendering (Press release / Conference call lines), drift detection (`calendar_event_drift_kind` returns 'fresh'/'text'/'shape').
- `coverage.py` — loads ticker universe from Coverage Manager exports + freshness check (`compute_coverage_freshness`, alerts on staleness >7d).
- `digest.py`, `notifications.py` — weekly digest + Slack Block Kit builders (cross-check, reconcile, unseen, urgent, heartbeat). Cross-check verdict logic includes EDGAR auto-correction recognition + split-day pattern downgrade.
- `ticktick.py` — TickTick task CRUD.
- `market_data.py` — yfinance wrappers. `Ticker.info`-based earnings timestamps (release + call) preserve time-of-day; `Ticker.calendar` strips it. Includes `fetch_yfinance_earnings_timestamps`, `fetch_yfinance_hour_for_date`, `fetch_yfinance_call_for_date`, `infer_hour_from_datetime` (NYSE session boundaries).
- `edgar_client.py` — SEC EDGAR 8-K fetcher + prior-year same-quarter cadence inference + `find_earnings_release_filing()` for Item 2.02 post-hoc tiebreaker.
- `rss_client.py` — RSS/Atom parser + conservative announcement detector for IR feeds. `_load_ir_feeds()` reads `ir_feeds.json` (committed defaults) and overlays `kv_store.ir_feed:*` (mutable via Slack reply). Regex constants reused by `gmail_client.py`.
- `gmail_client.py` — Gmail API wrapper for IR-alert scanning. `get_gmail_service` (auto-refreshes token), `list_message_ids`, `get_message` (HTML stripped), `detect_earnings_announcement` (delegates regex to rss_client), `is_known_ir_sender`. Reads from `gmail_token.json`.
- `scripts/authorize_gmail.py` — one-shot OAuth flow producing `gmail_token.json`. Reuses daily-reads OAuth client (`gmail_client_credentials.json` copied from `C:/Users/jroyp/Dropbox/API Keys/gmail_oauth_client.json`).
- `slack_api.py` — Slack Web API client (bot-token): `chat.postMessage`, `conversations.replies`. Used by the threaded question flow.
- `slack_replies.py` — reply-command parser + help/status text. `parse_reply(text, ctx) -> ParsedAction`. Caller dispatches the action via `_apply_action` in `main.py`.
