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
python main.py --refresh-descriptions # One-shot: rewrite title + description for all tagged upcoming calendar events
python main.py --lock TICKER:YYYY-MM-DD     # Pin a date so sync/reconcile won't move it
python main.py --unlock TICKER:YYYY-MM-DD   # Remove a lock
python main.py --list-locks        # Show currently-locked events
python main.py --weekly-digest     # Post weekly digest to Slack + write last_digest.html/.txt
python main.py --check-results [--date YYYY-MM-DD]  # Detect newly-reported actuals, post to Slack
python main.py --ticktick-status   # Show TickTick review queue
python main.py --no-ticktick       # Skip TickTick during --sync
python main.py --no-heartbeat      # Skip Slack success heartbeat at end of run
```

## Tier semantics

- **Tier 1** (Core Watchlist, ~22) — watchlist.csv where `Core=Y`. Gets Calendar events, TickTick tasks, full digest detail.
- **Tier 2** (~188) — universe in `Healthcare Services` or `MedTech` sectors. Gets Calendar + TickTick.
- **Tier 3** (~880) — everything else. No Calendar, no TickTick. Shows in digest with YTD + timing.

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

## Gotchas

- **`fetch_earnings` skips single-day ranges** (`while start < end` — if `from == to`, loop never runs). `run_check_results` works around this by passing `target → target+1` and filtering client-side.
- **Slack has no native underline.** `notifications._underline()` uses U+0332 combining low line. Works in most clients; if it breaks, fall back to `*━━ Day ━━*`.
- **Gmail send is MCP-only.** CI cannot create Gmail drafts — `--weekly-digest` in CI just uploads `last_digest.html` as an artifact; email draft happens when a human Claude session runs the MCP `create_draft` tool.
- **`Other` sector catches uncategorized tickers** the user follows but hasn't assigned a formal sector to. These fall to Tier 3. (Previous "PA" code was retired in Coverage Manager schema v2 on 2026-04-17 — collapsed into "Other".)
- **TickTick list naming uses *reporting quarter*, not release-date quarter.** April releases land in `1Q26 Earnings - *`, not `2Q26`. `_reporting_quarter()` in `ticktick.py`.
- **Idempotent result detection.** `run_check_results` skips events already marked `reported=1`. DB update happens *after* Slack post succeeds, so a Slack failure leaves records unmarked for the next run to retry.
- **`run()` shares `notify_results()` with `run_check_results`** — the 6 AM daily sync also posts Slack alerts when it detects overnight AMC actuals. Don't re-post from a separate path.
- **DB artifact is shared across three workflows.** `daily_earnings_check`, `reconcile_calendar`, `post_earnings_check` all restore/upload the `earnings-db` artifact. They share the `concurrency: group: earnings-db-writer` setting so they serialize and don't clobber each other. `weekly_digest` doesn't persist the DB so it's not in the group.
- **Schema is at v8.** `storage.py CURRENT_SCHEMA_VERSION=8`. Migrations are non-destructive. Fresh-DB `CREATE TABLE` duplicates v2–v8 columns; when adding a column, update both the migration and the fresh-DB statement. Column history: v3=`unseen_run_count`, v4=`date_locked`, v5=`last_xcheck_yf_dates`, v6=`date_confirmed`, v7=backfill for v6, v8=`announcement_url`.
- **SEC EDGAR requires contact info in User-Agent.** Default in `config.py` is `"earnings-agent (jroypeterson@gmail.com)"`. Override via `SEC_EDGAR_USER_AGENT` env var. SEC returns HTTP 403/malformed responses to generic User-Agents. Rate-limited self-throttle (~8 req/s) to stay under the 10 req/s ceiling.
- **`.ticker_cik_cache.json`** is a local 30-day cache of SEC's ticker→CIK mapping (800KB blob). Gitignored.
- **`ir_feeds.json` is empty by default.** Aggregator RSS testing showed Seeking Alpha / Nasdaq / Business Wire / PR Newswire do NOT carry company IR press releases (only analyst commentary). So `--check-announcements` silently skips any ticker without an explicit per-company IR RSS URL. To use: populate `ir_feeds.json` with `{"TICKER": "https://ir.example.com/rss"}` entries.

## Scheduled workflows (GitHub Actions)

| Workflow | Cron (UTC) | Local ET (EDT) | Purpose |
|---|---|---|---|
| `daily_earnings_check.yml` | `0 11 * * *` | ~7 AM | Full `main.py` daily sync + `--cross-check` |
| `daily_earnings_check.yml` | `0 19 * * 1-5` | ~3 PM (weekdays) | Afternoon redundancy — catches mid-day Finnhub updates |
| `reconcile_calendar.yml` | `0 14,17,20 * * 1-5` | ~10 AM / 1 PM / 4 PM | Lightweight drift auto-repair (silent unless drift found) |
| `weekly_digest.yml` | `0 16 * * 0` | Sunday ~12 PM | Weekly digest to Slack |
| `post_earnings_check.yml` | `0 22 * * 1-5` | Weekday ~6 PM | Results sweep (today + yesterday for AMC overnight catch-up) |

All workflows sparse-checkout `jroypeterson/Coverage-Manager/exports/` (the repo is public). All DB-writing workflows share `concurrency: group: earnings-db-writer`. Every workflow has an `if: failure()` step that Slacks the run URL on non-zero exit.

## Required secrets (GitHub Actions)

`FINNHUB_API_KEY`, `GOOGLE_CALENDAR_ID`, `GOOGLE_CREDENTIALS_JSON`, `TICKTICK_ACCESS_TOKEN`, `SLACK_WEBHOOK_EARNINGS`.

## Local `.env`

Same keys as above (minus the JSON-blob form of Google creds — local uses the `credentials.json` file path) plus `COVERAGE_MANAGER_PATH`, `EMAIL_TO`, and optionally `SEC_EDGAR_USER_AGENT` (overrides the hardcoded default contact info).

## Module map

- `main.py` — CLI entry + all top-level flows (`run`, `run_reconcile_calendar`, `run_check_results`, `run_cross_check`, `run_check_announcements`, `run_refresh_descriptions`, lock management).
- `finnhub_client.py` — Finnhub earnings calendar with adaptive chunk splitting + fail-fast.
- `storage.py` — SQLite schema + non-destructive migrations (v8) + upsert/lock helpers.
- `calendar_sync.py` — Google Calendar CRUD, deduplication, description rendering (confirmed/estimated status).
- `coverage.py` — loads ticker universe from Coverage Manager exports.
- `digest.py`, `notifications.py` — weekly digest + Slack Block Kit builders (cross-check, reconcile, unseen, urgent, heartbeat).
- `ticktick.py` — TickTick task CRUD.
- `market_data.py` — yfinance wrappers (YTD, post-earnings move, cross-check earnings date).
- `edgar_client.py` — SEC EDGAR 8-K fetcher + prior-year same-quarter cadence inference.
- `rss_client.py` — RSS/Atom parser + conservative announcement detector for IR feeds.
