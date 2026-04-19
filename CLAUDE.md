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
python main.py --weekly-digest     # Post weekly digest to Slack + write last_digest.html/.txt
python main.py --check-results [--date YYYY-MM-DD]  # Detect newly-reported actuals, post to Slack
python main.py --ticktick-status   # Show TickTick review queue
python main.py --no-ticktick       # Skip TickTick during --sync
```

## Tier semantics

- **Tier 1** (Core Watchlist, ~22) — watchlist.csv where `Core=Y`. Gets Calendar events, TickTick tasks, full digest detail.
- **Tier 2** (~188) — universe in `Healthcare Services` or `MedTech` sectors. Gets Calendar + TickTick.
- **Tier 3** (~880) — everything else. No Calendar, no TickTick. Shows in digest with YTD + timing.

## Gotchas

- **`fetch_earnings` skips single-day ranges** (`while start < end` — if `from == to`, loop never runs). `run_check_results` works around this by passing `target → target+1` and filtering client-side.
- **Slack has no native underline.** `notifications._underline()` uses U+0332 combining low line. Works in most clients; if it breaks, fall back to `*━━ Day ━━*`.
- **Gmail send is MCP-only.** CI cannot create Gmail drafts — `--weekly-digest` in CI just uploads `last_digest.html` as an artifact; email draft happens when a human Claude session runs the MCP `create_draft` tool.
- **`PA` sector = Personal Account** (uncategorized tickers the user follows). These fall to Tier 3 — not noise, intentional follows.
- **TickTick list naming uses *reporting quarter*, not release-date quarter.** April releases land in `1Q26 Earnings - *`, not `2Q26`. `_reporting_quarter()` in `ticktick.py`.
- **Idempotent result detection.** `run_check_results` skips events already marked `reported=1`. DB update happens *after* Slack post succeeds, so a Slack failure leaves records unmarked for the next run to retry.
- **`run()` shares `notify_results()` with `run_check_results`** — the 6 AM daily sync also posts Slack alerts when it detects overnight AMC actuals. Don't re-post from a separate path.

## Scheduled workflows (GitHub Actions)

| Workflow | Cron (UTC) | Local ET | Purpose |
|---|---|---|---|
| `daily_earnings_check.yml` | `0 11 * * *` | ~6-7 AM | Full `main.py` daily sync |
| `weekly_digest.yml` | `0 16 * * 0` | Sunday ~noon | Weekly digest to Slack |
| `post_earnings_check.yml` | `0 22 * * 1-5` | Weekday ~5-6 PM | Results sweep (today + yesterday for AMC overnight catch-up) |

All three sparse-checkout `jroypeterson/Coverage-Manager/exports/` (the repo is public).

## Required secrets (GitHub Actions)

`FINNHUB_API_KEY`, `GOOGLE_CALENDAR_ID`, `GOOGLE_CREDENTIALS_JSON`, `TICKTICK_ACCESS_TOKEN`, `SLACK_WEBHOOK_EARNINGS`.

## Local `.env`

Same keys as above (minus the JSON-blob form of Google creds — local uses the `credentials.json` file path) plus `COVERAGE_MANAGER_PATH`, `EMAIL_TO`.
