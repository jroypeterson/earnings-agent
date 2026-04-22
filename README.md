# Earnings Intelligence System

An earnings review execution system that monitors upcoming earnings, syncs to Google Calendar, creates review tasks in TickTick, and tracks your coverage workflow. Powered by Finnhub, Coverage Manager, and the TickTick API.

## What It Does

- Reads your ticker universe and tiers from **Coverage Manager** (Core Watchlist / HC Services & MedTech / Other)
- Queries Finnhub daily for upcoming earnings dates, timing (BMO/AMC), and consensus estimates
- Creates **Google Calendar** events for Tier 1 + Tier 2 names (with deduplication)
- Labels every event as **Confirmed vs Estimated** based on whether the company has announced timing
- Creates **TickTick** review tasks in quarterly lists (e.g. "1Q26 Earnings - Core Watchlist")
- Stores consensus estimate snapshots for building revision trends over time
- Tracks actuals (beat/miss) after earnings are reported and updates calendar events
- **Multi-source date correctness**: cross-checks Finnhub against yfinance + SEC EDGAR historical cadence, scans IR RSS feeds for pre-release announcements, alerts on Tier 1 moves within 5 business days
- Human override via `--lock TICKER:DATE` when Finnhub is wrong and IR page is verified
- Supports dry-run, backfill, cleanup, and reconcile modes

## Architecture

```
Coverage Manager (tickers + tiers)
        ↓
Earnings Agent (collect, enrich, sync)
        ↓ outputs to:
        ├── Google Calendar (Tier 1+2 events)
        ├── TickTick (quarterly review task lists)
        └── SQLite (workflow state + historical memory)
```

**Three systems of record:**
- **Coverage Manager** — source of truth for universe and tier classifications
- **Google Calendar** — durable source of truth for published event state
- **SQLite** — source of truth for workflow state, estimate history, and predictions

## Service Tiers

| Tier | Source | Calendar | TickTick | Notifications |
|------|--------|----------|----------|---------------|
| **Tier 1** (Core Watchlist) | `watchlist.csv` Core=Y | Yes | Yes (with model update checklist) | Full (planned) |
| **Tier 2** (HC Svcs + MedTech) | `universe_metadata.json` sector filter | Yes | Yes | Standard (planned) |
| **Tier 3** (Other) | Remainder | No | No | Digest mention only (planned) |

## Quick Start

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your values (see .env.example for all options)
```

Required:
- `FINNHUB_API_KEY` — free at https://finnhub.io/register
- `GOOGLE_CALENDAR_ID` — your calendar ID
- `GOOGLE_CREDENTIALS_PATH` — path to service account JSON
- `COVERAGE_MANAGER_PATH` — path to Coverage Manager project (for tickers + tiers)

Optional:
- `TICKTICK_ACCESS_TOKEN` — enables TickTick task creation
- `TIMEZONE` — defaults to America/New_York

### 2. Install & Run

```bash
pip install -r requirements.txt

# Preview what would happen (no side effects)
python main.py --dry-run

# Full sync: calendar events + TickTick tasks + estimate snapshots
python main.py

# Check past 30 days for missed earnings
python main.py --backfill

# Skip TickTick task creation
python main.py --no-ticktick

# View your TickTick earnings review queue
python main.py --ticktick-status

# Clean up duplicate calendar events
python main.py --cleanup
```

## Deploy on GitHub Actions

Four workflows ship in `.github/workflows/`:

| Workflow | Cron (UTC) | Local ET (EDT) | Purpose |
|---|---|---|---|
| `daily_earnings_check.yml` | `0 11 * * *` | ~7 AM | Full daily sync + B1 cross-check |
| `daily_earnings_check.yml` | `0 19 * * 1-5` | ~3 PM weekdays | Afternoon redundancy for mid-day Finnhub changes |
| `reconcile_calendar.yml` | `0 14,17,20 * * 1-5` | ~10 AM / 1 PM / 4 PM | Lightweight drift auto-repair |
| `weekly_digest.yml` | `0 16 * * 0` | Sunday ~12 PM | Weekly digest to Slack |
| `post_earnings_check.yml` | `0 22 * * 1-5` | Weekday ~6 PM | Results sweep + AMC overnight catch-up |

All workflows clone the public [Coverage-Manager](https://github.com/jroypeterson/Coverage-Manager) repo (sparse checkout of `exports/`). DB-writing workflows share `concurrency: earnings-db-writer` so they serialize on the shared `earnings-db` artifact. Every workflow has an `if: failure()` step that posts to Slack on non-zero exit.

In your repo → Settings → Secrets and variables → Actions, add:

**Secrets** (encrypted):
| Name | Used by | Value |
|------|---------|-------|
| `FINNHUB_API_KEY` | all | Your Finnhub API key |
| `GOOGLE_CALENDAR_ID` | all | Your calendar ID |
| `GOOGLE_CREDENTIALS_JSON` | all | Entire contents of `credentials.json` |
| `TICKTICK_ACCESS_TOKEN` | daily | TickTick OAuth token (optional) |
| `SLACK_WEBHOOK_EARNINGS` | all | Slack incoming webhook for `#earnings` |

Then go to repo → Actions tab → enable workflows.

> **Email drafts** are not created in CI (they need Gmail MCP, which runs locally). The weekly workflow uploads `last_digest.html` as an artifact so you can paste-to-email if you want. For a Gmail draft, run `python main.py --weekly-digest` locally and use the Gmail MCP draft flow.

## Local scheduling (alternative)

If you'd rather run the weekly digest from your local machine, `weekly_digest.bat` wraps `python main.py --weekly-digest` for Windows Task Scheduler. Setup:

1. Task Scheduler → **Create Basic Task**
2. Trigger: Weekly, Sundays at 12:00 PM
3. Action: Start a program → `weekly_digest.bat` in this directory

Logs land in `logs\weekly_digest_YYYYMMDD.log`.
## Project Structure

```
earnings_agent/
├── main.py              # CLI entry point + all top-level flows
├── config.py            # Environment, paths, constants
├── coverage.py          # Coverage Manager integration, tier resolution
├── storage.py           # SQLite schema (v8), non-destructive migrations
├── finnhub_client.py    # Finnhub API with adaptive chunk splitting + fail-fast
├── calendar_sync.py     # Google Calendar CRUD + dedup + confirmed/est rendering
├── edgar_client.py      # SEC EDGAR 8-K fetcher + cadence inference
├── rss_client.py        # RSS/Atom parser + IR announcement detector
├── market_data.py       # yfinance wrappers (YTD, post-earnings move, calendar)
├── ticktick.py          # TickTick list/task management
├── digest.py            # Weekly digest query + grouping + clustering
├── notifications.py     # Slack Block Kit builders for every alert surface
├── weekly_digest.bat    # Windows Task Scheduler wrapper for the weekly digest
├── earnings_agent.py    # Legacy entry point (delegates to main.py)
├── ir_feeds.json        # Per-ticker IR RSS URL mapping (for --check-announcements)
├── test_dedup.py        # Test suite
├── requirements.txt
├── .env.example
├── PLAN.md              # Detailed implementation plan (7 phases)
└── .github/workflows/
    ├── daily_earnings_check.yml
    ├── reconcile_calendar.yml
    ├── post_earnings_check.yml
    └── weekly_digest.yml
```

## TickTick Integration

Tasks are organized by **reporting quarter** and **tier**:
- `1Q26 Earnings - Core Watchlist` (Tier 1 names)
- `1Q26 Earnings - HC Svcs & MedTech` (Tier 2 names)

Each task includes consensus estimates, timing, and a review checklist (transcript, company docs, sell-side take, update model). Tasks are auto-created in the "Earnings / Analysis" folder.

The TickTick access token expires ~every 180 days. When it expires, the system detects the 401 and logs instructions to re-authenticate.

## Planned Features

See `PLAN.md` for the full 7-phase roadmap. Completed:
- [x] Phase 1: Foundation (modularize, Coverage Manager sync, retry logic)
- [x] Phase 2: TickTick integration (quarterly lists, review tasks)
- [x] Phase 3: Weekly digest (Slack + Gmail MCP draft)
- [x] Phase 4: Post-earnings alerts (T+0 Slack + TickTick close-loop)
- [x] Phase 4.5: Date-correctness hardening (A1 adaptive chunks + fail-fast, A2 reconcile, A3 urgent alert, B1 yfinance cross-check + EDGAR cadence, B2 unseen-ticker alert, D2 lock override, confirmed/estimated flag, IR RSS announcement scanner). See `CLAUDE.md` for architecture.
- [ ] Phase 5: Pre-earnings briefs (T-1 enriched context)
- [ ] Phase 6: Prediction tracking + accuracy analysis
- [ ] Phase 7: Reconcile mode + hardening (partial — reconcile job + failure alerts done; SLO tracking + event colors remaining)
