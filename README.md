# Earnings Intelligence System

An earnings review execution system that monitors upcoming earnings, syncs to Google Calendar, creates review tasks in TickTick, and tracks your coverage workflow. Powered by Finnhub, Coverage Manager, and the TickTick API.

## What It Does

- Reads your ticker universe and tiers from **Coverage Manager** (Core Watchlist / HC Services & MedTech / Other)
- Queries Finnhub daily for upcoming earnings dates, timing (BMO/AMC), and consensus estimates
- Creates **Google Calendar** events for Tier 1 + Tier 2 names (with deduplication)
- Creates **TickTick** review tasks in quarterly lists (e.g. "1Q26 Earnings - Core Watchlist")
- Stores consensus estimate snapshots for building revision trends over time
- Tracks actuals (beat/miss) after earnings are reported and updates calendar events
- Supports dry-run, backfill, and cleanup modes

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

## Deploy on GitHub Actions (Free Daily Runs)

This is the recommended way to run the agent automatically.

In your repo → Settings → Secrets and variables → Actions, add:

**Secrets** (encrypted):
| Name | Value |
|------|-------|
| `FINNHUB_API_KEY` | Your Finnhub API key |
| `GOOGLE_CALENDAR_ID` | Your calendar ID |
| `GOOGLE_CREDENTIALS_JSON` | Entire contents of your `credentials.json` file |
| `TICKTICK_ACCESS_TOKEN` | TickTick OAuth token (optional) |

Go to repo → Actions tab → enable workflows. The agent will run daily at ~6 AM ET.
## Project Structure

```
earnings_agent/
├── main.py              # CLI entry point and orchestrator
├── config.py            # Environment, paths, constants
├── coverage.py          # Coverage Manager integration, tier resolution
├── storage.py           # SQLite schema, non-destructive migrations
├── finnhub_client.py    # Finnhub API with retry + exponential backoff
├── calendar_sync.py     # Google Calendar operations with pagination
├── ticktick.py          # TickTick list/task management
├── earnings_agent.py    # Legacy entry point (delegates to main.py)
├── test_dedup.py        # Test suite (13 tests)
├── requirements.txt
├── .env.example
├── PLAN.md              # Detailed implementation plan (7 phases)
└── .github/workflows/
    └── daily_earnings_check.yml
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
- [ ] Phase 3: Weekly digest (Slack + email)
- [ ] Phase 4: Post-earnings alerts (T+0 beat/miss, T+1 close-loop)
- [ ] Phase 5: Pre-earnings briefs (T-1 enriched context)
- [ ] Phase 6: Prediction tracking + accuracy analysis
- [ ] Phase 7: Reconcile mode + hardening
