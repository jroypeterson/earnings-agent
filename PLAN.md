# Earnings Intelligence System — Implementation Plan

## Document Purpose

This is a detailed implementation plan for transforming the earnings_agent from a calendar-sync utility into a multi-surface earnings intelligence workflow. It is written to be self-contained — an engineer or AI reviewer should be able to understand the goals, constraints, architecture, data sources, and sequencing without additional context.

---

## 1. Context & Current State

### What exists today

A single-file Python script (`earnings_agent.py`, ~884 lines) that:
- Reads a watchlist of ~110 stock tickers from `tickers.txt`
- Queries Finnhub's free API for upcoming earnings dates, timing (pre/post market), and consensus EPS/revenue estimates
- Creates Google Calendar events with deduplication (SQLite DB + Google Calendar extendedProperties as fallback)
- Supports `--backfill` (past 30 days) and `--cleanup` (remove duplicate events)
- Tracks actuals (beat/miss) after earnings are reported
- Runs daily via GitHub Actions or Windows Task Scheduler

### What works well
- The dedup/backfill logic is solid
- Google Calendar extendedProperties for sync metadata is the right approach
- The script is pragmatic and automates quickly

### What's broken or missing
- **Identity model**: Fiscal quarter derived from calendar month — wrong for off-cycle fiscal years (Apple Q1 = Oct-Dec)
- **Destructive migration**: Schema changes drop the entire table, losing all history
- **Monolithic structure**: Config, DB, Finnhub client, Calendar client, dedup, and CLI all in one file
- **No multi-surface delivery**: Only outputs to Google Calendar
- **No earnings preparation workflow**: No pre-earnings briefs, no post-earnings alerts
- **No historical analysis**: Beat/miss data collected but never analyzed
- **Silent failures**: Generic `except Exception` handlers mask real errors
- **No retry logic**: A single transient API failure loses data for that run

### User's broader ecosystem

The user runs an integrated investment research system across ~10 Claude Code projects:

| Project | Role | Relevant to earnings |
|---------|------|---------------------|
| **Coverage Manager** | Source of truth for ticker universe, tiers, metadata | Provides tickers + tier classification |
| **sigma-alert** | Z-score price alerts to Slack | Could consume post-earnings moves |
| **13F Analyzer** | Quarterly institutional holdings tracking | Orthogonal |
| **forensic_triage** | Accounting red-flag screening | Could flag pre-earnings |
| **daily-reads** | Newsletter triage → TickTick tasks + Slack | Pattern for TickTick + Slack integration |
| **company-research-agent** | SEC filing + transcript aggregation | Not yet reliable enough to depend on |

**Key architectural principle**: Coverage Manager is the system of record for *what names to track and at what priority*. Earnings Agent should inherit from it, not maintain its own ticker list.

---

## 2. Goals (User-Prioritized)

### Primary
1. **Never miss an important earnings event** for names the user cares about
2. **Be prepared for earnings** — context to act on, not just a calendar reminder
3. **See a concise post-earnings result and reaction summary** (T+0 / T+1)
4. **Build a historical record of prediction accuracy** — detect biases by name, sector, or estimate type
5. **Track which earnings the user has manually reviewed** — via TickTick task lists by quarter
6. **Flag earnings outcomes for thesis monitoring** — output that a future thesis-monitoring project can consume

### Secondary
7. Know the expectation setup before the event (estimate revisions, price momentum)
8. Be alerted when materially important things change (guidance revisions, date changes)
9. Organize resources for manual earnings review (transcripts, company materials, sell-side takes)

---

## 3. Service Tiers

The system treats different stocks differently based on Coverage Manager classifications:

| Tier | Coverage Manager Source | Calendar Events | T-7 Digest | T-1 Brief | T+0 Alert | T+1 Summary | TickTick Task | Prediction Tracking | Materials Gathering |
|------|------------------------|----------------|------------|-----------|-----------|-------------|--------------|--------------------|--------------------|
| **Tier 1** (Core Watchlist) | `watchlist.csv` | Yes (colored) | Yes (detailed) | Yes (full) | Yes (Slack + Email) | Yes | Yes | Yes | Yes |
| **Tier 2** (HC Services + MedTech) | `universe_metadata.json` sector filter | Yes | Yes (standard) | Yes (standard) | Yes (Slack + Email) | Yes | Yes | Yes | No |
| **Tier 3** (Other/PA + S&P 500) | Remainder of universe | No | Digest mention only | No | No | No | No | No | No |

**Tier 1** = 20-30 names the user owns or is looking to buy. These get the full workflow.
**Tier 2** = Healthcare Services and MedTech sector names — core coverage but not positions.
**Tier 3** = Everything else. Mentioned in the weekly digest for awareness, but no calendar clutter or notifications.

---

## 4. Delivery Surfaces

| Surface | What | When | Infrastructure |
|---------|------|------|---------------|
| **Google Calendar** | Earnings events with timing, estimates, links | Created T-30+, updated as data changes | Existing (service account) |
| **Slack `#earnings`** | T-7 digest, T-1 briefs, T+0 alerts, T+1 summaries | Multiple touchpoints per earnings cycle | New webhook (same pattern as sigma-alert) |
| **Email** | T-7 weekly digest, T-1 briefs, T+0 alerts | Same content as Slack, different surface | Gmail API (same pattern as 13F Analyzer) |
| **TickTick** | Per-company review tasks in quarterly lists (e.g. "1Q26 Earnings") | Created when earnings are confirmed | Existing API pattern from daily-reads |
| **SQLite / CSV** | Historical predictions, accuracy records, earnings outcomes | Persistent local store | Existing + new tables |

---

## 5. Data Sources & Capabilities

### Available and sufficient

| Source | What it provides for this project | Tier | Rate limits |
|--------|----------------------------------|------|-------------|
| **Finnhub** (free) | Earnings calendar, timing (BMO/AMC), consensus EPS/revenue estimates, actuals post-report | Free | 60 req/min |
| **yfinance** (free) | Historical stock prices (for T-1/T-7/T-30 performance, post-earnings reaction), options chains (for implied move calculation) | Free | ~2k req/day |
| **Edgar Tools MCP** (paid, $25/mo) | Recent 8-K filings, financial statements, company briefs, disclosure search — useful for pre-earnings context and transcript access | Pro | Per-session |
| **Coverage Manager exports** | `universe.csv`, `watchlist.csv`, `universe_metadata.json` — ticker lists, tiers, sectors, subsectors | Local files | N/A |
| **Tavily** (configured) | Web search for earnings transcripts, press releases, sell-side commentary | Configured | Needs auth |
| **Gmail MCP** | Send email digests and briefs | Configured | N/A |
| **Slack webhooks** | Post formatted messages to channels | Existing pattern | N/A |
| **TickTick API** | Create tasks and potentially lists | Existing pattern | Undocumented |

### Not available (gaps)

| Data need | Status | Workaround |
|-----------|--------|-----------|
| **Earnings estimate revision history** (30/60/90 day consensus trend) | FMP Starter tier doesn't include it. Finnhub free doesn't have it. | Build our own revision history by storing each run's consensus values in SQLite. After ~30-90 days of operation, we'll have revision trend data. This is the pragmatic path — no new API cost. |
| **After-hours price data** (real-time) | No current source provides real-time AH quotes | yfinance can get post-market prices with a delay (30-60 min acceptable per user). Run T+0 check on a delay. |
| **Options implied move** | yfinance has options chains (unused currently) | Calculate implied move from at-the-money straddle price. Free, available, just needs implementation. Flag as future enhancement if implementation is complex. |
| **Fiscal quarter identifier** | Finnhub doesn't return which fiscal quarter earnings belong to | Store richer fingerprint: ticker + earnings_date. Build a lookup from historical patterns. Accept that some edge cases (off-cycle fiscal years) need manual correction or a secondary source. |
| **Earnings transcripts** | No API source in current stack | Edgar Tools MCP can search 8-K filings. Tavily web search can find transcript links. For Tier 1 names, attempt to gather links; don't block on failure. Seeking Alpha has transcripts but no API. |

### Recommendation: No new paid APIs needed now

The user's existing stack covers the core workflow. The main gap (estimate revision history) can be solved by accumulating our own data over time. Options IV is available via yfinance if we want it. After-hours prices work with a delay. If the user later wants real-time AH data or professional-grade estimate revisions, the best options would be:
- **Polygon.io** ($29/mo Starter) — real-time + after-hours quotes, options data
- **Quandl/Nasdaq Data Link** (~$50/mo) — estimate revision history
- **S&P Global** (already configured, needs auth) — comprehensive but likely expensive

These are future enhancements, not blockers.

---

## 6. Architecture

### Design Principles

1. **Coverage Manager is the source of truth** for tickers and tiers. Earnings Agent reads from its exports, never maintains its own ticker list.
2. **Three systems of record, each for a different concern:**
   - **Coverage Manager** is the source of truth for the universe and tier classifications
   - **Google Calendar** is the durable source of truth for published event state (dates, timing, estimates). Calendar extendedProperties make events recoverable even if the local DB is lost.
   - **SQLite** is the source of truth for workflow state and historical memory — predictions, review completion, estimate snapshots, enrichment history, and reporting. Calendar cannot store this data.
3. **Three surface types serve different purposes:**
   - **Awareness surfaces** (Google Calendar, Slack, Email) answer "when is this happening?" and "what happened?"
   - **Execution surface** (TickTick) answers "what do I need to do about it?" — this is where earnings review work gets tracked and completed
   - **Memory layer** (SQLite) ties the surfaces together and enables historical analysis
4. **Collection is separate from delivery.** Earnings data is collected and normalized first, then published to multiple surfaces. Adding a new surface should not require changing collection logic.
5. **Best-effort enrichment.** Missing data (no transcript link, no options IV, no revision history) never blocks core functionality. Events get created, alerts get sent — enrichment is additive.
6. **Tier-aware processing.** Every function that iterates over tickers should respect tier classification and adjust its behavior (depth of analysis, which surfaces to publish to).

### Module Structure

```
earnings_agent/
├── main.py                    # CLI entry point (argparse, mode dispatch)
├── config.py                  # Environment loading, tier definitions, paths
├── storage.py                 # SQLite schema, migrations (non-destructive), queries
├── finnhub_client.py          # Finnhub API: calendar, estimates, actuals
├── market_data.py             # yfinance: price history, post-earnings reaction, options IV
├── calendar_sync.py           # Google Calendar: create, update, dedup, reconcile
├── enrichment.py              # Pre-earnings brief assembly, materials gathering
├── notifications.py           # Slack + Email delivery (Block Kit formatting, Gmail API)
├── ticktick.py                # TickTick list/task management
├── predictions.py             # Prediction entry, storage, accuracy analysis
├── coverage.py                # Read Coverage Manager exports, resolve tiers
├── earnings_agent.py          # Preserved as legacy entry point (imports from main.py)
├── templates/                 # Email HTML templates (digest, brief, alert)
├── tests/
│   ├── test_storage.py        # Migration, upsert, query tests
│   ├── test_finnhub_client.py # API response parsing, chunking, error handling
│   ├── test_calendar_sync.py  # Dedup, reconcile, event update logic
│   ├── test_enrichment.py     # Brief assembly, materials gathering
│   ├── test_notifications.py  # Slack/email formatting
│   ├── test_predictions.py    # Accuracy calculation, bias detection
│   ├── test_coverage.py       # Tier resolution from Coverage Manager exports
│   └── conftest.py            # Shared fixtures (in-memory SQLite, mock API responses)
├── .github/workflows/
│   ├── daily_earnings.yml     # Daily: collect, sync calendar, T-1 briefs, T+0 alerts
│   └── weekly_digest.yml      # Sunday: T-7 digest email + Slack
├── requirements.txt
├── .env.example
└── README.md
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        COVERAGE MANAGER                             │
│   exports/universe.csv + watchlist.csv + universe_metadata.json     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ (read at startup)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      coverage.py                                    │
│   Resolves tickers → Tier 1 / Tier 2 / Tier 3                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    COLLECTION LAYER                                  │
│                                                                      │
│  finnhub_client.py ──→ earnings dates, timing, estimates, actuals   │
│  market_data.py    ──→ price performance, post-earnings reaction    │
│  enrichment.py     ──→ prior quarter context, materials links       │
│                                                                      │
│  All results normalized into EarningsEvent dataclass                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                                    │
│                                                                      │
│  storage.py (SQLite — workflow state & historical memory)            │
│  ├── events table (earnings dates, estimates, actuals, tier)        │
│  ├── estimate_history table (consensus snapshots per run)           │
│  ├── predictions table (user predictions + outcomes)                │
│  └── review_status table (TickTick task tracking)                   │
│                                                                      │
│  Google Calendar extendedProperties (published event state)         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DELIVERY LAYER (Surfaces)                         │
│                                                                      │
│  AWARENESS SURFACES:                                                │
│  calendar_sync.py  ──→ Google Calendar (Tier 1 + Tier 2)           │
│  notifications.py  ──→ Slack #earnings + Gmail (tier-aware)        │
│                                                                      │
│  EXECUTION SURFACE:                                                  │
│  ticktick.py       ──→ TickTick quarterly list (Tier 1 + Tier 2)   │
│                                                                      │
│  ANALYSIS:                                                           │
│  predictions.py    ──→ Accuracy reports (Tier 1 + Tier 2)          │
│                                                                      │
│  [Future surfaces: Notion, dashboard, thesis-monitor project]       │
└─────────────────────────────────────────────────────────────────────┘
```

### Database Schema

```sql
-- Core earnings events (local cache, recoverable from Calendar)
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL,
    event_date TEXT NOT NULL,           -- YYYY-MM-DD
    quarter TEXT,                       -- e.g. "Q1 2026" (best-effort, derived)
    source_fingerprint TEXT,            -- ticker + event_date composite for dedup
    tier INTEGER NOT NULL DEFAULT 3,    -- 1, 2, or 3
    hour TEXT,                          -- "bmo", "amc", "dmh", or NULL
    eps_estimate REAL,
    revenue_estimate REAL,
    eps_actual REAL,
    revenue_actual REAL,
    reported INTEGER DEFAULT 0,         -- 1 if actuals are in
    calendar_event_id TEXT,             -- Google Calendar event ID
    ticktick_task_id TEXT,              -- TickTick task ID
    company_name TEXT,
    ir_url TEXT,
    call_url TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(ticker, event_date)          -- dedup key: ticker + date, not ticker + quarter
);

-- Consensus estimate snapshots (for building revision history over time)
CREATE TABLE estimate_history (
    id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL,
    event_date TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,         -- when we recorded this
    eps_estimate REAL,
    revenue_estimate REAL,
    UNIQUE(ticker, event_date, snapshot_date)
);

-- User predictions (future: structured input via Slack or CLI)
CREATE TABLE predictions (
    id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL,
    event_date TEXT NOT NULL,
    predicted_direction TEXT,            -- "beat", "miss", "inline"
    predicted_eps REAL,                  -- future: user's EPS estimate
    predicted_revenue REAL,              -- future: user's revenue estimate
    predicted_ebitda REAL,               -- future: user's EBITDA estimate
    position_stance TEXT,                -- "long", "neutral", "short"
    thesis_note TEXT,                    -- freeform note
    prediction_date TEXT DEFAULT (datetime('now')),
    -- Outcome fields (filled after earnings)
    actual_direction TEXT,               -- "beat", "miss", "inline" (system-determined)
    was_correct INTEGER,                 -- 1 if predicted_direction == actual_direction
    post_earnings_move_pct REAL,         -- stock reaction (T+1 close vs T-1 close)
    UNIQUE(ticker, event_date)
);

-- Review tracking (synced with TickTick)
CREATE TABLE review_status (
    id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL,
    event_date TEXT NOT NULL,
    ticktick_task_id TEXT,
    reviewed INTEGER DEFAULT 0,          -- 1 when user marks complete in TickTick
    reviewed_at TEXT,
    UNIQUE(ticker, event_date)
);
```

---

## 7. Workflow Touchpoints

### T-30+: Event Discovery & Calendar Sync (Daily)

**What happens**: Daily Finnhub scan discovers upcoming earnings dates. Events are created/updated in Google Calendar for Tier 1 + Tier 2 names.

**Details**:
- Read tickers and tiers from Coverage Manager exports
- Query Finnhub earnings calendar (next 60 days, chunked into 7-day windows)
- For each earnings event:
  - Resolve tier from coverage.py
  - Skip calendar creation for Tier 3
  - Create/update Google Calendar event with extendedProperties (ticker, event_date, tier)
  - Store consensus snapshot in estimate_history table (builds revision trend over time)
  - Upsert into events table
- Calendar event colors: differentiate Tier 1 (e.g. tomato/red) vs Tier 2 (e.g. grape/purple)
- Calendar event description includes: timing, consensus EPS/revenue, prior quarter result if available

**Schedule**: Daily, 6 AM ET (pre-market). Accept DST drift (6 AM EST / 7 AM EDT) or use Windows Task Scheduler with timezone-aware scheduling.

### T-7: Weekly Digest (Sunday)

**What happens**: Email + Slack digest summarizing the upcoming earnings week.

**Content structure**:

```
EARNINGS WEEK AHEAD — Apr 21-25, 2026
══════════════════════════════════════

THIS WEEK — TIER 1 (Core Watchlist)
───────────────────────────────────
  Mon Apr 21 (BMO): UNH — EPS est $7.14 | Rev est $109.2B
    • Last Q: Beat EPS by 3.2%, revenue inline
    • Stock: +8% since last earnings, -2% last 30d
    • Revision trend: EPS est down 1.4% over 30d [if available]

  Wed Apr 23 (AMC): ISRG — EPS est $1.82 | Rev est $2.3B
    ...

THIS WEEK — TIER 2 (HC Services + MedTech)
──────────────────────────────────────────
  [Same format, slightly less detail]

THIS WEEK — TIER 3 (Other Names Reporting)
──────────────────────────────────────────
  [Ticker list only, no detail: MSFT, GOOG, META, ...]

UPCOMING — Next 30 Days
───────────────────────
  [Summary counts by week + notable Tier 1/2 names]

SECTOR CLUSTERING
─────────────────
  "5 HC Services names report this week (UNH, CI, HUM, ELV, CNC)"
  "Peak earnings week: 23 of your 110 names report"
```

**Delivery**: Slack `#earnings` + Email (Gmail API send to user)
**Schedule**: Sunday 12 PM ET via dedicated GitHub Actions workflow or Windows Task Scheduler

### T-1: Pre-Earnings Brief (Day Before)

**What happens**: For each Tier 1 and Tier 2 name reporting tomorrow, send a focused preparation brief.

**Content structure**:

```
EARNINGS TOMORROW: UNH (BMO)
═════════════════════════════

EXPECTATIONS
  EPS:  $7.14 consensus  |  Revenue: $109.2B
  [If revision history available]: EPS est revised down 1.4% over 30d

RECENT PRICE ACTION
  T-1: -0.3%  |  T-7: +1.2%  |  T-30: -4.5%  |  Since last earnings: +8.1%

LAST QUARTER (Q4 2025)
  EPS: $6.91 actual vs $6.72 est (+2.8% beat)
  Revenue: $107.1B vs $106.5B (+0.6% beat)
  Stock reaction: +3.2% next day

LAST 8 QUARTERS — BEAT/MISS HISTORY
  Q4'25: Beat  Q3'25: Beat  Q2'25: Beat  Q1'25: Miss
  Q4'24: Beat  Q3'24: Beat  Q2'24: Inline  Q1'24: Beat
  Batting average: 6/8 beats (75%)

[OPTIONS IMPLIED MOVE: ±4.2% — if available, otherwise omit]

MATERIALS [Tier 1 only]
  • IR Page: [link]
  • Recent 8-K: [link if found via Edgar Tools]
  • [Transcript link if discoverable]

YOUR PREDICTION: [Reply with beat/miss/inline or use /predict command]
```

**Delivery**: Slack `#earnings` + Email for Tier 1 and Tier 2
**Schedule**: Daily run checks for tomorrow's earnings, sends briefs for Tier 1 + Tier 2 names

**Data sources**:
- Finnhub: consensus estimates, actuals for last quarter
- yfinance: price history for performance calculations
- SQLite estimate_history: revision trends (after 30+ days of data accumulation)
- SQLite events: prior quarter beat/miss history
- Edgar Tools MCP: recent 8-K filings (for Tier 1 materials gathering — best-effort)
- yfinance options chains: implied move calculation (if feasible — see Phase 6 notes)

### T+0: Post-Earnings Alert (After the Print)

**What happens**: After earnings are released, send a concise beat/miss summary with after-hours price reaction.

**Timing**: Run 60-90 minutes after typical release windows:
- For BMO names: Run at ~8 AM ET (most BMO releases are 6-7 AM ET)
- For AMC names: Run at ~5:30 PM ET (most AMC releases are 4:00-4:15 PM ET)
- Implementation: Two scheduled runs, or a single run that checks timing

**Content structure**:

```
🟢 UNH BEAT — Q1 2026

  EPS:  $7.38 actual vs $7.14 est  (+3.4% beat)
  Rev:  $110.1B vs $109.2B          (+0.8% beat)

  Stock: +2.8% after-hours (was $512.30, now $526.64)

  [If user had a prediction]: You predicted: BEAT ✓
```

**Delivery**: Slack `#earnings` + Email (both, per user preference — can reduce later)
**Data sources**: Finnhub for actuals, yfinance for after-hours price (with acceptable delay)

### T+1: Close the Loop

**What happens**: Update the Google Calendar event with actual results. Record outcome in predictions table. Update the TickTick task with result context.

**Details**:
- Update Calendar event description: append actuals, beat/miss verdict, stock reaction (close-to-close)
- If user entered a prediction: score it (was_correct), calculate post_earnings_move_pct
- Update TickTick task content with results summary (so user sees outcome when reviewing)
- Flag earnings outcome for future thesis-monitoring project consumption (write to a structured output file or table)

**Schedule**: Morning run the next trading day

---

## 8. Prediction Tracking System

### Entry Mechanism

**Challenge**: Slack buttons require a Slack app with bot token and a callback server — significantly more infrastructure than the current webhook-only setup. The user's entire Slack integration is webhook-based.

**Recommended approach — Slack emoji reaction + CLI fallback**:

**Option A (Low infrastructure — recommended to start):**
- T-1 brief posts to Slack with a prompt: "Reply with your call: `beat`, `miss`, or `inline`"
- A lightweight scheduled job or the next daily run scans the `#earnings` channel for reply messages containing prediction keywords (using the Slack MCP's `slack_read_channel` tool)
- Predictions are stored in the `predictions` table
- This requires only the existing Slack MCP read access — no new Slack app needed

**Option B (Future — full interactive):**
- Register a Slack app in the workspace with bot token
- Post T-1 briefs with interactive buttons (beat / miss / inline)
- Button clicks POST to a callback URL → store prediction
- This is the ideal UX but requires: Slack app registration, a running callback server (or Lambda/Cloud Function), bot token management

**Option C (CLI):**
- `python main.py --predict UNH beat` — simple CLI command
- Could run anytime before earnings
- Low friction if user is already in terminal

**Recommendation**: Start with Option A (Slack reply parsing). Graduate to Option B when the user has a second use case that justifies building a Slack app. Option C as fallback for any name.

**Future prediction fields** (schema supports now, UI later):
- Revenue, EBITDA, and EPS point estimates
- Position stance: long / neutral / short
- Thesis note (freeform)

### Accuracy Analysis

**Periodic report** (monthly or quarterly, triggered by CLI flag `--accuracy-report`):

```
PREDICTION ACCURACY — Q1 2026
══════════════════════════════

OVERALL: 18/27 correct (66.7%)
  Beats correctly predicted:  12/15 (80%)
  Misses correctly predicted:  4/8  (50%)
  Inlines correctly predicted: 2/4  (50%)

BY SECTOR:
  HC Services:  8/10 (80%)  — Bullish bias detected: predicted beat 9/10 times
  MedTech:      6/9  (67%)  — No systematic bias
  Biopharma:    4/8  (50%)  — Small sample

BY NAME (Tier 1):
  UNH:   4/4 correct — you read this name well
  ISRG:  1/4 correct — consistently predicted beat, missed twice
  CI:    3/4 correct

BIAS ANALYSIS:
  • You predict "beat" 78% of the time — actual beat rate is 62%
  • You are more accurate on HC Services (+13% vs base rate) than MedTech (-5%)
  • Names you own (Tier 1): 72% accuracy. Names you watch (Tier 2): 58%
  • Largest miss: ISRG Q3 — predicted beat, was a miss, stock -8.2%

POSITION STANCE ACCURACY [when populated]:
  • Long calls correct: X/Y
  • Short calls correct: X/Y
```

**Storage**: SQLite predictions table. Export to CSV on request for external analysis.

---

## 9. TickTick Integration

### Quarterly List Management

- Each quarter gets a dedicated TickTick list: e.g. "1Q26 Earnings", "2Q26 Earnings"
- The system attempts to create the list via `POST /open/v1/project` (the TickTick API does support list creation)
- If list creation fails or the API doesn't cooperate, fall back to alerting the user via Slack to create it manually, then configure the list ID
- Store the list ID in `.env` or a local config file: `TICKTICK_LIST_EARNINGS_1Q26=<id>`

### Task Creation

- One task per company (Tier 1 + Tier 2 only), no subtasks
- Task title: `UNH Q1 2026 Earnings (Apr 21 BMO)`
- Task content (markdown):
  ```
  Consensus: EPS $7.14 | Rev $109.2B
  Last Q: Beat EPS +2.8%, Rev +0.6%
  
  Review checklist:
  - Transcript
  - Company documents / IR materials
  - Sell-side take
  - Update model if relevant
  
  [Updated post-earnings]:
  Result: Beat EPS +3.4%, Rev +0.8%
  Stock reaction: +2.8%
  ```
- Due date: earnings date
- Created during the daily sync run when earnings are confirmed
- Updated with results post-earnings (T+1 run)

### Task Status Tracking

- The review_status table tracks whether each earnings event has been reviewed
- Checking TickTick task completion status via API (`GET /open/v1/task/{id}`) can sync back to the DB
- This enables a "review completion rate" metric in the accuracy/digest reports

---

## 10. Materials Gathering (Tier 1 Only)

### Goal

For each Tier 1 name reporting earnings, attempt to assemble links to key resources the user needs for manual review. This is best-effort — a missing link should never block an alert or event.

### Implementation

A dedicated `enrichment.py` module that runs as part of the T-1 brief generation:

1. **IR Page Link**: Maintained in a static `ir_links.json` mapping (ticker → IR URL). Start with the ~20-30 Tier 1 names. Manually curated — IR pages rarely change.

2. **Recent SEC Filings**: Use Edgar Tools MCP (`company_filings`, `material_events`) to find:
   - Most recent 8-K (earnings release / material event)
   - Most recent 10-Q or 10-K
   - Link directly to the filing

3. **Earnings Transcript Link**: Use Tavily web search (when authenticated) to search for `"{ticker} Q{N} {year} earnings transcript"`. Extract the top result URL. Common sources: Seeking Alpha (paywalled but link is useful), Motley Fool, company IR pages.

4. **Press Release**: Often the 8-K itself or a link from the company IR page.

**Output**: A `materials` dict attached to the EarningsEvent dataclass:
```python
{
    "ir_url": "https://investor.unitedhealthgroup.com/",
    "filing_8k_url": "https://www.sec.gov/...",
    "filing_10q_url": "https://www.sec.gov/...",
    "transcript_url": "https://seekingalpha.com/...",  # best-effort
    "press_release_url": "https://..."  # best-effort
}
```

These links are included in the T-1 brief (Slack + Email) and added to the TickTick task content.

---

## 11. Implementation Phases

### Phase 1: Foundation (Code Quality + Coverage Manager Sync)
**Goal**: Fix critical bugs, modularize, and connect to Coverage Manager. No new features — just a solid base.

**Tasks**:
1. **Non-destructive DB migration** — Replace `DROP TABLE` with `ALTER TABLE ADD COLUMN`. Add migration version tracking.
2. **Fix event identity** — Change dedup key from `UNIQUE(ticker, quarter)` to `UNIQUE(ticker, event_date)`. Store `source_fingerprint` in extendedProperties. Note: `ticker + event_date` is a pragmatic improvement — two earnings events for the same ticker on the same date doesn't happen in practice. The `source_fingerprint` field exists for future refinement if needed.
3. **Modularize** — Split `earnings_agent.py` into `config.py`, `storage.py`, `calendar_sync.py`, `finnhub_client.py`, `coverage.py`, `main.py`. Preserve `earnings_agent.py` as a legacy entry point that delegates to `main.py`.
4. **Coverage Manager integration** — `coverage.py` reads `universe.csv`, `watchlist.csv`, `universe_metadata.json` from Coverage Manager's `exports/` directory. Resolves each ticker to Tier 1/2/3. Remove `tickers.txt` dependency.
5. **Replace generic exception handlers** — Catch specific exceptions across all ~7 sites.
6. **Add retry with exponential backoff** — Wrap Finnhub and Google Calendar API calls.
7. **Config cleanup** — Make timezone, Coverage Manager export path, and credential paths configurable via `.env`. Document clearly.
8. **Paginate Google Calendar queries** — Fix the 50-result cap in `find_calendar_event()`.

**Acceptance criterion**: The system reads tiers from Coverage Manager, syncs tier-aware calendar events, and no longer depends on `tickers.txt`. Existing backfill and cleanup modes still work. All existing tests pass.

**Dependencies**: Coverage Manager exports must exist at the configured path. No new APIs or credentials needed.

### Phase 2: TickTick Integration (Execution Surface) — MVP CRITICAL
**Goal**: Stand up the execution surface for earnings review work. TickTick is not a nice-to-have — it is the surface where the user tracks what they need to do about each earnings event. Calendar answers "when is this happening?" TickTick answers "what do I need to do about it?"

**Tasks**:
1. **Create `ticktick.py`** — Reuse authentication pattern from daily-reads. Implement list creation (`POST /open/v1/project`) with fallback to Slack alert asking user to create list manually. Task creation with earnings-specific content.
2. **Quarterly list management** — Auto-create "1Q26 Earnings" list (or alert user to create it). Store list ID in `.env` or local config.
3. **Task creation during daily sync** — For Tier 1 + Tier 2 names with confirmed earnings dates, create one TickTick task per company:
   - Title: `UNH Q1 2026 Earnings (Apr 21 BMO)`
   - Content: consensus estimates, last quarter result, review checklist (transcript, company docs, sell-side take, update model)
   - Due date: earnings date
4. **Review status tracking** — `review_status` table in SQLite tracks task IDs. Optional: periodic `GET /open/v1/task/{id}` to sync completion status back to DB.
5. **Dedup** — Don't create duplicate tasks if the daily sync runs multiple times. Check `ticktick_task_id` in events table before creating.

**Acceptance criterion**: Every Tier 1 and Tier 2 earnings event with a confirmed date produces a TickTick task in the correct quarterly list, with correct due date and prep content. No duplicate tasks on re-runs.

**Integration risk**: TickTick list creation via API (`POST /project`) is untested in the user's environment. The daily-reads project only creates tasks, never lists. Spike this early — if it fails, the fallback (Slack alert to user) must work reliably.

**Dependencies**: TickTick access token (exists from daily-reads, shared credential). New `TICKTICK_LIST_EARNINGS` env var.

### Phase 3: Weekly Digest + Slack Channel (T-7)
**Goal**: First awareness surface beyond Calendar. Weekly digest to Slack `#earnings` and email.

**Tasks**:
1. **Create `notifications.py`** — Slack webhook posting (Block Kit format, same pattern as sigma-alert) + Gmail API send.
2. **Create Slack `#earnings` channel** — User creates channel and webhook manually. Store webhook URL in `.env` as `SLACK_WEBHOOK_EARNINGS`.
3. **Implement weekly digest** — Query upcoming 7 days + 30 days from events table. Format by tier. Include sector clustering detection ("5 HC Services names this week"). Include earnings season cadence ("peak week: 23 names reporting").
4. **Add `--weekly-digest` CLI mode** — Triggers digest generation and delivery.
5. **Email HTML template** — Clean, scannable format for the weekly digest. Multipart (HTML + plaintext fallback).
6. **GitHub Actions workflow** (`weekly_digest.yml`) — Sunday 12 PM ET trigger.

**Acceptance criterion**: The weekly digest sends reliably every Sunday with no manual intervention. Tier 1/2 names show estimates and last-quarter context. Tier 3 names appear as a ticker list only. Slack and email both render correctly.

**Integration risk**: Gmail send permission is unproven in this project. The daily-reads project uses Gmail for reading, not sending. The 13F Analyzer sends via Gmail API but with its own OAuth setup. Test Gmail send early — if it fails, fall back to Gmail MCP draft creation (user reviews and sends manually).

**Dependencies**: Slack webhook URL for `#earnings`. Gmail API send credentials (test early).

### Phase 4: Post-Earnings Alerts + Close the Loop (T+0, T+1)
**Goal**: Close the biggest gap — what happened after earnings. Update all surfaces with results.

**Tasks**:
1. **Post-earnings alert (T+0)** — New `--check-results` CLI mode. Query Finnhub for actuals on today's (or yesterday's) earnings. Query yfinance for after-hours/post-market price (with acceptable delay). Format beat/miss summary. Send to Slack + Email.
2. **Create `market_data.py`** — yfinance wrapper for after-hours price quotes and stock performance calculations.
3. **T+1 close-the-loop** — Update Google Calendar event description with actuals. Update TickTick task content with results summary (so user sees outcome when reviewing). Record outcome in events table.
4. **Scheduling** — Two additional daily runs: ~8:30 AM ET (check BMO results) and ~5:30 PM ET (check AMC results). Start with a single evening run that checks all of today's earnings; split into two runs later if timing matters.

**Acceptance criterion**: Within 90 minutes of earnings release, the user receives a beat/miss alert on Slack and email with the stock's after-hours reaction. By the next morning, the Calendar event and TickTick task are updated with results.

**Dependencies**: yfinance (existing). Finnhub (existing). Additional scheduled runs (start with one, expand to two).

### Phase 5: Pre-Earnings Briefs + Enrichment (T-1)
**Goal**: The preparation layer. Give the user context to act on, not just a reminder.

**Tasks**:
1. **Expand `market_data.py`** — Add stock price performance at T-1, T-7, T-30, and since-last-earnings.
2. **Create `enrichment.py`** — Assemble pre-earnings brief: consensus estimates, prior quarter results, beat/miss history (last 8 quarters from events table), price action context, materials links (Tier 1 only).
3. **Prior quarter history** — Query Finnhub earnings calendar for historical quarters (or accumulate over time from daily runs). Store in events table with `reported=1`.
4. **Estimate revision tracking** — Each daily run stores current consensus in `estimate_history` table. After 30+ days, calculate 30/60/90-day revision trend. Include in T-1 brief when available.
5. **`ir_links.json`** — Static mapping of Tier 1 tickers to IR page URLs. Manually curated.
6. **Edgar Tools integration** — For Tier 1 names, query `company_filings` and `material_events` for recent 8-K/10-Q links. Best-effort, fail silently.
7. **Tavily integration** — Search for transcript links for Tier 1 names. Best-effort.
8. **T-1 brief delivery** — Slack + Email, formatted per the content structure in Section 7.
9. **Add `--t1-brief` CLI mode** — Manual trigger for testing.

**Acceptance criterion**: For every Tier 1 and Tier 2 name reporting tomorrow, the user receives a brief with consensus estimates, prior quarter result, 8-quarter beat/miss history, and recent price action. Tier 1 names also include materials links (best-effort). The brief arrives the evening before or morning of the day before earnings.

**Integration risk**: Edgar Tools MCP and Tavily are best-effort enrichment — both may fail for individual tickers. The brief must render correctly even when enrichment returns nothing. Tavily needs authentication which is currently unverified.

**Dependencies**: yfinance (existing). Edgar Tools MCP (existing, $25/mo). Tavily (needs auth — test if it works). Historical data accumulates over time — some brief fields will be sparse initially.

### Phase 6: Prediction Tracking + Accuracy Analysis
**Goal**: Build the feedback loop. Let the user track predictions and detect biases.

**Tasks**:
1. **Create `predictions.py`** — Prediction entry (CLI: `--predict TICKER beat`), storage, outcome matching, accuracy calculation.
2. **Slack reply parsing** — After T-1 brief is posted, the next daily run reads `#earnings` channel via Slack MCP (`slack_read_channel`). Parse replies for prediction keywords (beat/miss/inline). Store in predictions table.
3. **Outcome scoring** — During T+1 processing, match actuals against predictions. Determine was_correct. Calculate post_earnings_move_pct (T+1 close vs T-1 close via yfinance).
4. **Accuracy report** — `--accuracy-report` CLI mode. Generates the report described in Section 8: overall, by sector, by name, bias analysis.
5. **Future prediction fields** — Schema already supports `predicted_eps`, `predicted_revenue`, `predicted_ebitda`, `position_stance`. Leave UI entry for future enhancement but ensure storage and accuracy calculation can handle them when populated.
6. **Quarterly accuracy digest** — At end of each quarter, auto-generate and send accuracy report via Slack + Email.

**Acceptance criterion**: The user can enter predictions via CLI (and optionally via Slack reply). After earnings report, predictions are scored automatically. A quarterly accuracy report shows batting average, sector biases, and name-level patterns with at least one full quarter of data.

**Integration risk**: Slack reply parsing via `slack_read_channel` MCP tool is unproven for this use case. It may not reliably find replies to specific messages, or may hit rate limits. CLI entry (`--predict`) must work independently as the reliable fallback.

**Dependencies**: Slack MCP read access (for reply parsing). All prior phases operational.

### Phase 7: Reconcile Mode + Hardening
**Goal**: Make the system trustworthy for long unattended runs.

**Tasks**:
1. **`--reconcile` CLI mode** — Scan future Google Calendar events with earnings extendedProperties. For each:
   - Verify event still matches Finnhub source data (date, timing)
   - Fix missing or stale extendedProperties
   - Repair DB drift (calendar is source of truth for event state, DB is source of truth for workflow state)
   - Report discrepancies
2. **Failure notifications** — On any run failure, send Slack alert to `#earnings` with error summary.
3. **Log to file** — In addition to stdout, for CI post-mortem debugging.
4. **Calendar event colors** — Tier 1 vs Tier 2 differentiation. Post-earnings color change (e.g., green for beat, red for miss).
5. **Cache calendar events in-memory** per run to reduce Google API calls.
6. **DST-aware scheduling** — Document the DST shift explicitly, or implement timezone-aware scheduling in GitHub Actions / Windows Task Scheduler.

**Acceptance criterion**: A `--reconcile` run detects and repairs stale calendar events, missing extendedProperties, and DB/calendar drift. Any run failure (daily sync, digest, T+0 check) produces a Slack alert within 5 minutes.

### Phase 7: Future Enhancements (Not Scheduled)

These are captured for future consideration, not planned for immediate implementation:

1. **Options implied move** — Calculate from yfinance options chains (at-the-money straddle). Include in T-1 brief. Complexity: moderate (options chain parsing, expiration selection). Free data source available.
2. **Slack app with interactive buttons** — Register Slack app, implement callback server, replace reply-parsing with button clicks for predictions. Requires: Slack app registration, callback infrastructure (Lambda or always-on server).
3. **Real-time after-hours data** — Polygon.io ($29/mo) or Alpaca for faster T+0 alerts. Currently yfinance with 30-60 min delay is acceptable.
4. **Estimate revision history from professional source** — Quandl/Nasdaq Data Link (~$50/mo) for historical consensus revisions. Currently building our own via daily snapshots.
5. **Thesis monitoring integration** — Output earnings outcomes in a structured format consumable by a future thesis-monitoring project.
6. **Seeking Alpha transcript access** — No API available. Could potentially use web scraping but legally gray. Better to link to it and let user access manually.
7. **Management guidance tracking** — Track forward guidance changes quarter-over-quarter. Requires transcript parsing or a structured data source.
8. **Earnings season analytics** — Sector-level beat rates, average surprise magnitude, correlation analysis across portfolio names.
9. **Multi-user support** — Not needed now but architecture (env-based config, tier system) doesn't preclude it.
10. **S&P Global integration** — Already configured as MCP, needs authentication. Could provide professional-grade estimate data. Cost unknown — investigate.

---

## 12. Scheduling Summary

Start with two scheduled runs (daily sync + weekly digest). Add T+0 result checks in Phase 4. Avoid front-loading too many triggers before the core workflow is proven.

**After Phase 1-2 (Foundation + TickTick):**

| Run | Time | Trigger | Modes |
|-----|------|---------|-------|
| **Daily sync** | 6 AM ET | GitHub Actions or Windows Task Scheduler | `--sync` (collect, calendar, TickTick tasks, estimate snapshots) |

**After Phase 3 (Weekly Digest):**

| Run | Time | Trigger | Modes |
|-----|------|---------|-------|
| **Daily sync** | 6 AM ET | GitHub Actions or Windows Task Scheduler | `--sync` |
| **Weekly digest** | Sunday 12 PM ET | `weekly_digest.yml` or Task Scheduler | `--weekly-digest` |

**After Phase 4 (Post-Earnings Alerts) — full schedule:**

| Run | Time | Trigger | Modes |
|-----|------|---------|-------|
| **Daily sync** | 6 AM ET | GitHub Actions or Windows Task Scheduler | `--sync` (collect, calendar, TickTick tasks, estimate snapshots, T-1 briefs, T+1 close-loop) |
| **T+0 results check** | 5:30 PM ET | Separate scheduled run | `--check-results` for today's BMO + AMC names (single run; split into BMO 8:30 AM + AMC 5:30 PM later if timing matters) |
| **Weekly digest** | Sunday 12 PM ET | `weekly_digest.yml` or Task Scheduler | `--weekly-digest` + `--reconcile` (Phase 7) |
| **Accuracy report** | End of quarter | Manual or scheduled | `--accuracy-report` |

---

## 13. Configuration (.env)

```bash
# Data sources
FINNHUB_API_KEY=...

# Coverage Manager (source of truth for tickers)
COVERAGE_MANAGER_PATH=C:/Users/jroyp/Dropbox/Claude Folder/Coverage Manager

# Google Calendar
GOOGLE_CALENDAR_ID=...
GOOGLE_CREDENTIALS_PATH=credentials.json

# Slack
SLACK_WEBHOOK_EARNINGS=https://hooks.slack.com/services/...

# Email
EMAIL_TO=jroypeterson@gmail.com
# Gmail API credentials path or OAuth token

# TickTick
TICKTICK_ACCESS_TOKEN=...
TICKTICK_LIST_EARNINGS=...  # Current quarter list ID

# Settings
TIMEZONE=America/New_York
DRY_RUN=false
LOG_FILE=earnings_agent.log
```

---

## 14. Risk & Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Finnhub free tier rate limit (60/min) with 110+ tickers | Incomplete data collection | Chunked queries (existing), retry with backoff (Phase 1) |
| TickTick token expires every ~180 days | TickTick tasks stop creating | Detect 401, alert via Slack (same pattern as daily-reads) |
| Coverage Manager exports not found | No tickers loaded | Fail loudly with Slack alert. Fall back to hardcoded ticker list as emergency backup. |
| yfinance price data unreliable for AH | Inaccurate T+0 reaction numbers | Accept delay, clearly label as "delayed" in alerts. Flag real-time source as future enhancement. |
| Estimate revision history sparse initially | T-1 briefs missing revision trends | Clearly indicate "revision data available after 30+ days of operation." Build over time. |
| Gmail send permission issues | Emails don't deliver | Test Gmail API send early in Phase 2. Fall back to Gmail MCP draft creation if send fails. |
| Slack channel webhook misconfigured | Notifications fail silently | Validate webhook on startup, alert to stdout if it fails |

---

## 15. Product Framing

This system is an **execution system for earnings review work**, not merely an information system about earnings. The distinction matters for prioritization:

- An information system optimizes for delivery: did the user receive the alert?
- An execution system optimizes for completion: did the user actually review this earnings event and update their view?

TickTick (the execution surface) and prediction tracking (the feedback loop) are therefore core to the product, not peripheral features. Calendar and Slack (awareness surfaces) support the workflow but do not define success on their own.

**Success is measured by**: Did every important earnings event get reviewed? Did the user's prediction accuracy improve over time? Not: Did every Slack message send?

## 16. What This Plan Does NOT Cover

- **Thesis monitoring infrastructure** — Flagged as a separate project. This system will output structured earnings outcomes that the thesis project can consume.
- **Portfolio P&L tracking** — Not in scope. Coverage Manager + watchlist handles position awareness.
- **Real-time alerting** — Post-earnings alerts have a 30-90 minute delay. Real-time would require a different architecture (streaming, always-on process).
- **Automated trading signals** — This is an execution support system, not a trading system.
- **Sell-side research aggregation** — Beyond linking to publicly available resources. Seeking Alpha has no API.
- **Bidirectional TickTick sync** — Phase 2 creates tasks and optionally reads completion status. Full two-way sync (editing tasks in TickTick updates the DB) is out of scope until proven necessary.

---

## 17. Success Criteria

After full implementation, the system should:

1. **Never miss**: Every Tier 1 and Tier 2 earnings event appears on the calendar, in the weekly digest, and as a TickTick task — with zero manual intervention.
2. **Drive review completion**: TickTick tasks make it clear what needs to be done for each earnings event. The user can see at a glance which names they've reviewed and which are outstanding.
3. **Prepare the user**: T-1 briefs provide consensus estimates, revision trend (when available), prior quarter context, price action, and materials links — enough to form a view in 2 minutes.
4. **Close the loop**: Within 90 minutes of earnings release, the user gets a beat/miss alert with price reaction on Slack and email. The next morning, the calendar event and TickTick task are updated with results.
5. **Build institutional memory**: After 2+ quarters of operation, the user can run an accuracy report showing prediction batting average, sector biases, and name-level patterns.
6. **Stay out of the way**: Tier 3 names appear only in the weekly digest. No calendar clutter, no Slack noise, no TickTick tasks for names the user doesn't actively follow.
