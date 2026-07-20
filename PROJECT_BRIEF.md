# Project Brief — read this first (for reviewers, human or AI)

This file exists so a reviewer can (1) judge how close the project is to its
intended goal and (2) understand the key design decisions **before** giving
feedback. For mechanics — module map, CLI modes, schema history, the full
date-correctness safeguard stack — see `README.md` and `CLAUDE.md`. This brief
does not re-describe them.

> When reviewing, weigh findings against the **success criteria** and the
> **non-goals / accepted tradeoffs** below. The hardest design pressure on this
> project is "**never silently miss or misdate an earnings event**," and several
> apparent simplifications (`continue-on-error`, DB-first calendar moves,
> delete-first recreates, swallow-and-continue on a Finnhub cap) were tried and
> *deliberately rejected* because they reintroduce a silent-failure mode. If you
> think a rejected option is worth it, engage the stated rationale rather than
> re-proposing it.

---

## 1. Intended goal (the "why")

Make the owner's earnings-review workflow **run itself**, with the date being
right as the non-negotiable core. The owner is a solo, part-time,
healthcare-focused investor; earnings season is a recurring, high-stakes,
easy-to-drop-the-ball workload. The agent should:

- Know **which** names matter (driven by Coverage Manager's universe + tiers, not
  a hand-kept list), **when** they report (correctly, across disagreeing data
  sources), and surface that into the tools the owner already lives in —
  **Google Calendar** (events) and **TickTick** (per-quarter review tasks).
- After the print, push **beat/miss + stock-move** results to Slack `#earnings`
  so the owner sees outcomes without watching the tape.
- Be trustworthy unattended: a wrong or missed date is worse than a loud
  failure, so every degrade path must raise an alarm the owner will actually see
  (Slack, and an out-of-band email backup when Slack itself is the failure).

Success = the owner stops manually tracking earnings dates and trusts the
calendar/TickTick/Slack surfaces, *including* trusting that silence means
"nothing happened," not "the agent broke."

## 2. Success criteria — and current status

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Universe + tiers come from Coverage Manager, not a manual list | ✅ Done | `coverage.py` loads CM `exports/`; CI sparse-checks out `jroypeterson/Coverage-Manager`; freshness check alerts at >7d staleness |
| 2 | Upcoming earnings → Calendar events for Tier 1+2, deduped, confirmed vs estimated labelled | ✅ Done | `calendar_sync.py` CRUD + dedup; `date_confirmed` from Finnhub `hour`; " (est.)" title suffix |
| 3 | Per-quarter TickTick review tasks for Tier 1+2 with checklist | ✅ Done | `ticktick.py`; two lists/quarter; cross-list dedup; reporting-quarter (not release-quarter) naming |
| 4 | Earnings **date** correct despite source disagreement | ✅ Mostly | Multi-layer stack: Finnhub+FMP merge → yfinance cross-check → EDGAR 8-K 2.02 auto-correct (calendar-first, corroboration-gated lock) → IR RSS/email → manual `--lock`. UFPT/FIVE/ICLR cases handled live. **Gap:** foreign filers (6-K/20-F, e.g. ICLR/ICON) not auto-corrected |
| 5 | Beat/miss + stock-move results posted after the print | ✅ Done | `run_check_results` + `post_earnings_check.yml`; tiered/subgrouped Slack layout; AMC stock-move deferral via `_should_defer_post()` |
| 6 | Never silently miss a reporter | ✅ Mostly | EDGAR results backstop (`--check-missed-results`) runs a DB-candidate pass + a DB-independent Tier-1 blind sweep; unseen-ticker counter; `_run_safeguard` re-raises instead of `continue-on-error`. **Gap:** blind sweep is Tier-1 only; Tier-2-only misses outside the merge are still possible |
| 7 | Never silently misdate or lose persisted state | ✅ Done | Create-first calendar moves (single chokepoint); non-destructive schema migrations (v12); `EA_DB_BOOTSTRAPPED` fail-loud on missing DB artifact; same-quarter phantom guard protects `reported=1` rows |
| 8 | Loud, redundant failure delivery unattended | ✅ Done | Every workflow `if: failure()` → Slack **and** inline-SMTP email backup (no checkout dependency); critical alert-delivery failures *raise* so the run turns red |
| 9 | Schedule-aware liveness monitoring | ✅ Done | `watchdog.yml` 3×/day computes each workflow's expected trigger from cadence; alerts within ~24h of a skipped weekday run, no weekend false alarms; auto-dispatches recovery |
| 10 | Owner can override a wrong machine decision | ✅ Done | `--lock/--unlock/--list-locks`; Slack-native reply grammar (`--check-replies`, v9 per-question threads) drives DB state from in-thread replies |
| 11 | Pre-earnings briefs / prediction accuracy tracking | ⬜ Not yet | PLAN.md Phases 5–6 unstarted; estimate snapshots are being stored to enable Phase 6 later |
| 12 | Browsable public calendar of confirmed dates, past + future | 🟨 Built, not deployed | `scripts/build_calendar_page.py` → `docs/index.html`, rebuilt every daily run and committed back by `daily_earnings_check.yml`. Confirmation status derived (Locked / Announced / Reported); estimated dates badged and hidden by default. **GitHub Pages is not yet enabled on the repo** — awaiting review + JP's go-ahead |

**Overall: the v1 goal is met and the system is live.** The date-correctness and
no-silent-failure cores are mature and battle-tested against real incidents
(FIVE, UFPT, ICLR/ICON, calendar-TZ churn). Open items are coverage-edge
hardening (foreign filers, Tier-2 blind sweep) and the deferred
brief/prediction phases — not missing core function.

## 3. Key design decisions (and why)

1. **Three explicit systems of record.** Coverage Manager owns *which* names,
   Google Calendar owns *when* (published state), SQLite owns *workflow
   state + history*. Keeping these separate is what lets the calendar be
   self-healing (drift detection re-derives from Finnhub) without the DB and
   calendar fighting.
2. **Finnhub + FMP *merged*, not replaced.** A universe-wide bake-off showed FMP
   (already paid via CM's Starter plan) adds breadth + timelier actuals (fixed
   the FIVE lag) but carries more phantoms and no better dates. So the policy is
   a per-(ticker,quarter) merge with Finnhub holding date authority; degrades to
   Finnhub-only, logged loudly, if `FMP_API_KEY` is unset.
3. **EDGAR 8-K Item 2.02 is the date tiebreaker — calendar-first, lock only when
   corroborated.** SEC evidence overrides every heuristic. But auto-LOCK only
   fires when the filing date is within ±1d of a yfinance date, because an 8-K
   can be filed late; an uncorroborated third date is *surfaced for manual lock*,
   not silently locked. The move is applied to the calendar first (the lock makes
   reconcile skip the event, so a calendar failure would otherwise freeze a wrong
   date).
4. **Fail loud, never silent, with a redundant out-of-band path.** The whole
   "verify dates / don't miss results" layer refuses `continue-on-error`;
   `_run_safeguard` re-raises. Failure email is **inlined** in each workflow
   (stdlib `python3`, no repo checkout) so it survives even a checkout failure —
   explicitly *not* a call to `scripts/send_failure_email.py`.
5. **Create-first everywhere for calendar mutations, single chokepoint.** All
   moves/recreates go through `_move_calendar_event` (create new → delete old);
   delete-first anywhere could orphan an event with nothing able to recreate it.
6. **yfinance read via `Ticker.info`, compared in ET wall-clock.** `.calendar`
   and `get_earnings_dates()` strip time-of-day; the UTC-default calendar needs
   ET normalization (`_wall_clock_et`) or every timed event false-drifts and
   churns. Both were real bugs that caused mass delete/recreate.
7. **Schedule-aware watchdog over a flat "last success < N hours."** A flat
   window can't tell a skipped Friday run from a normal weekend gap; the watchdog
   computes each workflow's expected trigger from its cadence.

## 4. Non-goals / accepted tradeoffs

- **Not a real-time feed.** It's batch (daily/weekday cron + post-earnings
  sweep). Intraday Finnhub changes are caught by the afternoon redundancy run and
  the 3×/day reconcile, not instantly.
- **Not a fundamentals/valuation engine.** It reports the *event* (date, timing,
  consensus, beat/miss, stock move). Analysis of the result is the owner's job
  (and other workspace tools'). Estimate snapshots are stored only to *enable* a
  future revision/accuracy phase.
- **Tier 3 is digest-only by design** — no Calendar, no TickTick — to keep the
  ~840-name long tail from flooding the surfaces the owner acts on.
- **Foreign filers (6-K/20-F) are knowingly outside the EDGAR auto-correct path**
  (it keys on 8-K Item 2.02). Accepted; surfaced as a known gap, not silently
  ignored.
- **Gmail draft creation stays MCP/local-only** (CI can't run the Gmail MCP); the
  weekly workflow uploads `last_digest.html` as an artifact instead.
- **`--dry-run` is intentionally NOT side-effect-free** — it still seeds the
  SQLite DB (events + estimates), which the CI populate steps depend on.
  `--populate-db-only` is the self-documenting alias.

## 5. Known gaps / candidate next steps (feedback welcome here)

- **Foreign-filer date correctness.** No 6-K/20-F equivalent of the 8-K 2.02
  backstop; ICLR/ICON-class names rely on Finnhub/FMP/yfinance only.
- **EDGAR blind sweep is Tier-1 only.** A Tier-2 name that *both* Finnhub and FMP
  miss entirely would still go unreported. Extending the blind sweep to Tier 2 is
  the obvious next hardening (cost: more SEC requests/run).
- **Pre-earnings briefs (Phase 5) and prediction/accuracy tracking (Phase 6)** are
  unbuilt; estimate-snapshot history is accumulating to support Phase 6.
- **TickTick mid-quarter tier changes** don't move the existing task (cross-list
  dedup blocks recreation); the next quarter's sync fixes it. Slack diff alert
  surfaces the change for manual handling.
- **TickTick token** expires ~180d and needs manual re-auth (401 is detected and
  logged, not auto-refreshed).
- **No formal SLO/event-color tracking** (the remaining slice of PLAN.md Phase 7).
- **Calendar page history is only as deep as the DB's rolling window** (~trailing
  months), so "past earnings" on `docs/index.html` is not a full archive. A
  separate append-only history table would be needed for multi-year depth.
- **Pages must still be enabled** (Settings → Pages → `main` / `docs`) before the
  committed `docs/index.html` is reachable at
  `https://jroypeterson.github.io/earnings-agent/`. Not done — deliberately
  pending review/approval.

## 6. How to evaluate

- **Entry point + all top-level flows:** `main.py` (CLI modes documented in
  `CLAUDE.md` "CLI modes"). `earnings_agent.py` is a legacy delegator.
- **Core logic most worth scrutiny:**
  - Date arbitration & EDGAR auto-correct: `main.py` (`run`, `run_cross_check`,
    `_apply_edgar_auto_correction`, `_edgar_date_corroborated`) + `edgar_client.py`.
  - Calendar move/recreate invariant: `calendar_sync.py` `_move_calendar_event`
    and the drift/TZ helpers (`calendar_event_drift_kind`, `_wall_clock_et`).
  - Provider merge: `main._fetch_earnings_source` + `fmp_client.py` /
    `finnhub_client.py` (note the fail-fast on cap hits — do not soften).
  - Persistence safety: `storage.py` (v12 migrations, phantom guard, upsert).
- **Tests (do not need network/Calendar/TickTick):** `test_dedup.py` (the bulk —
  schema-invariant, dedup, phantom-survival regressions), `test_run_integration.py`,
  `test_subcategories.py`. ~98 test functions across the three files. Run with
  `python -m pytest -q` (`pip install -r requirements-dev.txt` first).
- **Most useful feedback:** (a) any silent-failure or silent-misdate path the
  safeguard stack still leaves open — that's the whole thesis; (b) whether the
  EDGAR corroboration-gated auto-lock is the right risk tradeoff vs. a stricter or
  looser rule; (c) which §5 gap (foreign filers vs Tier-2 blind sweep vs Phase
  5/6) is worth doing first given a solo part-time owner.
