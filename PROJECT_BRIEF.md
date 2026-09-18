# Project Brief — read this first (for reviewers, human or AI)

> **Staleness -- check before trusting any figure below.** This file is not auto-updated:
> `python ../scripts/audit_project_briefs.py --repo "earnings_agent"` counts the behaviour-changing
> commits landed since it was last touched. Section 3 rationale and section 4 non-goals age
> well; section 2 status, and every count, version, path and line number here, is a
> **hypothesis** until checked against the code -- and a disagreement between this file and
> the repo is a defect worth reporting, not a documentation nit.

This file exists so a reviewer can (1) judge how close the project is to its
intended goal and (2) understand the key design decisions **before** giving
feedback. For mechanics — module map, CLI modes, schema history, workflow
schedule, the full date-correctness safeguard stack — see `README.md` and
`CLAUDE.md`. This brief does not re-describe them.

> When reviewing, weigh findings against the **success criteria** and the
> **non-goals / accepted tradeoffs** below. The hardest design pressure on this
> project is "**never silently miss or misdate an earnings event**," and several
> apparent simplifications (`continue-on-error`, DB-first calendar moves,
> delete-first recreates, swallow-and-continue on a Finnhub cap, a wall-clock
> freshness check on the DB) were tried and *deliberately rejected* because they
> reintroduce a silent-failure mode — or, in the freshness case, became the
> outage. If you think a rejected option is worth it, engage the stated
> rationale rather than re-proposing it.

---

## 1. Intended goal (the "why")

Make the owner's earnings-review workflow **run itself**, with the date being
right as the non-negotiable core. The owner is a solo, part-time,
healthcare-focused investor; earnings season is a recurring, high-stakes,
easy-to-drop-the-ball workload. The agent should:

- Know **which** names matter (driven by Coverage Manager's universe, tiers and
  Position lists, not a hand-kept list), **when** they report (correctly, across
  disagreeing data sources), and surface that into the tools the owner already
  lives in — **Google Calendar** (events), **TickTick** (per-quarter review
  tasks), and a public **GitHub Pages** calendar.
- Before the print, push a **consensus preview** (metrics, the company's own
  prior guidance, setup, call time) to Slack `#street-account` and `#portfolio`.
- After the print, push **beat/miss + stock-move** results to Slack `#earnings`,
  a terse "X reported" ping to `#portfolio` for held/researching names, and a
  **season-progress** view of the owner's book (Slack + a sortable Pages page).
- Be trustworthy unattended: a wrong or missed date is worse than a loud
  failure, so every degrade path must raise an alarm the owner will actually see
  (Slack, and an out-of-band email backup when Slack itself is the failure).

Success = the owner stops manually tracking earnings dates and trusts the
calendar/TickTick/Slack surfaces, *including* trusting that silence means
"nothing happened," not "the agent broke."

This repo is also a **library for a sibling project**: `earnings_review/` (the
T+1 review card to `#portfolio`) imports `edgar_client`, `daily_summary` and
`guidance_parse` from here via `sys.path`, and restores the events DB with this
repo's `scripts/ci_restore_db_artifact.sh`. Changing those modules' public
functions changes that card. (This contract is recorded in `earnings_review`'s
code and in `EARNINGS_CARD_FORMAT.md`, not in this repo's `CLAUDE.md`.)

## 2. Success criteria — and current status

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Universe + tiers come from Coverage Manager, not a manual list | ✅ Done | `coverage.py` loads CM `exports/` (read `utf-8-sig`, so a BOM cannot zero the join); CI sparse-checks out `jroypeterson/Coverage-Manager`; freshness check alerts at >7d staleness; `_assert_coverage_not_collapsed` hard-stops a run when a tier loses >20% AND ≥25 names |
| 2 | Upcoming earnings → Calendar events for Tier 1+2, deduped, confirmed vs estimated labelled | ✅ Done | `calendar_sync.py` CRUD + dedup; `date_confirmed` from Finnhub `hour`; " (est.)" title suffix; 30-minute holds |
| 3 | Per-quarter TickTick review tasks for Tier 1+2, kept in step with the DB | ✅ Done | `ticktick.py`; two lists/quarter; cross-list dedup; reporting-quarter naming. `--reconcile-ticktick` runs with live writes on the reconcile cron and treats tasks as a **projection of DB truth**: a task is **dated only once the date is confirmed/locked/reported** (JP 2026-07-24), carries one uniform tickable review checklist, sector + Position tags, a `Reported` tag, and the one-day move in the title after the print |
| 4 | Earnings **date** correct despite source disagreement | ✅ Mostly | Multi-layer stack: Finnhub+FMP merge → yfinance cross-check → EDGAR 8-K 2.02 auto-correct (calendar-first, corroboration-gated lock) → foreign-filer **6-K fallback** (filename heuristic, same corroboration gate, never locks on a 6-K alone) → IR RSS/email → manual `--lock` (CLI, Slack reply, or the `reconcile_calendar.yml` operator lock input). **Gap:** the 6-K path is a filename heuristic, weaker than an item code |
| 5 | Beat/miss + stock-move results posted after the print | ✅ Done | `run_check_results` + `post_earnings_check.yml`; tiered/subgrouped Slack layout incl. a Biopharma subgroup and a generated marker legend; a capped card never drops a row it then marks reported; AMC stock-move deferral via `_should_defer_post()` |
| 6 | Never silently miss a reporter | ✅ Mostly | EDGAR results backstop (`--check-missed-results`) runs a DB-candidate pass + a DB-independent Tier-1 blind sweep; unseen-ticker lane escalates in-thread on a backoff instead of re-alerting as "New"; `_run_safeguard` re-raises instead of `continue-on-error`. **Gap:** blind sweep is Tier-1 only |
| 7 | Never silently misdate or lose persisted state | ✅ Done, recently re-hardened | Create-first calendar moves (single chokepoint); non-destructive schema migrations (`storage.CURRENT_SCHEMA_VERSION`); `closed_reason` terminal state behind the single `OPEN_EVENT_SQL`; question state carried across a vendor date move. **The DB lives only as a CI artifact** — deterministic newest-artifact restore (`scripts/ci_restore_db_artifact.sh`) + a content-based rollback guard (`scripts/ci_db_rollback_guard.sh`). Two real rollbacks got through before these (a 28-day one in Aug 2026, a 3.6-day one 2026-09-08), and the guard's first design itself caused a 3-day outage (2026-09-11..14) — see §3.11 |
| 8 | Loud, redundant failure delivery unattended | ✅ Done | Every workflow `if: failure()` → Slack **and** inline-SMTP email backup (no checkout dependency); critical alert-delivery failures *raise*; heartbeat renders abnormal zero counts as `partial`, not green |
| 9 | Schedule-aware liveness monitoring | ✅ Done | `watchdog.yml` 3×/day computes each workflow's expected trigger from cadence (incl. the deliberately-silent weekday season card); auto-dispatches recovery |
| 10 | Owner can override a wrong machine decision | ✅ Done | `--lock/--unlock/--list-locks`; Slack-native reply grammar (`--check-replies`, per-question threads); `reconcile_calendar.yml` operator lock input so a fix is not gated on JP typing in Slack. **Limit:** that input locks an event already on the date; it cannot *move* one (only the Slack reply path routes through `_apply_edgar_auto_correction`) |
| 11 | Pre-earnings briefs / prediction accuracy tracking | 🟨 Partial | A **consensus preview** (`consensus_preview.py`, built 2026-07-12 to a StreetAccount-derived spec, not to PLAN.md Phase 5) posts daily from CI to `#street-account` + `#portfolio` in a fixed section order (`EARNINGS_CARD_FORMAT.md`). PLAN.md Phase 5 as written (8-quarter beat/miss history, T-1 brief) and Phase 6 (predictions) are unbuilt. Whether the preview retires Phase 5 is not stated anywhere |
| 12 | Browsable public calendar of confirmed dates, past + future | ✅ Live | `scripts/build_calendar_page.py` → `docs/index.html`, rebuilt by `daily_earnings_check.yml` and committed back only when data changed; Pages enabled 2026-07-24 (per `PROJECTS.md`; URL returned HTTP 200, checked 2026-09-17). Estimated dates badged and hidden by default |
| 13 | Owner's book: who has reported this season and what the stock did | ✅ Live (2026-08-09) | `season_progress.py` + `season_progress.yml` (weekday card, silent unless something is unsettled; Sunday card + forward calendar, unconditional) + `docs/season.html`. Scope = Portfolio + Researching (JP's choice). Reaction = move + sigma + vs SPY. Plus the once-per-season `#portfolio` "X reported" ping |
| 14 | Guidance / KPI / capital-allocation extraction from the release | 🟨 Built; consumed elsewhere | `guidance_parse.py` + `daily_summary.py` extract guidance ranges (with basis and prior-vs-new), year-ago figures, operating KPIs, puts/takes, buybacks — deterministic, no LLM. Rendered by the preview here and by `earnings_review`'s T+1 card. Verified against FIVE only (per the commit messages); `--daily-summary` itself is unscheduled |

**Overall: the v1 goal is met and the system is live.** The date-correctness and
no-silent-failure cores are mature and battle-tested against real incidents
(FIVE, UFPT, ICLR/ICON, calendar-TZ churn, the 2026-07-26 BOM coverage
collapse). Since the last brief the scope grew — TickTick reconcile, the season
lane, the preview's second destination, the extraction layer — and the weakest
link moved: the recent incidents were all in **persistence of the CI-artifact
database**, not in date logic. Open items are coverage-edge hardening (Tier-2
blind sweep), the unbuilt Phase 5/6 work, and #298 part 2 (guidance vs
consensus).

## 3. Key design decisions (and why)

1. **Three explicit systems of record.** Coverage Manager owns *which* names,
   Google Calendar owns *when* (published state), SQLite owns *workflow
   state + history*. Keeping these separate is what lets the calendar be
   self-healing (drift detection re-derives from Finnhub) without the DB and
   calendar fighting. TickTick is **not** a system of record: it is a projection
   of the DB, reconciled idempotently.
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
   date). The 6-K fallback for foreign filers goes through the same gate.
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
8. **TickTick gets no date until the date is confirmed.** JP 2026-07-24: *"I
   don't want ANY date in TickTick until the date is confirmed. Estimated dates
   are worse than nothing."* This superseded an "(est.)" title-marker design the
   same day. Unconfirmed names stay on the list, undated. The one-day move lives
   in the title but in **no DB column**, so the reconcile compares titles with
   the move suffix stripped and carries it across rewrites — otherwise the
   reconcile and the results run fight at three writes a day, silently.
9. **Tier 1 = ownership, not ownership ∩ Core.** A held name is Tier 1: *"Ownership
   is a fact about the book; Core is an editorial marker on the universe, and the
   two drift"* (LLY sat at Tier 3 for the whole 2Q26 season). Researching keeps its
   Core gate. Core biopharma joins Tier 2 **Core-gated** rather than by sector, or
   every clinical-stage shell would get quarterly tasks. `events.tier` is re-derived
   from coverage every run, **future-dated rows only**, so finished seasons are
   never restated.
10. **"Closed" is separate from "reported", and there is one definition of open.**
    An acquired company can never report, so schema v13 adds `closed_reason`;
    `reported` keeps meaning "the numbers came out". Every query goes through
    `storage.OPEN_EVENT_SQL`, pinned by a source-scanning test — *"a terminal state
    only SOME call sites honour is worse than none."* Only past-dated rows close; an
    empty coverage set closes nothing; a ticker that reappears **reopens** —
    *"a terminal state a blip can enter and nothing can leave is not a state, it is
    data loss."* Closed tasks are completed, never deleted. Calendar does not yet
    reflect closure (deliberate; §5).
11. **The DB is a CI artifact; guard its content, not its clock.** The restore takes
    the newest unexpired artifact on `main` by `created_at` and downloads it by id,
    and distinguishes an unreachable API from an absent artifact (conflating them
    turns an outage into a fresh-DB run that erases locks and watermarks). The
    rollback guard asks *"does any recent artifact hold MORE accumulated actuals than
    the copy we are about to write to?"* — actuals only accumulate, so a decrease is
    a rollback — and it **heals and continues** rather than refusing: *"refusing to
    write is what turned a quiet week into a three-day outage."* It replaced a
    `now - MAX(updated_at) > 24h` trigger that fired on every quiet week, because
    `updated_at` moves when an event *changes*, not when a run *writes*. Age is now
    advisory; "have the workflows run" is the watchdog's question. The same lesson
    was applied to the TickTick reconcile's freshness guard on 2026-09-17: it keys on
    a `last_sync_completed` heartbeat stamped only after a **non-empty** fetch. A
    failed guard's DB is no longer uploaded (`if: success()`, not `always()`).
12. **Coverage collapse is a hard stop, placed before the snapshot re-baseline.** A
    tier losing >20% AND ≥25 names aborts the run; exiting before
    `_alert_coverage_changes_if_needed` preserves the last-good snapshot. Both
    thresholds must trip. `COVERAGE_ALLOW_MASS_REMOVAL=1` is the override.
13. **The preview widens scope without touching the tier rule, and each destination
    owns its ledger.** `select_upcoming_reporters` gained a Position arm that only
    ever widens, because a dozen consumers read `coverage.py`'s tier rule and
    re-scoping there would silently re-scope all of them. `#street-account` and
    `#portfolio` each keep their own posted-ledger: one shared key means a failure on
    either is remembered as a success for both.
14. **Extraction is deterministic and abstains rather than guesses.** No LLM in
    `guidance_parse`. Anything unparsed renders verbatim, so the layer only adds
    structure. Basis (GAAP vs adjusted) is load-bearing — consensus EPS is adjusted,
    and pairing across bases gave FIVE +418% y/y where the truth is +107%. Period
    ends and fiscal-year-end are read from the filing, never derived; KPI context
    returns `""` rather than a fabricated denominator. The year-ago base comes from
    the release because the DB's rolling window holds no comparable quarter.

## 4. Non-goals / accepted tradeoffs

- **Not a real-time feed.** It's batch (daily/weekday cron + post-earnings
  sweep). Intraday Finnhub changes are caught by the afternoon redundancy run and
  the 3×/day reconcile, not instantly.
- **Not a fundamentals/valuation engine.** It reports the *event* (date, timing,
  consensus, beat/miss, stock move) and extracts what the release *states*
  (guidance, KPIs, capital allocation). Judging the result is the owner's job.
  The T+1 review card composing all of this lives in `earnings_review/`, not here.
- **Tier 3 is digest-only by design** for Calendar and TickTick, to keep the long
  tail from flooding the surfaces the owner acts on. (The preview's Position arm
  is the one deliberate exception: Researching names get a preview whatever their
  tier.)
- **The `#portfolio` ping carries no move number** — deliberately. *"A number makes
  this a second, thinner results card competing with the real one."* It fires once
  per name per season on its own watermark, separate from the season table's.
- **The public season page publishes composition, never sizes.** JP cleared
  publishing which names are in the book (*"Its fine to have my portfolio public"*);
  share counts, cost basis and P&L must stay absent.
- **Local runs of anything that reads `earnings_events.db` are wrong by
  construction.** The DB is gitignored and exists only as an Actions artifact, so
  the season lane and the reconcile run in CI. A local `--lock` writes a throwaway
  copy.
- **Gmail draft creation stays MCP/local-only** (CI can't run the Gmail MCP); the
  weekly workflow uploads `last_digest.html` as an artifact instead.
- **`--dry-run` is intentionally NOT side-effect-free** — it still seeds the
  SQLite DB (events + estimates), which the CI populate steps depend on, and it
  stamps the sync heartbeat. `--populate-db-only` is the self-documenting alias.

## 5. Known gaps / candidate next steps (feedback welcome here)

- **EDGAR blind sweep is Tier-1 only.** A Tier-2 name that *both* Finnhub and FMP
  miss entirely would still go unreported. Extending the blind sweep to Tier 2 is
  the obvious next hardening (cost: more SEC requests/run).
- **Foreign-filer detection is a filename heuristic.** `is_likely_earnings_6k_doc`
  keys on tokens in the 6-K's `primaryDocument`; an issuer with uninformative
  filenames falls back to Finnhub/FMP/yfinance only.
- **Pre-earnings briefs (Phase 5) and prediction/accuracy tracking (Phase 6)** are
  unbuilt as PLAN.md specifies them; the consensus preview covers part of Phase 5's
  ground. Estimate-snapshot history is accumulating to support Phase 6.
- **#298 part 2 (guidance vs Street consensus) is not built.** The annual consensus
  fetchers exist in `consensus_preview.py` and nothing consumes them — see
  `PLAN_298.md`. Currency must come from `income-statement.reportedCurrency`, never
  `profile.currency` (NVO: USD vs DKK).
- **Closing an event does not reach Google Calendar.** A closed event's entry still
  reads "Date passed (results pending)". Declined on purpose while closes are
  strictly past-dated; worth building if a future-dated close becomes possible.
- **TickTick mid-quarter tier changes** don't move the existing task (cross-list
  dedup blocks recreation); the next quarter's sync fixes it. Event tier itself is
  now restamped every run (§3.9); the task-list half of the limitation is as
  documented in `CLAUDE.md` and was not re-verified for this brief.
- **The operator lock input cannot move a date** (see §2 row 10).
- **TickTick token** expires ~180d and needs manual re-auth (401 is detected and
  logged, not auto-refreshed).
- **No formal SLO/event-color tracking** (the remaining slice of PLAN.md Phase 7).
- **Calendar page history is only as deep as the DB's rolling window**, so "past
  earnings" on `docs/index.html` is not a full archive. A separate append-only
  history table would be needed for multi-year depth.
- **Extraction has one verified issuer.** The guidance/KPI/buyback parsers were
  built and checked against Five Below's filings; every defect found so far
  (loss signs, cumulative periods, expense lines read as revenue, prior-column
  mis-pairing) was wrong in a plausible direction. Breadth across issuers is
  unmeasured.

## 6. How to evaluate

- **Entry point + all top-level flows:** `main.py` (CLI modes documented in
  `CLAUDE.md` "CLI modes"; workflow schedule in `CLAUDE.md` "Scheduled
  workflows"). `earnings_agent.py` is a legacy delegator.
- **Core logic most worth scrutiny:**
  - Date arbitration & EDGAR auto-correct: `main.py` (`run`, `run_cross_check`,
    `_apply_edgar_auto_correction`, `_edgar_date_corroborated`) + `edgar_client.py`
    (incl. `find_results_6k`).
  - Calendar move/recreate invariant: `calendar_sync.py` `_move_calendar_event`
    and the drift/TZ helpers (`calendar_event_drift_kind`, `_wall_clock_et`).
  - Provider merge: `main._fetch_earnings_source` + `fmp_client.py` /
    `finnhub_client.py` (note the fail-fast on cap hits — do not soften).
  - Persistence safety: `storage.py` (migrations, `OPEN_EVENT_SQL`, phantom
    guard, upsert carry-over of task pointer + question state, sync heartbeat)
    and the CI artifact pair `scripts/ci_restore_db_artifact.sh` +
    `scripts/ci_db_rollback_guard.sh`.
  - TickTick projection: `ticktick.py` `reconcile_ticktick_tasks` (confirmed-only
    dating, move-suffix carry, closed-task completion).
  - Preview + extraction: `consensus_preview.py`, `guidance_parse.py`,
    `daily_summary.py` (the latter two are also imported by `earnings_review/`).
- **Tests (do not need network/Calendar/TickTick):** the `test_*.py` files at the
  repo root; `python -m pytest -q` is the authority on the count
  (`pip install -r requirements-dev.txt` first). `tests.yml` runs the same on
  every push. Several regressions are source-scanning tests that pin query shape
  or call order — read the failure message before "fixing" one.
- **Most useful feedback:** (a) any silent-failure or silent-misdate path the
  safeguard stack still leaves open — that's the whole thesis — with **the
  CI-artifact DB lifecycle (restore → guard → write → upload) first**, since
  that is where the last three incidents were; (b) extraction values that are
  wrong in a plausible direction (sign, basis, period, scale) — they reach a card
  JP reads; (c) whether the EDGAR corroboration-gated auto-lock is the right risk
  tradeoff vs. a stricter or looser rule; (d) which §5 gap is worth doing first
  given a solo part-time owner.
