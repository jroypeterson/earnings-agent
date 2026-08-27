# Board #298 — Biopharma category + guidance-vs-consensus square

**Status: parts 1 and 3 SHIPPED (`5d11830`). Part 2 (the square) is NOT built.**
Row logged `partial`, so it stays open.

JP, 2026-08-06. Ease 3 × Impact 5 = 15 — his biggest earnings ask that week.

---

## The ask, in three parts

1. **Biopharma gets its own results category** — it fell into "Other". ✅ shipped
2. **A guidance-vs-consensus coloured square** — RED when updated guidance is below that
   fiscal year's Street consensus on *either* revenue or EPS; GREEN only when every reported
   metric is at or above forward estimates. Worked example: *"guidance cut to $1B against $2B
   consensus"*. ❌ **not built**
3. **A legend** naming the squares. ✅ shipped

---

## Ownership — settled, do not relitigate

**Build wholly inside `earnings_agent`. Do NOT consolidate with `earnings_kpi`.**
Two independent reviews (Fable on architecture, Codex on the plan) reached this separately.

The deciding argument is plumbing, not philosophy: **`earnings_kpi` has no git remote** while
this renderer runs in GitHub Actions, so a local-only producer cannot feed a CI consumer. Its
batch clock would also render the square grey on print day — the only day it matters.

`mgmt_credibility` is not the owner either; its brief explicitly disclaims consensus
comparison (*"a deliberate deferral, not an oversight"*).

Codex's refinement, adopted: guidance extraction belongs in a shared internal **`guidance.py`
service called by BOTH result paths** — not as a dependency of the narrative module.

---

## What already exists (verified, not assumed)

- `daily_summary.py` — a **live deterministic guidance extractor**: `extract_guidance_lines()`,
  `_GUIDANCE_PERIOD/_VERB/_NOISE`, and `_guidance_rank()` which already ranks a *revision*
  above a *reaffirmation*. It produces guidance **sentences for a narrative card**, not
  structured values.
- `consensus_preview.py` — **`fetch_fmp_annual_estimates()`** and
  **`fetch_fmp_reported_currency()`** (`c2a965e`). Forward annual revenue + adjusted EPS,
  avg/low/high, analyst counts. 14 offline tests. **Nothing consumes them yet.**
- `notifications.py` — `_RESULT_MARKER_SLOTS` is the single source for the marker row; adding
  a 4th slot makes it appear in the line *and* the legend automatically.

## Three verified blockers

1. **`edgar_client.fetch_release_document` DOES NOT EXIST.** `daily_summary.attach_release()`
   (`daily_summary.py:415`) calls it, so that lane raises `AttributeError` the moment it gets
   past the filing lookup. Unnoticed because `--daily-summary` is not scheduled.
   **This is a live latent bug independent of #298.**
2. **`attach_release()` only searches 8-Ks** and ignores 6-K filers, so NVO never reaches
   extraction — despite `edgar_client.find_results_6k()` existing at `edgar_client.py:326`.
3. **Nothing routes guidance into `ResultRow`.** `daily_summary` owns `SummaryRow`; results are
   built at `main.py:410` / `main.py:2529` and rendered at `main.py:2216` / `main.py:2665`,
   none of which touch `daily_summary`. The workflow also stages only `upcoming_events.json`
   and `consensus_preview.json` (`daily_earnings_check.yml:288`), so a new export would be
   ephemeral on the runner.

---

## The correctness traps — each measured, not hypothesised

1. **Scope is the one most likely to ship silently wrong, and JP's own example is its shape.**
   *"Full-year 2026 **oncology product** revenue of $1.0B"* parses as revenue, matches FY2026
   and USD, and compares against $2.0B **consolidated** consensus. It passes every period,
   currency, basis and analyst-count gate and emits a confident, meaningless RED.
   → A `scope` field (`consolidated|segment|product|organic|unknown`) is **mandatory**;
   anything but consolidated must abstain.
2. **GAAP vs adjusted.** FMP `epsAvg` is adjusted-only. UNH FY2024: **27.66 adjusted vs 15.51
   GAAP diluted** — a fabricated 44% "miss". Companies guide on both bases in one release.
   `gaap|adjusted` is itself too coarse for EPS (diluted, continuing-ops, core, company-defined).
3. **Currency, and a field that lies.** FMP `analyst-estimates` carries **no currency at all**,
   and `profile.currency` returns **USD for NVO** — the ADR's *trading* currency — while the
   company reports in **DKK**. Only `income-statement.reportedCurrency` agrees with the figures.
   Already handled in `fetch_fmp_reported_currency()`; a comparison must **abstain** when it is
   None, never assume USD.
4. **Fiscal ≠ calendar.** MCK's FY ends 2027-03-31. Key on the fiscal period end. The
   `AnnualConsensus` dataclass deliberately exposes no calendar-year field.
5. **Thin coverage.** QDEL returns **1 EPS analyst**. A one-analyst "consensus" must not drive
   a red square; the floor must be visible, not a silent degrade.
6. **Guidance is a RANGE with an operator** ("at least $X", "up to $X", "$1.0–1.2B"), consensus
   is a point. `is_range` + midpoint cannot encode this — **store the bound operator**.
7. **Reaffirmation ≠ update.** "Updated guidance" means a *change*. Needs prior state, and
   prior state needs `prior_low`/`prior_high` — old $1–$3 → new $1.5–$2.5 is *narrowed*, not a
   scalar raise or cut.
8. **Consensus must be snapshotted BEFORE the release.** Analysts revise toward guidance within
   hours; a post-revision comparison dilutes the exact signal the square exists to show. The
   morning workflow runs ~7:13 AM ET, after a 6:30 AM release. **There is no vendor history, so
   a prerelease point not captured is lost forever** — this is the piece that is lossy if
   deferred. `estimate_history` (`storage.py:172`) is quarterly and carries no fiscal period
   end, basis, currency, analyst count or source; it is not a substitute.
9. **Observation identity.** Multiple quarterly releases update the same FY metric. A record
   needs earnings event id, filing accession, release timestamp and a source hash, or a rerun
   attaches a Q3 cut to a historical Q2 result.
10. **Grey is one-shot.** After a successful post the event is marked reported
    (`main.py:2690-2715`) and never re-rendered, so a late EDGAR fetch cannot correct a grey.
    Either accept and document permanent grey, or define a deduplicated follow-up path.

---

## Square semantics — refusal-first, asymmetric

Both reviewers converged here. Precision over recall: a square that fires rarely and is never
wrong beats one that fires often and is sometimes wrong — it sits beside three deterministic,
trusted squares and would launder its uncertainty into their credibility.

- **RED** only when the guidance range's **high end** is below consensus, on matched basis,
  matched currency, matched fiscal period, analyst floor met, scope = consolidated.
- **GREEN** only when the range's **low end** is at or above consensus under the same gates.
  (Not the midpoint: $80–$120 against $100 has half the range below consensus.)
- *"At least $X"* can be GREEN, never provably RED. *"Up to $X"* can be RED, never provably GREEN.
- A bare **reaffirmation renders neutral**, not green.
- **GREY otherwise, carrying the reason** — never collapsed into one ambiguous glyph.
- If the square measures direction *as well as* level, the legend must say so: a raise from
  $0.8B to $1.0B against $2.0B consensus is an updated outlook wholly below Street.

## Extraction

Deterministic parsing, not free LLM extraction. An LLM may **propose** candidate records, but
numbers, units, basis, scope, period and quote spans must be **deterministically revalidated**;
LLM outage or validation failure yields GREY. Otherwise same-day CI becomes nondeterministic
and "never silently wrong" is false.

Source: **EX-99 press releases**, not transcripts. Transcripts lag the call by hours-to-days
(killing the same-day square) and state guidance as spoken paraphrase rather than a table.

⚠ Specific things in the existing extractor that mislead when asked for structured values:
`_GUIDANCE_PERIOD` (`:119`) cannot derive an off-calendar fiscal period end; `_GUIDANCE_VERB`
(`:125`) accepts broad `expects`/`projects` without proving consolidated scope;
`_GUIDANCE_REVISION` (`:149`) treats `narrowed`/`updated`/`now expects` as equally directional;
`_guidance_rank()` (`:162`) prefers a *segment* revision over a consolidated reaffirmation; the
table/length rejection (`:229-231`) discards the source most likely to hold exact low/high and
basis labels; the 80-char dedupe (`:236-239`) can collapse separate EPS and revenue sentences;
the four-line cap (`:244`) lets segment noise crowd out consolidated guidance.

---

## Resume here — suggested order

1. **Prerelease annual-consensus snapshot table + collector.** The only lossy-if-deferred piece.
   Needs fiscal period end, basis, currency, analyst counts, source, snapshot timestamp.
2. **Fix `edgar_client.fetch_release_document`** (blocker 1) and extend `attach_release()` to
   6-K filers (blocker 2). Worth doing regardless of #298.
3. **`guidance.py`** — structured extraction + the assessment function, pure and unit-tested,
   with `scope` and bound operators. Assessment returns GREY until records exist.
4. **Wire into `ResultRow`** via `main.py` and stage the export in the workflow (blocker 3).
5. **Add the 4th slot to `_RESULT_MARKER_SLOTS`** — line and legend update together.
   Keep it behind a feature flag until a real-release acceptance corpus passes.

## Review record

Plan reviewed by Fable (architecture) and Codex (technical). Code reviewed by Codex over four
adversarial rounds, ending with **no Critical and no High**. Seven defects found in review,
**none by the test suite**; three were pre-existing. Saved output in `codex_feedback/`
(`2026-08-26_*`). Seven mutation runs, each reverting one fix, are all caught by the suite.
