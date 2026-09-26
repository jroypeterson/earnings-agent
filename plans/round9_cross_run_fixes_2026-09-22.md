# Plan — earnings_agent #298 Phase A, Codex round 9 fixes (2026-09-22)

**Status:** PLAN, awaiting Fable review. No code written yet.
**Branch:** `overnight/298-phase-a` (19 commits, rebased on main, 651 tests green)

Round 9 used a deliberately new lens — *run the schedule forward* — after eight rounds of
single-run correctness review. It returned four Highs, all cross-run. I verified each against
the code before writing this; verdicts and evidence below.

---

## H1 — CONFIRMED AND REPRODUCED. The long-move guard cannot fire across a calendar-quarter boundary.

**What I verified, by calling the real functions:**

```
prior 2026-09-01 -> event 2026-10-20   gap 49 days
storage.date_to_quarter: 2026-09-01 -> "2026Q2"   2026-10-20 -> "2026Q3"
consensus_snapshot._same_cycle(production labels) -> False    <- guard SILENT
consensus_snapshot._same_cycle("2026Q3","2026Q3") -> True     <- what the test asserts
```

`_same_cycle` has two arms: within `SAME_CYCLE_DAYS` (45) always true; beyond that up to
`MOVED_SAME_QUARTER_MAX_DAYS` (135) only when a *strong* prior row's quarter label **equals**
the upcoming event's. Production derives that label in one place — `main.py:972`,
`quarter = date_to_quarter(earnings_date)` — and `date_to_quarter` is a pure month mapping
whose own docstring says it is "a rough mapping ... used for display purposes".

**So the 135-day arm is dead precisely for the case it was written for.** A report that slips
from September to October changes calendar quarter, so the labels differ and the arm returns
False; the 45-day arm has already lapsed at 49 days. The run then stores **post-print
consensus as pre-print**, which is the one thing this module exists to prevent.

**And its regression test cannot see this.** `test_a_confirmed_same_quarter_move_blocks_beyond_45_days`
(test_consensus_snapshot.py:1194) passes `quarter="2026Q3"` to **both** `upsert_event` calls —
a label production would never produce for a September date. The test docstring even narrates
the 09-01 -> 10-20 move. It is green against the defect. Fleet class:
**a-check-that-silently-matches-nothing**.

### Options considered

- **(A) Widen `SAME_CYCLE_DAYS` past 135.** Rejected: it would suppress *legitimate* snapshots
  for any name whose previous print was <135 days ago, i.e. nearly every quarterly reporter.
  It trades a rare false-negative for a constant false-positive.
- **(B) Treat adjacent quarter labels as same-cycle.** Rejected: "adjacent" is arithmetic on a
  label that is already the wrong abstraction, and it silently merges genuinely different
  cycles for an odd-fiscal-year filer. It makes the rough mapping load-bearing in a second
  place instead of removing the dependence.
- **(C) [PROPOSED] Stop keying cycle identity on the display label.** The row itself already
  carries stronger evidence: a *recorded confirmed move* (`_record_moves_later`, added round 4)
  links old date -> new date explicitly, and a surviving open same-quarter row is the other
  path. Use the recorded move as the same-cycle proof for the long arm, and fall back to the
  label only when both labels exist AND are equal (today's behaviour), so nothing that works
  now stops working.

### Invariant it must hold

> If a name's prior event in the same reporting cycle has already printed, no consensus
> snapshot is stored for the upcoming date — **regardless of which calendar quarters the two
> dates fall in.**

### How I will know it worked

- A new test driving `snapshot_annual_consensus` end to end with **production-derived**
  quarters (dates chosen so the labels genuinely differ), asserting `skipped_open_prior`.
- **Mutation check:** revert the `_same_cycle` change and the new test must fail while the old
  one still passes — that is the proof the old test was vacuous.
- The existing test is **rewritten, not deleted**, to stop hand-assigning labels.

---

## H2 — REAL GAP. "Restore succeeded" and "no artifact existed" are the same exit code.

`scripts/ci_restore_db_artifact.sh` exits **0** with `"no unexpired artifact ... treating as
bootstrap"` when the listing is empty, deliberately, because the abort-if-missing step owns
that case for the MAIN database (`EA_DB_BOOTSTRAPPED=true`).

The **side** artifact (`consensus-snapshots`, `EA_DB_VERIFY_TABLE=consensus_snapshot`) has no
equivalent owner. An empty successful listing therefore produces: no file, no merge, an export
of only the current DB's table, `exported=true`, and an upload that becomes the newest
side artifact — **resetting cumulative snapshot history**. The workflow's three alert
predicates all read `steps.*.outcome`, which is `success`, so none can fire.

**Proposed:** give the side-artifact restore the same explicit bootstrap contract the main DB
has — an env flag (`EA_SNAPSHOTS_BOOTSTRAPPED`) that turns "listing empty, post-bootstrap"
into a loud failure, and make the export step refuse to upload a non-cumulative file when the
restore reported bootstrap rather than a real restore.

**Alternative rejected:** infer it from row count (an empty table is legitimate in a first
season). A count cannot distinguish "nothing yet" from "history lost".

---

## H3 — VALID CONCERN, cheap. The selector reads one page.

One `gh api ...?name=&per_page=50` call, then filter (`expired==false`, `head_branch==main`)
and sort **within that page**. Filtering after the fetch means a page full of other-branch or
expired rows can push the true newest onto page 2.

Honest probability: **low.** GitHub returns this endpoint newest-first in practice, and this
script is itself the fix for a *real* stale-artifact incident (2026-09-08), so the current
behaviour is empirically working. But the script's own comment claims selection is
"deterministic ... nothing to select wrongly", and that claim stops being true past page one —
**prose-outliving-the-artifact**.

**Proposed:** follow pagination until a page yields no new candidates, capped
(`EA_DB_ARTIFACT_MAX_PAGES`, default 4). Keep `per_page` at 50. Do **not** rely on API
ordering — the existing sort already does not.

---

## H4 — REAL, needs a human trigger. A re-run of an old workflow is accepted as newest.

Artifacts are filtered by `head_branch` only; nothing records which *code* wrote one. A
GitHub re-run of a pre-Phase-A workflow runs the old `upsert_event` (no `_record_moves_later`),
deletes the moved row, and uploads with a newest `created_at`. Accumulated actuals are
unchanged, so the rollback guard passes it.

**Proposed:** stamp a writer protocol version into the DB (`kv_store.writer_protocol`) at
write time and have the restore **refuse to select** an artifact whose protocol is older than
the running code's, falling through to the next candidate. This is an identity check, not a
time check — the same shape as the actuals-based rollback guard, which already rejects
"newest" when content says otherwise.

**Scope note:** this is the largest of the four and the only one requiring a new persisted
field. If Fable judges it too big for this branch, it becomes its own board row rather than
riding a Phase-A merge — say so.

---

## What I am explicitly NOT doing

- Not touching `date_to_quarter` itself. It is a display label with many consumers; changing
  its semantics to serve one guard is how a local fix becomes a fleet-wide incident.
- Not widening any window constant.
- Not editing any Codex verdict or gate to make a round read clean.

## Order of work

1. H1 (correctness, and the vacuous test) — the only one that silently produces wrong data.
2. H2 (a silent history reset).
3. H3 (cheap hardening).
4. H4 — implement, or split to its own row on Fable's call.

Then Codex round 10 on the result, per the no-Critical/High exit condition.

---
---

# REVISION 2 — after Fable's plan review (2026-09-22, ~00:50)

**Fable returned 2 Critical + 1 High, all in this plan's own mechanism rather than in the
diagnosis of symptoms. Everything above this line is superseded where it conflicts.** The
original is kept for provenance: the record of a fix that could not have worked is worth more
than a tidy document.

## What Fable killed, and why it was right

**Critical 1 — option (C) cannot fire either.** The recorded move is keyed on the **new** row's
label: `storage.py:866` passes the label `main.py:972` computed for the new date, and
`storage.py:919-925` selects `WHERE ticker = ? AND quarter = ? AND event_date < ?`. So a
confirmed 2026-09-01 (`2026Q2`) followed by a 2026-10-20 (`2026Q3`) upsert is **neither deleted
nor recorded**. A move record exists only when the labels already match — exactly the case
where today's comparison already returns True. **(C) is a strict subset of the path it was
meant to back up.** Fable reproduced this against real `init_db` / `upsert_event`.

And my own success criterion would have inverted: a new test with production labels asserting
`skipped_open_prior` fails *before and after* (C), because neither evidence path exists. The
implementation-time temptation would then be to seed a move record by hand in the fixture —
the same vacuity one layer down.

**It also corrected my harm estimate.** `_cycle_floor` (`consensus_snapshot.py:153-171`) already
floors at 00:00 ET the day after the most recent **reported** print, and
`pre_release_snapshot_set` caps at `taken_at < cutoff`. So a snapshot taken after a *reported*
09-01 print is never served for that release. The genuinely harmful sub-case is narrower: the
09-01 print was **not recorded on the 09-01 row** and the actuals later attach to the
cross-label duplicate. The invariant must say that, or it gets defended in the wrong place.

**And it measured why no window constant works.** Over 700 consecutive reported-print pairs in
the local DB: **9 pairs under 60 days, 3 under 50** — REPL 46d, OKYO 47d, NBP 30d. The 49-day
phantom is indistinguishable by gap from REPL's real 46-day Q1→Q2 print. That kills option (A)
at any constant and any "adjacent label" rule.

**Critical 2 — H4's stamp is inert or an outage.** A re-run restores a *new-code* artifact, runs
old code that never touches `kv_store.writer_protocol`, and re-uploads it with the stamp intact
— `a-recorded-flag-that-nothing-enforces`. Reject unstamped instead and the first run after
this branch merges falls through every candidate to "bootstrap", where
`EA_DB_BOOTSTRAPPED=true` aborts. That is the latch shape the guard's own comments warn about.

**High — H3's stop condition reproduces H3.** "Until a page yields no new candidates" stops at
page 1 in precisely the scenario H3 names (a page full of expired/other-branch rows yields zero
candidates *after the filter*).

## REVISED SCOPE — what I will actually build

**H1 — ship the honest state, not an arm that cannot fire.**
- Rewrite `test_a_confirmed_same_quarter_move_blocks_beyond_45_days` to use
  **production-derived** labels (`date_to_quarter`), so it exercises the world production
  actually produces instead of a hand-labelled one.
- The rewritten test **asserts the hole**, marked `pytest.mark.xfail(strict=True)` so it flips
  loudly the day the real fix lands rather than sitting green over a gap.
- Add a named residual comment beside the existing one at `consensus_snapshot.py:309`.
- **File a board row** for the real fix: both vendors supply a fiscal period and the code
  discards it (`finnhub_client.py:110-113` passes raw; `fmp_client._normalize:54-63` rebuilds
  the dict and drops FMP's field). That is a v15 column touching `upsert_event`'s same-quarter
  DELETE — a project, not a round-9 patch. Fable's caveat carried forward: no fixture holds the
  raw Finnhub fields, so verify against a live response before building on it.
- **Explicitly NOT shipping option (C).**

**H2 — one mechanism, not two.** Add a generic `EA_DB_REQUIRE_ARTIFACT=true` to
`ci_restore_db_artifact.sh` so an empty listing exits non-zero for a caller that declares the
artifact mandatory. Do **not** add a second refusal in the yml: `persist_snapshots` already
gates on `steps.restore_snapshots.outcome == 'success'`, so a non-zero exit is sufficient and
both alert steps fire on it.

**H3 — correct the stop condition.** Page until the **raw** page is shorter than `per_page` or
empty, capped by `EA_DB_ARTIFACT_MAX_PAGES` (default 4). Filter *after* paging, never as the
loop condition. Fixture: **page 1 all expired, page 2 the live artifact** — a fixture with
candidates on page 1 passes against the wrong loop and proves nothing.

**H4 — two lines per workflow, and split the rest.** Pin `actions/checkout` with `ref: main` in
the four uploading workflows. All four are `schedule` + `workflow_dispatch` only, so on every
normal trigger `github.sha` is already main's tip and this is a no-op — but a re-run of an old
run then executes *current* main code, removing H4's mechanism outright. Source-scan-testable
like the existing yml tests. The run-id-bound stamp becomes its own board row if wanted later.

## Revised invariant

> A consensus snapshot is never stored for an upcoming date when the same reporting cycle has
> already printed **and that print is recorded on the row the snapshot is keyed to**. Where the
> cycle cannot be identified — because the only available identity is a calendar-month display
> label — the gap is **named in code and asserted by a strict-xfail test**, not papered over
> with a rule that cannot fire.

## How I will know it worked

- H1: the rewritten test fails (xfail-strict) against today's code and would XPASS the day a
  real cycle identity lands. No hand-assigned labels anywhere in it.
- H2/H3: new shell tests with the `gh` stub, page-1-all-expired fixture, mutation-checked —
  removing the pagination or reverting the stop condition must fail at least one.
- H4: a source-scan test asserting all four uploading workflows pin `ref: main`.
- Full suite green (651 today), then Codex round 10 with a fresh lens.

---
---

# REVISION 3 — after Fable round 2 (2026-09-22, ~01:45)

**Round 2: one High, no Critical.** H2, H3 and H4-as-revised cleared. The High is in the
revised H1 and Fable prescribed the remedy, which is adopted verbatim below.

## The High, and why it kills the xfail

I proposed marking the rewritten test `xfail(strict=True)`. Fable found the **mirror test**:
`test_a_different_quarter_row_54_days_back_does_not_block` (`:1270-1276`) hand-labels
2026-07-25 `2026Q2` and 2026-09-19 `2026Q3` and asserts the snapshot **proceeds** — but
production maps *both* to `2026Q2`, so under real labels that scenario **blocks**. Two tests,
the same impossible world, opposite assertions.

It swept the block: **9 of 11 `_qevent` calls (`:1263-1320`) carry a label `date_to_quarter`
would not produce, and all 15 `upsert_event` calls in the round-5..8 tests pass a literal
`quarter=`.**

**So the gap has no correct expectation on today's inputs.** REPL's real 46-day 06-29 → 08-14
cross-label print must NOT block; the 49-day phantom must. Nothing in the current columns
separates them. An XPASS could therefore only be reached by a rule that *also* blocks REPL —
**the day the xfail flips green it certifies a regression on a working path.** `xfail(strict=True)`
suits a gap with a known correct expectation; this one has none until a fiscal identity exists.
That answers my own Q1: it does not merely read as handled, it aims the future fix at the wrong
target.

## REVISED H1 — adopted exactly as prescribed

1. **Delete** `test_a_confirmed_same_quarter_move_blocks_beyond_45_days` (`:1186-1198`). It
   tests a world production cannot produce and there is nothing honest to rewrite it *to*.
2. **Rewrite** `:1270` with production-derived dates that genuinely cross a boundary
   (06-28 → 08-22, labels `2026Q1`/`2026Q2`, 55 d) as a **plain green test** pinning today's
   behaviour, its docstring naming it as the REPL case and the phantom twin as the residual,
   citing the board row.
3. **Add the property test the sweep shows is missing:** no test in this file may pass a label
   that disagrees with `date_to_quarter(date)` — `_qevent` derives the label and refuses an
   override, plus a source-scan for a literal `quarter="` in `upsert_event` calls. *Sweep the
   tests too*: one rewritten test beside nine hand-labelled neighbours re-drifts next round.
4. **Acceptance criteria onto the board row:** the fix must key on a vendor fiscal identity, not
   on any rule over current columns.
5. **NEW, from Q3 — measure the incidence.** In `_select_window`, when a *kept* ticker had a
   strong, past-cutoff prior beyond 45 days with a differing label, log a WARNING naming both
   dates and the board row. It fires only on real occurrences (incidence-measured, never
   always-true), so after one season the CI logs price the v15 row with a real count instead of
   a hypothesis. Today `:340` logs only the blocked set, so the residual's incidence is invisible.

## H4 — better spelling, same cost

`ref: ${{ github.ref }}`, **not** `ref: main`. `actions/checkout` pins to `github.sha` only when
`ref` is empty; an explicit ref takes that branch's tip at checkout time. So a re-run of an old
main run gets current main (H4 closed) **and** a branch dispatch still gets the branch — which
`ref: main` would silently remove (measured: 0 of the last 100 runs were on a non-main branch,
so it costs nothing today, but removing a path for free is not free later). Commit-back verified
safe: gated `if: github.ref == 'refs/heads/main'` (`daily_earnings_check.yml:425`), and nothing
in the four workflows reads `github.sha`. The source-scan test asserts that exact value.

## H2 / H3 — carried notes

- **H3:** the `gh` stub must honour the `page=` query parameter or page 2 replays page 1. With
  the "raw page shorter than `per_page`" stop the test still fails correctly either way, but a
  stub ignoring `page=` cannot *prove* page-2 selection.
- **H2:** before setting `EA_DB_REQUIRE_ARTIFACT=true` on the side-artifact restore, confirm with
  one listing that a `consensus-snapshots` artifact exists on main — otherwise the first run
  under the flag is a self-inflicted failure.

## Status

H2/H3/H4 are **cleared to implement**. H1 as revised above is sent for round-3 confirmation;
implementation of H1 waits on it, the other three do not.
