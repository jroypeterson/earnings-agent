# RESUME — earnings_agent #298 Phase A

**Written 2026-09-24 so this branch can be picked up cold.** Board row `#405`
(pinned) is the parent: five `overnight/*` branches behind the Codex gate, of
which this is one.

## State in one line

**2026-09-25: round 20 came back CLEAN on `f6df191`. See the round-20 section.**

Branch `overnight/298-phase-a`, pushed, NOT merged, 681 tests green
(as of round 16, 2026-09-25). Merging is blocked on a clean review round.

```
cd earnings_agent && git log --oneline origin/main..overnight/298-phase-a
```

## The standing instruction

JP, 2026-09-23: *"if it comes back clean then merge"* — conditional on a Codex
round returning **no Critical/High**. Rounds 13, 14 and 15 each came back with
findings, so nothing has been merged. That instruction is still live: a clean
round authorises the merge without asking again. `main` merges cleanly
(verified, 0 conflicts; `main` carries one CI exports-refresh commit).

## Where it actually stands: rounds 9 → 15, seven for seven

**Every round has found the defect inside the previous round's fix.** That is
the single most important fact about this branch and it should drive whatever
comes next.

| round | what it found | status |
|---|---|---|
| 10 | 4 Critical + 2 High on the branch | all fixed |
| 11 | my cap guard refused a COMPLETE listing | fixed |
| 12 | offset pagination is mutable; walk trusted it | fixed |
| 13 | the round-12 check could not fire in production's shape (826 % 100 = 26) | superseded by the redesign |
| 14 | count-preserving replacement missed; concurrency gate had 3 escapes | fixed |
| 15 | 3 High: head not API-ordered, matrix escape, silent head-read failure | all fixed (`df6a447`) |
| 16 | 1 High: the invariant's first trip was fatal, and GitHub's expired-artifact purge (outside the group) trips it | fixed (`762bfc9`) |
| 17 | 1 High: the duplicate term deduped whole ROWS, so an id whose `expired` flag flipped mid-walk counted twice | fixed (`837bb20`) |
| 18 | 1 High: a single-source AMC label earned the 16:00 ET cutoff, so a mislabelled BMO print could be stored as pre-print | fixed (`5112685`) — **reverses a Fable-gated spec choice** |
| 19 | 6 High on the operator's view: 4 real (fixed `f6df191`), 2 declined as design | fixed / declined |
| **20** | **CLEAN -- "No Critical/High issues"** on code HEAD `f6df191` (peak-load lens) | **merge-eligible** |

A Fable gate between 13 and 14 produced the reframing that matters: **the
mid-walk listing mutation rounds 12–13 kept chasing is unreachable in
production**, because all four `earnings-db` uploaders share
`concurrency: group: earnings-db-writer` and each restores inside that same
serialized job. Measured: 4/4 uploaders, 0 job-level overlaps across 159 group
runs. The configuration IS the guarantee — which is why
`test_the_earnings_db_writers_are_SERIALIZED` exists and why round 14's gate
escapes mattered so much.

## Round 20 — CLEAN (2026-09-25 overnight, #460)

Lens: **peak load — the busiest day of the season at full scale.** Prompt/log:
`codex_feedback/round20_*`. Verdict, verbatim: **"No Critical/High issues"**; no
pre-existing Critical/High. Reviewed code HEAD `f6df191` (later commits touch only this
note). Its peak-load audit: a cold-cache 704-ticker run is ~1,408 FMP requests (~352 s at
4/s, under the 300/min cap); the 900 s budget trips before the 1,200 s step timeout, both
loud; ~38 GitHub REST calls for the busiest adjacent pair vs 1,000/hour; SQLite read/merge/
export linear and sub-second at 100k rows.

**JP's standing instruction ("if it comes back clean then merge") is now satisfied.**
Merge is the orchestrator's step, not done here. Two things JP should know: round 18
**reversed a Fable-gated spec choice** (the AMC cutoff now needs a corroborated session
label), and round 19 declined F1-rest / F4 / F5 as design (table below).

## Round 19 — `f6df191` (2026-09-25 overnight, #460)

Lens: **the operator's view — does every failure reach a human, is every alert true.**
Prompt/log: `codex_feedback/round19_*`. Verdict: **0 Critical, 6 High**, no pre-existing.

| # | finding | verdict |
|---|---|---|
| F1 | restore WARNING/DEGRADED are log-only; a failed post-walk count read was silent | **part real**: the silent count read now logs DEGRADED. Log-only warnings otherwise **declined — design** (round 15: advisory; completeness rests on the concurrency group) |
| F2 | `EA_CONSENSUS_BOOTSTRAPPED=TRUE` (or any non-`true`) left the guard unarmed AND silenced the reminder | **real, fixed**: script normalizes case/space; reminder fires on `!= 'true'` |
| F3 | HTTP-200 `[]` for every ticker counted as `empty`, run passed as healthy | **real, fixed**: ≥5 attempted with 0 ok fails the step |
| F4 | an all-blocked window returns before skip rows are written | **declined — Low**: a record gap, not an operator signal (skip rows alert nobody either way) |
| F5 | post-cutoff skips excluded from every failure signal | **declined — design** (Fable review H); the 07:13 ET capture precedes the 15:23 run |
| F6 | undeliverable partial-failure notice was a log line on a green step; its "retried next run" text was false on the last run | **real, fixed**: undeliverable now fails the step; text corrected |

Mutation-checked per file (main.py 2 kills, script 4, workflow 1). 689 green.

## Round 18 — FIXED in `5112685` (2026-09-25 overnight, #460)

Lens: **the data contract of stored rows.** Prompt/log: `codex_feedback/round18_*`.
Verdict: **0 Critical, 1 High**, no pre-existing Critical/High.

- **High — a same-day early release could be stored as pre-print.** `date_confirmed`
  confirms the date, not the session; a lone AMC label (420 of 480 Jul–Sep 2026 AMC
  events have no second opinion; where both exist they disagree ~1 in 60) earned the
  16:00 ET cutoff, so a mislabelled BMO print's day-of capture was served as pre-print.
  **Verdict: real, introduced by the branch.** Fix: 16:00 ET only for a CORROBORATED
  AMC (both hours = amc); otherwise 00:00 ET, so the prior day's capture is returned.
  ⚠ **This reverses plan v3 H1-NEW** ("NULL yf is no second opinion"), and the test that
  pinned it is inverted. Justification: JP's "a wrong number is worse than an absent
  one", and the cost is up to a day of freshness, not absence. One-line revert in
  `pre_release_cutoff` if JP disagrees. Mutation-checked (reverting fails 5 tests). 682 green.

## Round 17 — FIXED in `837bb20` (2026-09-25 overnight, #460)

Lens: **interruption and partial state.** Prompt/log: `codex_feedback/round17_*`.
Verdict: **0 Critical, 1 High**, no pre-existing Critical/High.

- **High — the duplicate-id term matched nothing when metadata changed.** `sort -u`
  ran over whole projected rows; the same artifact read on two pages with a
  different `expired` flag counted as two. Pages `[900,200 false]+[200 true,100]`,
  total 4 → every term silent. **Verdict: real, introduced by the branch** (the
  invariant is branch code). Fix: unique by the id column. Mutation-checked. 682 green.

## Round 16 — FIXED in `762bfc9` (2026-09-25 overnight, #460)

Lens: **the production runner environment vs the offline harness.** Prompt and raw
log: `codex_feedback/round16_prompt.md`, `round16_full_log.txt` (gitignored).
Verdict: **0 Critical, 1 High**, no pre-existing Critical/High.

- **High — the snapshot invariant could abort a healthy restore.** It exited 1 on
  its first trip. Two triggers sit outside the concurrency group: GitHub purging
  expired artifacts (the script said "EXPIRED ROWS NEVER LEAVE IT" — **measured
  false**: the live listing fell **830 → 444** between 2026-09-23 and 2026-09-25, all
  444 survivors unexpired), and the endpoint ordering an unchanged collection
  differently across page requests (inference from the API contract, not observed).
  **Verdict: real, introduced by the branch.** Fix: a trip fails the ATTEMPT and
  re-walks with fresh counts; only a trip on every attempt is fatal, with its own
  message. Mutation-checked (3 mutants, all killed). 681 green. Live restore rc 0.

## Round 15 — FIXED in `df6a447`, kept here for the reasoning

Full text: `codex_feedback/codex_feedback_2026-09-23_round15.md`
(raw log: `codex_feedback/round15_full_log.txt`).

⚠ **`codex_feedback/` is gitignored**, so those files exist only in this
working tree — they are NOT in git history. The tree is Dropbox-synced, so they
survive a session ending, but not a fresh clone. The findings are summarised
below in full for that reason.

1. **The "HEAD" is not an API-defined head.** `scripts/ci_restore_db_artifact.sh`
   around the `HEAD_BEFORE` / `HEAD_AFTER` comparison. A healthy listing can either
   evade the invariant or — worse — falsely stop every restore. ⚠ Price this
   one first: no restore step has `continue-on-error`, and this lane has
   already shipped **three** guards that became the outage they were written
   to prevent.
2. **The serialization gate treats a matrix as one job**
   (`test_ci_db_restore.py::test_the_earnings_db_writers_are_SERIALIZED`). GitHub executes each
   matrix expansion as an independent, parallel job, so a matrix uploader
   would race with the group satisfied. Fourth escape from that gate.
3. **A failed HEAD request silently disables the round-14 fix**
   (`ci_restore_db_artifact.sh:257` area; the `|| HEAD_AFTER=''` fallback) — no retry, no
   warning. This is the `|| true` pattern that was just fixed for the
   pre-walk count and reintroduced for the head. `a-check-that-silently-
   matches-nothing`, again.

Codex supplied a runnable reproduction for #3 (`test_round15_failed_head_read_
cannot_disable_invariant`); it currently fails, as it should.

## Before shipping ANY predicate on this lane — two rules that were learned expensively

1. **Run the shape matrix.** `scripts/`-external harness, recreate it from the
   commit message of `7215d81` if lost: evaluate the predicate against listing
   sizes 826/900/100/99/101/1 at per_page 100 and 3/4/5 at per_page 2. It must
   be SILENT on all of them and still fire on a real mutation. Rounds 10, 11
   and 12 each shipped a predicate nobody had run against the production
   shape — round 10's was true on 100% of runs, round 12's could not fire at
   any non-multiple.
2. **Mutation-test with one case per OR-branch.** The first run of the current
   invariant had four survivors because a single fixture tripped several terms
   at once. An OR-condition needs a case per branch or the other branches are
   decoration. Current state: seven mutants, each killed by a distinct test.

## Also live, deliberately not fixed here

Board **`#458`** — a 45-day Q4→Q1 gap silently loses the only pre-print
consensus snapshot (`_same_cycle`'s first arm). Pre-existing, reproduced twice,
out of scope for this branch: the obvious fix (exempt a printed prior) is
wrong, because a printed prior is exactly the evidence that a nearby upcoming
row is post-print. Needs its own design pass. Bites ~May 2027.

## Useful commands

```bash
cd earnings_agent
python -m pytest -q                      # 676 green
python -m pytest test_ci_db_restore.py -q  # 30, the restore/selection suite

# live end-to-end, writes only into a temp dir:
T=$(mktemp -d) && cd "$T" && GITHUB_REPOSITORY=jroypeterson/earnings-agent \
  GH_TOKEN="$(gh auth token)" EA_DB_RESTORE_TRIES=1 \
  bash ".../scripts/ci_restore_db_artifact.sh"
```

Reviews run from the FLEET ROOT, not this repo:
`cd "Claude Folder" && MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' wsl bash
scripts/codex_review.sh --prompt-file '<abs /mnt/c path>' --cwd '<abs /mnt/c path>'`
⚠ The wrapper has not been writing its banner file on these runs — capture the
output yourself and save it into `codex_feedback/`.

## The judgement call waiting for JP

Seven consecutive rounds have each found a real defect in the previous fix. The
findings are getting narrower (round 10: 4 Critical; rounds 13–15: 0 Critical),
but "merge when a round comes back clean" has not converged in three attempts.
Worth asking whether this branch should merge on a different basis — e.g. merge
the parts with no open findings and carry the restore-script hardening
separately — rather than continuing to gate the whole thing on one clean round.
That is a risk-appetite decision, not a technical one.
