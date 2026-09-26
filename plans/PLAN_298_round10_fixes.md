# Plan — clearing Codex round 10 on `overnight/298-phase-a`

**Date:** 2026-09-23 · **Branch:** `overnight/298-phase-a` (not merged, must not be)
**Review:** `codex_feedback/codex_feedback_2026-09-22_233355.md` — 4 Critical + 2 High.

Round 10's lens was *attack round 9's own fixes*. It worked: three of the four Criticals are
defects in tests written to close round 9, two of them the
`a-check-that-silently-matches-nothing` class — shipped inside the fix for that class.

## Gate history

| rev | verdict | outcome |
|---|---|---|
| v1 | **3 Critical + 3 High** | rejected — two of the Criticals were production outages |
| rev 2 | **0 Critical, 4 High** | Critical bar cleared; all four Highs adopted below |
| rev 3 | *this document* | implementing; **Codex round 11 gates the code** with a new lens |

Rev 3 adopts H5-a (advisory born always-true), H5-b (`PAGE > 100` silently truncates),
H5-c (`total_count` breaks the stub seam), C1-a (the flip must announce itself), H6-a (the KV
evidence path must fail closed) and C4-a (an unlocked carry is transient). Each is marked
inline at the point it changes the design. **Adopting a fix is exactly where this branch keeps
breaking** — rounds 9 and 10 each found the defect inside the previous round's fix — so the
adoptions are gated by Codex on the code, not by a third same-lens pass over the prose.

## ⛑ REVISION 2 — the first version of this plan was killed by a Fable gate

Fable returned **3 Critical + 3 High against the plan itself**, and I verified all six
against live data and source before rewriting. Every one stood. What v1 got wrong:

| v1 proposed | why it was wrong | verified by |
|---|---|---|
| H5: fail when the final page is full at the cap | **Guaranteed outage.** `earnings-db` has **825** artifacts against a cap of 4×50=200, so the final page is full on *every* run. No restore step has `continue-on-error`. | `gh api ... total_count` = 825 |
| C1: literal `EA_DB_REQUIRE_ARTIFACT: "true"` | **Permanent deadlock.** `consensus-snapshots` total_count = **0** — never uploaded. The flag makes the restore fail, the persist step is gated on that step succeeding, so the artifact can never be created. | `gh api ... total_count` = 0 |
| C4: `moved_from=` requires `locked=1` | **A third production path exists.** `run_check_results` (`main.py:2797`) preserves `existing["quarter"]`, writes at the new date (`:2839`), and **never locks** — verified: no `set_date_lock` anywhere in `main.py:2760-2860`. | source |
| H6: day-count bound of 75 | **Wrong instrument, measured in the season that flatters it.** | below |
| C2: rewrite the pagination test | **Already done** in `89ad829`; call-log assertions live at `test_ci_db_restore.py:492,520`. | `git log` |
| C3: parse with `yaml.safe_load` | **PyYAML is not a declared dependency** — absent from both `requirements.txt` and `requirements-dev.txt`; it imports locally only transitively. | grep |

The lesson I keep re-learning on this branch: I measured what was easy to measure
(a day-gap histogram) instead of the thing the warning is actually about.

---

## H6 — the residual fires on ordinary quarters. Key on PRINT STATUS, not days.

### The finding
`consensus_snapshot.py:366`. The residual fires for any strong prior 46–135 days back whose
quarter label differs. A normal consecutive print is ~91 days apart and *always* carries a
different label, so the ordinary case is inside the window.

### What v1 proposed, and why it was wrong
v1 narrowed the window to 46–75 days. Measured over strong cross-label pairs on open rows:

| | prior already PRINTED | prior did NOT print |
|---|---|---|
| 46–75d | **22** | 5 |
| 76–135d | **1,446** | 4 |

So a 75-day cut still admits **22 late filers** and **discards 4 of the 9 real cases** — wrong
in both directions. Worse, v1's own "46-day boundary slip" positive fixture (REPL 06-29 →
08-14) carries **distinct actuals on both dates** (-0.76, then -0.72): two genuine prints of a
March-FYE company. The test would have enshrined ordinary cadence as the target.

And the measurement was taken in the season that minimises the band: the DB holds
**2026Q1/Q2/Q3 and zero 2025Q4 rows**, so the Q4→Q1 transition — 10-K late, then 10-Q by
May 15, the longest legitimate gaps — is absent entirely. My claim that "staleness changes
which events exist, not the distribution" is **false for a windowed DB**, because the gap
distribution is seasonal.

### The fix
The residual's own stated worry is *"if the prior already printed, this snapshot is
post-print."* A prior carrying actuals or `reported=1` **did print** — there is no ambiguity
to measure. The ambiguity is exactly a prior that is strong, past, and has **not** printed.
That is a fact recorded in the row, not an inference from a day count.

Add `AND not _has_printed(prior)` to the residual predicate and **keep the existing 46–135d
frame unchanged**. No new constant, no new arbitrary line.

Firing count: **1,477 → 9**, and all 9 real cases are retained (4 of them live at 76–135d,
where v1 would have dropped them).

Seasonality is no longer a hazard: print status does not vary by season the way a gap does.

### ⛑ The KV evidence path must fail CLOSED (gate rev-2, H6-a)
The prior tuple is built from **two** paths that must feed one rule
(`consensus_snapshot.py:281-282`, round 6): DB rows at `:288-298`, and `EVENT_MOVED_LATER_KV`
entries at `:324-326` — **where the row is gone, so there are no actuals to read.**
`_has_printed` is a predicate on a row; the KV path has none.

It must supply `printed=False`, i.e. **fires**. A confirmed date moved later after it arrived
is exactly the possible-print case the KV record exists to remember; defaulting it to `True`
would silently delete that whole evidence path from the residual — the
`a-check-that-silently-matches-nothing` class, for the third time on this branch.

The SELECT at `:288-298` gains `eps_actual, rev_actual`. Named mutation: **making the KV path
supply `True` must fail a test.**

### Honest count
**4–5 cases fire today, not 9.** Of the nine, two have a prior in the *future* (LITS 09-24,
MU 09-30) and cannot enter `_select_window` at all; three more (AYTU, PTN, RZLT) are 1–7 days
past and their successors are ~50 days out, so they cannot fire before late October. The real
shape today is CLSD ×2, KALA, SGMO — plus ICLR 07-21 (locked, unreported, no actuals: the
documented phantom) when its Q3 successor enters the window, which is a correct hit. The
frozen-clock fixtures are still valid as *fixtures*; they are not evidence of live incidence.

### Tests
1. **Two** printed negatives, because one gap length does not cover the band: a 46-day pair
   (REPL 06-29 → 08-14 — note this is **46 days, not 92**; v1 mislabelled it) and one in
   76–135d. Both have actuals on the prior; both must emit nothing.
2. A prior that is strong, past-cutoff and **unprinted** *does* emit — CLSD 05-12 → 09-14
   (125d) and SGMO 08-11 → 11-04 (**85d**, deliberately outside v1's proposed 75-day window).
3. Mutations, each must fail a named test: dropping the print-status term (fails test 1);
   restoring a 75-day bound (fails test 2's SGMO case); the KV path supplying `printed=True`.

---

## C4 — `_qevent`'s refusal encodes a FALSE invariant, and there are THREE paths

### The finding
`test_qevent_refuses_a_hand_assigned_quarter` cannot fail from any production edit, and the
invariant it defends is untrue.

### The three real label shapes
| path | label | locks? |
|---|---|---|
| `main.py:972` sync | `date_to_quarter(event_date)` | n/a |
| `main.py:4269` operator/EDGAR move | prior label, new date | **yes** (`:4283`) |
| `main.py:2797` `run_check_results` actuals migration | prior label, new date | **no** |

v1 knew only the first two and would have forced `locked=1` — making the third path's output
(`2026-07-01 / 2026Q1 / locked=0 / reported=1`) unrepresentable. That is the *same* defect
C4 names, re-entered through the fix for it.

### The fix
- `_qevent(..., moved_from=<old date>, locked=<independent>)` — the label derives from the old
  date; `locked` is a free parameter, because two of the three paths disagree about it.
- Keep the bare `quarter=` refusal (an arbitrary label is still an impossible world).
- Rewrite the docstring to name all three paths.

### ⛑ A fourth DYNAMIC: an unlocked carry is transient (gate rev-2, C4-a)
There is no fourth *source* — every `quarter` write goes through `upsert_event`
(`storage.py:776` UPDATE via `COALESCE(?, quarter)`, `:874-892` INSERT). But there is a
fourth *behaviour*, and the three-shapes table above is wrong without it:

**the derive path overwrites an unlocked carried label.** `run()` matches the existing row
(`main.py:1006`, or ±14d via `find_event_for_ticker_near_date` at `:1008`), `continue`s at
`:1160-1167` **only if `date_locked`**, then upserts at `:1243` with the derived quarter,
which `COALESCE` takes because it is non-null. So `run_check_results`' output
`2026-07-01 / 2026Q1 / locked=0` survives only until the next sync re-emits that date —
normally the next 11:13 or 19:23 UTC run — after which it reads `2026Q2`.

That explains the DB: its only two non-derived rows (HYPR 05-12, CTRE 05-07) are **both
locked**. The lock is what makes a carried label *persist*; it is not what *produces* one.

### Coupling tests (this is the substance of C4, not the helper change)
Four tests that **drive production** and assert the stored label, so a change to any path
fails a test rather than silently orphaning fixtures:
- the sync upsert at `:972` (answers **H-2**: v1 left the derive path uncoupled, and the
  board's own stated direction, "keep the vendor fiscal period", changes exactly this line);
- the locked move at `:4269`;
- the unlocked actuals migration at `:2839` — asserted **immediately after
  `run_check_results` and before any `run()`**, or the assertion races the overwrite above;
- **lifetime:** carry via `:2839`, then let `run()` re-emit the same date — unlocked ⇒ label
  re-derived; locked ⇒ preserved. This is the question the next review round will ask of the
  fixture, and it is the one v1 and rev-2 both left unanswered.

`test_run_integration.py` already drives `run()` (5 call sites) and `test_dedup.py` reaches
`run_check_results` / `_apply_edgar_auto_correction` (4), so these build on existing harness
rather than re-implementing the paths.

---

## C1 — `EA_DB_REQUIRE_ARTIFACT` is inert, but a literal deadlocks the lane

`ci_restore_db_artifact.sh:191` gates the refusal on the flag; no workflow sets it, and
`test_ci_db_restore.py:505` injects it by hand.

**A literal `"true"` cannot ship.** `consensus-snapshots` total_count is **0**: the first
post-merge run would find an empty listing, exit 1, and the persist step (`:305`, gated on
`outcome == 'success'`) would never create the artifact. Twice-daily alerts, forever, with no
manual out.

**Fix — reuse the shape `earnings-db` already solved this with:** a repo variable flipped
after the first successful upload, exactly like `vars.EA_DB_BOOTSTRAPPED`
(`daily_earnings_check.yml:114`). Set `EA_DB_REQUIRE_ARTIFACT: ${{ vars.EA_CONSENSUS_BOOTSTRAPPED }}`
on the consensus restore step only; the variable stays unset until the artifact exists.

Unset semantics confirmed: an unset `vars.*` renders as the empty string, so the env var is
`""`, `"${EA_DB_REQUIRE_ARTIFACT:-}" = "true"` at `:191` is false, and the bootstrap `exit 0`
at `:199-201` runs. No quoting shape yields a literal `"false"`; the one that renders a
boolean word (`${{ vars.X == 'true' }}`) renders `false`, which also fails the `= "true"` test.

### ⛑ The flip must announce itself (gate rev-2, C1-a)
A repo variable a human must remember to set is "never set in production" with extra steps —
the exact state round 10 filed as Critical. **This repo's own precedent is an ~85-day lag:**
the oldest expired `earnings-db` artifact is 2026-02-09 and `EA_DB_BOOTSTRAPPED` was not set
until 2026-05-04.

So the persist step (`daily_earnings_check.yml:305`) posts one line to `#status-reports` after
a successful upload **while `vars.EA_CONSENSUS_BOOTSTRAPPED` is still empty**: *"consensus-
snapshots bootstrapped (artifact N) — set `EA_CONSENSUS_BOOTSTRAPPED=true`."* The inert window
then announces itself every run instead of depending on memory. Add the variable to
CLAUDE.md's "Required repo variables" in the same commit.

Flipping it *early* (before an artifact exists) re-creates the deadlock — but unlike the
literal it is recoverable in one command, `gh variable delete EA_CONSENSUS_BOOTSTRAPPED`.

**Scope:** one step is right. The other three restoring workflows already carry the
abort-if-missing owner, so a flag there is a second refusal for the same case, not a live
instance of the class.

**Test:** assert the **variable reference** in the workflow, not a literal — a test pinning
`"true"` would fail the correct implementation.

---

## C2 — already fixed; do not redo

`89ad829` ("r10: the pagination tests could not see the thing they were about") added the
call log at `test_ci_db_restore.py:116` and the sequence assertions at `:492` / `:520`.
Fable exercised the stated mutation: deleting `ci_restore_db_artifact.sh:147` fails both.
**Mark done in the round-11 prompt so the reviewer is not answering a stale plan.**

---

## C3 — the workflow scan pins text, not effective YAML

`test_ci_db_restore.py:585` string-matches, so commenting out `ref: ${{ github.ref }}` leaves
both assertions true while checkout reverts to the run SHA.

**Fix:** `yaml.safe_load` the workflow; assert the effective `with.ref` per checkout step.
Two constraints v1 missed:
- **Add `pyyaml` to `requirements-dev.txt`.** It is not declared; it imports locally only
  transitively, so the author would never see the CI failure. Never `importorskip` — that
  converts the whole check into a silent skip, which is the class being fixed.
- **Keep the existing self-checkout scoping** (`test_ci_db_restore.py:597-602`). The
  Coverage-Manager sparse checkout (`daily_earnings_check.yml:72-79`) must **not** get
  `ref: ${{ github.ref }}` — a branch dispatch would try to resolve `overnight/298-phase-a`
  in the CM repo and fail.

---

## H5 — cap exhaustion: the review is right about the gap and wrong about the remedy

`ci_restore_db_artifact.sh:129`. Reaching `MAX_PAGES` with a full final page is treated as a
complete listing.

**The proposed remedy is an outage** (825 artifacts vs a 200 cap — see the revision table).
Expired artifacts never leave the listing, so *any* fixed cap truncates eventually; failing on
truncation is a guaranteed future outage, not a hypothetical one.

**Fix — walk the WHOLE listing; stop rationalising the prefix (revised after gate rev-2).**
My rev-2 adjudication was mechanically sound but economically lazy. A deterministic complete
walk costs `total_count / 100` = **9 requests today**, growing +1 per ~14 days, against a
1,000 req/hour `GITHUB_TOKEN` budget and 28 runs/day. That removes the ordering assumption
**entirely** for a cost I was already spending — and the prefix shortcut would have
contradicted the script's own header (`:34-37`, `:158-162`: *"explicitly sorted here rather
than trusting the API's (undocumented) ordering"*), which I would then have had to rewrite to
say the opposite at page level.

- Raise `PAGE` to 100 (the API maximum) and **refuse `PAGE > 100` at `:74`.** Live-probed:
  `per_page=200` returns **100** rows — the API clamps silently, and the short-page test at
  `:147` is `rows < PAGE`, so page 1 (100 < 200) breaks on the *short-page* branch and a
  truncated listing is declared **complete** in one request. The plan is already changing
  this knob; that is the moment to bound it. (gate rev-2, H5-b)
- **Size the walk by `total_count`**, not by a fixed cap: walk `ceil(total_count / PAGE)`
  pages with a sanity cap of **50** (≈2 years of growth at 7 uploads/day) so exhaustion is
  anomalous rather than routine. At the current `MAX_PAGES=4` the prefix is 400 against 825,
  so a "truncation advisory" would have fired on **100% of runs from the first one** — an
  always-true flag shipped inside the H5 fix. (gate rev-2, H5-a)
- Cap exhaustion now means something is genuinely wrong, so it **fails loudly**.
- **Fetch `total_count` in a separate `--jq '.total_count'` call, and update the test stub in
  the same commit.** The row projection at `:114` is consumed positionally (`NF==5` at
  `:164`) and the raw count at `:146` counts every non-empty line, so emitting `total_count`
  into that same stream makes a `PAGE-1` page parse as full — and the stub at
  `test_ci_db_restore.py:79-90` emits only rows, so production would parse a stream the tests
  never see. That is the `test-the-seam` class this branch has already paid for once.
  (gate rev-2, H5-c)
- **Test:** feed a listing **longer than the cap**; assert the newest is selected, that the
  walk requested exactly `ceil(total_count/PAGE)` pages, and that sanity-cap exhaustion fails.
  The current page-2 test never reaches exhaustion.

---

## Order and evidence

H6 → C4 → C3 → C1 → H5. (C2 done.) H6 and C4 change what the code *means*; C1 and H5 touch
production CI and are the ones that can take four workflows down, so they go last and get
exercised against a real listing.

Every fix carries a **named mutation** that must fail its test. This branch has produced three
vacuous guards across rounds 9 and 10; a green suite has never been evidence on it.

Then Codex round 11 with a **new** lens — not a re-run of round 10's.
