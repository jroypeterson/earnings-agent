# Plan — stop patching the artifact walk; decide what it should be

**Date:** 2026-09-23 · **Branch:** `overnight/298-phase-a` · **Not merged.**

## Why this is a rethink and not another patch

Five consecutive Codex rounds have each found the defect inside the previous
round's fix, and **my last three fixes to this one guard have each been wrong**:

| round | my fix | what the next round found |
|---|---|---|
| 10 | fail when the final page is full at the cap | 100% of runs would fail (825 artifacts vs a 200 cap) |
| 11 | size the walk by `total_count` | a COMPLETE listing was refused whenever it ended exactly at the cap |
| 12 | dedupe by id + compare unique count to `total_count` | the check **cannot fire in production's shape** |

Round 13's finding is the worst of the three because it is the
`a-check-that-silently-matches-nothing` class *and my own test selected the one
arithmetic case where the guard works*:

- For `total_count = q*PAGE + r` with `1 <= r < PAGE`, a front-insertion adds one
  extra final-page row **and** one duplicated boundary row. After dedupe the
  unique count is **exactly `total_count`**, so `uniq_rows < TOTAL_COUNT` is false.
- **Live shape: `826 % 100 = 26`.** Production is precisely the case that cannot
  fire. My regression test used `4 % 2 == 0` — the exceptional exact-boundary case.
- Reproduced: `total_count=3, PAGE=2`, pages `[900,200]` then `[200,100]` after
  `999` is inserted at the front. Selects `900`; `999` never observed.

Second finding, same round: a transient `total_count` failure is swallowed with
`|| true`, which **disables shift detection for the whole walk** while the
short-page path also skips the truncation check. A count outage silently
downgrades the script to its pre-round-12 behaviour.

## The actual problem

**Counting rows cannot detect mutation of an offset-paginated listing**, because
insert-and-shift is count-preserving after dedupe. I have now tried to make
counting work three times.

## The two candidate designs

### Option A — detect the shift by its SIGNATURE, not its count

A front-insertion between page N and page N+1 necessarily **duplicates the
boundary row** (round 13's own reproduction shows `200` on both pages). So the
duplicate *is* the signal, and round 12's fix threw it away by deduping silently.

- **insertion** ⇒ an id appears on two different pages ⇒ shifted ⇒ retry.
- **deletion** ⇒ rows move up, a row is skipped with no duplicate ⇒ caught by
  `unique < total_count` (keep that check, for this direction only).
- **count unavailable** ⇒ duplicate detection still works; log that the shrink
  half is degraded, and let the failure consume a retry rather than `|| true`.

Cost: none — the information is already in hand and is currently discarded.
Covers both directions and does not depend on the modulus.

### Option B — stop walking; read page 1 and trust newest-first

The multi-page walk exists ONLY to hedge an ordering assumption. Measured across
all 825 artifacts: newest at index 0, 3 adjacent inversions, all minutes apart —
the API orders by id descending, and displacement is bounded by an upload's
duration. Under that, the newest is on page 1 and a mid-walk insertion can only
add something *newer*, never hide the newest.

The hedge has now produced five rounds of defects. It is worth asking directly
whether the hedge is costing more correctness than it buys.

Against it: the script's own header says it sorts "rather than trusting the API's
(undocumented) ordering", and I rejected prefix-selection at round 11 on exactly
that basis. Reversing it now needs to be a deliberate, documented decision, not a
convenience.

## Recommendation

**Option A**, because it is strictly more information from the same requests and
does not require reversing a stated design principle. Option B is the fallback if
A is judged still unsound.

## Questions for the gate

1. Does Option A's duplicate-signature detector have a hole of its own? Construct
   a mutation of the listing that shifts pages and produces **no** cross-page
   duplicate and **no** count shortfall.
2. Is the boundary-duplicate claim actually always true, or only for
   insert-at-front? What about an insert in the middle, or several at once?
3. Is Option B defensible given the measured ordering, or is "undocumented
   ordering" the kind of thing that must never be relied on in a restore path
   whose failure is silent data loss?
4. Is there a third option neither of us has named — e.g. a different endpoint,
   a cursor, or making the selection robust rather than the listing complete?
5. Given five rounds of failed guards here, is there an argument for making this
   lane FAIL LOUD on any doubt rather than attempting to be clever?
