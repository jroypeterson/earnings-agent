# web_search tool replay — `web_search_20250305` vs `web_search_20260209` (board #409)

**Date:** 2026-10-01 · **Model:** `claude-sonnet-4-6` (the model `web_resolver.py` uses) ·
**Spend:** $7.46 USD of an $8.00 cap · **Raw data:** `web_search_tool_replay_2026-10-01.jsonl` (80 rows)

## Outcome

**The production default stays on `web_search_20250305`.** The resolver's parser now handles both
response shapes, so switching later is one constant (`web_resolver._WEB_SEARCH_TOOL`).

| Measure (40 paired cases) | `20250305` (legacy) | `20260209` (dynamic filtering) |
|---|---|---|
| Exact-date accuracy, raw | **38 / 40** | **38 / 40** |
| Exact, excluding hits whose cited source post-dates the simulated day (hand-adjudicated) | **~34–35 / 40** | **~30–31 / 40** |
| Same, URL-visible heuristic only (`score_web_search_replay.py`) | 35 / 40 | 30 / 40 |
| Wrong date named | 2 (THC, MED) | 2 (THC, MED) — same cases |
| `high` confidence after the trust gate | 33 (1 wrong) | 29 (1 wrong) |
| Would auto-lock (high + matches a candidate) | 32 (0 wrong) | 28 (0 wrong) |
| Input tokens | 683,383 | 998,664 (+46%) |
| Output tokens | 13,110 (peak 798/call) | 27,881 (peak **1,611**/call) |
| Web searches | 81 | 99 |
| Cost | $3.06 ($0.076/call) | $4.40 ($0.110/call, +44%) |
| `stop_reason` | 40 × `end_turn` | 40 × `end_turn` (no `pause_turn`) |

Costs use Sonnet 4.6 list prices read 2026-10-01 from platform.claude.com pricing: $3 / $15 per
million input / output tokens, $10 per 1,000 searches; code execution is free alongside
`web_search_20260209`.

## Why "ties go to new" did not decide it

The decision rule was: switch only if the new tool's exact-date accuracy is ≥ the old one's. The raw
numbers tie. But the replay searches **today's** web while pretending it is 8 days before each report,
so a model can "find" the date in the results release itself, or in an announcement published after
the simulated day (Codex round 1 caught this). Those answers were impossible for production at that
moment, so they are not accuracy.

The new tool leaned on post-dated evidence far more often. Its cited sources include results releases
(AZTA `…Reports-Third-Quarter…`, ELAN, GRAL, MRVI `-reports-`, HYPR's 8-K Exhibit 99.1) and a
date-announcement published after the simulated day (PAHC, Business Wire 2026-08-19 vs simulated
2026-08-18). Legacy's leaks: DH (GlobeNewswire 2026-08-03 vs simulated 2026-08-02), SCI (the results
release), PAHC (same Aug 19 release, via Yahoo), and possibly RXST (same EDGAR URL in both arms).
Excluding them, legacy leads by ~4 cases of 40. That is not significant on its own: of the cases
where only one arm stayed clean, 5 favour legacy (AZTA, ELAN, GRAL, HYPR, MRVI) and 1 favours new (SCI),
a two-sided sign test p ≈ 0.22. **But it is the only accuracy reading that
reflects what production can see, and on it the new tool is not ≥ the old.** Cost (+44%) and the
auto-lock rate (28 vs 32) point the same way. So the old tool stays.

## Method

- **Ground truth:** reported Tier 1/2 events from 2026-07-08 to 2026-09-15 in the local
  `earnings_events.db` (`reported=1`, actuals present, `date_confirmed=1`, US tickers). The date
  had to be **independently confirmed by SEC EDGAR**: an 8-K Item 2.02 filed on exactly that day
  (`edgar_client.find_earnings_release_filing(t, d, d)`). One case per ticker; 40 cases; seed 409.
- **Disagreement:** the true date on one side and a plausible wrong date on the other (±1 or ±7
  days, moved to a weekday). Which provider holds the truth is randomised. The simulated "today" is
  the true date minus 8 days.
- **Both tools ran through production code.** Each call went through
  `web_resolver.resolve_with_meta(..., tool_type=…)`, so the parser, the citation check and the
  trust gate were all measured. The order of the two arms was randomised per case, with 4 workers.
- Reproduce: `python scripts/replay_web_search_tool.py --n 40 --budget 8 --out <jsonl>` then
  `python scripts/score_web_search_replay.py <jsonl>`.

## Limitations

- **Leakage is adjudicated by hand and is approximate.** The URL heuristic flags TSLA's
  production-update 8-K (pre-announcement, a false positive). It misses PAHC-legacy (Yahoo URL, the
  date appears only in the note) and DH-new (an IR URL with no date in it). A leak-free measurement
  needs prospective cases: dates that are still upcoming, scored after they report. The 3Q26 season
  (mid-October 2026) would supply them.
- **Both arms share 2 wrong cases.** MED is a ticker/company mismatch: the model searched for Medartis
  instead of Medifast. THC: both arms read Tenet's own "to report" release as July 24, while EDGAR has
  the 2.02 filing on July 23. These say nothing about the tool comparison.
- **n = 40, one season, one sample per case.** Run-to-run noise was not measured (no repeat calls),
  because the budget cap would not allow it.

## What changed in the code regardless (the parser hardening)

Verified live first: under `20260209` the API still returns a `web_search_tool_result` block for
each search, called from code (with a `caller` field). It also adds `server_tool_use(name=
"code_execution")` and `code_execution_tool_result`, whose content is an **object** with
`encrypted_stdout`, not a list. The parser now:

- **Takes the verdict from the final answer.** That is the text after the last non-text block,
  scanning JSON objects last-first. A verdict recovered only from text written *before* a later tool
  call is capped at `medium` (Codex r1).
- **Resumes `pause_turn`** up to 2 times and keeps every turn's search results for the citation
  check. A turn still paused, or cut off at `max_tokens`, cannot produce a `high` verdict.
- Treats a `refusal` stop as no verdict.
- Reads only **list-shaped** search results, so an error object is never iterated as citations.
- Reads blocks as SDK objects **or** plain dicts.
- Raises `max_tokens` from 1,500 to **4,000**. The new tool peaked at 1,611 output tokens, which
  would have truncated it under the old cap.
- If the API rejects a non-legacy tool type (HTTP 400), retries once on `web_search_20250305`.

## Review (Codex, 5 rounds)

1. Correctness: a provisional verdict written before a later tool call could authorize a lock (fixed: capped at medium). The replay has look-ahead leakage (accepted: the scorer and this note now separate leak-free accuracy, and the decision rests on it).
2. Resilience: the replay's spend reservation sat below a legal 3-request chain, and `max_uses` reset on every continuation (both fixed).
3. Fallback rows could be scored as the dynamic arm, and a 400 mid-chain restarted a whole legacy chain. This did not reproduce on production data: 0 fallbacks, 0 continuations in 80 real calls. Fixed anyway because the fixes were cheap.
4. Usage from a failed continuation was dropped from the meta. Did not reproduce (0 continuations); fixed.
5. An empty trailing text block promoted a provisional verdict to final. Did not reproduce: all 80 real responses end in non-empty text. Fixed with explicit provenance. The `--budget` cap rests on an assumed 100k input tokens per request, so it is an estimate (real peak: 60,295). Documented, not changed.
