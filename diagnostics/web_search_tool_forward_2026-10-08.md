# web_search tool FORWARD test — `web_search_20250305` vs `web_search_20260209` (board #522)

**Asked:** 2026-10-08 02:43–03:33 UTC (evening of 2026-10-07 ET; every row's `asked_on` is
`2026-10-07`) · **Model:** `claude-sonnet-4-6` through the production resolver ·
**Raw data:** `web_search_tool_forward_2026-10-08.jsonl` (20 rows) · **Frozen sample:**
`web_search_tool_forward_2026-10-08_sample.json` (40 cases, frozen 02:43:08 UTC, before the
first measured call) · **Score on/after: 2026-11-20** (see Scoring plan)

## Outcome (interim — no date has happened yet)

**No production change.** `_WEB_SEARCH_TOOL` stays `web_search_20250305`.

- **10 of 40 frozen cases ran with both tools.** The budget gate stopped the run before the
  other 30. Nine of the 10 are Tier 1/2 names whose providers disagree, which is the population
  the production cross-check sends to the resolver. TSLA is a control.
- **The two tools gave the same date on all 10 names.** 7 were null on both (no announcement
  found yet). DXCM was 2026-10-29 on both, GKOS 2026-10-28 on both, TSLA 2026-10-21 on both.
- **They differed on confidence twice, in opposite directions:**
  - **GKOS:** dynamic said `high` (Business Wire, cited). Legacy said `medium` because it cited
    biospace.com, which is not a trusted host. Production would auto-lock this one only under dynamic.
  - **TSLA:** legacy said `high` (ir.tesla.com). Dynamic was downgraded to `medium` because it
    cited an sec.gov exhibit, which is not a trusted host.
- **Dynamic cost 4.5× as much per call on this cohort.** Mean $0.601 vs $0.135; the 2026-10-01
  replay measured +44%. The gap is widest on names with no announcement yet, where dynamic
  spends all 4 searches and up to 394k input tokens hunting for one: DVA $1.30, IMVT $0.92,
  PACS $0.87. Legacy stops at about 36k.

Correctness cannot be scored until the companies report. So far, the tools have not differed on
any date.

| Measure (10 paired cases) | `20250305` (legacy) | `20260209` (dynamic) |
|---|---|---|
| Named a date | 3 (DXCM, GKOS, TSLA) | 3 (same three, same dates) |
| `high` after the trust gate | 2 (DXCM, TSLA) | 2 (DXCM, GKOS) |
| Would auto-lock (`high` AND equals a provider candidate) | 2 (DXCM = yf, TSLA = yf) | 2 (DXCM = yf, GKOS = yf) |
| Input tokens | 333,893 | 1,745,640 (5.2×) |
| Output tokens (peak per call) | 4,472 (683) | 26,290 (**5,052**) |
| Web searches | 28 | 38 |
| Cost (recorded rows) | **$1.35** ($0.135/call) | **$6.01** ($0.601/call) |
| `stop_reason` / continuations | 10 × `end_turn` / 0 | 10 × `end_turn` / 0 |

Prices are the replay's (Sonnet 4.6 list, read 2026-10-01): $3 / $15 per million input / output
tokens, $10 per 1,000 searches, code execution free alongside `web_search_20260209`. The peak of
5,052 output tokens is above the resolver's `_MAX_TOKENS` = 4,000 only because it is summed over
a call's requests. No single request hit `max_tokens`: every row ended `end_turn`.

## Spend

| Item | USD |
|---|---|
| 20 recorded calls (the JSONL `cost` field) | $7.36 |
| 2 dynamic calls (PACS, SPCX) killed in flight, never recorded | est. $1.10 (range ~$0.8–$2.6; same-cohort dynamic calls ran $0.40–$1.30) |
| Credit check (one Haiku call, 13 tokens) | < $0.01 |
| **Total** | **≈ $8.5 (range ≈ $8.2–$10.0)** against a ~$8 estimate and a $12 ceiling |

**Why calls were killed:** the first run used 2 workers and grouped jobs by case, not by cohort.
After 6 calls it was clearly heading for ~$15, so it would have hit the budget gate partway
through, with control names consuming budget. It was stopped. The harness was changed to run
one call at a time, run the production-eligible cases first, and reserve both arms of a case
before starting it. It then resumed with `--resume --prior-unrecorded 1.10 --budget 10.5`. The
first 6 rows (CI ×2, TSLA ×2, PACS/SPCX legacy) came from that first harness version. The
resolver call and the recorded fields are the same; only scheduling differed.

## Method

- **Sample** (`forward_web_search_tool.py freeze`, seed 522): open 3Q26 events in
  `earnings_events.db` dated after 2026-10-09, US tickers with a SEC CIK, one per ticker. The
  quota was 10 Tier 1, 20 Tier 2 and 10 Tier 3. Within each tier, the order of preference was:
  date **not** company-confirmed and providers disagree; then unconfirmed and providers agree;
  then confirmed (about 1 in 5, as controls). yfinance dates were read live through the
  production `market_data.fetch_yfinance_earnings_date`. The local database's daily-sync
  heartbeat was `2026-10-07`, which is current. The freeze step now refuses a database whose
  heartbeat is stale.
- **Calls:** each arm went through `web_resolver.resolve_with_meta(ticker, name, db_date,
  yf_dates, today, tool_type=…)`. That is the production prompt, model, parser, citation check
  and trust gate. The only addition is a pass-through wrapper on `_run_search` that copies every
  retrieved search result (url, title, `page_age`) into `meta.search_results`. The production
  module was not edited.
- **Not touched:** `_WEB_SEARCH_TOOL`, the events database (opened read-only), calendar,
  TickTick, exports and Slack.
- **Why this cannot leak:** each answer could only rest on pages that existed when the call was
  made. DXCM's and GKOS's own date announcements were dated 2026-10-07 (Business Wire
  `/20261007…`), the day they were asked.

## Frozen sample and per-name answers

`Prod?` = Tier 1/2 and providers disagree, i.e. the cross-check would send it to the resolver
(ignoring the 14-day window; see Limitations). L = legacy, D = dynamic. `—` = null answer.

| Ticker | Tier | Confirmed? | DB date | yfinance | Prod? | L answer (conf) | D answer (conf) |
|---|---|---|---|---|---|---|---|
| CI | 1 | yes | 2026-10-29 | 2026-11-05 | yes | — (low) | — (low) |
| PACS | 1 | no | 2026-11-17 | 2026-11-09 | yes | — (low) | — (low) |
| SPCX | 1 | no | 2026-11-05 | 2026-11-03 | yes | — (low) | — (low) |
| DVA | 2 | yes | 2026-10-23 | 2026-10-28 | yes | — (low) | — (low) |
| GKOS | 2 | no | 2026-11-04 | 2026-10-28 | yes | 2026-10-28 (medium) | 2026-10-28 (**high**) |
| DXCM | 2 | no | 2026-10-22 | 2026-10-29 | yes | 2026-10-29 (high) | 2026-10-29 (high) |
| GILD | 2 | no | 2026-10-22 | 2026-10-29 | yes | — (low) | — (low) |
| IMVT | 2 | no | 2026-10-30 | 2026-11-09 | yes | — (low) | — (low) |
| PLSE | 2 | no | 2026-11-03 | 2026-11-05 | yes | — (low) | — (low) |
| TSLA | 1 | yes | 2026-10-20 | 2026-10-21 | no | 2026-10-21 (**high**) | 2026-10-21 (medium) |

**Not run (budget stop), in run order.**
- Production-eligible: PGNY, TMDX, QDEL, NVCR, HIMS, AORT, CON, CRSP, IRTC, ITGR, TECH.
- Controls: ATEC, KRC, MSFT, CNI, ENSG, GRAL, TDOC, ACHC, UHS, RGNX, PYXS, LYEL, RNXT, NGEN,
  AVTX, GRFS, LFMD, NHI, DHC.

All 40 cases, with both provider dates, are in the sample JSON.

Two side observations that are not about the tool choice:
- **CI and DVA are `date_confirmed=1` in the database, but neither tool found an announcement.**
  `date_confirmed` comes from Finnhub's `hour` field, not from a company release.
- **The database date was wrong where an answer exists.** On DXCM and GKOS, both tools found the
  company's own release naming the **yfinance** date. The database dates (DXCM 2026-10-22, GKOS
  2026-11-04) are Finnhub's. The cross-check should catch both within its 14-day window.

## Scoring plan

**When:** on or after **2026-11-20**. The latest scored database date is PACS 2026-11-17, and
the scorer waits up to 30 days past each case's latest candidate. Anything still without a
filing is reported as PENDING, not as a miss.

**Command** (from `earnings_agent/`):

```
python scripts/score_web_search_forward.py diagnostics/web_search_tool_forward_2026-10-08.jsonl --save-truth diagnostics/web_search_tool_forward_2026-10-08_truth.json
```

**Ground truth**, in order of authority:
1. **`--truth <csv>`** (`ticker,date,source`). The hand adjudication always wins.
2. **The earliest SEC 8-K Item 2.02 filed after the ask date.** It stands on its own only when no
   tool answered a different date, AND it equals a provider candidate or a tool answer that rests
   on a verified company or wire source.
3. **An earnings-looking 6-K** (foreign filers). This is always flagged for adjudication.

**What to do with the output:**
- **Each `ADJUDICATE` line:** open the company's results release (the 8-K's EX-99.1 is fine).
  Add `TICKER,YYYY-MM-DD,release` to a CSV and re-run with `--truth that.csv`.
- **Reading the decision:**
  - Only the **production cohort** decides. The rule is the replay's: switch only if dynamic's
    exact count is ≥ legacy's AND its wrong-auto-lock count is ≤ legacy's.
  - **This dataset will print `NOT FINAL` and `INDICATIVE ONLY` permanently, by design.** 11
    production cases never ran, and every case was asked 15–41 days ahead, outside production's
    14-day cross-check window.
  - Read the cohort numbers as evidence. They are not an automatic switch.
- **What would change the default:** dynamic naming a correct date that legacy missed, or
  legacy auto-locking a wrong date that dynamic did not. On 10/10 identical answers so far,
  neither has happened, and dynamic's cost is 4.5×.

**Tested:**
- `test_score_web_search_forward.py` has 22 tests. One is a differential test that re-scores the
  2026-10-01 replay JSONL and reproduces its recorded numbers: 38/38 exact, 33/29 high,
  32/28 auto-lock, 0 wrong locks. Every guard was mutation-checked (each mutation fails at least
  one test).
- `test_forward_web_search_tool.py` has 4 tests of the run loop, all offline with the resolver
  stubbed.
- On the real data today, the scorer prints all 10 as PENDING and the decision as `UNDECIDED`.

## Limitations

- **The cohort was asked 15–41 days ahead; production asks at most 14.** JP's spec required
  dates after 2026-10-09, and most companies had not announced yet, so 7 of 10 pairs are null on
  both tools. This measures *early* resolution, not the exact production moment. The scorer
  treats it as gating: it cannot mark this dataset final.
- **n = 10 paired, 9 decision-shaped.** It cannot resolve a ~4-case accuracy difference. The
  replay's sign test was p ≈ 0.22 on 6 discordant cases, and here there are 0 discordant answers
  so far.
- **No row carries `sample_sha256`.** Binding to the sample was added after the run. Rows are
  admitted by exact case-payload match against the frozen sample, and the scorer prints a note
  saying so.
- **One sample per arm.** Run-to-run noise is unmeasured, the same as in the replay.

## Review (Codex, 6 rounds)

1. **Round 1.** The scorer could be final with missing pairs; it trusted 8-K and 6-K dates as
   exact; it decided on controls; it recorded calls made without a key. All fixed.
2. **Round 2.** No 14-day window; uncorroborated 8-K dates; arms asked on different days could
   pair; a query bypassed `OPEN_EVENT_SQL` (this failed `test_closed_events.py`); rows came from
   a different runner.
   - Fixed: corroboration, same-day pairing, and `OPEN_EVENT_SQL`.
   - The window is now gating in the scorer (round 3).
   - The runner provenance is disclosed above rather than re-bought.
3. **Round 3.** Window not gating; failed calls scored as null; one-day gap allowed; resume not
   bound to a sample. All fixed.
4. **Round 4.** The decision was vacuously "switch" with zero evidence (this reproduced on the
   real data); controls blocked finality; resume allowed a +1-day gap; a stale local database
   could be frozen. All fixed. The database heartbeat check was verified against the real
   database (`2026-10-07`).
5. **Round 5.** Rows had no sample hash (true on the real data; disclosed, and the scorer now
   rejects rows bound to a different sample); an 8-K equal only to an unverified answer was not
   corroborated. Both fixed.
6. **Round 6.** The first attempt did not finish (no completion marker; not counted as clean). The retry returned no Critical or High findings.
