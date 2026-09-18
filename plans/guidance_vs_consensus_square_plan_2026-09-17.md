# Board #298 part 2: guidance-vs-consensus square (DESIGN PLAN)

2026-09-17. Design only. No code has changed. It supersedes the "Resume here" list in
`PLAN_298.md` where the two differ, and says where below. No metered API calls were made
to write it. Every number was measured offline, from files on disk.

---

## 1. The ask, verbatim

JP, 2026-08-06, as recorded in `PLAN_298.md`:

> A guidance-vs-consensus coloured square: RED when updated guidance is below that fiscal
> year's Street consensus on *either* revenue or EPS; GREEN only when every reported metric
> is at or above forward estimates. Worked example: *"guidance cut to $1B against $2B
> consensus"*. Plus a legend naming the squares.

The Biopharma subgroup and the legend already shipped in `5d11830` (2026-08-26). Only the
square is left.

---

## 2. What exists (verified 2026-09-17)

| Piece | Where | State |
|---|---|---|
| `ResultRow` | `notifications.py:30-45` | Holds EPS/revenue actual and estimate, move, tier, sector and call time. **It has no guidance field.** |
| Results row built (sync path) | `main.py:411` (inside `_record_actuals`, `main.py:378`, called from `run()`) | Built from vendor calendar fields only. **No release text is present.** |
| Results row built (sweep path) | `main.py:2645` (`run_check_results`) | Same: vendor fields only. |
| **Single choke point** | `main.py:2303` `notify_results()`, called at `main.py:1518` and `main.py:2804` | Both paths pass through here before `build_results_slack_blocks`. **This is where the enrichment belongs.** |
| Marker slots + legend | `notifications.py:669-695` `_RESULT_MARKER_SLOTS`, `_MARKER_COLOUR_KEY`, `build_results_legend_text()` | Adding a 4th tuple entry adds it to the line and the legend together. The colour key is a separate hand-written string, so it must be edited too. |
| Release fetch | `edgar_client.py:743` `fetch_release_document`, `:208` `find_earnings_release_filing`, `:327` `find_results_6k` | **PLAN_298 blocker 1 is FIXED:** the function now exists (added 2026-09-09). The self-throttle is 0.12 s per request (`:47`). `Filing8K` (`:109`) carries **no acceptance timestamp**. |
| Guidance sections | `daily_summary.py:668` `extract_guidance_blocks` | Returns (label, lines). The period comes from the heading. Lines containing `\|` or longer than 400 chars are dropped (`:733`). |
| Guidance → numbers | `guidance_parse.py:182` `parse_guidance_line`, `:269` `parse_guidance_blocks`, `:286` `select` | See §2a. **Nothing in earnings_agent imports it.** Its only runtime consumer is `earnings_review/src/earnings_review/guidance.py`. |
| Annual consensus | `consensus_preview.py:792` `fetch_fmp_annual_estimates`, `:765` `fetch_fmp_reported_currency`, dataclass `:118` | Tests are in `test_annual_consensus.py`. **Nothing consumes them.** Both return `[]` or `None` on failure **and** on "no coverage", so a failure cannot be told apart from an absence (§7). |
| Prior art, other repo | `earnings_review/src/earnings_review/table_card.py:436-516` (`_effective_basis`, `_vs_street`, `_street_for_period`), commit `1ed17f4` | Already compares a guide midpoint to **post-print** FMP annual consensus, and labels it "consensus as it stands now". FIVE worked example: pre-print FY EPS consensus $9.29, post-print $10.25, against a guide of $9.83–10.31. **The post-print comparison inverts the verdict.** |
| CI | `daily_earnings_check.yml:133,257`, `post_earnings_check.yml:114,122,140` | `FMP_API_KEY` is present in the sync, preview and both check-results steps. Crons are 11:13 UTC daily, 19:23 UTC weekdays, and 22:37 UTC weekdays (post-earnings). |

### 2a. How `guidance_parse` expresses things

- **Units.** `unit` is `"currency"` only when a `$` is present, `"pct"` for %, and `"count"` otherwise. Scale words (`billion/bn/b`, `million/mm/m`, `thousand/k`) are multiplied out to absolute units. EPS stays per-share (`$1.85` → `1.85`). **Any `$` counts as currency.** CPKC's cached release writes CAD as `$4.2 billion`, so `$` does not prove USD.
- **Fiscal year.** There is no year field. `period` is the outlook block's heading text ("For the full year of Fiscal 2026", "2026 Outlook"), or `""` from the sentence-level fallback. Whether a range is quarterly or annual is **not decided anywhere**.
- **Currency.** Not recorded.
- **Basis.** `"adjusted"` if the line contains adjusted/non-GAAP/underlying/core, `"gaap"` if it contains GAAP, otherwise `""`. The word "adjusted" can come from a *different* metric on the same line. Example: SGRY "revenue and Adjusted EBITDA guidance" marks revenue as adjusted.
- **Bound operators.** "At least" and "up to" are not modelled. `_POINT` handles only approximately/about/around/roughly/~.
- **Prior.** A second same-unit range on the line is read as the issuer's prior column.

---

## 3. Measured parse coverage

**Corpus.** The fleet EDGAR text cache (`Claude Folder/cache/edgar/`, 2,157 files). I kept
EX-99 files that look like earnings releases: a results/quarter phrase in the first 4 KB and
a per-share figure. That left **238 releases**. The cache is 80-column wrapped text, not
`_html_to_text` output, so paragraphs were re-joined before parsing. Raw-text numbers are
given for comparison. Only 2 releases are on disk as raw HTML (`earnings_review/documents/`).
Both parse identically under the production converter. Script:
`scratchpad/measure_guidance.py`. It is a throwaway and is not committed.

| Measure (238 releases) | Unwrapped | Raw wrapped |
|---|---|---|
| No guidance block or line found | 87 (37%) | 60 |
| Any parsed range | 68 (29%) | 84 |
| ≥1 FY **revenue** range in `$` | 35 | 29 |
| ≥1 FY **EPS** range in `$` | 21 | 21 |
| FY revenue **or** EPS | **46 (19%)** | 40 |
| Both | 10 | 10 |
| Revenue guided only as a growth % | 7 | 18 |

"FY" here means my classifier: period or line mentions a year and the period label does not
say "quarter". **All 67 FY revenue/EPS parses were then adjudicated by hand against the
source sentence:**

| Outcome | Rows | Examples |
|---|---|---|
| Correct and comparable | 41 | VRTX, EXEL, OPCH, IDXX, NTRA, FIVE, UPS, BE, SGRY, LIN FY, VLTO FY |
| **Quarterly guide taken as FY** | **10** | LULU "For the third quarter of 2026…" under a "2026 Outlook" heading. LIN Q1. VLTO Q1/Q3/Q4. `0001810806` "Q3 2025 Guidance…Third Quarter Revenue" ×3 |
| **Product-scope revenue** (JP's example shape) | **4** | INSM "full-year 2026 **ARIKAYCE** revenues $450–470M" ×3; "**BRINSUPRI** revenues $1.25–1.40B" |
| **`from X to Y` read backwards** | **2** | Healthpeak "Diluted EPS from $0.34–$0.38 to $0.46–$0.50" is stored as new = 0.34–0.38, prior = 0.46–0.50. **A raise reads as a cut.** |
| **Sign bug** | 1 | NTRA "($2.74B-$2.82B)": the opening parenthesis is read as negative, giving low = −$2.74B |
| Boilerplate taken as guidance | 1 | VLTO "With annual sales of approximately $5.5 billion, Veralto is a global leader…" |
| GAAP EPS, basis blank (inferable from an adjusted sibling) | 4 | FIVE diluted EPS. `_effective_basis` in earnings_review already infers these. |
| EPS basis blank and **not** inferable | 4 | IDXX ×2 (reports GAAP only), LULU diluted, Healthpeak (a REIT; its consensus "EPS" may be FFO) |

A further 2 rows had a garbage prior column (SGRY's EBITDA range recorded as revenue's
prior). That does not affect a level comparison.

**Headline.** **18 of 67 (27%)** raw FY parses would give a wrong current value. **After gates
G1–G6 of §5 are applied by hand, 33 of 238 releases (14%) keep at least one comparable FY
metric.** G7 (analyst floor) and G8 (snapshot) could not be simulated offline, so treat 14% as
an upper bound. The other 205 render a neutral marker. The square will be mostly neutral. That is
the correct cost of "never silently wrong", and JP should hear it before the square ships.

Caveat: the cache is a mixed fleet corpus and was not sampled from this lane's coverage
list. The acceptance corpus in §11 has to be drawn from coverage.

---

## 4. Design

### 4.1 Where things run

```
daily 11:13 UTC + weekday 19:23 UTC       (new step, before the DB save)
  snapshot_annual_consensus(conn)
    for each unreported event where the release has not happened yet:
      event_date in (today, today+4]  or  (event_date == today and hour == 'amc')
    -> FMP analyst-estimates?period=annual     (1 call per ticker, every run)
    -> reportedCurrency  (1 call per ticker, cached in kv_store for 30 days)
    -> upsert consensus_snapshot(ticker, event_date, fiscal_period_end, taken_at, ...)

notify_results(conn, results, as_of)      (main.py:2303, BOTH paths)
  attach_guidance_verdicts(conn, results)   <- NEW, before build_results_slack_blocks
    per row: find 8-K 2.02, or a results 6-K -> fetch_release_document
             -> extract_guidance_blocks -> parse_guidance_blocks
             -> guidance_compare.assess(ranges, snapshots, release_meta) -> GuidanceVerdict
  build_results_slack_blocks(...)            (4th slot reads r.guidance_verdict)
```

- **Pre-print consensus is a snapshot, not a live fetch.** earnings_review measured this
  directly: FIVE's post-print consensus had already moved 10% onto the guide. There is no
  vendor history, so a snapshot that was not taken is lost for good. The results path
  **never** falls back to a live, post-print fetch. When no snapshot exists it renders the
  "no pre-print consensus" state.
- **Pre-release cutoff, with no new EDGAR field:** use `taken_at < event_date 00:00 ET` for
  BMO/DMH/unknown hour, and `taken_at < event_date 16:00 ET` for AMC. Among the snapshots
  that qualify, the latest wins. When `event_date` moved after the snapshot was taken (FMP
  corrects dates), the check runs against the **record date**. A snapshot from before a
  date move is still pre-print, so it stays valid.
- **The snapshot table** is `consensus_snapshot(ticker, event_date, fiscal_period_end,
  currency, revenue_avg, revenue_analysts, eps_avg, eps_analysts, taken_at, source,
  fetch_status)`. It needs a schema bump (`storage.py:21`, v13 → v14). Its PK is `(ticker,
  event_date, fiscal_period_end, taken_at)`. Rows are pruned 60 days after the event. It
  lives in the DB, so the CI artifact round-trip that the calendar relies on also carries
  it. It inherits the stale-artifact risk recorded in CLAUDE.md (2026-09-08).
- **Release fetch at post time.** The post already waits for the stock move. BMO posts at
  the 22:37 UTC sweep on day X; AMC posts at 22:37 UTC on day X+1. By then the 8-K has
  normally been on EDGAR for hours. The finder is 8-K 2.02 first, then `find_results_6k`.
  This is the same order as `earnings_review.guidance._find_release`, so the two cards read
  the same document.
- **One pure module, `guidance_compare.py`**, holding `assess()` and every gate. It has no
  I/O, so it can be unit-tested. earnings_review should later import it in place of its own
  `_street_for_period` / `_vs_street`, so the Slack line and the card cannot disagree about
  one company. That migration is **out of scope** here and is noted as a follow-up.

### 4.2 Scope

Every results row, all tiers. A column that is blank for Tier 3 would read as "no
guidance". Cost stays bounded (§8).

### 4.3 Deviations from PLAN_298 "Resume here"

- Blocker 1 is done. Blocker 2 is handled by reusing the 8-K-then-6-K finder rather than
  editing `attach_release`.
- A `guidance.py` shared service is **not** built. `guidance_parse` plus the new pure
  `guidance_compare` cover it.
- Reaffirmation handling is changed (§6).

---

## 5. The comparison rule (exact)

For each metric M in {revenue, EPS}, pick the guide range G with `select()`-like logic. It
must pass **every** gate below. A failed gate records a named reason and makes M
**abstain**. An abstention is never a pass.

| Gate | Rule | Measured failure it prevents |
|---|---|---|
| G1 FY, not quarter | Period label **or source line** gives FY evidence: `full[- ]year`, `fiscal (year )?20\d\d`, `for (the )?(fiscal )?year`, `annual`, or a bare `20\d\d` next to outlook/guidance. **The source line must not contain** `(first\|second\|third\|fourth) quarter`, `Q[1-4]`, `for the quarter` or `quarterly`. The line overrides the heading. | 10 of 67 quarterly guides taken as FY |
| G2 Fiscal-year match | See §5.1 | FY26 guide compared to FY27 consensus |
| G3 Consolidated scope (revenue) | The 4 tokens before the metric word may contain only an allowlist: `total, net, consolidated, reported, gaap, full, year, fiscal, annual, company's, its, the, of, for, 20xx`, plus punctuation. **Anything else abstains**, including product names, `product`, `segment`, `organic`, `constant currency`, `excluding`. | 4 of 67 (INSM ARIKAYCE/BRINSUPRI) |
| G4 Basis (EPS) | Accept only when the **metric label itself** (`r.label` plus the 40 chars before it) carries adjusted/non-GAAP/core. Blank, or GAAP by sibling inference (`_effective_basis`), abstains as `basis`. Revenue has no basis gate. | UNH FY2024: 27.66 adjusted vs 15.51 GAAP, a 44% false miss. 4 FIVE GAAP rows. |
| G5 Currency | Reported currency (from the snapshot) must be `USD`, the guide unit must be `currency`, and the `$` is taken as USD. Any other reported currency, or `None`, abstains as `currency`. **v1 is US-dollar reporters only.** | NVO: profile says USD, reports DKK. CPKC: `$` means CAD. |
| G6 Sanity | Revenue `low > 0`. `low ≤ high`. `high/low ≤ 1.5`. The range is within 0.2×–5× of consensus. The line does not match `from \$?[\d.]+.{0,40}\bto\b` (a restated-range sentence); abstain until the parser handles from/to. It does not match `(?:with\|has) annual (?:sales\|revenue)` / `is a (?:global\|leading)` (boilerplate). | NTRA sign bug, Healthpeak inversion, VLTO boilerplate |
| G7 Analyst floor | `revenue_analysts` / `eps_analysts` ≥ **3** | QDEL: 1 EPS analyst |
| G8 Snapshot | A pre-release snapshot exists for the matched fiscal period (§4.1) | Circular post-print comparison |

### 5.1 Fiscal-year match (G2)

Labels are not dates. Keep the snapshot's `fiscal_period_end` values. The FYE month is the
month shared by those rows. Then:

1. If the guide carries an explicit year Y (`fiscal 2026`, `FY26`, `full year 2026`,
   `2026 outlook`), the expected period end falls in calendar year **Y**, except when the
   FYE month is **January or early February** (52/53-week retailers name the year it
   starts), where it falls in **Y+1**.
   - FIVE "Fiscal 2026" → 2027-01-31.
   - MCK "fiscal 2027" → 2027-03-31.
   - ADI "fiscal 2026" → Nov 2026.
   - A Jun-FYE "fiscal 2027" → 2027-06-30.
2. If there is no explicit year, take the nearest `fiscal_period_end` that is later than the
   reported quarter end and no more than 380 days after it. The quarter end comes from
   `guidance_parse.extract_period_end(text, event_date)`. If that returns `None`, abstain.
3. Either way, the matched end must be after the reported quarter end and no more than 550
   days after it. This allows "initial next-year outlook" in a Q3 release, which
   earnings_review's 400-day rule silently rejects. Zero matches or more than one → abstain
   as `period`.

### 5.2 Per-metric verdict

`c` = consensus average, and `tol = max(0.01 × |c|, half a unit of the guide's last stated
digit)`. For example, "$5.5 billion" → ±$0.05B, and "$1.85" → ±$0.005.

- **BELOW** if `G.high < c − tol`
- **AT/ABOVE** if `G.low ≥ c − tol`
- **STRADDLES** otherwise

Why the ends and not the midpoint:

- For $80–120 against $100, half the range is below the Street. The midpoint would call it
  "at".
- The ends make RED mean the company's *best case* is below the Street. That is the
  unambiguous reading of JP's example, and the rule both earlier reviewers converged on.
- Using the stated precision in `tol` stops "approximately $5.5B" against $5.52B from
  firing red on rounding.
- A loss-maker's negative EPS works unchanged: `tol` uses `|c|` and has a $0.005 floor.

### 5.3 Row verdict (asymmetric)

Let C be the metrics that were **compared** and A the guided metrics that **abstained**.

- **RED**: any metric in C is BELOW. One is enough, even if the other metric abstained.
  Red cannot be undone by missing information.
- **GREEN**: C is not empty, every metric in C is AT/ABOVE, **and A is empty**. This covers
  "every guided metric at or above", so revenue at/above while EPS was GAAP-only is **not**
  green.
- **YELLOW (in line)**: C is not empty, nothing is BELOW, and at least one metric STRADDLES,
  with A empty.
- **Partial**: C is not empty, no RED, and A is not empty → the partial state (§6).
- A company that guides **only one** of revenue/EPS (trap 5): the metric it did not guide is
  neither in C nor in A, so one compared metric can make the row GREEN. The reason text says
  "revenue only".

---

## 6. Marker semantics

| Marker | State | When |
|---|---|---|
| 🟩 | Guide ≥ Street | §5.3 GREEN |
| 🟥 | Guide below Street | §5.3 RED |
| 🟨 | Guide straddles Street | §5.3 YELLOW |
| ⬜ | No FY number guided | The release was read, and no FY revenue/EPS range in `$` exists. This covers no outlook, qualitative only ("reaffirmed" with no figure), growth % only, and quarterly guide only. **This is a fact about the company.** |
| ◻️ | Guide found, not comparable | ≥1 FY range parsed but every guided metric abstained, or the partial case. The reason is counted in the footer. |
| ⚠️ | Could not check | EDGAR had no release, the fetch failed, there was no pre-print snapshot, or the snapshot fetch failed. **A pipeline gap, not a company fact.** It is also counted in #status-reports (§7). |

- A numeric reaffirmation is **compared on level**. The square answers "where is the
  company's FY guide against the Street", and a reaffirmed $1B against $2B is exactly as red
  as a cut to $1B. This departs from PLAN_298 ("reaffirmation renders neutral"). That rule
  was written for a *bare* reaffirmation with no number, which is still ⬜ here. The legend
  says "level, not direction" so no one reads 🟩 as "raised".
- **Legend.** Add a slot labelled `FY guide vs Street (pre-print)`. Replace
  `_MARKER_COLOUR_KEY` with:
  `🟩 beat/up/≥ · 🟥 miss/down/below · 🟨 guide straddles · ⬜ n/a · ◻️ guide not comparable · ⚠️ no data`.
  The existing ⬜ and ⚠️ keep their current meanings ("n/a", "no data") in all four slots.
  Only 🟨 and ◻️ are new.
- **Reason footer.** One context element, only when non-zero. Example: `FY guide square:
  4 compared · 9 no FY guide · 2 not comparable (GAAP EPS 1, product revenue 1) · 1 could
  not check (NVDA: no pre-print snapshot)`. The reason goes in the footer rather than the
  line, which keeps the line short (§8).
- The feature is behind `GUIDANCE_SQUARE=1`, off by default. When it is off, the slot and
  the legend entry are absent, so the flag cannot leave a dead column behind.

---

## 7. Failure surfaces (each has to be visible)

| Failure | Behaviour |
|---|---|
| FMP call fails in the snapshot step (HTTP, 429, timeout) | The fetcher gets a sibling `fetch_fmp_annual_estimates_checked()` that returns `(rows, error)`. The existing never-raise `[]` stays for current callers. The snapshot row is written with `fetch_status='error:<code>'`. The step's summary goes to #status-reports when errors > 0. On a 429: back off 5/15/30 s (same schedule as `post_with_retry`), then give up for that ticker. The next run retries while the event is still in the future. |
| FMP returns `[]` (no coverage) | `fetch_status='empty'`. At post time this is ◻️ "no Street FY consensus", **not ⚠️**. It is a fact about coverage. |
| No snapshot at post time (new event, date moved earlier, step skipped) | ⚠️, named in the footer and in the #status-reports alert. |
| EDGAR finds no release / release fetch fails | ⚠️ "no release on EDGAR". The same distinction as `earnings_review.guidance`: "could not read" ≠ "no guidance". |
| Parser raises | Caught per row → ⚠️ "parser error". Logged with the ticker. The post is never blocked. |
| Whole enrichment raises | `notify_results` posts without verdicts, **every row shows ⚠️**, and a #status-reports alert fires. **The results post itself must never depend on this feature.** |
| Snapshot step itself crashes | Its own workflow step runs with `continue-on-error: true`, **plus** an `if: failure()` Slack alert, the same shape as the existing safeguards. Downstream, every affected row renders ⚠️, so the gap is visible twice. |
| **One-shot grey** | The row is marked reported after posting and is never re-rendered, so a ⚠️ is permanent. This is accepted and documented in v1. Because the post waits for the stock move, EDGAR has hours to catch up. The ⚠️ rate is measured in the first two weeks (§10). |

---

## 8. Cost

- **FMP (metered), snapshot step only.** Per event:
  - 1 `analyst-estimates` call on each run while the event is 1–4 days out. That is about
    4–5 runs, because both daily crons run.
  - 1 `income-statement` call per ticker per 30 days (currency, cached in kv_store).
  - Season volume from the local `earnings_events.db` copy, whose newest reported row is
    2026-09-17. CLAUDE.md warns that a local copy can be stale, so treat these as approximate: 956 reported events from 2026-07-15 to 2026-08-20
    (27 days; median 11/day; peak 163 on 2026-08-06).
  - That comes to roughly **4,300–5,000 estimate calls per season**, plus ≤ ~1,250 currency
    calls.
  - The heaviest single run is the one before the peak: about 400 tickers (4 days of events)
    × 1 call. Throttled to 4/s, that is about 100 s, under the Starter limit of 300/min.
  - Cheaper alternative: refresh only the day before the event, which is ~1,000 calls per
    season. The price is a snapshot up to 24 h stale. **Recommended default: refresh every
    run.** The final pre-print value is what the square exists to show.
- **Results path today makes 0 per-ticker FMP calls.** It uses the bulk calendar
  (`fmp_client.fetch_fmp_earnings`), and this design keeps it that way. The post-time
  verdict reads the DB only.
- **EDGAR (free, 10 req/s ceiling).** About 3–4 requests per row: submissions,
  index-headers, and 1–2 exhibits. The peak day of 163 rows is about 650 requests, or
  **~80 s** at the 0.12 s self-throttle. Neither `post_earnings_check.yml` nor
  `daily_earnings_check.yml` sets `timeout-minutes` (grep, 2026-09-17), so the only limit is
  GitHub's 6 h job default.
- **Slack.** Measured by re-rendering the real reported rows from `earnings_events.db` with
  a stub 4th slot:

  | Day | Rows | Blocks (3 → 4 slots) | Max section chars | Max line | Legend chars |
  |---|---|---|---|---|---|
  | 2026-08-06 | 163 | 17 → 17 | 2893 → 2891 | 158 → 160 | 119 → 152 |
  | 2026-08-05 | 105 | 15 → 15 | — | 154 → 156 | — |
  | 2026-08-12 | 70 | 11 → 11 | — | 152 → 154 | — |

  The block cap (`SLACK_MAX_BLOCKS = 48`) is not approached. The footer adds 1 context
  element inside an existing block (+0 blocks).

---

## 9. Rejected alternatives

1. **Live post-print consensus at result time.** It is circular (FIVE: $9.29 pre vs $10.25
   post against a $9.83–10.31 guide). A green that is really red.
2. **Midpoint comparison.** It hides a range that is half below the Street, and it disagrees
   with the red/green semantics both earlier reviews set.
3. **An LLM to extract guidance.** The CI output would not be deterministic, and it
   contradicts `guidance_parse`'s stated contract ("a hallucinated range is worse than an
   absent one").
4. **Accept blank-basis EPS when the release never mentions "adjusted".** It would gain IDXX
   (GAAP-only reporter). It would also accept LULU and REITs, whose FMP "EPS" may be adjusted
   or FFO. **Deferred** to Open question 2.
5. **Convert growth-% revenue guides to dollars** using the prior-year actual. 7 of 238
   releases would gain. Most such guides are organic or constant-currency, so it fails G3 in
   a less visible way. Deferred.
6. **Diffing against a transcript.** Transcripts lag by hours to days and state guidance as
   paraphrase (PLAN_298).
7. **Building it in earnings_kpi.** That repo has no remote, and this lane runs in CI
   (PLAN_298, settled).
8. **One grey for every neutral case.** "No guidance", "not comparable" and "could not
   check" are opposite facts. JP asked that the fetch-failure case never be a silent grey.

---

## 10. Invariant

> **The 4th square is 🟩 or 🟥 only when a consolidated, FY, USD, matched-basis guide range
> was compared against a consensus snapshot taken before that release, with ≥3 analysts.
> Every other path renders ⬜, ◻️ or ⚠️ with a counted reason, and no failure in this
> feature can block or delay the results post.**

A runtime assertion enforces this. `GuidanceVerdict` can only be constructed as
GREEN/RED/YELLOW with a non-empty `compared` list, and each entry carries `snapshot_taken_at
< release_cutoff`. The constructor raises otherwise. The test in §11 builds each colour
through the public path, not through the constructor.

---

## 11. Verification

**Tests that fail on current code** (new `test_guidance_square.py`, pure, no network):

1. JP's example: FY revenue guide $1.0B against a $2.0B snapshot → 🟥.
2. The legend names 4 slots, and the colour key contains 🟨 and ◻️. `_format_results_line`
   starts with 4 markers when the flag is on and 3 when it is off.
3. Verbatim corpus lines (from §3), one test each:
   - INSM ARIKAYCE → ◻️ (scope)
   - LULU "For the third quarter of 2026…" under "2026 Outlook" → not FY
   - Healthpeak `from…to` → abstain
   - NTRA `($2.74B-$2.82B)` → abstain, never a negative revenue
   - VLTO "annual sales of approximately $5.5 billion" → not guidance
   - FIVE GAAP diluted EPS → EPS abstains while revenue is compared → ◻️ (partial), not 🟩
4. FY match: FIVE Fiscal 2026 → 2027-01-31, MCK fiscal 2027 → 2027-03-31, ADI fiscal 2026 →
   Nov 2026, calendar Q3 "initial 2027 outlook" → 2027-12-31, and an ambiguous case → abstain.
5. A straddling range → 🟨. $80–120 vs $100 → 🟨, never 🟩.
6. A snapshot taken at 21:00 UTC on an AMC event date → rejected (after the 16:00 ET
   cutoff) → ⚠️.
7. Currency None / DKK → ◻️. A fetch error → ⚠️, and it is distinct from `empty` → ◻️.
8. **Seam test.** Run `notify_results` end to end with stubbed EDGAR + seeded
   `consensus_snapshot` rows + a captured Slack payload. Assert that 🟥 appears in the
   **rendered payload** for the JP row. Then make the enrichment raise, and assert that the
   post still happens with ⚠️ and a status alert. Per the fleet rule, this is the test that
   catches a deleted producer.
9. Mutation check. Revert each gate G1–G8 one at a time. Every revert must fail at least
   one test.

**Dry-run render over recorded data.** Script
`scripts/dryrun_guidance_square.py --corpus <dir> --db earnings_events.db`:

- It parses the cached EX-99s in §3.
- It seeds synthetic snapshots at the vendor-free values in the fixture file (no FMP calls).
- It renders `build_results_slack_blocks` for 2026-08-06 and prints the per-state counts,
  the block count and the max section length.

Acceptance before the flag goes on in CI:

- A **coverage-drawn** corpus of ≥40 releases from the 2026 Q2 season, fetched once from
  EDGAR (free) and committed as fixtures.
- Every 🟩/🟥 verdict adjudicated by hand against the source sentence, with **zero wrong
  colours**.
- The ⚠️ rate reported. If it is above 10% of rows over the first two live weeks, revisit
  the one-shot-grey decision.

---

## 12. Open questions (defaults are recommended; each is decidable cold)

1. **Mostly neutral.** About 14% of releases in the measured corpus produce a comparable FY
   figure. Ship a square that is ⬜/◻️ on most lines, or only render the slot for rows with
   a verdict? **Default: always render.** A missing column would shift the other three
   squares out of alignment and make "no guide" invisible.
2. **EPS with no basis label.** IDXX guides GAAP EPS only and has no adjusted figure. Accept
   it against FMP's adjusted consensus? **Default: no (◻️).** Revisit after the acceptance
   corpus shows how often this happens in coverage.
3. **Tolerance.** 1% of consensus, or half a unit of stated precision, whichever is larger.
   **Default: as stated.** 0.5% would flip more rows to 🟥 on rounding.
4. **Snapshot refresh cadence.** Every run while 1–4 days out (~4–5k FMP calls per season)
   vs only the day before (~1k). **Default: every run.** Well inside 300/min.
5. **Non-USD reporters** (NVO, CP, ADRs). v1 abstains. **Default: accept for v1.** A v2
   could compare in reported currency once ADR ratios and `$`=CAD are handled.

---

## v2 — Fable round 1 (2026-09-17 23:xx): design verdict + corrections

Round 1: **NOT BUILDABLE, 2 Critical + 4 High.** **v2 wins over everything above.**

### Design verdict (Fable as tie-breaker, adopted)
**Build it, reshaped.** (1) Ship the **pre-print consensus snapshot FIRST** — it is the only piece
that is lossy if deferred (~5k FMP calls/season vs 300/min). (2) The square ships in v1 for the
**Tier 1 ∪ Position-list cohort only (~85 names)**; the slot renders on every line for alignment.
(3) Every 🟩/🟥/🟨 carries the **verbatim guide sentence + both snapshot figures** in the footer so
a colour is checkable in five seconds — the hedge JP's own "a wrong number is worse than an absent
one" rule demands. Expand to Tier 2/3 after two weeks at zero wrong colours. An 86%-neutral column
is acceptable: a rare 🟥 in a white column stands out more, not less.

### Criticals
- **C1 — FY mapping is STRUCTURAL, never from the label.** Jan-FYE names split both ways (FIVE,
  LULU name the year they start; WMT, NVDA, CRM, WDAY and **ADSK — held** — the year they end).
  Match = the first `fiscal_period_end` after `extract_period_end(text, event_date)` within 380 days
  (the second one for a Q4 "initial next-year outlook"); the explicit year is used only to ABSTAIN
  when it fits neither convention.
- **C2 — quarter-as-FY.** (a) `daily_summary.py:712`: accept a `_GUIDANCE_PERIOD` match as a
  sub-heading with or without a trailing colon; (b) the FY range must be the **largest** same-metric
  currency range in the release; (c) G6 ratio floor 0.2× → **0.4×** (JP's example is 0.5×).

### Highs
- **H1** — snapshot lookup by `ticker + fiscal_period_end + taken_at < cutoff`; `event_date` is
  provenance only (FMP moves dates after the snapshot — FIVE 06-02→05-27).
- **H2** — EDGAR enrichment inside `notify_results` gets a global budget (~150 s) and a circuit
  breaker on `get_request_stats()` failures; unfetched rows render ⚠️ "budget". The results post
  and `run()`'s critical sync must never wait on a hanging SEC.
- **H3** — G6 requires `sign(guide mid) == sign(consensus)`; `_is_loss` ignores a parenthetical
  "(loss)" (probed: "net income (loss) per diluted share of $0.10 to $0.20" → −0.2..−0.1).
- **H4** — when `event_hour_yf` ≠ `event_hour`, or `date_confirmed=0`, the snapshot cutoff is
  00:00 ET of the event date, not 16:00 ET.

### Mediums (adopted)
M1 scope-narrower DENYlist instead of the G3 allowlist, and re-measure coverage IN CODE before
quoting it · M2 from/to gate only when `from` precedes the first range · M3 ⬜ labelled "no FY $
range parsed", with `at least / up to` hits counted in the footer · M4 a distinct glyph for "not
comparable" (not ⬜ vs ◻️) · M5 snapshot window +10 d for `date_confirmed=0` · M6 new FMP pacing
(4/s, 5/15/30 s backoff) with `continue-on-error` + `if: failure()` alert.
Lows: ZoneInfo not fixed offsets (Nov DST); never use `universe.csv` Currency (trading ccy) for G5;
schema v14 = migration AND fresh-DB CREATE.

### Defaults taken for JP (batched, reversible)
1. Reaffirmation scored on LEVEL (a reaffirmed $2.0–2.1B vs $2.08B renders 🟨).
2. v1 cohort = Tier 1 ∪ Position lists (Fable's recommendation).
3. GAAP-only EPS reporters (IDXX): abstain.

### Build order
Phase A: snapshot table + CI step + tests (no rendering change). Phase B: the square for the cohort.

---

## v3 — Fable round 2 (2026-09-17): PHASE A BUILDABLE · PHASE B not yet

Verdict: **PHASE A buildable** (its one High is a one-line rule, encoded below); **PHASE B NOT
buildable (1 Critical, 3 High)**. v3 > v2 > v1.

### Phase A — final spec (build this now)
- Schema **v14**: `consensus_snapshot(ticker, fiscal_period_end, taken_at, event_date, revenue_avg,
  eps_avg, num_analysts_revenue, num_analysts_eps, currency, fetch_status)`; migration AND fresh-DB
  CREATE (`test_fresh_db_schema_matches_migration_path`, `test_dedup.py:1818`).
- **Scope: ALL tiers** (M3 — snapshots are lossy; Tier 2/3 expansion must not start with no
  history). ~4.3–5k FMP calls/season, under 300/min.
- `snapshot_annual_consensus(conn, today, fetcher)` selects open rows in `(today, today+4]` ∪
  today-AMC, `+10d` when `date_confirmed=0`; one row per `(ticker, fiscal_period_end, taken_at)`;
  an empty window makes zero fetcher calls.
- `fetch_status`: `ok` · `empty` · `error:<code>` — a 429 and an empty list are DISTINGUISHABLE
  (today `fetch_fmp_annual_estimates` collapses both to `[]`; surface the status code, L2).
- `latest_pre_release_snapshot(conn, ticker, fiscal_period_end, cutoff)`: newest `taken_at < cutoff`,
  `event_date` ignored (H1). **Cutoff rule (H1-NEW):** 16:00 ET of the event date for AMC; 00:00 ET
  for BMO, for `date_confirmed=0`, or when `event_hour_yf` and `event_hour` are **both non-empty
  AND differ** — a NULL `event_hour_yf` (80% of rows) is "no second opinion", not a disagreement.
  ET via `ZoneInfo`.
- CI: a new flag on `main.py`, invoked in `daily_earnings_check.yml` AFTER "Run earnings agent",
  with `FMP_API_KEY`, `continue-on-error: true` and an `if: failure()` alert; FMP pacing 4/s with
  5/15/30 s backoff. Source-scan test on the workflow (new pattern, L1).
- **No rendering change in Phase A.** Nothing in the Slack output moves.

### Phase B — carried forward (fix before building the square)
- **C1-NEW:** "largest same-metric range" only among ranges whose explicit year maps to the SAME
  period as the structural match; two mapped periods for one metric → compare only the one equal
  to the first period end; ambiguous → abstain `period`.
- **H2-NEW:** floor 0.4× only when FY evidence is heading-only; source line says full year/fiscal
  20xx → accept down to 0.2×; any sub-floor abstention carries the verbatim sentence.
- **H3-NEW:** relax the colon only inside the `_GUIDANCE_PERIOD` branch at `daily_summary.py:712-720`;
  keep `endswith(":")` on the terminator branch; FIVE fixture must still attribute adjusted EPS.
- M1 FYE-change stub: matched end must be 330–400d after the previous period end, else abstain.
- M2 enrich cohort rows only, sorted `(tier, position_rank)`, before spending the budget.
