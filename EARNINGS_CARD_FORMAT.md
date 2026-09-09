# The earnings card format

One fixed section order for **every** earnings preview and review, so a card can
be read by position rather than by reading it.

JP, 2026-09-09: *"The consensus preview looks messy. Lets use bullets or something so that I
can tell what each line is about more quickly… each earnings review or preview should include
a metrics section and a guidance section. Maybe we should define a format you follow
deterministically?"*

Modelled on **StreetAccount's own earnings cycle**, reverse-engineered from the FIVE emails of
2026-08-31 → 09-02 (`sa-monitor/template-library.md` §9A–§13 has the locked templates).

## Consumers

| Card | Project | Lands |
|---|---|---|
| Consensus preview | `earnings_agent/consensus_preview.py` | `#portfolio` + `#street-account`, daily CI |
| T+1 review | `earnings_review/` | `#portfolio`, weekdays 08:30 ET |

## Section order — fixed, and the same for both

```
HEADER      `TICKER` Company — <date> · BMO/AMC · $price (±day)
METRICS     the numbers
GUIDANCE    what the company itself said about the future
SETUP       (preview)  positioning into the print
REACTION    (review)   what the stock did
CALL        (preview)  when it is
TAKEAWAYS   (review)   what management said
NOT AVAILABLE          every source that failed, named
```

Rules that hold in both:

1. **Every line sits under a bold section label, as a bullet.** No line's meaning may depend
   on remembering where it fell in a run-on sentence. This is the whole reason the format
   exists.
2. **A section is always present, even when empty**, and says which kind of empty it is —
   `none stated in the release` (a fact about the company) is not `n/a (could not read)`
   (a fact about us). Those two must never render the same way.
3. **Every number carries its unit**, and revenue carries **no currency symbol** — the figure
   is whatever the company reported, and FMP's `profile.currency` lies for ADRs (NVO reads
   USD, reports DKK).
4. **A rounded-away value keeps its sign.** `-0.004%` renders `-0.0%`, never `0.0%`.
5. **The card never drops a row silently** — overflow past Slack's 50-block cap is named and
   counted by row.

## METRICS

| | Preview | Review |
|---|---|---|
| EPS | consensus mean | actual **vs** consensus, with surprise % |
| Revenue | consensus mean | actual **vs** consensus, with surprise % |
| Estimate range | count + low/high | — |
| Company KPIs | — | from `earnings_kpi`, where a template exists |

Surprise arithmetic, both cards:

- **The denominator is `abs(estimate)`; the beat flag is `actual > estimate`.** NNOX printed
  EPS −0.79 against −0.1666 on 2026-09-09: signed, that is **+374%**, and a deepening loss
  renders as a large beat.
- **Below a $0.10 consensus the percentage is withheld with its reason**, never blanked —
  ARXS printed $0.28 against $0.0292, a true and useless +859%. The estimate and actual always
  survive so the reader can divide for themselves.

## GUIDANCE

The section that makes a card worth reading, and the one SA leads with.

| | Preview | Review |
|---|---|---|
| Source | the **prior** quarter's 8-K EX-99 | **this** print's 8-K EX-99 |
| Meaning | what management already promised for the quarter about to be reported | what management just said about the quarters ahead |

Extraction is **deterministic — regex over the filed release, no LLM and no API key**, in
`earnings_agent/daily_summary.py`:

1. `extract_guidance_blocks` — the primary path. Reads the **period from the heading** and the
   **figures from the bullets beneath it**, because that is how a release is organised. A
   sentence-level extractor requiring period and verb in one sentence returned only headings
   for Five Below's Q1 release and dropped every number.
2. `extract_guidance_lines` + `rank_guidance_lines` — the fallback, for issuers that guide in
   running prose under no heading (Five Below's *Q2* release is laid out that way, so both
   paths are load-bearing on the same company). Ranking keeps only lines carrying a **figure**
   or a **revision** — "we are raising our full year outlook" is the news even with no number.

Verified against SA's own preview for the same print: SA printed `guidance $1.18-1.20B` and
`$1.17-1.29`; the block extractor returns `$1.18 billion to $1.20 billion`, `7% to 9%`
comparable sales, and `$1.17 to $1.29`.

## Known gaps against StreetAccount

Named here so the gap is visible rather than quietly absent:

| SA has | We have | Why |
|---|---|---|
| Comps / GM / SG&A / operating-margin **consensus** | EPS + revenue only | needs FactSet-grade estimates; FMP Starter gates quarterly `analyst-estimates` (402) |
| Next-quarter and full-year **consensus** columns | current quarter only | same gate; annual estimates ARE reachable and unused |
| Guidance rendered as `metric $x–$y` | the release's own sentence | parsing prose into metric/value pairs is not yet built |
| Actual **vs prior guidance** on the review | actual vs consensus only | the prior-guidance join exists on the preview and is not yet wired into the review |
| Themes performance, dial-in numbers | — | not sourced |
| **Forward-guidance beat rate** | EPS/revenue beat rate only | not computed |

The first and second rows are one purchase apart; the third and fourth are work, not data.
