# Slack readability reference for dense earnings cards

Written 2026-09-10. A standing reference so card formatting stops being iterated one
request at a time. Companion to `EARNINGS_CARD_FORMAT.md` (which fixes the *section order*);
this file fixes *how a line looks* and why.

**How the facts in here were established.** Three tiers, and every claim below is tagged:

| Tag | Meaning |
|---|---|
| **[DOC]** | Quoted from Slack's current developer docs (`docs.slack.dev`) on 2026-09-10 |
| **[MEASURED]** | Rendered in Slack's own Block Kit Builder (`app.slack.com/block-kit-builder`, workspace `T0ALW5MJJK0`) on 2026-09-10, desktop and the builder's 320 px "Mobile" preview, with pixel offsets read from the DOM. The mobile preview is Slack's web renderer at phone width, **not** the iOS/Android native renderer — treat mobile numbers as indicative |
| **[SOURCE]** | Published research or a named book, URL given |
| **[CONVENTION]** | Industry practice observed in StreetAccount / Bloomberg output on disk or in the wild; no standards body behind it |
| **[UNVERIFIED]** | Could not be confirmed; stated so it can be tested later, not relied on |

---

## 1. What Slack mrkdwn can and cannot do

### 1.1 Formatting that exists in a `section` / `context` mrkdwn text object

| Want | Syntax | Notes |
|---|---|---|
| Bold | `*text*` | Single asterisks. **[DOC]** |
| Italic | `_text_` | **[DOC]** |
| Strikethrough | `~text~` | **[DOC]** |
| Inline code | `` `text` `` | Monospace; "will not use any other formatting" inside. **[DOC]** |
| Code block | ```` ```text``` ```` | Monospace, `white-space: pre-wrap`, 12 px vs 15 px body. **[DOC]**, sizes **[MEASURED]** |
| Blockquote | `>` at line start | Works mid-section: a `>` line between plain lines rendered as a quote bar. **[MEASURED]** |
| Link | `<https://url\|label>` | **[DOC]** |
| Line break | `\n` | **[DOC]** |
| Emoji | `:tada:` or the Unicode char | Slack converts Unicode emoji to `:code:` form. **[DOC]** — and see the `▪` trap in 1.3 |
| Date | `<!date^ts^{date_short}\|fallback>` | Renders in the reader's timezone. **[DOC]** |
| Escapes | `&amp; &lt; &gt;` | The only three that need escaping. **[DOC]** |

### 1.2 Formatting that does NOT exist in mrkdwn

| Want | Status | Evidence |
|---|---|---|
| Lists (any kind) | **None.** "There's no specific list syntax in app-published text, but you can mimic list formatting with regular text and line breaks." | **[DOC]** formatting-message-text |
| Nested lists | None — follows from the above | **[DOC]** |
| Headings inside text | None. Only the `header` block, and it is `plain_text` only | **[DOC]** |
| Tables inside text | None in mrkdwn. See 1.5 — Slack now has *blocks* for tables | **[DOC]** |
| Underline | None in mrkdwn or `rich_text` styles (bold/italic/strike/code only) | **[DOC]**; the U+0332 hack in 1.3 |
| Text colour / size | None | **[DOC]** |
| Hanging indent | None — a wrapped line returns to the left margin under the bullet | **[MEASURED]**, desktop and mobile |
| Alignment (right-align a number) | None outside a code block or a table block | **[MEASURED]** |

### 1.3 What Slack does to whitespace and glyphs (measured, 15 px Lato, desktop web)

| Input at start of line | Rendered indent | Reading |
|---|---|---|
| 4 regular spaces | **12 px** (≈3 space-widths of 3.85 px) | One space is lost; runs are *partly* preserved, not collapsed to one — but the exact count is renderer behaviour, not a contract. **[MEASURED]** |
| 4 NBSP (U+00A0) | **16 px** (all four kept) | Linear and reliable on the web renderer. **[MEASURED]** |
| 2 em-spaces (U+2003) | **23 px** (11.25 px each) | Largest indent per character. **[MEASURED]** |
| Tab (`\t`) | rendered as a visible gap | Width not controlled. **[MEASURED]** |
| Internal run `A    B    C` | gaps visibly preserved | **[MEASURED]** |
| `rich_text_list` `indent: n` | **28 px per level**, real bullet glyphs (• ○ ■ ●), hanging indent on wrap | The only *native* nesting. **[MEASURED]** |

The same offsets held at the 320 px mobile preview (12 / 16 / 23 / 28 px). **[MEASURED]**

Glyphs:

| Glyph | Result |
|---|---|
| `•` U+2022, `◦` U+25E6, `–` U+2013, `—` U+2014, `·` U+00B7 | Rendered as text. Safe. **[MEASURED]** |
| `▪` U+25AA | **Silently converted to the `:black_small_square:` emoji image** — different size, colour and baseline from text, and it broke text extraction at that point. Do not use it as a bullet. **[MEASURED]** |
| U+0332 combining low line after each character (the "underline hack" in `notifications.py:189`) | Renders as an underline on desktop web. Font-dependent by construction; **[UNVERIFIED]** on iOS/Android native |

### 1.4 Hard limits **[DOC]**

| Object | Limit |
|---|---|
| Blocks per message | **50** (100 in modals / Home tabs) |
| `section.text` | 1–**3,000** chars |
| `section.fields` | max **10** items, **2,000** chars each |
| `header.text` | `plain_text` only, **150** chars, optional `level` 1–4 |
| `context.elements` | max **10** elements (image or text objects); no separate per-element cap is documented, so the text-object cap of **3,000** applies |
| Any text composition object | **3,000** chars |
| `block_id` | 255 chars |
| `markdown` block | **12,000** chars cumulative across all markdown blocks in one payload; `block_id` ignored; "passing a single block may result in multiple blocks after translation" |
| `table` block | **100** rows × **20** cells; **10,000** chars across all cells per table *and* per message |
| `data_table` block | 1–**20** columns; 2–**201** rows (1 header + 200); **20,000** chars per table and per message; `page_size` 1–100, default **5** |

`section.expand: true` prevents the "see more" truncation on long sections. **[DOC]**

### 1.5 The 2025–2026 blocks that change the "no tables, no lists" answer **[DOC]**

| Block | What it gives you | Cost / caveat |
|---|---|---|
| `rich_text` (`rich_text_section`, `rich_text_list`, `rich_text_quote`, `rich_text_preformatted`) | Real bullet/ordered lists with `indent`, `offset`, `border`; styles bold/italic/strike/code; hanging indent on wrap | Verbose JSON; **no documented max `indent`**; no links-with-label shorthand — a link is a `{"type":"link","url":..,"text":..}` element |
| `markdown` | Standard Markdown incl. ordered/unordered/task lists, tables, headers (all one size), code | Meant for LLM output. Slack *translates* it into other blocks, so you do not control the result block-for-block; nested list support is not documented and third-party reports call it unreliable **[UNVERIFIED]** |
| `table` | Cells of `rich_text`, `raw_text`, `raw_number`; `column_settings.align` left/center/right; `is_wrapped` | Static; header row not styled distinctly; announced 2025 (slack.dev blog dated 2026-04-20 lists Card, Alert, Carousel, Data Table, Code as the newest five) |
| `data_table` (changelog **2026-05-20**) | Header row, per-column sort, numeric sort when a column is all `raw_number`, pagination | Default `page_size` 5 — set it or the reader sees 5 rows and a pager; `rich_text` not allowed in header cells |

Slack says the new components "render natively across Slack (web, desktop, iOS, and Android)". Mobile rendering of `table` / `data_table` inside a channel is **[UNVERIFIED]** by me; the builder's mobile preview was not exercised for them.

**Implication for the card.** The "Results" block — metric / actual / estimate / delta — is a table in disguise. Slack can now render it as one. Whether to switch is a decision (see §6), not a formatting tweak.

---

## 2. Hierarchy and indentation without nested lists — the honest comparison

| Technique | Desktop | 320 px mobile | Verdict |
|---|---|---|---|
| **Bold run-in label** `*Revenue:* $1.14B vs FS $1.12B` | Strong. Bold is the scan anchor NN/g recommends | Same — bold survives any width | **Primary tool.** Costs nothing, degrades nowhere |
| **NBSP indentation** (4 NBSP = 16 px ≈ one level) | Reliable, linear | Same offset, but a wrapped line snaps back to the margin, so an indented item that wraps looks like a new top-level line | Good for **one** level of short lines. Butterick calls NBSP-for-layout "duct tape" — correct, but it is the only tape mrkdwn has |
| **Regular-space indentation** (what `notifications.py` does today with 2 and 4 spaces) | 4 spaces → 12 px; 2 spaces → ~4 px, i.e. invisible | Same | **Do not rely on it.** Undocumented, and 2 spaces reads as nothing |
| **Bullet glyph per level** (• then ◦ then –) | Distinguishable only if the reader has learned the code | Same | Weak on its own. Bertin classifies *shape* as an associative, **non-ordered** visual variable — a glyph change does not read as "deeper", only as "different". Position (indent) is ordered. Use glyph *with* indent, never instead of it |
| **`rich_text_list` with `indent`** | Real nesting, real hanging indent, 28 px/level | Level 4 starts 112 px into a 320 px card → ~25 chars of text per line | **Best fidelity, two levels max on mobile.** Migration cost: the builder emits different JSON |
| **Separate `context` block for sub-readings** | Smaller grey text visually subordinate to the section above | Same, and the grey 13 px text is the first thing that gets hard to read on a phone | Good for provenance/footnotes ("14 est, range 3.35–3.48"), poor for anything the reader must act on |
| **`divider`** | Clear section break | Same | Fine, but spends a block (50 cap). Bold section labels usually make it redundant |
| **Code block for aligned columns** | Perfect alignment, tabular figures for free | `pre-wrap`: any line past ~40 monospace chars wraps and the columns collapse. Also 12 px instead of 15 px | Use only when every line is ≤ 40 chars, or accept that mobile readers lose it |
| **`table` / `data_table` block** | Real alignment, `raw_number` right-aligns | Native per Slack; unverified here | The correct answer for a metric grid if mobile holds up |

Measured line capacity at 320 px, 15 px Lato: roughly **50–55 proportional characters** before wrap; the sample line `1 bold run-in: Revenue $1.14B vs FS $1.12B (+1.8%)` (50 chars) already wrapped. **[MEASURED]** Butterick's comfortable range is 45–90 characters per line; NN/g's chunking guidance says 50–75. A phone is at the bottom of both ranges, so a card line that fits desktop comfortably is at its mobile limit.

**Recommendation, in order:** bold run-in labels for level 1 → one level of NBSP-indented short sub-lines (or `rich_text_list` indent 1) for level 2 → nothing deeper. If a third level seems needed, the section is doing two jobs; split it.

---

## 3. Principles for scannable, dense numeric content

### 3.1 How readers actually scan

- **Readers scan, they do not read.** On an average page visit users read at most **28 %** of the words, realistically **20 %**; each extra 100 words buys 4.4 s of attention. **[SOURCE]** Nielsen, *How Little Do Users Read?*, 2008 — https://www.nngroup.com/articles/how-little-do-users-read/
- **The F-pattern is the failure mode, not the target.** It appears when text "has little or no formatting", the reader is trying to be efficient, and is not committed enough to read every word — exactly a morning skim. Consequence: "users miss big chunks of content based merely on how text flows." Fixes: front-load the information-bearing words, bold key phrases, group related content, use lists. **[SOURCE]** Pernice, *F-Shaped Pattern of Reading*, 2017 (reviewed 2026) — https://www.nngroup.com/articles/f-shaped-pattern-reading-web-content/
- **The layer-cake pattern is the target.** Eyes land on headings/subheadings and dip into body text only where a heading earns it; "by far the most effective way to scan pages." It requires headings that (1) visually stand out, (2) describe everything under them, (3) lead with the important word. **[SOURCE]** Pernice, *The Layer-Cake Pattern of Scanning*, 2019 — https://www.nngroup.com/articles/layer-cake-pattern-scanning/
- **A fixed left edge is what makes the vertical stroke of the scan work.** Both NN/g patterns are anchored on the left margin; every line whose first word is a label, and whose label starts at the same x, gets scanned; a line whose first word is "The" or a number does not. This is the reason for run-in labels rather than trailing ones. (Inference from the two articles above, not a separate finding.)

### 3.2 Chunking and how many items survive

- Chunk with short paragraphs, headings that contrast with body text, bolded keywords, bulleted lists, limited line length (50–75 chars). **[SOURCE]** Moran, *How Chunking Helps Content Processing*, 2016 — https://www.nngroup.com/articles/chunking/
- **Working memory holds ~4 chunks, not 7.** Miller's 7±2 (1956) measured performance with rehearsal; Cowan (2001) measured capacity when rehearsal is blocked and found ~4, now the mainstream figure. **[SOURCE]** Cowan, *The magical number 4 in short-term memory*, Behav. Brain Sci. 2001 — https://philpapers.org/rec/COWTMN. Practical reading: a section with more than ~5 sibling lines is no longer a chunk; the reader will not hold the third comparator in mind while reading the fifth line.
- **Bulleted-list rules that have research behind them:** parallel structure (same part of speech first); similar item lengths; numbers only when order or count matters; a lead-in line saying what the list is; ≥3 items to justify a vertical list; **"avoid embedding lists within lists, as they're difficult to follow."** **[SOURCE]** Loranger, *7 Tips for Presenting Bulleted Lists*, 2017 — https://www.nngroup.com/articles/presenting-bulleted-lists/
- **Mixed bullet + numbered lists** cost more than either alone: numbers signal sequence or rank, and a reader who sees `1.` looks for `2.`; interleaving them makes the reader decide, per line, which grammar applies. Same article, plus Butterick on lists. The card has no sequence anywhere, so it has no business containing a numbered list.

### 3.3 Emphasis and heading discipline

- **Bold or italic, never both; and as little as possible** — "when everything is emphasized, nothing is." **[SOURCE]** Butterick, *Practical Typography*, "Bold or italic" — https://practicaltypography.com/bold-or-italic.html
- **At most three heading levels, two is better**; differentiate levels by space and weight, not by progressive indentation, which "ends up looking random and messy." **[SOURCE]** Butterick, "Headings" — https://practicaltypography.com/headings.html
- **Hollow bullets over solid** because they are more subtle; asterisks are bad bullets. **[SOURCE]** Butterick, "Bulleted and numbered lists" — https://practicaltypography.com/bulleted-and-numbered-lists.html. Opinion, and it is the case for `◦` at level 2 under `•`.
- **Do not use NBSP to build layout**: "like fixing a flat tire with duct tape." **[SOURCE]** Butterick, "Nonbreaking spaces" — https://practicaltypography.com/nonbreaking-spaces.html. Where Butterick and this document disagree: he has CSS; a Slack section does not. Use the tape, one layer deep, and know it is tape.

### 3.4 When a table beats a list, and what to do without one

- Use a table when the display is for **looking up individual values, comparing individual values, when precise values are required, when more than one unit of measure is involved, or when summary and detail sit together.** Every one of those describes a Results block. **[SOURCE]** Few, *Show Me the Numbers*, 2nd ed., 2012, ch. 3 (summarised at https://www.perceptualedge.com/images/Show_Me_Full_Outline.pdf)
- Few's table rules: **right-align numbers; give every value in a column the same number of decimals** so decimal points align; thousands separators; enough row height to track across. Same source.
- **Tabular (fixed-width) figures "are essential for one purpose: vertically aligned columns."** Proportional figures — Lato's default — cannot align. **[SOURCE]** Butterick, "Alternate figures" — https://practicaltypography.com/alternate-figures.html. This is *why* a code block aligns and a section never will.
- NN/g on comparison tables: converting a table to a list "does not support compensatory decision making as well because users have to remember the attributes" — i.e. a list of metric lines forces the reader to carry the estimate in memory to compare it with the actual. **[SOURCE]** Moran, *Comparison Tables*, 2017 — https://www.nngroup.com/articles/comparison-tables/
- **Without a table (plain mrkdwn), the fallback is a fixed line grammar**: identical token order on every metric line, so the column exists in the reader's expectation even though it does not exist on screen. `*Metric* actual vs est (±Δ)` — always in that order, always those separators. This is what StreetAccount does (§4).

### 3.5 Tufte and Bertin — what applies

- **"Clutter and confusion are failures of design, not attributes of information."** Density is fine; noise is not. **[SOURCE]** Tufte, *Envisioning Information*, 1990, ch. 3 ("Layering and Separation")
- **1 + 1 = 3.** Two marks placed together create a third visual event (the gap, the alignment or misalignment between them). Every divider, every extra glyph, every bracket adds one. Same source. Applied: a section that uses bold, a bullet, an indent, *and* a code span on the same line has four marks fighting.
- **Small multiples**: the same structure repeated so the eye learns it once. **[SOURCE]** Tufte, *Envisioning Information*, ch. 4. This is the argument for a fixed section order and a fixed line grammar across every ticker's card.
- **Bertin's visual variables**: only *size* is quantitative; *value* (light/dark) and *size* are ordered; *shape* is associative but **not ordered**. **[SOURCE]** Bertin, *Semiology of Graphics*, 1967/1983 (summary: https://www.axismaps.com/guide/visual-variables). In Slack text the ordered variables available are **position** (indent), **weight** (bold), and **size** (context block is smaller). Bullet-glyph choice is shape. Hence: indent and bold carry hierarchy; glyphs decorate it.

### 3.6 Mobile

- Comprehension of *simple* text on a phone equals desktop; **difficult** text slows readers by ~30 ms per word because the small viewport pushes context into working memory. Advice: brevity, front-loading, and for "highly technical domains (finance...)" test on the device. **[SOURCE]** Moran, *Reading Content on Mobile Devices*, 2016 — https://www.nngroup.com/articles/mobile-content/. The 2011 study putting mobile comprehension at 48 % of desktop used a dense privacy policy, which is closer to an earnings card than to a news article. **[SOURCE]** https://www.nngroup.com/articles/mobile-content-is-twice-as-difficult-2011/

---

## 4. Conventions specific to financial summaries

Primary evidence on disk: `sa-monitor/template-library.md` §9A–§13, reverse-engineered from real StreetAccount emails (JPM 2026-07-10, IDXX 2026-05-05, VRTX 2026-05-05). Bloomberg sample from a public re-post of terminal headlines (AVGO, 2025-12). All **[CONVENTION]** unless marked.

### 4.1 Actual vs estimate vs guidance

| Source | Form | Sample |
|---|---|---|
| StreetAccount print | `{Metric} ${actual}{ ex-items} vs FactSet ${est} [{N} est, ${L}-{H}]` | `EPS $3.47 vs FactSet $3.41 [13 est, $3.35-3.48]` |
| SA dense recap | same, `FactSet` → `FS` | `Water $50.3M vs FS $49.1M` |
| SA guidance | `{Metric} ${new lo}-${new hi} vs prior guidance ${old lo}-${old hi} and FactSet ${est} [{N} est, ${L}-${H}]` | `EPS $14.45 - $14.90 vs prior guidance $14.29-$14.80 and FactSet $14.54 [13 est, $14.37-14.71]` |
| Bloomberg headline | `*{NAME} {n}Q ADJ EPS ${actual}, EST. ${est}` / `*{NAME} SEES {n}Q REV. ABOUT ${x}, EST. ${est}` | `*BROADCOM 4Q ADJ EPS $1.95, EST. $1.87` / `*BROADCOM SEES 1Q REV. ABOUT $19.1B, EST. $18.48B` |
| SA takeaways prose | beat/miss as % **and** absolute | `EPS ex-items beat by 5.4%/$0.23` |

What is constant across all of them, and therefore the standard:

1. **Actual first, estimate second, always.** The reader's question is "what happened", then "against what".
2. **The comparator is named** (`vs FactSet`, `EST.`), never implied. If we compare against FMP consensus, the line says so, once per card is enough (a legend/context line) — but the word `vs` must appear.
3. **Non-GAAP is flagged inline** (`ex-items`, `ADJ`), and its absence means GAAP. Never the reverse.
4. **Guidance comparison order is new → prior → consensus.** Raise/hold/cut is read off the first comparison; beat/miss on the guide is read off the second.
5. **Estimate count and range travel in square brackets**, after the comparison, so they can be skipped.
6. **Guidance verbs**: Bloomberg `SEES`, SA `guides`/`vs prior guidance`; both put the *period* next to the verb (`SEES 1Q`, `FY Guidance (Dec 2026):`).

### 4.2 Ranges, percent vs basis points, negatives

- **Ranges**: en-dash or hyphen, unit once at the end when identical (`$4.675-$4.760B` in SA, `$1.18-1.20B` in the shorter form). Currency symbol on the low end at least.
- **Percent changes** carry `%` glued to the numeral; **basis points** for changes in a rate or margin under a few points (`+80bps y/y`, `(10bps) y/y` in SA). Wall Street uses bps because "up 1 %" is ambiguous between relative and absolute; **AP style** since 2019: `%` with a numeral, `percentage points` spelled out, leading zero below 1 % (`0.6%`). **[SOURCE]** https://www.poynter.org/reporting-editing/2019/ap-says-the-percentage-sign-now-ok-when-used-with-a-numeral-thats-shift5/
- **Negatives**: sell-side and SA use **parentheses** `(7.1%)`, `(10bps)`, `($0.01)`; Bloomberg uses a minus. Parentheses are the accounting convention and they survive being read at a glance better than a 1-px hyphen. Either is fine; **mixing them in one card is not.**
- **Margins** are stated as level vs consensus vs year-ago, then delta in bps: `Gross margin 63.4% vs FS 62.5% and year-ago 62.4%`, `Water 72.7% +190bps y/y`.
- **Multiples** always with a context: `NTM P/E 20.9x vs five-year average 22.0x and high 71.0x, low 12.0x` — a multiple alone is never printed.

### 4.3 GAAP alongside adjusted

- SEC Reg G / Item 10(e) C&DI **102.10**: a non-GAAP measure may not be presented "before the most directly comparable GAAP measure or omitting the comparable GAAP measure altogether, including in an earnings release headline or caption," nor with styling that emphasises it, nor called "record" without the GAAP figure getting the same adjective. **[SOURCE]** https://www.sec.gov/corpfin/non-gaap-financial-measures.htm (updated 2022-12-13). This binds *issuers*, not us — but it is why every issuer's release has both numbers, and why a card that shows only "EPS $3.47" without saying which one is ambiguous by construction.
- Practice on the Street: **consensus is adjusted**, so the headline compare is adjusted-vs-adjusted, and GAAP appears as a second line or a parenthetical (`GAAP EPS $2.91`). Memory file `project_fiscal_estimates_extractor.md` already records the trap: consensus "EPS" is adjusted for every vendor.

### 4.4 Period labels for off-calendar fiscal years

- No standards body. Observed forms: `F1Q26` (Bloomberg/US sell-side), `FQ1'26`, `1QFY26` (Asia/India), `Q1 FY2026`. **[CONVENTION]** Whether the year label is the *ending* or *starting* calendar year also varies by issuer (Microsoft FY26 ends Jun-2026; Nvidia FY26 ended Jan-2026). Therefore:
- **A fiscal label must be anchored to a calendar period the first time it appears**: `F2Q27 (3 mo ended Jul-26)` — the parenthetical is not optional, it is the only unambiguous part. Memory rule `feedback_fiscal_labels_are_not_calendar_dates.md` is the same lesson from the other direction.
- StreetAccount's own choice: `Q2` in headlines, with the *report date* and *FY end month* stated in the body (`FY Guidance (Dec 2026):`). That is the cheaper form of the same anchor.

---

## 5. The checklist — priority order, each rule testable

Apply top-down; a block that fails rule 1 is not improved by passing rule 12.

| # | Rule | Test |
|---|---|---|
| 1 | **Every line begins with a bold label at the left margin, and the label is a noun the reader is looking for** (`*EPS*`, `*FY26 guide*`, `*Reaction*`). | Cover everything right of the first space: can you still tell what each line is about? |
| 2 | **Fixed line grammar within a section: `*Metric* actual vs est (±Δ)` — same token order, same separators, on every metric line in every card.** | Diff two cards' Results sections token-by-token; only the numbers should differ. |
| 3 | **Actual before estimate; comparator named at least once per card; adjusted flagged inline, GAAP unflagged.** | Search the card for a number that is a comparison target with no `vs` before it → fail. Search for `EPS` with neither `adj.`/`ex-items` nor `GAAP` when both exist → fail. |
| 4 | **One numeric convention per card**: negatives either all `(x)` or all `-x`; `%` glued to numerals; bps for margin/rate deltas; same decimals within a metric. | Regex the card: `\(\d` and `-\d` both present → fail. `\d %` (space) → fail. |
| 5 | **Hierarchy is at most two levels: bold run-in (L1) and one indented sub-line (L2).** No L3, ever. | Count distinct indent offsets per section: > 2 → fail. |
| 6 | **Indent L2 with 4 NBSP (or `rich_text_list` indent 1), never with regular spaces or tabs.** | Grep the builder for `"\n  ` / `"\n    ` string literals in card text. |
| 7 | **L2 lines are ≤ 50 characters so they never wrap on a phone** (a wrapped L2 snaps back to the margin and impersonates an L1). | `len(line) <= 50` for every indented line. |
| 8 | **≤ 5 sibling lines per section.** Beyond that, split or demote to a `context` footnote. | Count lines between bold section labels. |
| 9 | **No numbered lists.** Nothing in a card is a sequence. | Regex `^\d+\.\s` → fail. |
| 10 | **Bullet glyphs: `•` at L1 only if the section mixes labelled and unlabelled lines; `◦` at L2. Never `▪` (becomes an emoji image), never `*` or `-` (Slack markup collisions / too small).** | Grep for `▪`, `^- `, `^\* `. |
| 11 | **Emphasis budget: bold is for labels; italic for provenance/caveats only; never both on one span; no bold inside a value.** | Count `*` pairs whose content contains a digit → should be 0 except for the label itself. |
| 12 | **Dates and periods are labelled and anchored: `reports Tue 14-Jul BMO · call 08:30 ET`, `F2Q27 (3 mo ended Jul-26)`.** | Any bare date or bare `Q2` without a label word before it and a calendar anchor → fail. |
| 13 | **Units on every number, currency on every money figure except revenue (which is reported-currency; `EARNINGS_CARD_FORMAT.md` rule 3).** | Regex a numeral followed by whitespace and then a letter/newline with no `%`, `B`, `M`, `x`, `bps`, `pts` → inspect. |
| 14 | **Code blocks only for a genuine grid, and only if every line ≤ 40 monospace characters (the 320 px mobile wrap point).** | `max(len(l)) <= 40` per block. |
| 15 | **Section text ≤ 2,800 chars (headroom under 3,000), card ≤ 50 blocks, overflow named and counted rather than dropped.** | Already asserted in `notifications.py` (`_SLACK_SECTION_MAX_CHARS = 2800`); keep the test. |
| 16 | **`context` blocks carry only what the reader may skip**: source, estimate count/range, as-of time, legends. Nothing decision-bearing at 13 px grey. | Read the card with all `context` blocks deleted: is any decision lost? → fail. |
| 17 | **Dividers only between cards or between the metrics half and the narrative half. Bold labels already separate sections.** | Count dividers per card: > 2 → question it. |
| 18 | **Links are labelled with the artefact's name (`<url\|8-K EX-99.1>`, `<url\|transcript>`) and sit at the end of the line, never mid-sentence.** | Regex for `<http` not preceded by end-of-clause punctuation or a label word. |
| 19 | **Load the rendered card, at desktop and at 320 px, before shipping a format change** — Block Kit Builder URL with the payload in the fragment renders both without posting. (Memory rule: *load the page you generated*.) | A screenshot pair exists in the PR / session notes. |

---

## 6. Decisions this reference surfaces (not made here)

1. **Move the Results grid to a `table` / `data_table` block?** Gains: right-aligned `raw_number` columns, a header row, no line-grammar workaround. Costs: 10 k/20 k char caps per message (fine), `data_table` default `page_size` 5 must be set, and **mobile rendering inside a channel is unverified** — one test post to a private channel settles it. Until then the fixed line grammar (rule 2) is the substitute.
2. **Adopt `rich_text_list` for L2 instead of NBSP?** Gains: hanging indent on wrap (fixes the rule-7 failure mode outright), real glyphs. Costs: every mrkdwn convenience (`<url|label>`, `` `code` ``) becomes an element object; builder rewrite in `notifications.py` / `consensus_preview.py`.
3. **Keep the U+0332 underline hack?** It works on desktop web; it is unverified on native mobile and it defeats search/copy (each letter carries a combining mark). Bold already does the job of a label; underline adds a second emphasis channel Butterick would strike.

---

## 7. Sources

Slack (all fetched 2026-09-10):
- Formatting message text — https://docs.slack.dev/messaging/formatting-message-text
- Block Kit blocks index (50/100 block caps; block list) — https://docs.slack.dev/reference/block-kit/blocks
- Section block — https://docs.slack.dev/reference/block-kit/blocks/section-block
- Header block — https://docs.slack.dev/reference/block-kit/blocks/header-block
- Context block — https://docs.slack.dev/reference/block-kit/blocks/context-block
- Text composition object — https://docs.slack.dev/reference/block-kit/composition-objects/text-object
- Rich text block — https://docs.slack.dev/reference/block-kit/blocks/rich-text-block
- Rich text list element — https://docs.slack.dev/reference/block-kit/block-elements/rich-text-list-element/
- Markdown block — https://docs.slack.dev/reference/block-kit/blocks/markdown-block/
- Table block — https://docs.slack.dev/reference/block-kit/blocks/table-block/
- Data table block — https://docs.slack.dev/reference/block-kit/blocks/data-table-block/
- Changelog 2026-05-20, data table — https://docs.slack.dev/changelog/2026/05/20/block-kit-more-new-blocks/
- slack.dev blog 2026-04-20, five new components — https://slack.dev/build-richer-agent-experiences-with-block-kit/
- Block Kit Builder (rendering checks) — https://app.slack.com/block-kit-builder/

Reading and typography:
- Nielsen 2008, *How Little Do Users Read?* — https://www.nngroup.com/articles/how-little-do-users-read/
- Pernice 2017, *F-Shaped Pattern of Reading* — https://www.nngroup.com/articles/f-shaped-pattern-reading-web-content/
- Pernice 2019, *Layer-Cake Pattern* — https://www.nngroup.com/articles/layer-cake-pattern-scanning/
- Loranger 2017, *7 Tips for Presenting Bulleted Lists* — https://www.nngroup.com/articles/presenting-bulleted-lists/
- Moran 2016, *Chunking* — https://www.nngroup.com/articles/chunking/
- Moran 2016, *Reading Content on Mobile Devices* — https://www.nngroup.com/articles/mobile-content/
- Nielsen 2011, *Mobile Content Is Twice as Difficult* — https://www.nngroup.com/articles/mobile-content-is-twice-as-difficult-2011/
- Moran 2017, *Comparison Tables* — https://www.nngroup.com/articles/comparison-tables/
- Butterick, *Practical Typography* — bold-or-italic, headings, lists, nonbreaking-spaces, alternate-figures, line-length pages at https://practicaltypography.com/
- Few, *Show Me the Numbers*, 2nd ed. 2012; outline https://www.perceptualedge.com/images/Show_Me_Full_Outline.pdf
- Tufte, *Envisioning Information*, 1990 (chapters 3–4)
- Bertin, *Semiology of Graphics*, 1967 (Eng. 1983); visual-variables summary https://www.axismaps.com/guide/visual-variables
- Cowan 2001, *The magical number 4* — https://philpapers.org/rec/COWTMN

Finance conventions:
- `sa-monitor/template-library.md` §9A, §10, §11, §13 (StreetAccount captures, 2026-05 to 2026-07)
- Bloomberg headline sample (AVGO 4Q, re-posted publicly) — https://x.com/DeItaone/status/1999226949017674113
- SEC Corp Fin C&DI 102.10 — https://www.sec.gov/corpfin/non-gaap-financial-measures.htm
- AP Stylebook 2019 percent change — https://www.poynter.org/reporting-editing/2019/ap-says-the-percentage-sign-now-ok-when-used-with-a-numeral-thats-shift5/

Not found, so not claimed: a public Bloomberg headline style guide; a documented maximum for `rich_text_list.indent`; a documented per-element character cap for `context`; any Slack statement on how regular-space runs are treated (the 4 → 12 px result is observed behaviour only).
