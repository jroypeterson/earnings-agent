"""Same-day NARRATIVE earnings summaries for coverage names.

What this is NOT
----------------
`--check-results` already ships the *numbers*: a beat/miss card with EPS, revenue
and the post-earnings stock move (`notifications.build_results_slack_blocks`).
That layer is untouched. This module answers the question the numbers don't:
**what did the quarter actually say?**

Three sourced parts per name, in a fixed standard format:

1. **Numbers** - reused verbatim from the existing results layer (SQLite actuals
   + consensus estimates + stock move). No new fetcher.
2. **Guidance** - pulled from the earnings press release itself. Deterministic
   sentence extraction (`extract_guidance_lines`), so this half works with no
   LLM and no API key at all.
3. **What moved the story** - 2-3 bullets, LLM-written strictly from the release
   text (`build_narrative`). Optional: absent an `ANTHROPIC_API_KEY` the card
   still renders, with an explicit "narrative unavailable" line rather than a
   silently-thinner card.

Source of the release text
--------------------------
SEC EDGAR 8-K Item 2.02 -> its EX-99.* press-release exhibit, via
`edgar_client.find_earnings_release_filing` + `edgar_client.fetch_release_document`.
That path is **free** (data.sec.gov + Archives, no auth, no metered quota) and is
the same filing the agent already trusts as ground truth for the report date.
Deliberately NOT the transcript: on a same-day cadence the call may still be in
progress, but the 8-K is filed with the release.

No silent failures
------------------
Every degradation is rendered on the card, never dropped: no 8-K found, no EX-99
exhibit, LLM disabled, LLM failed. See `SummaryRow.degradations`.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, timedelta

import edgar_client
from config import ANTHROPIC_API_KEY

logger = logging.getLogger("earnings_agent")

# Same model family the cross-check web resolver uses (web_resolver.py).
_MODEL = "claude-sonnet-4-6"

# The 8-K is normally filed the same day as the release, but a late-afternoon
# AMC release occasionally lands on the filing agent's next business day.
_FILING_WINDOW_BACK = 1
_FILING_WINDOW_FWD = 2

# How much release text to hand the model. An earnings release runs 15-45k chars;
# the narrative content (headline, CEO quote, segment commentary, guidance) is
# front-loaded, and the tail is GAAP reconciliation tables that add tokens
# without adding story. Truncation is REPORTED on the card, never silent.
_MAX_RELEASE_CHARS = 24_000


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Narrative:
    """LLM output: what the quarter actually said."""
    headline: str                       # one sentence, the story in the operator's words
    movers: list[str] = field(default_factory=list)   # 2-3 bullets
    model: str = ""


@dataclass
class SummaryRow:
    """One coverage name's same-day summary."""
    ticker: str
    company_name: str
    event_date: str
    event_hour: str | None
    tier: int
    sector: str = ""
    subsector: str = ""
    position: str = ""
    quarter: str = ""

    eps_actual: float | None = None
    eps_estimate: float | None = None
    rev_actual: float | None = None
    rev_estimate: float | None = None
    call_datetime_utc: str | None = None

    release_url: str = ""
    release_doc_type: str = ""
    release_filed: str = ""
    release_chars: int = 0

    guidance: list[str] = field(default_factory=list)
    narrative: Narrative | None = None

    # Human-readable reasons this card is thinner than it should be. Rendered.
    degradations: list[str] = field(default_factory=list)

    @property
    def has_release(self) -> bool:
        return bool(self.release_url)


# ---------------------------------------------------------------------------
# Guidance extraction (deterministic - no LLM, no key required)
# ---------------------------------------------------------------------------

# A guidance sentence names a forward period AND a guidance verb/noun. Requiring
# both keeps out the historical-results sentences that dominate the release.
_GUIDANCE_PERIOD = re.compile(
    r"\b(full[- ]year|fiscal\s*(?:year)?\s*20\d\d|FY\s*20\d\d|FY\d\d|"
    r"(?:first|second|third|fourth)\s+quarter\s+(?:of\s+)?20\d\d|"
    r"[1-4]Q\s*20?\d\d|Q[1-4]\s*20\d\d|20\d\d\s+outlook)\b",
    re.IGNORECASE,
)
_GUIDANCE_VERB = re.compile(
    r"\b(guidance|outlook|expects?|expected|anticipates?|forecasts?|"
    r"rais(?:e[sd]?|ing)|lower(?:s|ed|ing)?|narrow(?:s|ed|ing)?|"
    r"reaffirm(?:s|ed|ing)?|reiterat(?:e[sd]?|ing)|updat(?:e[sd]?|ing)|"
    r"now\s+(?:sees|expects)|targets?|projects?)\b",
    re.IGNORECASE,
)
# Boilerplate that matches both patterns but says nothing.
_GUIDANCE_NOISE = re.compile(
    r"(forward[- ]looking statements|safe harbor|Private Securities Litigation|"
    r"risks? and uncertaint|undue reliance|Regulation G|"
    r"we undertake no (?:obligation|duty))",
    re.IGNORECASE,
)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z\"'(])")
# Bullet glyphs issuers use in the highlights block at the top of a release.
_BULLET_SPLIT = re.compile(r"\s*[•●▪·‣⁃]\s*")
_BULLET_STRIP = re.compile(r"^[\s•●▪·‣⁃\-–—]+")

# A guidance REVISION is the news; a reaffirmation is second; a bare "expects"
# is usually descriptive prose. Rank accordingly so the top-of-release headline
# ("raises full-year EPS guidance to...") wins over a mid-body segment sentence
# that happens to contain "expected".
_GUIDANCE_REVISION = re.compile(
    r"\b(rais(?:e[sd]?|ing)|increas(?:e[sd]?|ing)|lower(?:s|ed|ing)?|"
    r"reduc(?:e[sd]?|ing)|cut(?:s|ting)?|narrow(?:s|ed|ing)?|"
    r"updat(?:e[sd]?|ing)|now\s+(?:sees|expects))\b",
    re.IGNORECASE,
)
_GUIDANCE_REAFFIRM = re.compile(
    r"\b(reaffirm(?:s|ed|ing)?|reiterat(?:e[sd]?|ing)|maintain(?:s|ed|ing)?|"
    r"continues? to expect)\b",
    re.IGNORECASE,
)


def _guidance_rank(sent: str, order: int) -> tuple:
    """Sort key for a guidance candidate - lower is more newsworthy."""
    if _GUIDANCE_REVISION.search(sent):
        tier = 0
    elif _GUIDANCE_REAFFIRM.search(sent):
        tier = 1
    else:
        tier = 2
    # Explicit "guidance"/"outlook" nouns beat an incidental forward verb.
    named = 0 if re.search(r"\b(guidance|outlook)\b", sent, re.IGNORECASE) else 1
    return (tier, named, order)


# A line that is nothing but a footnote marker. Issuers set these as superscripts,
# which HTML->text renders onto their OWN line, splitting the sentence around them:
#
#   FY 2026 diluted EPS
#   1
#    guidance raised to at least $20.10; adjusted diluted EPS ...
#
# Left alone, neither fragment carries both a period marker and a verb, so ELV's
# headline guidance raise was invisible to the extractor. Verified on the real
# 2026-07-15 ELV release.
_FOOTNOTE_LINE = re.compile(r"^\(?\d{1,2}\)?[.)]?$")
_ENDS_SENTENCE = re.compile(r"[.!?:;]\"?\)?$")


def _reflow(text: str) -> list[str]:
    """Rejoin sentences that HTML->text conversion split across lines.

    Drops bare footnote-marker lines, then glues a line onto the previous one
    when the previous line did not end a sentence. Flattened table rows (which
    contain '|') are never glued - they are data, not prose.
    """
    out: list[str] = []
    for raw in text.split("\n"):
        line = " ".join(raw.split())
        if not line or _FOOTNOTE_LINE.match(line):
            continue
        if (out and "|" not in line and "|" not in out[-1]
                and not _ENDS_SENTENCE.search(out[-1])):
            out[-1] = f"{out[-1]} {line}"
        else:
            out.append(line)
    return out


def extract_guidance_lines(text: str, *, limit: int = 4) -> list[str]:
    """Sentences from a press release that state forward guidance.

    Deterministic and offline: this is the half of the summary that must keep
    working when the LLM is unavailable or the key is unset.
    """
    if not text:
        return []
    cands: list[tuple[tuple, str]] = []
    seen: set[str] = set()
    order = 0

    # Reflow glues a release's whole bullet block into one line (the bullets sit
    # between the fragments, not at line ends), so split on the bullet glyph
    # BEFORE sentence-splitting - otherwise the headline guidance raise is one
    # 900-char "sentence" that the length filter discards.
    for chunk in _reflow(text):
        for piece in _BULLET_SPLIT.split(chunk):
            for sent in _SENTENCE_SPLIT.split(piece):
                s = _BULLET_STRIP.sub("", " ".join(sent.split())).strip()
                # Skip fragments and flattened table rows (pipes = tabular data).
                if len(s) < 30 or len(s) > 400 or "|" in s:
                    continue
                if _GUIDANCE_NOISE.search(s):
                    continue
                if not (_GUIDANCE_PERIOD.search(s) and _GUIDANCE_VERB.search(s)):
                    continue
                key = s.lower()[:80]
                if key in seen:
                    continue
                seen.add(key)
                cands.append((_guidance_rank(s, order), s))
                order += 1

    cands.sort(key=lambda c: c[0])
    return [s for _, s in cands[:limit]]


# ---------------------------------------------------------------------------
# Narrative (LLM)
# ---------------------------------------------------------------------------

_NARRATIVE_SYSTEM = (
    "You are an equity analyst writing a same-day earnings note for a healthcare-"
    "focused investor. You are given the company's own earnings press release. "
    "Extract what actually moved the story this quarter.\n\n"
    "Rules:\n"
    "- Use ONLY the release text provided. Never add outside knowledge, never "
    "infer a number that is not stated.\n"
    "- The reader ALREADY has the EPS/revenue beat-or-miss versus consensus. Do "
    "not spend a bullet restating it. Explain what drove it, or what the release "
    "says that the headline numbers hide.\n"
    "- Prefer: margin/ratio moves and their stated cause, segment divergence, "
    "one-time or below-the-line items flattering or hurting the result, "
    "membership/volume/backlog changes, capital allocation, management's stated "
    "reason for a guidance change.\n"
    "- Be specific and quantitative. 'Margins compressed' is useless; "
    "'benefit expense ratio 89.7%, +80bp y/y on Government cost trend' is useful.\n"
    "- If the release genuinely says little, return fewer bullets. Never pad.\n\n"
    'Return ONLY JSON: {"headline": "<one sentence>", "movers": ["<bullet>", ...]} '
    "with 2-3 movers, each under 220 characters."
)


def _default_narrator(prompt: str) -> str | None:
    """Call Anthropic. Returns raw text, or None on any failure (never raises)."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, max_retries=2)
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=900,
            system=_NARRATIVE_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
    except Exception as exc:  # noqa: BLE001 - narrative is best-effort by design
        logger.warning(f"daily-summary: narrative call failed: {exc}")
        return None


def _parse_narrative(raw: str | None) -> Narrative | None:
    if not raw:
        return None
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
    except ValueError:
        return None
    movers = [str(x).strip() for x in (data.get("movers") or []) if str(x).strip()]
    headline = str(data.get("headline") or "").strip()
    if not headline and not movers:
        return None
    return Narrative(headline=headline, movers=movers[:3], model=_MODEL)


def build_narrative(
    row: SummaryRow, release_text: str, *, today: date, narrator=None
) -> Narrative | None:
    """Ask the model what moved the story. None when unavailable (caller reports).

    `narrator` is injectable so the renderer can be exercised end-to-end offline
    without spending API credits.
    """
    if not release_text:
        return None
    body = release_text[:_MAX_RELEASE_CHARS]
    truncated = len(release_text) > _MAX_RELEASE_CHARS

    # Today's date is passed explicitly: without it a model with an earlier
    # training cutoff flags a current-quarter release as "future"/implausible.
    # See feedback_llm_judge_pass_current_date.
    prompt = (
        f"Today's date is {today.isoformat()}.\n"
        f"Company: {row.company_name} ({row.ticker})\n"
        f"Reported: {row.event_date} ({(row.event_hour or 'timing n/a').upper()})\n"
        f"Reporting quarter: {row.quarter or 'n/a'}\n"
        + (
            "NOTE: the release text below is truncated; guidance tables near the "
            "end may be missing.\n" if truncated else ""
        )
        + f"\n--- EARNINGS PRESS RELEASE ({row.release_doc_type}) ---\n{body}\n--- END ---"
    )
    return _parse_narrative((narrator or _default_narrator)(prompt))


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


def collect_day(
    conn: sqlite3.Connection,
    coverage_map: dict,
    target: date,
    *,
    max_tier: int = 2,
) -> list[SummaryRow]:
    """Tier 1/2 coverage names whose earnings event is on `target`.

    Reads the SAME `events` rows `--check-results` reads. A name with no actuals
    yet still returns a row: on a same-day cadence the release is filed with the
    8-K well before the aggregators post actuals, so the narrative half is often
    available first - which is the entire point of this lane.
    """
    cur = conn.execute(
        """
        SELECT ticker, company_name, event_date, event_hour, quarter, tier,
               eps_actual, eps_estimate, rev_actual, rev_estimate,
               call_datetime_utc
          FROM events
         WHERE event_date = ? AND tier <= ?
         ORDER BY tier, ticker
        """,
        (target.isoformat(), max_tier),
    )
    rows: list[SummaryRow] = []
    for r in cur:
        tk = r[0]
        info = coverage_map.get(tk)
        rows.append(SummaryRow(
            ticker=tk,
            company_name=r[1] or (getattr(info, "company_name", "") or tk),
            event_date=r[2],
            event_hour=r[3],
            quarter=r[4] or "",
            tier=r[5] or 3,
            eps_actual=r[6], eps_estimate=r[7],
            rev_actual=r[8], rev_estimate=r[9],
            call_datetime_utc=r[10],
            sector=getattr(info, "sector", "") or "",
            subsector=getattr(info, "subsector", "") or "",
            position=getattr(info, "position", "") or "",
        ))
    # Renderers emit a subgroup header on each (tier, subgroup) change, so the
    # rows must arrive grouped - SQL's ORDER BY ticker alone would interleave
    # two MedTech names around a Managed Care one and print the header twice.
    rows.sort(key=lambda r: (r.tier, _subgroup(r), r.ticker))
    return rows


def attach_release(row: SummaryRow) -> str:
    """Fetch + attach the press release for `row`. Returns the release text ("" on miss).

    Free (SEC EDGAR only). Records a degradation reason on any miss.
    """
    try:
        ev = date.fromisoformat(row.event_date)
    except ValueError:
        row.degradations.append("unparseable event date")
        return ""

    filing = edgar_client.find_earnings_release_filing(
        row.ticker,
        ev - timedelta(days=_FILING_WINDOW_BACK),
        ev + timedelta(days=_FILING_WINDOW_FWD),
    )
    if not filing:
        row.degradations.append("no 8-K Item 2.02 on file yet")
        return ""

    doc = edgar_client.fetch_release_document(row.ticker, filing)
    if not doc or not doc.text:
        row.degradations.append(
            f"8-K {filing.accession} filed, but no readable EX-99 release exhibit"
        )
        return ""

    row.release_url = doc.url
    row.release_doc_type = doc.doc_type
    row.release_filed = doc.filing_date
    row.release_chars = doc.char_count
    row.guidance = extract_guidance_lines(doc.text)
    if not row.guidance:
        row.degradations.append("no explicit guidance language found in the release")
    return doc.text


def build_day(
    conn: sqlite3.Connection,
    coverage_map: dict,
    target: date,
    *,
    max_tier: int = 2,
    with_narrative: bool = True,
    narrator=None,
    today: date | None = None,
) -> list[SummaryRow]:
    """Full pipeline for one day: collect -> release -> guidance -> narrative."""
    today = today or date.today()
    rows = collect_day(conn, coverage_map, target, max_tier=max_tier)
    for row in rows:
        text = attach_release(row)
        if not text:
            continue
        if not with_narrative:
            row.degradations.append("narrative disabled (--no-llm)")
            continue
        if not ANTHROPIC_API_KEY and narrator is None:
            row.degradations.append("narrative unavailable (ANTHROPIC_API_KEY unset)")
            continue
        nar = build_narrative(row, text, today=today, narrator=narrator)
        if nar is None:
            row.degradations.append("narrative generation failed - numbers only")
        row.narrative = nar
    return rows


# ---------------------------------------------------------------------------
# Rendering - the STANDARD FORMAT
# ---------------------------------------------------------------------------


def _pct(actual: float | None, est: float | None) -> str:
    if actual is None or est in (None, 0):
        return ""
    return f" ({(actual - est) / abs(est) * 100:+.1f}%)"


def _marker(actual: float | None, est: float | None) -> str:
    if actual is None or est is None:
        return "⬜"
    return "\U0001f7e9" if actual >= est else "\U0001f7e5"


def _money(v: float | None) -> str:
    if v is None:
        return "n/a"
    for cut, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= cut:
            return f"${v / cut:.2f}{suf}"
    return f"${v:,.0f}"


def _eps(v: float | None) -> str:
    return "n/a" if v is None else f"${v:.2f}"


def _timing(row: SummaryRow) -> str:
    h = (row.event_hour or "").lower()
    return {"bmo": "BMO", "amc": "AMC", "dmh": "DMH"}.get(h, "timing TBD")


def _subgroup(row: SummaryRow) -> str:
    """Mutually-exclusive subgroup label. Position beats sector - mirrors the
    priority order in notifications._results_subcategory so the two earnings
    cards group the same name the same way."""
    for pos in ("Portfolio", "Researching", "Ready to Buy", "Ready to Short",
                "Following for Interest"):
        if row.position == pos:
            return pos
    return row.subsector or row.sector or "Other"


def render_text(rows: list[SummaryRow], target: date) -> str:
    """Plaintext rendering - the dry-run surface and the Slack fallback text."""
    head = target.strftime("%a %b %d, %Y")

    if not rows:
        return f"Earnings Summaries - {head}\n\nNo Tier 1/2 coverage names reported."

    lines = [f"Earnings Summaries - {head}",
             f"{len(rows)} coverage name{'s' if len(rows) != 1 else ''} reported", ""]

    last_key = None
    for row in rows:
        key = (row.tier, _subgroup(row))
        if key != last_key:
            lines.append(f"== Tier {row.tier} - {_subgroup(row)} ==")
            last_key = key

        lines.append(
            f"{_marker(row.eps_actual, row.eps_estimate)} "
            f"{_marker(row.rev_actual, row.rev_estimate)}  "
            f"{row.ticker}  {row.company_name} - "
            f"{row.event_date} {_timing(row)}"
            + (f" - {row.quarter}" if row.quarter else "")
        )
        lines.append(
            f"     EPS {_eps(row.eps_actual)} vs {_eps(row.eps_estimate)} est"
            f"{_pct(row.eps_actual, row.eps_estimate)}"
            f"  |  Rev {_money(row.rev_actual)} vs {_money(row.rev_estimate)} est"
            f"{_pct(row.rev_actual, row.rev_estimate)}"
        )

        if row.narrative and row.narrative.headline:
            lines.append(f"     Story: {row.narrative.headline}")
        if row.guidance:
            lines.append("     Guidance:")
            for g in row.guidance:
                lines.append(f"       - {g}")
        if row.narrative and row.narrative.movers:
            lines.append("     What moved the story:")
            for m in row.narrative.movers:
                lines.append(f"       - {m}")
        if row.has_release:
            lines.append(
                f"     Source: {row.release_doc_type} filed {row.release_filed} "
                f"({row.release_chars:,} chars) {row.release_url}"
            )
        for d in row.degradations:
            lines.append(f"     [!] {d}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_slack_blocks(rows: list[SummaryRow], target: date) -> list[dict]:
    """Slack Block Kit. Context blocks use `elements[]` (a bare `text` field is
    an invalid_blocks 400) - see reference_slack_context_block_elements."""
    head = target.strftime("%a %b %d, %Y")

    blocks: list[dict] = [{
        "type": "header",
        "text": {"type": "plain_text", "text": f"Earnings Summaries - {head}", "emoji": True},
    }]

    if not rows:
        blocks.append({"type": "section", "text": {
            "type": "mrkdwn", "text": "_No Tier 1/2 coverage names reported._"}})
        return blocks

    last_key = None
    for row in rows:
        key = (row.tier, _subgroup(row))
        if key != last_key:
            blocks.append({"type": "context", "elements": [{
                "type": "mrkdwn", "text": f"*Tier {row.tier} · {_subgroup(row)}*"}]})
            last_key = key

        parts = [
            f"{_marker(row.eps_actual, row.eps_estimate)} "
            f"{_marker(row.rev_actual, row.rev_estimate)}  "
            f"`{row.ticker}` *{row.company_name}* · {row.event_date} {_timing(row)}"
            + (f" · {row.quarter}" if row.quarter else ""),
            f"EPS {_eps(row.eps_actual)} / {_eps(row.eps_estimate)} est"
            f"{_pct(row.eps_actual, row.eps_estimate)} · "
            f"Rev {_money(row.rev_actual)} / {_money(row.rev_estimate)} est"
            f"{_pct(row.rev_actual, row.rev_estimate)}",
        ]
        if row.narrative and row.narrative.headline:
            parts.append(f"_{row.narrative.headline}_")
        if row.guidance:
            parts.append("*Guidance:*\n" + "\n".join(f"• {g}" for g in row.guidance))
        if row.narrative and row.narrative.movers:
            parts.append("*What moved the story:*\n"
                         + "\n".join(f"• {m}" for m in row.narrative.movers))
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": "\n".join(parts)}})

        ctx: list[str] = []
        if row.has_release:
            ctx.append(f"<{row.release_url}|{row.release_doc_type} press release> "
                       f"· filed {row.release_filed}")
        ctx.extend(f":warning: {d}" for d in row.degradations)
        if ctx:
            blocks.append({"type": "context", "elements": [
                {"type": "mrkdwn", "text": " · ".join(ctx)}]})

    return blocks
