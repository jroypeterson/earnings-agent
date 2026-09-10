"""Guidance sentences -> structured (metric, low, high) ranges.

`daily_summary.extract_guidance_blocks` finds the sentences a company's release
devotes to its outlook. This turns those sentences into numbers, which is what
lets a card say three things it otherwise cannot:

  - how the quarter came in **against the company's own guidance**, not only
    against the Street;
  - how new guidance compares to **prior** guidance at the low end, the high
    end and the midpoint, in currency and percent;
  - what the company itself flagged as a **put or take** inside the range.

JP, 2026-09-09: *"For metrics you need to say how they did vs their own
guidance as well. For guidance you need to say how the new guidance compares to
prior guidance as well (both the high end and the low end vs prior guidance and
the change at the midpoint in dollars and %)."*

DELIBERATELY DETERMINISTIC. No LLM and no API key: guidance is the number a
reader will act on, and a hallucinated range is worse than an absent one.
Anything the patterns do not recognise is left unparsed and the sentence still
renders verbatim -- this layer only ever ADDS structure, it never replaces the
company's own words.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# --- canonical metric keys --------------------------------------------------
# Order matters: the FIRST pattern that matches wins, so the most specific
# ("adjusted diluted eps") must precede the general ("eps").
_METRIC_PATTERNS: list[tuple[str, str]] = [
    ("comps", r"comparable\s+(?:store\s+)?sales|comp(?:arable)?\s+sales|same[- ]store\s+sales"),
    ("eps", r"(?:diluted\s+)?(?:income|earnings|loss)\s+per\s+(?:common\s+)?(?:diluted\s+)?share"
            r"|(?:diluted\s+)?eps|earnings\s+per\s+share"),
    ("revenue", r"net\s+sales|total\s+revenues?|revenues?|net\s+revenues?|sales"),
    ("ebitda", r"ebitda"),
    ("op_income", r"operating\s+(?:income|profit|earnings)|income\s+from\s+operations"),
    ("op_margin", r"operating\s+margin"),
    ("gross_margin", r"gross\s+margin"),
    ("net_income", r"net\s+(?:income|loss|earnings)"),
    ("capex", r"capital\s+expenditures?|capex"),
    ("fcf", r"free\s+cash\s+flow"),
    ("stores", r"net\s+new\s+stores|new\s+stores"),
    ("tax_rate", r"(?:effective\s+)?tax\s+rate"),
    ("shares", r"(?:diluted\s+)?weighted\s+average\s+shares"),
]
_METRIC_RES = [(key, re.compile(pat, re.IGNORECASE)) for key, pat in _METRIC_PATTERNS]

_ADJUSTED = re.compile(r"\b(adjusted|non[- ]gaap|underlying|core)\b", re.IGNORECASE)
_GAAP = re.compile(r"\bgaap\b", re.IGNORECASE)

_SCALE = {"billion": 1e9, "bn": 1e9, "million": 1e6, "mm": 1e6, "m": 1e6, "b": 1e9,
          "thousand": 1e3, "k": 1e3}

# "$1.18 billion to $1.20 billion" / "$1.17 to $1.29" / "7% to 9%" /
# "+8% to +10%" / "$250M to $260M"
_RANGE = re.compile(
    r"(?P<neg1>[-+(]?)\s*(?P<c1>\$)?\s*(?P<v1>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<s1>billion|million|thousand|bn|mm|[bmk])?\s*(?P<p1>%)?\s*"
    r"(?:to|-|–|—|through)\s*"
    r"(?P<neg2>[-+(]?)\s*(?P<c2>\$)?\s*(?P<v2>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<s2>billion|million|thousand|bn|mm|[bmk])?\s*(?P<p2>%)?",
    re.IGNORECASE,
)
# A single-point guide: "approximately $250 million", "about 40 net new stores"
_POINT = re.compile(
    r"(?:approximately|about|around|roughly|~)\s*"
    r"(?P<c>\$)?\s*(?P<v>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<s>billion|million|thousand|bn|mm|[bmk])?\s*(?P<p>%)?",
    re.IGNORECASE,
)


@dataclass
class GuidanceRange:
    """One guided metric, as a range (low == high for a point estimate)."""
    key: str                 # canonical metric key
    label: str               # the company's own wording
    low: float
    high: float
    unit: str                # "currency" | "pct" | "count"
    basis: str               # "adjusted" | "gaap" | ""
    period: str              # the outlook block's period label
    source_line: str
    # When the release prints a "Current Outlook / Prior Outlook" table, the
    # company states its own previous range on the same line. That is strictly
    # better than diffing against last quarter's release: it is the issuer's
    # own restatement, it needs no second fetch, and it cannot mis-pair a
    # reaffirmed metric with a differently-worded one.
    prior_low: Optional[float] = None
    prior_high: Optional[float] = None

    @property
    def midpoint(self) -> float:
        return (self.low + self.high) / 2.0

    @property
    def prior_midpoint(self) -> Optional[float]:
        if self.prior_low is None or self.prior_high is None:
            return None
        return (self.prior_low + self.prior_high) / 2.0

    @property
    def has_prior(self) -> bool:
        return self.prior_low is not None

    @property
    def is_point(self) -> bool:
        return self.low == self.high


def _to_number(value: str, scale: Optional[str], negative: bool) -> float:
    n = float(value.replace(",", ""))
    if scale:
        n *= _SCALE.get(scale.lower(), 1.0)
    return -n if negative else n


def _metric_for(text: str) -> Optional[tuple[str, str]]:
    """(canonical key, matched label) for the metric a sentence is about."""
    for key, rx in _METRIC_RES:
        m = rx.search(text)
        if m:
            return key, m.group(0)
    return None


def _basis_for(text: str) -> str:
    if _ADJUSTED.search(text):
        return "adjusted"
    if _GAAP.search(text):
        return "gaap"
    return ""


def parse_guidance_line(line: str, *, period: str = "") -> Optional[GuidanceRange]:
    """Structure one guidance sentence, or None if it names no metric+number."""
    if not line:
        return None
    # The metric is named BEFORE the figure; searching the whole line would let
    # a trailing clause ("...on approximately 55.7 million diluted weighted
    # average shares") rename the metric of the sentence.
    m = _RANGE.search(line)
    point = None
    if not m:
        point = _POINT.search(line)
        if not point:
            return None

    head = line[: (m or point).start()]
    found = _metric_for(head) or _metric_for(line)
    if not found:
        return None
    key, label = found

    if m:
        pct = bool(m.group("p1") or m.group("p2"))
        cur = bool(m.group("c1") or m.group("c2"))
        low = _to_number(m.group("v1"), m.group("s1"), m.group("neg1") in ("-", "("))
        high = _to_number(m.group("v2"), m.group("s2"), m.group("neg2") in ("-", "("))
        # "$1.18 billion to $1.20 billion" repeats the scale; "$250M to $260M"
        # too. But "in the range of $1.18 to $1.20 billion" states it once, on
        # the HIGH side only, and the low must inherit it or it reads as $1.18.
        if m.group("s2") and not m.group("s1"):
            low *= _SCALE.get(m.group("s2").lower(), 1.0)
    else:
        pct = bool(point.group("p"))
        cur = bool(point.group("c"))
        low = high = _to_number(point.group("v"), point.group("s"), False)

    unit = "pct" if pct else ("currency" if cur else "count")
    # A margin or a comp is a percentage even when the % sign sits elsewhere.
    if key in ("comps", "op_margin", "gross_margin", "tax_rate") and not cur:
        unit = "pct"
    if low > high:
        low, high = high, low

    # A second range of the SAME shape immediately after the first is the
    # "Prior Outlook" column of the issuer's own table.
    prior_low = prior_high = None
    if m:
        tail = line[m.end():]
        m2 = _RANGE.match(tail.lstrip()) or _RANGE.search(tail[:60])
        if m2:
            pct2 = bool(m2.group("p1") or m2.group("p2"))
            cur2 = bool(m2.group("c1") or m2.group("c2"))
            # Same unit, or it is a different metric bleeding in from the next
            # column rather than the prior value of this one.
            if pct2 == pct and cur2 == cur:
                pl = _to_number(m2.group("v1"), m2.group("s1"),
                                m2.group("neg1") in ("-", "("))
                ph = _to_number(m2.group("v2"), m2.group("s2"),
                                m2.group("neg2") in ("-", "("))
                if m2.group("s2") and not m2.group("s1"):
                    pl *= _SCALE.get(m2.group("s2").lower(), 1.0)
                if pl > ph:
                    pl, ph = ph, pl
                prior_low, prior_high = pl, ph

    return GuidanceRange(
        key=key, label=label.strip(), low=low, high=high, unit=unit,
        basis=_basis_for(line), period=period, source_line=line,
        prior_low=prior_low, prior_high=prior_high,
    )


def parse_guidance_blocks(blocks) -> list[GuidanceRange]:
    """Structure every line of every outlook block, in document order."""
    out: list[GuidanceRange] = []
    for block in blocks:
        label = getattr(block, "label", "") or ""
        for line in getattr(block, "lines", []) or []:
            # One sentence can carry two metrics ("Net sales ... $1.18 billion
            # to $1.20 billion ... and assumes an approximate 7% to 9%
            # increase in comparable sales"). Split on the conjunction so the
            # comp range is not discarded with the rest of the sentence.
            for piece in re.split(r"\s+(?:and\s+assumes|and\s+expects|;)\s+", line):
                parsed = parse_guidance_line(piece, period=label)
                if parsed:
                    out.append(parsed)
    return out


def select(ranges: list[GuidanceRange], key: str,
           *, basis: str = "", period_hint: str = "") -> Optional[GuidanceRange]:
    """The best range for one metric.

    `basis` matters and is not cosmetic: consensus EPS is ADJUSTED for every
    vendor, so comparing an actual or an estimate against GAAP guidance
    manufactures a false beat or miss. When an adjusted figure is asked for and
    only GAAP exists, this returns None rather than the wrong basis.
    """
    cands = [r for r in ranges if r.key == key]
    if basis:
        exact = [r for r in cands if r.basis == basis]
        if exact:
            cands = exact
        elif basis == "adjusted":
            # Reaching here means NO adjusted line was parsed for this metric.
            # Two cases, and they are indistinguishable from here:
            #   - the company draws no GAAP/adjusted distinction, so its single
            #     measure IS the one consensus is quoted on; or
            #   - it does, and the extractor missed the adjusted line.
            # Only an EXPLICITLY GAAP-labelled range can be ruled out.
            #
            # ⚠ An earlier version also tried `any(r.basis == "adjusted")` here
            # as a "the company splits, so abstain" guard. That condition is
            # unreachable: this branch runs only when the adjusted list is
            # empty, so the test was always False. It was a check that could
            # never fire -- deleting it changed no behaviour and no test, which
            # is exactly why it had to go rather than stay as reassurance.
            cands = [r for r in cands if r.basis != "gaap"]
    if period_hint:
        hinted = [r for r in cands if period_hint.lower() in (r.period or "").lower()]
        if hinted:
            cands = hinted
    return cands[0] if cands else None


@dataclass
class GuidanceDelta:
    """New guidance against the guidance it replaced."""
    key: str
    unit: str
    new: GuidanceRange
    prior: GuidanceRange

    @property
    def low_delta(self) -> float:
        return self.new.low - self.prior.low

    @property
    def high_delta(self) -> float:
        return self.new.high - self.prior.high

    @property
    def mid_delta(self) -> float:
        return self.new.midpoint - self.prior.midpoint

    @property
    def mid_delta_pct(self) -> Optional[float]:
        """Percent change at the midpoint.

        The denominator is ABSOLUTE: a company guiding out of a loss moves from
        a negative midpoint, and a signed denominator flips the sign of an
        improvement. Same rule as the EPS surprise on the review card.
        """
        base = abs(self.prior.midpoint)
        if base == 0:
            return None
        return self.mid_delta / base * 100.0

    @property
    def direction(self) -> str:
        """raised / lowered / reaffirmed, decided at the midpoint.

        A range can widen at one end and narrow at the other; the midpoint is
        the single summary that does not depend on which end you look at.
        """
        if self.mid_delta > 0:
            return "raised"
        if self.mid_delta < 0:
            return "lowered"
        return "reaffirmed"


def diff_guidance(new: list[GuidanceRange],
                  prior: list[GuidanceRange]) -> list[GuidanceDelta]:
    """Pair new guidance with prior guidance, metric by metric.

    Only pairs that agree on unit AND basis: an adjusted-vs-GAAP or a
    percent-vs-currency pairing would produce a confident wrong delta.
    """
    out: list[GuidanceDelta] = []
    for n in new:
        match = None
        for p in prior:
            if p.key != n.key or p.unit != n.unit or p.basis != n.basis:
                continue
            match = p
            break
        if match:
            out.append(GuidanceDelta(key=n.key, unit=n.unit, new=n, prior=match))
    return out


# --- reported (backward-looking) metrics ------------------------------------
# JP, 2026-09-09: "Metrics also likely should include something like EBIT or
# EBITDA. You can report what they report but most companies give some type of
# operating figure."
#
# So this reads the operating line the company chose to publish rather than
# imposing one definition. A release states it as
#   "Operating income was $154.2 million compared to $50.8 million in the
#    first quarter of fiscal 2025."
# which carries the year-ago figure too, and therefore a YoY for free.

_REPORTED = re.compile(
    # Digits belong in the metric name: releases carry footnote markers, and
    # "Adjusted diluted income per common share (1) was $1.68 compared to
    # $0.54" is the ONLY place the adjusted year-ago figure appears. Excluding
    # them dropped every adjusted comparison, which is the one that matches
    # consensus.
    r"(?P<metric>[A-Z][A-Za-z0-9 ()\-]{2,60}?)\s+"
    # "increased BY 22.9% TO $1.3 billion" -- the growth rate sits between the
    # verb and the level, so the verb must step over it or the sentence never
    # matches and revenue loses its year-ago base entirely.
    r"(?:was|were|totaled|totalled|came in at|of"
    r"|(?:increased|decreased|grew|declined|rose|fell)"
    r"(?:\s+by\s+[\d.,]+\s*%)?\s+to)\s+"
    # A closing paren sits BETWEEN the number and its scale word in
    # "$(10.0) million", so the scale must be reachable past it -- otherwise a
    # parenthesised loss parses as -10.0 rather than -10.0 million, off by 1e6
    # and silently plausible.
    r"(?P<neg1>\(?)\s*\$?\s*\(?\s*(?P<v1>\d[\d,]*(?:\.\d+)?)\s*\)?\s*"
    r"(?P<s1>billion|million|thousand|bn|mm|[bmk])?\s*(?P<p1>%)?"
    # "$(10.0) million" puts the currency symbol OUTSIDE the negation paren,
    # so the paren must be allowed on either side of it or a swing out of a
    # loss loses its prior-year figure entirely and reports no YoY at all.
    # "from $970.5 million" is the other half of "Net sales increased by 22.9%
    # to $1.3 billion from $970.5 million in the second quarter of fiscal
    # 2025" -- the same year-ago comparison in a different sentence shape.
    r"(?:\s*,?\s*(?:compared|versus|vs\.?|from)\s+(?:to|with)?\s*"
    r"(?P<neg2>\(?)\s*\$?\s*\(?\s*(?P<v2>\d[\d,]*(?:\.\d+)?)\s*\)?\s*"
    r"(?P<s2>billion|million|thousand|bn|mm|[bmk])?\s*(?P<p2>%)?)?",
    re.IGNORECASE,
)

# EPS and revenue are included even though the consensus data already supplies
# the actual, because the RELEASE is the only source of the YEAR-AGO figure:
# the events DB is a rolling window (earliest row 2026-04-21 on 2026-09-09), so
# it cannot reach back four quarters for any name. The card takes the actual
# from consensus and the comparison base from here.
_REPORTED_KEYS = ("ebitda", "op_income", "op_margin", "gross_margin", "comps",
                  "revenue", "eps", "net_income")


@dataclass
class ReportedMetric:
    key: str
    label: str
    value: float
    unit: str
    basis: str
    prior_year: Optional[float] = None

    @property
    def yoy_pct(self) -> Optional[float]:
        """Year-over-year change, on an ABSOLUTE denominator.

        A company crossing from an operating loss into profit has a negative
        base; a signed denominator reports that improvement as a decline.
        """
        if self.prior_year in (None, 0):
            return None
        return (self.value - self.prior_year) / abs(self.prior_year) * 100.0


def extract_reported_metrics(text: str, *, limit: int = 6) -> list[ReportedMetric]:
    """Operating figures the release states for the quarter just reported."""
    if not text:
        return []
    # Metrics that are AMOUNTS. A percentage captured for one of these is the
    # growth rate, not the level: "Net sales increased by 22.9% to $1.3 billion
    # from $970.5 million" otherwise records revenue as 22.9.
    amount_keys = {"revenue", "eps", "op_income", "net_income", "ebitda", "capex"}

    best: dict[tuple[str, str], ReportedMetric] = {}
    order: list[tuple[str, str]] = []
    for sent in re.split(r"(?<=[.!?])\s+|\n", text):
        s = " ".join(sent.split())
        if len(s) < 20 or len(s) > 320:
            continue
        m = _REPORTED.search(s)
        if not m:
            continue
        found = _metric_for(m.group("metric"))
        if not found or found[0] not in _REPORTED_KEYS:
            continue
        key, label = found
        basis = _basis_for(m.group("metric"))
        pct = bool(m.group("p1"))
        if pct and key in amount_keys:
            continue
        value = _to_number(m.group("v1"), m.group("s1"), m.group("neg1") == "(")
        prior = None
        if m.group("v2"):
            # The paren may sit before OR after the currency symbol, so the
            # capture group alone is not proof of a negative. Read the text
            # actually between "compared to" and the digits.
            gap = s[m.end("neg2"):m.start("v2")]
            negative = m.group("neg2") == "(" or "(" in gap
            prior = _to_number(m.group("v2"), m.group("s2"), negative)
        unit = "pct" if (pct or key in ("op_margin", "gross_margin", "comps")) else "currency"
        cand = ReportedMetric(key=key, label=m.group("metric").strip(),
                              value=value, unit=unit, basis=basis, prior_year=prior)

        # A release states the same metric more than once, and only some of
        # those sentences carry the year-ago comparison. First-wins discarded
        # the useful one: EPS appeared first without a prior and the later
        # "compared to $0.77" sentence was then skipped as a duplicate.
        slot = (key, basis)
        held = best.get(slot)
        if held is None:
            best[slot] = cand
            order.append(slot)
        elif held.prior_year is None and cand.prior_year is not None:
            best[slot] = cand

    return [best[k] for k in order][:limit]


# --- puts and takes ---------------------------------------------------------
# JP: "a lot of times the call will talk about puts and takes for guidance so in
# guidance section call out any discrete headwinds or tailwinds they noted as
# factored into guidance range."

_TAILWIND = re.compile(
    r"\b(tailwind\w*|benefit\w*|favorab\w+|upside|boost\w*|accretive|"
    r"momentum|strength\w*|outperform\w*|better than expected|"
    r"ahead of (?:our )?(?:plan|expectations))\b",
    re.IGNORECASE,
)
_HEADWIND = re.compile(
    r"\b(headwind\w*|pressur\w+|drag|unfavorab\w+|dilutive|"
    r"offset\w*|adversely|conservat\w+|prudent|cautious|uncertain\w*|"
    r"weigh\w*\s+on|inflation\w*|deleverag\w+|softness|cost\s+increase\w*)\b",
    re.IGNORECASE,
)
# NOT in the headwind vocabulary: "tariff". It is a TOPIC, not a direction, and
# including it labelled "we would expect some level of tailwinds moving into
# the back half" as a headwind purely because the sentence opened with "as we
# look forward into the tariff environment".
# Sentences that tie an effect to the OUTLOOK, and only those.
#
# ⚠ The first version of this also matched "full-year", "fiscal" and the
# quarter names, which appear in every call's opening boilerplate ("welcome to
# the Five Below Second Quarter 2026 Earnings Conference Call"). Measured on
# that transcript: 47 sentences matched, none of them about guidance, and the
# intersection with a directional word was ZERO. Narrowed to the words a
# management team actually uses when attributing something to its own guide --
# taken from the FIVE 2026-09-02 call, where the real lines read "as we've
# constructed the guide, certainly, run rate momentum of the business factored
# into that" and "we would expect some level of tailwinds moving into the back
# half". That cut it to 20 sentences, all genuinely about the outlook.
_GUIDANCE_CONTEXT = re.compile(
    r"\b(guidance|outlook|guide|assum\w+|embedd?\w*|contemplat\w+|"
    r"factored|baked in|built in(?:to)?|implied?\s+in)\b",
    re.IGNORECASE,
)


@dataclass
class PutTake:
    direction: str   # "tailwind" | "headwind"
    text: str


def extract_puts_and_takes(text: str, *, limit: int = 4) -> list[PutTake]:
    """Discrete headwinds/tailwinds a company tied to its own guidance.

    Requires BOTH a directional word and guidance context in the same sentence.
    A transcript mentions pressure and benefit constantly; without the context
    gate this returns the whole call.
    """
    if not text:
        return []
    out: list[PutTake] = []
    seen: set[str] = set()
    for sent in re.split(r"(?<=[.!?])\s+", text):
        s = " ".join(sent.split())
        if len(s) < 40 or len(s) > 320:
            continue
        # An analyst's QUESTION is not the company's put or take. Quoting
        # "Or is this just taking more of a prudent or conservative approach in
        # the outlook?" as a headwind attributes the analyst's framing to
        # management. Callers should also pass management turns only; this is
        # the backstop for when the whole transcript is handed over.
        if s.endswith("?"):
            continue
        if not _GUIDANCE_CONTEXT.search(s):
            continue
        tail, head = _TAILWIND.search(s), _HEADWIND.search(s)
        if not (tail or head):
            continue
        # A sentence naming both is a genuine "puts and takes" statement; the
        # earlier-occurring word decides which way it leans.
        if tail and head:
            direction = "tailwind" if tail.start() < head.start() else "headwind"
        else:
            direction = "tailwind" if tail else "headwind"
        key = s.lower()[:70]
        if key in seen:
            continue
        seen.add(key)
        out.append(PutTake(direction=direction, text=s))
        if len(out) >= limit:
            break
    return out
