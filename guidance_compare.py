"""FY guide vs pre-print Street consensus -- the 4th results square (board #298 Phase B).

Pure: no I/O. `assess()` takes the release text, the reported event date and the
pre-release snapshot SET (consensus_snapshot.pre_release_snapshot_set) and
returns a GuidanceVerdict. Every gate that fails makes that metric ABSTAIN with a
named reason; an abstention is never a pass.

Plan: plans/guidance_vs_consensus_square_plan_2026-09-17.md -- v2 design verdict,
v3 "Phase B carried forward", v4, v5 and the round-5 note (later wins).

Invariant (plan section 10): the square is green/red/yellow ONLY when a
consolidated, FY, USD, matched-basis guide range was compared against a
consensus snapshot taken before the release cutoff, with >= 3 analysts. The
GuidanceVerdict constructor enforces the snapshot half of that at runtime.
"""

from __future__ import annotations

import html
import math
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from daily_summary import extract_guidance_blocks, is_column_header  # noqa: F401
from guidance_parse import GuidanceRange, extract_period_end, parse_guidance_blocks

# ---------------------------------------------------------------------------
# Glyphs (plan v4 M3 / H3, v5). One place, so the legend can list them all.
# ---------------------------------------------------------------------------

GREEN = "\U0001F7E9"      # guide >= Street
RED = "\U0001F7E5"        # guide below Street
YELLOW = "\U0001F7E8"     # guide straddles Street
NOT_COMPARABLE = "\U0001F532"  # guide found, not comparable (abstained / partial / no Street FY)
NO_FY_GUIDE = "⬜"   # no FY $ range parsed
OUTSIDE = "⬛"        # not checked: outside the v1 cohort
NO_CHECK = "⚠️"  # could not check: no snapshot / no release / budget / error

STATE_GLYPH = {
    "green": GREEN, "red": RED, "yellow": YELLOW,
    "not_comparable": NOT_COMPARABLE, "no_guide": NO_FY_GUIDE,
    "outside": OUTSIDE, "no_check": NO_CHECK,
}
COLOURED = frozenset({"green", "red", "yellow"})

# ---------------------------------------------------------------------------
# Gate vocabulary
# ---------------------------------------------------------------------------

# ONE FY-evidence regex (plan v4 M1), used by G1 AND by the 0.2x/0.4x floor split,
# so the two cannot disagree about what "full year" looks like.
_FY_EVIDENCE = re.compile(
    r"\b(?:full[- ]year|annual|for\s+the\s+(?:fiscal\s+)?year|"
    r"fiscal\s+year|fiscal\s+(?:20)?\d\d|FY\s*'?(?:20)?\d\d|"
    r"20\d\d\s+(?:outlook|guidance))\b",
    re.IGNORECASE,
)
_QUARTER = re.compile(
    r"\b(?:(?:first|second|third|fourth)\s+quarter|Q[1-4]|[1-4]Q|"
    r"for\s+the\s+quarter|quarterly)\b",
    re.IGNORECASE,
)
_EXPLICIT_YEAR = re.compile(
    r"\b(?:fiscal\s+(?:year\s+)?|FY\s*'?|full[- ]year\s+(?:of\s+)?)((?:20)?\d\d)\b"
    r"|\b(20\d\d)\b",
    re.IGNORECASE,
)
# G3 scope narrowers (plan v2 M1: a DENYlist, not the v1 allowlist).
_SCOPE_NARROWER = re.compile(
    r"\b(?:segment|organic|constant[- ]currency|excluding|ex-|product|"
    r"franchise|division|business|brand|platform|services?|subscription)\b",
    re.IGNORECASE,
)
# An all-caps product name in the words before "revenue" (INSM ARIKAYCE).
_PRODUCT_CAPS = re.compile(r"\b(?!GAAP\b|USD\b|FY\b|EPS\b)[A-Z]{4,}\b")
_EPS_ADJUSTED = re.compile(r"\b(adjusted|non[- ]gaap|core)\b", re.IGNORECASE)
_FROM_TO = re.compile(r"\bfrom\s+\$?\(?-?[\d.,]+", re.IGNORECASE)
_BOILERPLATE = re.compile(
    r"(?:with|has)\s+annual\s+(?:sales|revenues?)|\bis\s+a\s+(?:global|leading)\b",
    re.IGNORECASE,
)

_CURRENT_TOK = {"current", "updated", "revised", "new"}
_PRIOR_TOK = {"prior", "previous", "original", "initial"}
_COL_TOKEN = re.compile(
    r"\b(current|updated|revised|new|prior|previous|original|initial|"
    r"non[- ]gaap|gaap|adjusted|low|high)\b",
    re.IGNORECASE,
)

ANALYST_FLOOR = 3
RATIO_FLOOR_LINE = 0.2      # FY evidence on the source line itself
RATIO_FLOOR_HEADING = 0.4   # FY evidence only from the heading (plan v2 C2c / v3 H2-NEW)
RATIO_CEIL = 5.0
SENTENCE_BEFORE = 130
SENTENCE_AFTER = 25


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class Comparison:
    metric: str               # "revenue" | "eps"
    low: float
    high: float
    consensus: float
    analysts: int
    period_end: str
    snapshot_taken_at: str
    verdict: str              # "below" | "at_above" | "straddles"
    sentence: str             # verbatim, truncated around the figure


@dataclass
class GuidanceVerdict:
    state: str
    reason: str = ""
    compared: list[Comparison] = field(default_factory=list)
    abstained: dict[str, str] = field(default_factory=dict)   # metric -> reason
    sentence: str = ""
    cutoff_iso: str = ""

    def __post_init__(self):
        if self.state not in STATE_GLYPH:
            raise ValueError(f"unknown guidance state {self.state!r}")
        if self.state in COLOURED:
            # Section 10 invariant, enforced where it cannot be bypassed.
            if not self.compared:
                raise ValueError("a coloured guidance verdict needs >=1 comparison")
            if not self.cutoff_iso:
                raise ValueError("a coloured guidance verdict needs its release cutoff")
            for c in self.compared:
                if not c.snapshot_taken_at or not c.snapshot_taken_at < self.cutoff_iso:
                    raise ValueError(
                        f"snapshot {c.snapshot_taken_at!r} is not before the "
                        f"release cutoff {self.cutoff_iso!r}")

    @property
    def glyph(self) -> str:
        return STATE_GLYPH[self.state]


def outside_cohort() -> GuidanceVerdict:
    return GuidanceVerdict("outside", "not checked (outside v1 cohort)")


def could_not_check(reason: str) -> GuidanceVerdict:
    return GuidanceVerdict("no_check", reason)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def truncate_around(line: str, span: Optional[tuple[int, int]]) -> str:
    """`...` + line[end-130 : end+25] + `...`, HTML-escaped for Slack mrkdwn.

    Truncating from the START cut off the high end of 4 of 67 FY ranges (v5).
    """
    if span is None:
        end = min(len(line), SENTENCE_BEFORE)
    else:
        end = span[1]
    lo = max(0, end - SENTENCE_BEFORE)
    hi = min(len(line), end + SENTENCE_AFTER)
    text = line[lo:hi].strip()
    if lo > 0:
        text = "…" + text
    if hi < len(line):
        text = text + "…"
    # Slack mrkdwn treats <, > and & as control characters (plan v4 Lows).
    return html.escape(text, quote=False)


def _column_verdict(header: str, metric: str) -> Optional[str]:
    """None when column 1 is unambiguously the current (and adjusted) range.

    Returns an abstention reason otherwise. `Low | High` is ONE current range
    (v5 M), `Current | Prior` passes (FIVE), `Previous | Updated` (EHC, FICO)
    and `GAAP | Adjusted` (SPGI) abstain. `adjusted`-first is non-abstaining
    (round 5).
    """
    if not header:
        return None
    toks = [t.lower().replace(" ", "-") for t in _COL_TOKEN.findall(header)]
    toks = [t for t in toks if t not in ("low", "high")]
    if not toks:
        return None  # Low | High
    time_toks = [t for t in toks if t in _CURRENT_TOK or t in _PRIOR_TOK]
    basis_toks = [t for t in toks if t in ("gaap", "adjusted", "non-gaap")]
    if time_toks and time_toks[0] in _PRIOR_TOK:
        return "columns"
    if basis_toks and basis_toks[0] == "gaap" and len(set(basis_toks)) > 1:
        return "columns"
    if basis_toks == ["gaap"] and metric == "eps":
        return "columns"
    return None


def _fy_evidence(line: str, label: str) -> tuple[bool, bool]:
    """(is_fy, evidence_on_line). The source line overrides the heading."""
    if _QUARTER.search(line):
        return False, False
    if _FY_EVIDENCE.search(line):
        return True, True
    if label and _FY_EVIDENCE.search(label) and not _QUARTER.search(label):
        return True, False
    return False, False


def _explicit_year(line: str, label: str) -> Optional[int]:
    for text in (line, label):
        for m in _EXPLICIT_YEAR.finditer(text or ""):
            raw = m.group(1) or m.group(2)
            y = int(raw)
            return y + 2000 if y < 100 else y
    return None


def _year_fits(year: int, period_end: date) -> bool:
    """Either naming convention (plan v2 C1): the year it ends, or the year a
    Jan/early-Feb 52/53-week year starts (FIVE, LULU)."""
    if period_end.year == year:
        return True
    return period_end.month in (1, 2) and period_end.year == year + 1


def _half_unit(r: GuidanceRange) -> float:
    """Half a unit of the guide's last stated digit ("$5.5 billion" -> 0.05e9)."""
    if r.figure_span is None:
        return 0.0
    frag = r.source_line[r.figure_span[0]:r.figure_span[1]]
    nums = re.findall(r"\d[\d,]*(?:\.(\d+))?", frag)
    decimals = len(nums[-1]) if nums else 0
    scale = 1.0
    sm = re.findall(r"(billion|million|thousand|bn|mm|[bmk])\b", frag, re.IGNORECASE)
    if sm:
        scale = {"billion": 1e9, "bn": 1e9, "b": 1e9, "million": 1e6, "mm": 1e6,
                 "m": 1e6, "thousand": 1e3, "k": 1e3}[sm[-1].lower()]
    return 0.5 * (10 ** -decimals) * scale


def _sign(x: float) -> int:
    return 0 if x == 0 else (1 if x > 0 else -1)


def _metric_of(r: GuidanceRange) -> Optional[str]:
    if r.key == "revenue":
        return "revenue"
    if r.key in ("eps", "adj_eps"):
        return "eps"
    return None


def _label_window(r: GuidanceRange, chars: int = 40) -> str:
    i = r.source_line.lower().find(r.label.lower())
    if i < 0:
        return r.label
    return r.source_line[max(0, i - chars): i + len(r.label)]


# ---------------------------------------------------------------------------
# Structural FY match (plan v2 C1, v3 C1-NEW + M1)
# ---------------------------------------------------------------------------

def _period_candidates(quarter_end: date, period_ends: list[date]) -> list[date]:
    return sorted(p for p in period_ends
                  if quarter_end < p <= quarter_end + timedelta(days=550))


def _map_period(r_line: str, label: str, quarter_end: date,
                period_ends: list[date]) -> tuple[Optional[date], str]:
    cands = _period_candidates(quarter_end, period_ends)
    if not cands:
        return None, "period"
    first = cands[0]
    if first > quarter_end + timedelta(days=380):
        return None, "period"
    second = cands[1] if len(cands) > 1 else None
    year = _explicit_year(r_line, label)
    if year is None:
        matched = first
    elif _year_fits(year, first):
        matched = first
    elif second is not None and _year_fits(year, second):
        matched = second
    else:
        return None, "period"
    # FYE-change stub (v3 M1): only when a previous period exists.
    earlier = [p for p in period_ends if p < matched]
    if earlier:
        gap = (matched - max(earlier)).days
        if not 330 <= gap <= 400:
            return None, "period"
    return matched, ""


# ---------------------------------------------------------------------------
# assess()
# ---------------------------------------------------------------------------

def snapshot_gap_reason(snapshot_status: str) -> str:
    """The stated reason for a non-ok, non-empty snapshot status."""
    if snapshot_status == "missing":
        return "no pre-print snapshot"
    if snapshot_status.startswith("partial:currency_error"):
        return f"snapshot currency unresolved ({snapshot_status})"
    return f"snapshot not usable ({snapshot_status or 'no status'})"


def assess(
    release_text: str,
    event_date,
    snapshot_status: str,
    snapshots: dict[str, dict],
    cutoff_iso: str,
) -> GuidanceVerdict:
    """The row verdict for one release against its pre-release snapshot set."""
    if isinstance(event_date, str):
        event_date = date.fromisoformat(event_date)

    blocks = extract_guidance_blocks(release_text or "", max_blocks=6, max_lines=12)

    # --- candidate FY $ ranges per metric (G1 + unit) ---------------------
    fy: dict[str, list[tuple[GuidanceRange, object, bool]]] = {"revenue": [], "eps": []}
    for b in blocks:
        for r in parse_guidance_blocks([b]):
            metric = _metric_of(r)
            if metric is None or r.unit != "currency":
                continue
            is_fy, on_line = _fy_evidence(r.source_line, b.label)
            if is_fy:
                fy[metric].append((r, b, on_line))

    if not fy["revenue"] and not fy["eps"]:
        return GuidanceVerdict("no_guide", "no FY $ range parsed")

    # --- snapshot state (pipeline gaps vs coverage facts) -----------------
    # The status vocabulary is consensus_snapshot.pre_release_snapshot_set's
    # (Phase A's contract wins): "ok" | "empty" | "missing" |
    # "partial:<reason>". Anything that is not "ok" or "empty" is a pipeline
    # gap and the square degrades with the status named -- an unknown status
    # must never fall through to a comparison.
    if snapshot_status == "empty":
        return GuidanceVerdict("not_comparable", "no Street FY consensus")
    if snapshot_status != "ok":
        return could_not_check(snapshot_gap_reason(snapshot_status))
    if not snapshots:
        return GuidanceVerdict("not_comparable", "no Street FY consensus")

    qe_iso = extract_period_end(release_text, event_date)
    period_ends = sorted(date.fromisoformat(p) for p in snapshots)

    compared: list[Comparison] = []
    abstained: dict[str, str] = {}
    first_sentence = ""

    for metric in ("revenue", "eps"):
        cands = fy[metric]
        if not cands:
            continue  # not guided: neither compared nor abstained (trap 5)
        if qe_iso is None:
            abstained[metric] = "period"
            continue
        qe = date.fromisoformat(qe_iso)

        passing: list[tuple[GuidanceRange, bool, date]] = []
        reasons: list[str] = []
        for r, b, on_line in cands:
            reason = _gates_g3_g6(r, b, metric)
            if reason:
                reasons.append(reason)
                continue
            matched, why = _map_period(r.source_line, b.label, qe, period_ends)
            if matched is None:
                reasons.append(why)
                continue
            passing.append((r, on_line, matched))
        if not passing:
            abstained[metric] = reasons[0] if reasons else "period"
            continue

        # C1-NEW: compare only ranges mapped to ONE period -- the first
        # structural period when any map there, else the next (Q4 initial
        # next-year outlook). Then the LARGEST range among those that already
        # passed G1-G5 (v4 H1).
        mapped = sorted({p for _, _, p in passing})
        target = mapped[0]
        pool = [x for x in passing if x[2] == target]
        r, on_line, matched = max(pool, key=lambda x: abs(x[0].high))

        snap = snapshots[matched.isoformat()]
        if (snap.get("currency") or "").upper() != "USD":
            abstained[metric] = "currency"
            continue
        c = snap.get("revenue_avg" if metric == "revenue" else "eps_avg")
        n = snap.get("num_analysts_revenue" if metric == "revenue" else "num_analysts_eps")
        if c is None or (isinstance(c, float) and math.isnan(c)):
            abstained[metric] = "no Street figure"
            continue
        if n is None or n < ANALYST_FLOOR:
            abstained[metric] = "analysts<3"
            continue
        mid = r.midpoint
        # H3: the sign must agree; sign(0) matches either (v4 Lows).
        if _sign(mid) and _sign(c) and _sign(mid) != _sign(c):
            abstained[metric] = "sign"
            continue
        if c != 0 and mid != 0:
            ratio = abs(mid) / abs(c)
            floor = RATIO_FLOOR_LINE if on_line else RATIO_FLOOR_HEADING
            if ratio < floor or ratio > RATIO_CEIL:
                abstained[metric] = f"ratio {ratio:.2f}x"
                continue

        tol = max(0.01 * abs(c), _half_unit(r), 0.005 if metric == "eps" else 0.0)
        if r.high < c - tol:
            verdict = "below"
        elif r.low >= c - tol:
            verdict = "at_above"
        else:
            verdict = "straddles"
        sentence = truncate_around(r.source_line, r.figure_span)
        first_sentence = first_sentence or sentence
        compared.append(Comparison(
            metric=metric, low=r.low, high=r.high, consensus=float(c),
            analysts=int(n), period_end=matched.isoformat(),
            snapshot_taken_at=snap.get("taken_at") or "", verdict=verdict,
            sentence=sentence,
        ))

    return _row_verdict(compared, abstained, first_sentence, cutoff_iso)


def _gates_g3_g6(r: GuidanceRange, block, metric: str) -> str:
    """G3 scope, G4 basis, columns, G6 sanity. Returns an abstention reason or ''."""
    line = r.source_line
    # Columns first: an EPS line under a GAAP | Adjusted header reads as
    # unlabelled, and "basis" would hide the real reason (plan v4 test 2).
    col = _column_verdict(getattr(block, "column_header", "") or "", metric)
    if col:
        return col
    if metric == "revenue":
        head = _label_window(r, 40)
        before = head[: max(0, len(head) - len(r.label))]
        if _SCOPE_NARROWER.search(before) or _PRODUCT_CAPS.search(before):
            return "scope"
        if r.low <= 0:
            return "sanity"
    else:
        # G4: the metric label itself (plus 40 chars before) must say adjusted.
        if not _EPS_ADJUSTED.search(_label_window(r, 40)):
            return "basis"
    if r.low > r.high:
        return "sanity"
    if r.low > 0 and r.high / r.low > 1.5:
        return "sanity"
    # M2: a restated range only when `from` precedes the first figure.
    fm = _FROM_TO.search(line)
    if fm and r.figure_span and fm.start() <= r.figure_span[0]:
        return "from/to"
    if _BOILERPLATE.search(line):
        return "boilerplate"
    return ""


def _row_verdict(compared, abstained, sentence, cutoff_iso) -> GuidanceVerdict:
    """Section 5.3: asymmetric. RED survives missing information; GREEN does not."""
    if not compared:
        why = ", ".join(f"{m} {r}" for m, r in abstained.items()) or "not comparable"
        return GuidanceVerdict("not_comparable", why, abstained=abstained)
    kinds = {c.verdict for c in compared}
    only = "" if len(compared) == 2 or abstained else f" ({compared[0].metric} only)"
    if "below" in kinds:
        return GuidanceVerdict("red", "guide below Street" + only, compared,
                               abstained, sentence, cutoff_iso)
    if abstained:
        why = ", ".join(f"{m} {r}" for m, r in abstained.items())
        return GuidanceVerdict("not_comparable", f"partial: {why}",
                               abstained=abstained)
    if "straddles" in kinds:
        return GuidanceVerdict("yellow", "guide straddles Street" + only, compared,
                               abstained, sentence, cutoff_iso)
    return GuidanceVerdict("green", "guide at/above Street" + only, compared,
                           abstained, sentence, cutoff_iso)
