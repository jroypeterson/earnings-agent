"""Board #298 Phase B: the FY-guide-vs-pre-print-Street square.

Pure and offline: release text is inline (reconstructed in the SHAPES the plan
records for EHC, SPGI, FIVE and FICO -- not byte-for-byte EDGAR recordings),
snapshots are stubbed dicts or rows in a temp SQLite DB, EDGAR is
monkeypatched. No FMP, no EDGAR network, no Slack.

Numbering follows plan v4 "Tests that must fail on a naive Phase B" (1-11) and
v5 test 12.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone

import pytest

import guidance_compare as gc
from daily_summary import extract_guidance_blocks
from guidance_compare import assess


CUTOFF = "2026-08-27T04:00:00Z"
TAKEN = "2026-08-26T12:00:00Z"


def snap(period_end, revenue=None, eps=None, n_rev=12, n_eps=12,
         currency="USD", taken_at=TAKEN):
    return {
        "ticker": "X", "fiscal_period_end": period_end, "taken_at": taken_at,
        "event_date": "2026-08-27", "revenue_avg": revenue, "eps_avg": eps,
        "num_analysts_revenue": n_rev, "num_analysts_eps": n_eps,
        "currency": currency, "fetch_status": "ok",
    }


def snaps(*rows):
    return {r["fiscal_period_end"]: r for r in rows}


# --- fixtures in the recorded shapes ---------------------------------------

JP_EXAMPLE = """
Acme Corp Reports Second Quarter 2026 Results
Results for the three months ended June 30, 2026 were strong.

2026 Outlook
For the full year 2026, the Company expects revenue of $0.95 billion to $1.05 billion.
"""

# EHC prints Previous | Updated -- previous FIRST (plan v4 C1).
EHC = """
Encompass Health Reports Results for Second Quarter 2026
Quarter ended June 30, 2026.

Full-Year 2026 Guidance
Previous Guidance Updated Guidance
Net operating revenues $5,900 million to $6,000 million $5,950 million to $6,050 million
Adjusted earnings per share from continuing operations $5.81 to $6.10 $5.89 to $6.11
"""

# SPGI prints GAAP | Adjusted.
SPGI = """
S&P Global Reports Second Quarter 2026 Results
For the quarter ended June 30, 2026.

2026 Guidance
GAAP Adjusted
Diluted EPS $17.00 to $17.25 $18.10 to $18.35
"""

# FIVE Q2 FY26: Current | Prior, figures wrapped under their long label, and a
# GAAP line whose range is LARGER than the adjusted one (v4 H1).
FIVE_Q2 = """
Five Below, Inc. Announces Second Quarter Fiscal 2026 Financial Results
Net sales for the thirteen weeks ended August 1, 2026 increased.

Third Quarter and Fiscal 2026 Outlook:
For the full year of Fiscal 2026:
Current Outlook Prior Outlook
Net sales $5.55 billion to $5.62 billion $5.40 billion to $5.48 billion
Diluted income per common share $12.10 to $12.58 $10.20 to $10.60
Adjusted diluted income per common share
$9.83 to $10.31 $8.65 to $9.05
"""

# FICO: Previous Fiscal 2026 | Updated Fiscal 2026 -- no "outlook"/"guidance" in
# the header line (v5 H-A).
FICO = """
FICO Announces Earnings of $7.40 per Share for Third Quarter Fiscal 2026
Quarter ended June 30, 2026.

Fiscal 2026 Guidance
Previous Fiscal 2026 Updated Fiscal 2026
Revenues $2.35 billion to $2.40 billion $2.40 billion to $2.45 billion
Non-GAAP EPS $38.17 to $38.50 $39.00 to $39.50
"""

FIVE_PERIODS = snaps(
    snap("2026-01-31", revenue=4.6e9, eps=7.0),
    snap("2027-01-31", revenue=5.58e9, eps=9.60),
    snap("2028-01-31", revenue=6.2e9, eps=11.0),
)


# --- 1: EHC column order --------------------------------------------------

def test_1_ehc_previous_first_table_never_compares_the_previous_range():
    v = assess(EHC, "2026-07-28", "ok",
               snaps(snap("2025-12-31", revenue=5.4e9, eps=5.5),
                     snap("2026-12-31", revenue=6.0e9, eps=6.0)), CUTOFF)
    for c in v.compared:
        assert not (c.metric == "eps" and c.low == 5.81 and c.high == 6.10)
    assert v.abstained.get("eps") == "columns"
    assert v.state == "not_comparable"


# --- 2: SPGI GAAP | Adjusted ------------------------------------------------

def test_2_spgi_gaap_first_columns_abstain():
    v = assess(SPGI, "2026-07-30", "ok",
               snaps(snap("2025-12-31", eps=15.0), snap("2026-12-31", eps=18.0)), CUTOFF)
    assert v.abstained.get("eps") == "columns"
    assert v.state == "not_comparable"


# --- 3: FIVE Q2 FY26 --------------------------------------------------------

def test_3_five_q2_compares_adjusted_9_83_to_10_31():
    v = assess(FIVE_Q2, "2026-08-27", "ok", FIVE_PERIODS, CUTOFF)
    eps = [c for c in v.compared if c.metric == "eps"]
    assert eps, (v.state, v.reason, v.abstained)
    assert (eps[0].low, eps[0].high) == (9.83, 10.31)
    assert eps[0].period_end == "2027-01-31"
    # 9.83-10.31 vs 9.60 -> at/above; revenue 5.55-5.62B vs 5.58B is within
    # the 1% tolerance at the low end -> at/above. The GAAP 12.10-12.58 range
    # (larger) never wins, because it failed G4 before the largest-range pick.
    assert v.state == "green"


# --- 4: snapshot status -----------------------------------------------------

def test_4_empty_is_not_comparable_and_every_gap_is_could_not_check():
    # Status vocabulary is consensus_snapshot.pre_release_snapshot_set's
    # (Phase A): ok | empty | missing | partial:<reason>. Phase B invents none.
    assert assess(JP_EXAMPLE, "2026-08-05", "empty", {}, CUTOFF).state == "not_comparable"
    v = assess(JP_EXAMPLE, "2026-08-05", "missing", {}, CUTOFF)
    assert v.state == "no_check" and "no pre-print snapshot" in v.reason
    v = assess(JP_EXAMPLE, "2026-08-05", "partial:currency_error:402", {}, CUTOFF)
    assert v.state == "no_check" and "partial:currency_error:402" in v.reason
    # A status nobody anticipated degrades, naming itself; it never compares.
    v = assess(JP_EXAMPLE, "2026-08-05", "something_new",
               snaps(snap("2026-12-31", revenue=2.0e9)), CUTOFF)
    assert v.state == "no_check" and "something_new" in v.reason


@pytest.fixture
def db(tmp_path):
    from storage import init_db
    conn = init_db(tmp_path / "t.db")
    yield conn
    conn.close()


def _ins(conn, ticker, period, taken, status="ok", rev=1e9, eps=1.0):
    conn.execute(
        "INSERT INTO consensus_snapshot (ticker, fiscal_period_end, taken_at, "
        "event_date, revenue_avg, eps_avg, num_analysts_revenue, num_analysts_eps, "
        "currency, fetch_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ticker, period, taken, "2026-08-05", rev, eps, 10, 10, "USD", status))


def _reported_event(conn, ticker, event_date):
    conn.execute(
        "INSERT INTO events (ticker, event_date, event_hour, date_confirmed, "
        "reported, tier) VALUES (?,?,?,?,?,1)", (ticker, event_date, "bmo", 1, 1))
    conn.commit()


def test_4_snapshot_set_statuses_from_the_db(db):
    """Phase B reads Phase A's reader and its labels -- nothing of its own."""
    from consensus_snapshot import pre_release_snapshot_set
    cutoff = datetime(2026, 8, 5, 4, 0, tzinfo=timezone.utc)
    assert pre_release_snapshot_set(db, "AAA", cutoff) == ("missing", {})

    _ins(db, "BBB", "", "2026-08-04T12:00:00Z", status="empty")
    assert pre_release_snapshot_set(db, "BBB", cutoff) == ("empty", {})

    # Phase A's contract: the newest in-cycle ANSWER wins, so an empty after
    # an ok is "empty" (Phase B's former "suspect_empty" is not a label).
    _ins(db, "CCC", "2026-12-31", "2026-08-03T12:00:00Z")
    _ins(db, "CCC", "", "2026-08-04T12:00:00Z", status="empty")
    assert pre_release_snapshot_set(db, "CCC", cutoff) == ("empty", {})

    # Only error rows: no answer in this cycle -> missing.
    _ins(db, "DDD", "", "2026-08-04T12:00:00Z", status="error:429")
    assert pre_release_snapshot_set(db, "DDD", cutoff) == ("missing", {})

    # An error AFTER a clean fetch keeps the clean pre-print set.
    _ins(db, "EEE", "2026-12-31", "2026-08-03T12:00:00Z")
    _ins(db, "EEE", "2027-12-31", "2026-08-03T12:00:00Z")
    _ins(db, "EEE", "", "2026-08-04T12:00:00Z", status="error:429")
    status, rows = pre_release_snapshot_set(db, "EEE", cutoff)
    assert status == "ok" and sorted(rows) == ["2026-12-31", "2027-12-31"]

    # A snapshot AFTER the cutoff is invisible (circular post-print).
    _ins(db, "FFF", "2026-12-31", "2026-08-05T21:00:00Z")
    assert pre_release_snapshot_set(db, "FFF", cutoff) == ("missing", {})

    # Currency unresolved: the partial label passes through verbatim.
    _ins(db, "GGG", "2026-12-31", "2026-08-04T12:00:00Z",
         status="partial:currency_error:402")
    assert pre_release_snapshot_set(db, "GGG", cutoff) == (
        "partial:currency_error:402", {})


def test_consensus_snapshot_defines_each_top_level_name_once():
    """The shadowing guard. Phase B once re-defined pre_release_snapshot_set
    lower in consensus_snapshot.py; Python kept the LATER def, silently
    replacing Phase A's cycle-scoped reader (and latest_pre_release_snapshot,
    which is built on it) with no error anywhere."""
    import ast
    import collections
    from pathlib import Path
    for name in ("consensus_snapshot.py", "guidance_compare.py"):
        tree = ast.parse(Path(__file__).with_name(name).read_text(encoding="utf-8"))
        defs = collections.Counter(
            n.name for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)))
        dupes = sorted(k for k, c in defs.items() if c > 1)
        assert not dupes, f"{name} defines {dupes} more than once"


class _Edgar:
    def __init__(self, text):
        self.text = text

    def find_earnings_release_filing(self, ticker, lo, hi):
        return "filing"

    def find_results_6k(self, ticker, lo, hi):
        return None

    def fetch_release_document(self, ticker, filing, should_stop=None):
        from types import SimpleNamespace
        return SimpleNamespace(text=self.text)


@pytest.mark.parametrize("taken,expected", [
    # Taken AFTER the previous print (07-20), before this cutoff: this cycle's.
    ("2026-07-25T12:00:00Z", "red"),
    # Taken BEFORE the previous print, yet inside the 30-day age bound: that
    # print's consensus. Served here it would grade Q2 against stale Street.
    ("2026-07-18T12:00:00Z", "no_check"),
])
def test_phase_b_path_never_serves_a_prior_cycles_snapshot(db, taken, expected):
    """End to end through main._guidance_verdict_for -> Phase A's reader. The
    shadowing defect served the 07-18 row as this release's consensus."""
    from types import SimpleNamespace
    import consensus_snapshot as cs
    import main
    _reported_event(db, "ACME", "2026-07-20")
    db.execute(
        "INSERT INTO events (ticker, event_date, event_hour, date_confirmed, "
        "reported, tier) VALUES ('ACME', '2026-08-05', 'bmo', 1, 0, 1)")
    _ins(db, "ACME", "2025-12-31", taken, rev=1.8e9)
    _ins(db, "ACME", "2026-12-31", taken, rev=2.0e9)
    db.commit()
    r = SimpleNamespace(ticker="ACME", event_date="2026-08-05", event_hour="bmo")
    v = main._guidance_verdict_for(db, r, cs, _Edgar(JP_EXAMPLE), gc)
    assert v.state == expected, v.reason
    if expected == "no_check":
        assert "no pre-print snapshot" in v.reason


# --- 5: structural match from a period set ----------------------------------

@pytest.mark.parametrize("line,qe,periods,expected", [
    # FIVE "Fiscal 2026" -> 2027-01-31 (names the year it starts)
    ("Fiscal 2026 net sales", "2026-08-01", ["2026-01-31", "2027-01-31", "2028-01-31"], "2027-01-31"),
    # MCK "fiscal 2027" -> 2027-03-31
    ("fiscal 2027 adjusted EPS", "2026-06-30", ["2026-03-31", "2027-03-31", "2028-03-31"], "2027-03-31"),
    # ADI fiscal 2026 -> Nov 2026
    ("fiscal 2026 revenue", "2026-08-01", ["2025-11-01", "2026-10-31", "2027-10-30"], "2026-10-31"),
    # calendar Q3 "initial 2027 outlook" -> 2027-12-31 (the SECOND period)
    ("initial 2027 outlook revenue", "2026-09-30", ["2025-12-31", "2026-12-31", "2027-12-31"], "2027-12-31"),
    # no explicit year -> nearest
    ("full year revenue", "2026-06-30", ["2025-12-31", "2026-12-31", "2027-12-31"], "2026-12-31"),
    # a year that fits neither structural period -> abstain
    ("fiscal 2029 revenue", "2026-06-30", ["2025-12-31", "2026-12-31", "2027-12-31"], None),
])
def test_5_structural_fy_match(line, qe, periods, expected):
    got, _ = gc._map_period(line, "", date.fromisoformat(qe),
                            [date.fromisoformat(p) for p in periods])
    assert (got.isoformat() if got else None) == expected


# --- 6: glyphs and legend ---------------------------------------------------

def test_6_non_cohort_renders_black_and_every_glyph_is_in_the_legend(monkeypatch):
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    from notifications import _render_result_markers, build_results_legend_text
    from main import _in_guidance_cohort
    r = _row("ZZZ", tier=3, position="")
    assert not _in_guidance_cohort(r)
    r.guidance_verdict = gc.outside_cohort()
    assert _render_result_markers(r).split()[-1] == gc.OUTSIDE
    legend = build_results_legend_text()
    for glyph in gc.STATE_GLYPH.values():
        assert glyph in legend, glyph
    assert "FY guide vs Street (pre-print)" in legend


def test_flag_off_means_three_squares_and_no_legend_entry(monkeypatch):
    monkeypatch.delenv("GUIDANCE_SQUARE", raising=False)
    from notifications import _render_result_markers, build_results_legend_text
    assert len(_render_result_markers(_row("A")).split()) == 3
    assert "FY guide" not in build_results_legend_text()
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    assert len(_render_result_markers(_row("A")).split()) == 4


def test_unenriched_row_renders_could_not_check_never_a_colour(monkeypatch):
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    from notifications import _render_result_markers
    assert _render_result_markers(_row("A")).split()[-1] == gc.NO_CHECK


# --- 7: footer budget -------------------------------------------------------

def _red_verdict(ticker, sentence_len=380):
    filler = "x" * (sentence_len - 90)
    line = (f"The company {filler} expects full year 2026 revenue of "
            f"$0.95 billion to $1.05 billion for the year.")
    v = assess(f"Quarter ended June 30, 2026.\n2026 Outlook\n{line}\n",
               "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.5e9), snap("2026-12-31", revenue=2.0e9)),
               CUTOFF)
    assert v.state == "red", (v.state, v.reason)
    return v


def test_7_footer_budget_eleven_long_coloured_rows(monkeypatch):
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    from notifications import SLACK_MAX_BLOCKS, build_results_slack_blocks
    rows = []
    for i in range(11):
        r = _row(f"T{i:02d}", tier=1, position="Portfolio")
        r.guidance_verdict = _red_verdict(r.ticker)
        rows.append(r)
    # Pad with enough Tier 3 rows to hit the block cap and the overflow path.
    for i in range(2000):
        rows.append(_row(f"Z{i:04d}", tier=3))
    blocks = build_results_slack_blocks(rows, date(2026, 8, 5))
    assert len(blocks) <= SLACK_MAX_BLOCKS
    footer = blocks[-1]
    assert footer["type"] == "context"
    assert footer["elements"][0]["text"].startswith("FY guide square:")
    assert len(footer["elements"]) <= 10
    assert all(len(e["text"]) <= 2000 for e in footer["elements"])
    body = "\n".join(e["text"] for e in footer["elements"])
    for i in range(11):
        assert f"`T{i:02d}`" in body or "more coloured row" in body
    # Both ends of the range survive truncation around the figure (v5 M).
    assert "$0.95 billion" in body and "$1.05 billion" in body
    # Guard: the fixture really reaches the cap, or this test is vacuous.
    assert len(blocks) == SLACK_MAX_BLOCKS
    # Row accounting (the card NEVER drops a row): every padded row is rendered
    # in full, named in the overflow list, or covered by an exact "+N more".
    import re as _re
    text = "\n".join((b.get("text") or {}).get("text", "")
                     for b in blocks if b["type"] == "section")
    named = set(_re.findall(r"\bZ\d{4}\b", text))
    more = sum(int(n) for n in _re.findall(r"_\+(\d+) more not shown_", text))
    assert len(named) + more == 2000, (len(named), more)


def test_truncate_around_keeps_both_ends_and_escapes():
    line = ("<b>" + "a" * 300 + " revenue of $1.00 billion to $1.10 billion & more "
            + "b" * 100)
    s = line.index("$1.00")
    e = line.index("billion &") + len("billion")
    out = gc.truncate_around(line, (s, e))
    assert "$1.00 billion to $1.10 billion" in out
    assert out.startswith("…") and out.endswith("…")
    assert "&amp;" in out and "<b>" not in out


# --- 8: FY spellings agree under G1 and the floor ---------------------------

@pytest.mark.parametrize("spelling", [
    "full year", "full-year", "annual", "for the year", "FY26", "FY'26",
    "fiscal 2026", "fiscal year",
])
def test_8_fy_spellings(spelling):
    line = f"The Company expects {spelling} revenue of $0.45 billion to $0.50 billion."
    is_fy, on_line = gc._fy_evidence(line, "")
    assert is_fy and on_line
    # 0.475 / 2.0 = 0.24x: above the 0.2x line floor, below the 0.4x heading floor.
    v = assess(f"Quarter ended June 30, 2026.\nOutlook\n{line}\n", "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.8e9), snap("2026-12-31", revenue=2.0e9)),
               CUTOFF)
    assert v.state == "red", (spelling, v.state, v.reason, v.abstained)


def test_8_heading_only_evidence_uses_the_0_4_floor():
    text = ("Quarter ended June 30, 2026.\nFull Year 2026 Outlook\n"
            "Revenue of $0.45 billion to $0.50 billion.\n")
    v = assess(text, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.8e9), snap("2026-12-31", revenue=2.0e9)),
               CUTOFF)
    assert v.state == "not_comparable" and v.abstained["revenue"].startswith("ratio")


def test_quarter_line_overrides_fy_heading():
    assert gc._fy_evidence("For the third quarter of 2026, revenue $1B to $1.1B",
                           "2026 Outlook") == (False, False)


# --- 9, 10: extractor heading rules -----------------------------------------

def test_9_53_week_footnote_leaves_the_label_alone():
    text = ("Fiscal 2026 Outlook:\n"
            "For the full year of Fiscal 2026:\n"
            "Net sales are expected to be in the range of $5.40 billion to $5.48 billion.\n"
            "Fiscal 2026 is a 53-week year\n"
            "Adjusted diluted income per common share of $8.65 to $9.05.\n")
    blocks = extract_guidance_blocks(text, fy_headings=True)
    assert [b.label for b in blocks] == ["For the full year of Fiscal 2026"]
    assert len(blocks[0].lines) == 2


def test_9_colon_less_period_heading_opens_a_block():
    text = ("2026 Outlook\nFor the second quarter of Fiscal 2026\n"
            "Net sales of $1.18 billion to $1.20 billion.\n"
            "For the full year of Fiscal 2026\n"
            "Net sales of $5.40 billion to $5.48 billion.\n")
    labels = [b.label for b in extract_guidance_blocks(text, fy_headings=True)]
    assert "For the full year of Fiscal 2026" in labels


def test_10_full_year_caption_keeps_fy_evidence_under_a_column_header():
    blocks = extract_guidance_blocks(EHC, fy_headings=True)
    assert len(blocks) == 1
    b = blocks[0]
    assert b.label == "Full-Year 2026 Guidance" and b.period_label
    assert b.column_header == "Previous Guidance Updated Guidance"


def test_five_current_prior_header_is_captured_and_passes():
    blocks = extract_guidance_blocks(FIVE_Q2, fy_headings=True)
    fy = [b for b in blocks if "full year" in b.label.lower()]
    assert fy and fy[0].column_header == "Current Outlook Prior Outlook"
    assert gc._column_verdict(fy[0].column_header, "eps") is None
    # FIVE fixture must still attribute adjusted EPS (v3 H3-NEW).
    assert any("Adjusted diluted income per common share" in l and "$9.83" in l
               for l in fy[0].lines)


@pytest.mark.parametrize("header,metric,expected", [
    ("Current Outlook Prior Outlook", "eps", None),
    ("Previous Guidance Updated Guidance", "revenue", "columns"),
    ("GAAP Adjusted", "eps", "columns"),
    ("Adjusted GAAP", "eps", None),
    ("Low High", "revenue", None),
    ("Current FY Previous FY", "eps", None),
])
def test_column_verdicts(header, metric, expected):
    assert gc._column_verdict(header, metric) == expected


def test_reconciliation_caption_is_not_a_column_header():
    from daily_summary import is_column_header
    assert not is_column_header("Reconciliation of GAAP to Non-GAAP Financial Measures")
    assert is_column_header("GAAP | Non-GAAP")


# --- 12: FICO ---------------------------------------------------------------

def test_12_fico_previous_first_header_without_outlook_word_abstains():
    v = assess(FICO, "2026-07-30", "ok",
               snaps(snap("2025-09-30", revenue=2.0e9, eps=30.0),
                     snap("2026-09-30", revenue=2.4e9, eps=39.0)), CUTOFF)
    assert v.abstained.get("revenue") == "columns"
    assert v.abstained.get("eps") == "columns"
    assert v.state == "not_comparable"


# --- the plan's section 11 behaviours ---------------------------------------

def test_jp_example_one_billion_guide_against_two_billion_is_red():
    v = assess(JP_EXAMPLE, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.8e9), snap("2026-12-31", revenue=2.0e9)),
               CUTOFF)
    assert v.state == "red" and v.glyph == gc.RED
    assert "$0.95 billion to $1.05 billion" in v.sentence


def test_straddle_80_to_120_vs_100_is_yellow_never_green():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full year revenue of $80 million to $120 million.\n")
    v = assess(text, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=90e6), snap("2026-12-31", revenue=100e6)),
               CUTOFF)
    assert v.state == "yellow"


def test_gaap_only_eps_with_revenue_compared_is_partial_not_green():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full year revenue of $2.10 billion to $2.20 billion.\n"
            "Full year diluted EPS of $4.10 to $4.30.\n")
    v = assess(text, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.9e9, eps=3.5),
                     snap("2026-12-31", revenue=2.0e9, eps=4.0)), CUTOFF)
    assert v.abstained == {"eps": "basis"}
    assert v.state == "not_comparable"


def test_currency_not_usd_abstains():
    v = assess(JP_EXAMPLE, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.8e9, currency="DKK"),
                     snap("2026-12-31", revenue=2.0e9, currency="DKK")), CUTOFF)
    assert v.abstained == {"revenue": "currency"}


def test_quarterly_only_guide_is_no_fy_range():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "For the third quarter of 2026, revenue of $0.95 billion to $1.05 billion.\n")
    assert assess(text, "2026-08-05", "ok", {}, CUTOFF).state == "no_guide"


def test_coloured_verdict_constructor_rejects_a_post_cutoff_snapshot():
    c = gc.Comparison("revenue", 1, 2, 3, 5, "2026-12-31",
                      "2026-08-27T21:00:00Z", "below", "s")
    with pytest.raises(ValueError):
        gc.GuidanceVerdict("red", "x", [c], {}, "s", CUTOFF)
    with pytest.raises(ValueError):
        gc.GuidanceVerdict("green", "x", [], {}, "s", CUTOFF)


# --- seam: attach_guidance_verdicts -> rendered payload ----------------------

class _Doc:
    def __init__(self, text):
        self.text = text


def test_seam_enrichment_to_rendered_payload(db, monkeypatch):
    """Drives the real producer (attach_guidance_verdicts) into the real card
    builder, with EDGAR stubbed and snapshots seeded in the DB. Deleting the
    producer's assignment leaves every square ⚠️ and fails this test."""
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    import edgar_client
    import main
    from notifications import build_results_slack_blocks

    _ins(db, "JPX", "2025-12-31", "2026-08-04T12:00:00Z", rev=1.8e9)
    _ins(db, "JPX", "2026-12-31", "2026-08-04T12:00:00Z", rev=2.0e9)
    monkeypatch.setattr(edgar_client, "find_earnings_release_filing",
                        lambda t, a, b: object())
    monkeypatch.setattr(edgar_client, "find_results_6k", lambda t, a, b: None)
    monkeypatch.setattr(edgar_client, "fetch_release_document",
                        lambda t, f, should_stop=None: _Doc(JP_EXAMPLE))
    monkeypatch.setattr(edgar_client, "get_request_stats", lambda: (0, 0))

    held = _row("JPX", tier=1, position="Portfolio", event_date="2026-08-05")
    other = _row("TIII", tier=3, event_date="2026-08-05")
    nosnap = _row("NOSN", tier=2, position="Researching", event_date="2026-08-05")
    alerts = main.attach_guidance_verdicts(db, [other, nosnap, held])
    assert held.guidance_verdict.state == "red"
    assert other.guidance_verdict.state == "outside"
    assert nosnap.guidance_verdict.state == "no_check"
    assert alerts == ["NOSN: no pre-print snapshot"]

    blocks = build_results_slack_blocks([held, other, nosnap], date(2026, 8, 5))
    payload = str(blocks)
    jpx_line = next(l for b in blocks if b["type"] == "section"
                    for l in b["text"]["text"].split("\n") if "`JPX`" in l)
    assert jpx_line.split()[3] == gc.RED
    assert "$0.95 billion to $1.05 billion" in payload


def test_seam_budget_exhausted_renders_could_not_check(db, monkeypatch):
    import edgar_client
    import main
    monkeypatch.setattr(edgar_client, "get_request_stats", lambda: (0, 0))
    ticks = iter([0.0, 1000.0, 1000.0, 1000.0])
    r = _row("JPX", tier=1, position="Portfolio")
    main.attach_guidance_verdicts(db, [r], budget_s=150, clock=lambda: next(ticks))
    assert r.guidance_verdict.state == "no_check" and r.guidance_verdict.reason == "budget"


def test_seam_enrichment_raise_still_posts_with_could_not_check(monkeypatch):
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    import main
    posted = []
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", "https://example.invalid/e")
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", "https://example.invalid/s")
    monkeypatch.setattr(main, "post_slack", lambda wh, blocks, fb: posted.append((wh, blocks)))
    monkeypatch.setattr(main, "load_coverage", lambda: [])
    monkeypatch.setattr(main, "compute_season_stats", lambda *a, **k: None)
    monkeypatch.setattr(main, "get_ticktick_config", lambda: None)

    def boom(*a, **k):
        raise RuntimeError("SEC down")
    monkeypatch.setattr(main, "attach_guidance_verdicts", boom)
    r = _row("JPX", tier=1, position="Portfolio")
    assert main.notify_results(sqlite3.connect(":memory:"), [r], date(2026, 8, 5))
    card = [b for wh, b in posted if wh.endswith("/e")]
    status = [b for wh, b in posted if wh.endswith("/s")]
    assert card and gc.NO_CHECK in str(card[0])
    assert status and "SEC down" in str(status[0])


# --- helpers ----------------------------------------------------------------

def _row(ticker, tier=1, position="", event_date="2026-08-05"):
    from notifications import ResultRow
    return ResultRow(
        ticker=ticker, company_name=ticker, event_date=event_date,
        event_hour="bmo", eps_actual=1.0, eps_estimate=0.9, rev_actual=None,
        rev_estimate=None, tier=tier, sector="Other", position=position,
    )


# --- Codex 2026-09-29 round 1 regressions ------------------------------------

@pytest.mark.parametrize("line", [
    "For the second half of 2026, revenue is expected to be $900 million to $950 million.",
    "Second-half revenue is expected to be $900 million to $950 million.",
    "H2 2026 revenue is expected to be $900 million to $950 million.",
    "For the remainder of the year, revenue is expected to be $900 million to $950 million.",
    "Revenue for the nine months ending December 31, 2026 of $900 million to $950 million.",
])
def test_r1_a_half_year_guide_does_not_inherit_fy_from_an_annual_heading(line):
    """Under a generic '2026 Outlook' heading, a SUB-annual range graded against
    FY Street ($1.8B/$2.0B) is a false red. It must not be read as FY at all."""
    text = f"Quarter ended June 30, 2026.\n2026 Outlook\n{line}\n"
    v = assess(text, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.8e9), snap("2026-12-31", revenue=2.0e9)),
               CUTOFF)
    assert v.state == "no_guide", (v.state, v.reason)


def test_r1_control_a_full_year_line_under_the_same_heading_still_compares():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full year revenue is expected to be $0.95 billion to $1.05 billion.\n")
    v = assess(text, "2026-08-05", "ok",
               snaps(snap("2025-12-31", revenue=1.8e9), snap("2026-12-31", revenue=2.0e9)),
               CUTOFF)
    assert v.state == "red"


@pytest.mark.parametrize("status", ["missing", "partial:currency_error:402"])
def test_r1_a_snapshot_gap_surfaces_even_when_the_release_has_no_dollar_guide(status):
    """The gap is checked before no_guide, so a broken snapshot pipeline is
    alerted on a qualitative-outlook release instead of rendering a quiet
    'no guide'."""
    text = "Quarter ended June 30, 2026.\nOutlook\nWe remain confident in our strategy.\n"
    v = assess(text, "2026-08-05", status, {}, CUTOFF)
    assert v.state == "no_check"
    expected = "no pre-print snapshot" if status == "missing" else status
    assert expected in v.reason


def test_r1_budget_is_rechecked_between_edgar_calls_within_a_row(db, monkeypatch):
    """One row makes up to three blocking SEC calls. Once the budget is spent
    mid-row, the remaining calls must not run."""
    import edgar_client
    import main
    calls = []
    now = [0.0]

    def slow_8k(t, a, b):
        calls.append("8k")
        now[0] = 1000.0          # this call alone exhausts the budget
        return None

    import consensus_snapshot
    # A usable snapshot, so the row reaches EDGAR at all (since r2 a snapshot
    # gap returns before any SEC call).
    monkeypatch.setattr(consensus_snapshot, "pre_release_snapshot_set",
                        lambda conn, t, cutoff: ("ok", {}))
    monkeypatch.setattr(edgar_client, "get_request_stats", lambda: (0, 0))
    monkeypatch.setattr(edgar_client, "find_earnings_release_filing", slow_8k)
    monkeypatch.setattr(edgar_client, "find_results_6k",
                        lambda t, a, b: calls.append("6k") or object())
    monkeypatch.setattr(edgar_client, "fetch_release_document",
                        lambda t, f, should_stop=None: calls.append("doc"))
    r = _row("JPX", tier=1, position="Portfolio", event_date="2026-08-05")
    main.attach_guidance_verdicts(db, [r], budget_s=150, clock=lambda: now[0])
    assert calls == ["8k"]
    assert r.guidance_verdict.state == "no_check" and r.guidance_verdict.reason == "budget"


# --- flag-off invariance: the default extractor is the consensus preview's ----
# (2026-09-30 review). Phase B's block-splitting rules changed 17 of 80 live
# releases in the PRODUCTION consensus preview with GUIDANCE_SQUARE off; CON
# lost its whole outlook to a period-captioned press-release title.

CON_SHAPED = """
Concentra Reports Results For Its Second Quarter Ended June 30, 2026 and Raises FY 2026 Guidance
Revenue of $606.0 million, an increase of 10.0% from $550.8 million in Q2 2025
Adjusted EBITDA of $140.9 million, an increase of 22.5% from $115.0 million in Q2 2025
2026 Business Outlook
Revenue in the range of $2.325 billion to $2.375 billion
Adjusted EBITDA in the range of $485 million to $495 million
"""


def _outlook_lines(blocks):
    return [l for b in blocks if b.label == "2026 Business Outlook" for l in b.lines]


@pytest.mark.parametrize("fy_headings", [False, True])
def test_a_period_captioned_title_does_not_swallow_the_real_outlook(fy_headings):
    blocks = extract_guidance_blocks(CON_SHAPED, fy_headings=fy_headings)
    assert any("$2.325 billion" in l for l in _outlook_lines(blocks))


def test_default_mode_does_not_open_a_block_on_a_colon_less_period_line():
    text = ("2026 Outlook:\n"
            "Net sales of $5.40 billion to $5.48 billion.\n"
            "Full Year Results\n"
            "Net sales of $1,518.1 million for the year ended June 30, 2026.\n")
    labels = [b.label for b in extract_guidance_blocks(text)]
    assert labels == ["2026 Outlook"]


def test_default_mode_caption_is_not_period_labelled():
    assert not any(b.period_label for b in extract_guidance_blocks(EHC))


# --- Codex 2026-09-30 round 2 regressions ------------------------------------

from guidance_compare import COLOURED

_FY26 = lambda **kw: snaps(snap("2025-12-31", **kw), snap("2026-12-31", **kw))


@pytest.mark.parametrize("line", [
    "For 1H26, revenue is expected to be $900 million to $950 million.",
    "2H'26 revenue is expected to be $900 million to $950 million.",
    "1Q26 revenue is expected to be $900 million to $950 million.",
    "Revenue for the 9-month period ending December 31, 2026 of $900 million to $950 million.",
    "Fourth-quarter revenue is expected to be $900 million to $950 million.",
])
def test_r2_compact_sub_annual_labels_do_not_inherit_fy(line):
    text = f"Quarter ended June 30, 2026.\n2026 Outlook\n{line}\n"
    v = assess(text, "2026-08-05", "ok", _FY26(revenue=2.0e9), CUTOFF)
    assert v.state == "no_guide", (v.state, v.reason)


def test_r2_another_metrics_adjusted_does_not_qualify_gaap_eps():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full-year 2026 adjusted EBITDA of $100 million and diluted EPS of $1.00 to $1.10.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(eps=1.5), CUTOFF)
    assert v.state not in COLOURED, (v.state, v.reason)


def test_r2_control_adjusted_eps_still_compares():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full-year 2026 adjusted diluted EPS of $1.00 to $1.10.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(eps=1.5), CUTOFF)
    assert v.state == "red"


def test_r2_a_product_named_after_the_metric_narrows_scope():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full-year 2026 net sales guidance for ARIKAYCE is $900 million to $950 million.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(revenue=2.0e9), CUTOFF)
    assert v.state not in COLOURED, (v.state, v.reason)


def test_r2_control_unqualified_net_sales_still_compares():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full-year 2026 net sales guidance is $1.90 billion to $1.95 billion.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(revenue=2.0e9), CUTOFF)
    assert v.state == "red"


def test_r2_the_year_nearest_before_the_figure_picks_the_snapshot():
    text = ("Quarter ended September 30, 2026.\n2027 Outlook\n"
            "Compared with fiscal 2026, fiscal 2027 revenue is expected at "
            "$2.00 billion to $2.10 billion.\n")
    v = assess(text, "2026-10-28", "ok",
               snaps(snap("2026-12-31", revenue=3.0e9), snap("2027-12-31", revenue=2.0e9)),
               "2026-10-29T04:00:00Z")
    assert v.state == "green", (v.state, v.reason)
    assert v.compared[0].period_end == "2027-12-31"


@pytest.mark.parametrize("prefix", ["C$", "A$", "EUR "])
def test_r2_a_foreign_currency_guide_abstains(prefix):
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            f"Full-year 2026 revenue is expected to be {prefix}2.00 billion to $2.10 billion.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(revenue=1.5e9), CUTOFF)
    assert v.state not in COLOURED, (v.state, v.reason)


def test_r2_control_us_dollar_prefix_still_compares():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full-year 2026 revenue is expected to be US$2.00 billion to $2.10 billion.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(revenue=1.5e9), CUTOFF)
    assert v.state == "green", (v.state, v.reason)


def test_r2_a_snapshot_gap_is_named_before_any_edgar_early_return(db, monkeypatch):
    """No snapshot AND no release on EDGAR must still say 'no pre-print
    snapshot' -- and spend no SEC calls finding that out."""
    import edgar_client
    import main
    calls = []
    monkeypatch.setattr(edgar_client, "get_request_stats", lambda: (0, 0))
    monkeypatch.setattr(edgar_client, "find_earnings_release_filing",
                        lambda t, a, b: calls.append("8k"))
    monkeypatch.setattr(edgar_client, "find_results_6k",
                        lambda t, a, b: calls.append("6k"))
    r = _row("JPX", tier=1, position="Portfolio", event_date="2026-08-05")
    alerts = main.attach_guidance_verdicts(db, [r], budget_s=150, clock=lambda: 0.0)
    assert r.guidance_verdict.state == "no_check"
    assert r.guidance_verdict.reason == "no pre-print snapshot"
    assert calls == []
    assert alerts == ["JPX: no pre-print snapshot"]


# --- Codex 2026-09-30 round 3 regressions (resilience) -----------------------

def test_r3_a_zero_revenue_consensus_abstains_rather_than_grading_green():
    text = ("Quarter ended June 30, 2026.\n2026 Outlook\n"
            "Full year revenue is expected to be $0.95 billion to $1.05 billion.\n")
    v = assess(text, "2026-08-05", "ok", _FY26(revenue=0.0), CUTOFF)
    assert v.state not in COLOURED, (v.state, v.reason)


def test_r3_release_fetch_stops_between_exhibit_requests(monkeypatch):
    """A run of slow EX-99 candidates must not hold the results post: the
    caller's budget is consulted before each further request."""
    import edgar_client
    headers = "".join(
        f"<DOCUMENT>\n<TYPE>EX-99.{i}\n<SEQUENCE>{i}\n<FILENAME>ex99{i}.htm\n"
        for i in (1, 2, 3))
    fetched = []

    def fake_get(url):
        fetched.append(url)
        return headers if url.endswith("-index-headers.html") else "<p>short</p>"

    monkeypatch.setattr(edgar_client, "get_cik", lambda t: "123")
    monkeypatch.setattr(edgar_client, "_get_text", fake_get)
    f = edgar_client.Filing8K("8-K", "2026-08-05", "0000000123-26-000001", "", ("2.02",))
    stop = lambda: len(fetched) >= 2      # budget spent after the first exhibit
    assert edgar_client.fetch_release_document("X", f, should_stop=stop) is None
    assert len(fetched) == 2
    # Default: unchanged behaviour, every candidate tried.
    fetched.clear()
    assert edgar_client.fetch_release_document("X", f) is None
    assert len(fetched) == 4


def test_r3_a_budget_spent_inside_the_fetch_reads_budget_not_fetch_failed(db, monkeypatch):
    import consensus_snapshot
    import edgar_client
    import main
    now = [0.0]
    monkeypatch.setattr(consensus_snapshot, "pre_release_snapshot_set",
                        lambda conn, t, cutoff: ("ok", {}))
    monkeypatch.setattr(edgar_client, "get_request_stats", lambda: (0, 0))
    monkeypatch.setattr(edgar_client, "find_earnings_release_filing",
                        lambda t, a, b: object())

    def slow_fetch(t, f, should_stop=None):
        now[0] = 1000.0
        return None

    monkeypatch.setattr(edgar_client, "fetch_release_document", slow_fetch)
    r = _row("JPX", tier=1, position="Portfolio", event_date="2026-08-05")
    main.attach_guidance_verdicts(db, [r], budget_s=150, clock=lambda: now[0])
    assert r.guidance_verdict.reason == "budget"


# --- Codex 2026-09-30 round 4 regression --------------------------------------

def test_r4_a_fiscal_year_between_adjusted_and_eps_keeps_the_basis():
    """HUM 1Q25 wording: the year digits are not a metric separator."""
    text = ("Quarter ended March 31, 2025.\n2025 Outlook\n"
            "Affirms Adjusted FY 2025 EPS guidance of $16.00 to $16.50.\n")
    v = assess(text, "2025-04-30", "ok",
               snaps(snap("2025-12-31", eps=16.0, taken_at="2025-04-29T12:00:00Z"),
                     snap("2026-12-31", eps=18.0, taken_at="2025-04-29T12:00:00Z")),
               "2025-05-01T04:00:00Z")
    assert v.state == "green", (v.state, v.reason)
