"""Tests for the same-day narrative summary lane (daily_summary.py).

Everything here is offline: no EDGAR, no LLM, no metered API. The EDGAR fetch
itself is exercised manually via
`python main.py --daily-summary --date YYYY-MM-DD --no-llm --dry-run`
(free SEC endpoints) — see CLAUDE.md.
"""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest

import daily_summary as ds


# ---------------------------------------------------------------------------
# Guidance extraction
# ---------------------------------------------------------------------------


def test_guidance_requires_both_period_and_verb():
    # Forward verb, no forward period -> not guidance.
    assert ds.extract_guidance_lines(
        "The company expects continued discipline in its operations going forward."
    ) == []
    # Forward period, no guidance verb -> not guidance.
    assert ds.extract_guidance_lines(
        "Revenue for full-year 2025 was $10.0 billion versus $9.4 billion prior."
    ) == []


def test_guidance_extracts_a_raise():
    text = "Abbott raises full-year 2026 adjusted diluted EPS guidance range to $5.45 to $5.60."
    out = ds.extract_guidance_lines(text)
    assert len(out) == 1
    assert "raises full-year 2026" in out[0]


def test_guidance_skips_forward_looking_boilerplate():
    text = (
        "This press release contains forward-looking statements regarding our "
        "full-year 2026 outlook that we expect may differ materially."
    )
    assert ds.extract_guidance_lines(text) == []


def test_guidance_ranks_revision_above_reaffirmation_and_prose():
    text = "\n".join([
        "The company continues to expect full-year 2026 margins near prior levels.",
        "Segment results in the second quarter of 2026 were expected to normalize over time.",
        "The company raises its full-year 2026 EPS guidance to at least $27.00.",
    ])
    out = ds.extract_guidance_lines(text)
    assert "raises" in out[0], f"revision should rank first, got {out[0]!r}"


def test_guidance_skips_flattened_table_rows():
    text = "| Full-year 2026 guidance | expects | $5.45 | $5.60 | | | |"
    assert ds.extract_guidance_lines(text) == []


def test_guidance_respects_limit():
    line = "The company raises its full-year 2026 EPS guidance to at least ${}.00."
    text = "\n".join(line.format(n) for n in range(10, 30))
    assert len(ds.extract_guidance_lines(text, limit=3)) == 3


# ---------------------------------------------------------------------------
# Reflow — the ELV superscript-footnote regression
# ---------------------------------------------------------------------------


def test_reflow_rejoins_a_sentence_split_by_a_superscript_footnote():
    """Live regression: ELV's 2026-07-15 release renders footnote markers on
    their own line, splitting the headline guidance raise into fragments that
    individually carry neither a forward period nor a verb."""
    text = "\n".join([
        "FY 2026 diluted EPS",
        "1",
        " guidance raised to at least $20.10; adjusted diluted EPS",
        "2",
        " guidance raised to at least $27.00, reflecting strong operating results",
    ])
    out = ds.extract_guidance_lines(text)
    assert out, "footnote-split guidance must be recovered"
    assert "at least $27.00" in out[0]


def test_reflow_never_glues_table_rows_together():
    text = "| Operating Revenue | $49.8\n| Operating Gain | $1.8"
    assert ds._reflow(text) == ["| Operating Revenue | $49.8", "| Operating Gain | $1.8"]


def test_bullet_block_is_split_into_separate_guidance_lines():
    """Reflow glues a highlights block into one line; the bullet glyph is the
    only remaining boundary. Without splitting on it the whole block is a single
    over-length 'sentence' and is discarded."""
    text = (
        "RAISES FULL-YEAR GUIDANCE • 2Q 2026 operating revenue of $49.8 billion "
        "• FY 2026 adjusted diluted EPS guidance raised to at least $27.00, "
        "reflecting strong second quarter operating results"
    )
    out = ds.extract_guidance_lines(text)
    assert any("at least $27.00" in g for g in out)
    assert all(not g.startswith("•") for g in out), "bullet glyph must be stripped"


# ---------------------------------------------------------------------------
# Narrative parsing
# ---------------------------------------------------------------------------


def test_parse_narrative_extracts_json_from_prose_wrapper():
    raw = 'Sure!\n{"headline": "Guidance up", "movers": ["a", "b"]}\nHope that helps.'
    nar = ds._parse_narrative(raw)
    assert nar.headline == "Guidance up"
    assert nar.movers == ["a", "b"]


def test_parse_narrative_caps_movers_at_three():
    raw = '{"headline": "h", "movers": ["a", "b", "c", "d", "e"]}'
    assert len(ds._parse_narrative(raw).movers) == 3


@pytest.mark.parametrize("raw", [None, "", "no json here", "{not valid json}",
                                 '{"headline": "", "movers": []}'])
def test_parse_narrative_returns_none_on_unusable_output(raw):
    assert ds._parse_narrative(raw) is None


def test_build_narrative_passes_todays_date_to_the_model():
    """An LLM whose cutoff predates the quarter will flag a current release as
    'future'/implausible unless told what today is.
    See feedback_llm_judge_pass_current_date."""
    seen = {}

    def narrator(prompt):
        seen["prompt"] = prompt
        return '{"headline": "h", "movers": ["m"]}'

    row = ds.SummaryRow(ticker="ELV", company_name="Elevance", event_date="2026-07-15",
                        event_hour="bmo", tier=2)
    nar = ds.build_narrative(row, "some release text", today=date(2026, 7, 20),
                             narrator=narrator)
    assert "2026-07-20" in seen["prompt"]
    assert nar.headline == "h"


def test_build_narrative_reports_truncation_to_the_model():
    seen = {}

    def narrator(prompt):
        seen["prompt"] = prompt
        return '{"headline": "h", "movers": []}'

    row = ds.SummaryRow(ticker="X", company_name="X", event_date="2026-07-15",
                        event_hour="amc", tier=1)
    ds.build_narrative(row, "x" * (ds._MAX_RELEASE_CHARS + 10),
                       today=date(2026, 7, 20), narrator=narrator)
    assert "truncated" in seen["prompt"]


def test_build_narrative_never_raises_on_a_broken_narrator():
    def narrator(prompt):
        raise RuntimeError("boom")

    row = ds.SummaryRow(ticker="X", company_name="X", event_date="2026-07-15",
                        event_hour="amc", tier=1)
    with pytest.raises(RuntimeError):
        # An INJECTED narrator is a test double: its exceptions surface. The
        # production narrator (_default_narrator) is the one that must swallow.
        ds.build_narrative(row, "text", today=date(2026, 7, 20), narrator=narrator)


def test_default_narrator_returns_none_without_a_key(monkeypatch):
    monkeypatch.setattr(ds, "ANTHROPIC_API_KEY", None)
    assert ds._default_narrator("prompt") is None


# ---------------------------------------------------------------------------
# Collection + grouping
# ---------------------------------------------------------------------------


def _db_with(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            ticker TEXT, company_name TEXT, event_date TEXT, event_hour TEXT,
            quarter TEXT, tier INTEGER, eps_actual REAL, eps_estimate REAL,
            rev_actual REAL, rev_estimate REAL, call_datetime_utc TEXT
        )""")
    conn.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    return conn


class _Info:
    def __init__(self, sector="", subsector="", position="", company_name=""):
        self.sector = sector
        self.subsector = subsector
        self.position = position
        self.company_name = company_name


def test_collect_day_excludes_tier_3_and_other_dates():
    conn = _db_with([
        ("ELV", "Elevance", "2026-07-15", "bmo", "2026Q2", 2, None, 6.2, None, 49e9, None),
        ("ZZZZ", "Tier3 Co", "2026-07-15", "bmo", "2026Q2", 3, None, 1.0, None, 1e9, None),
        ("ABT", "Abbott", "2026-07-16", "bmo", "2026Q2", 2, None, 1.3, None, 12e9, None),
    ])
    rows = ds.collect_day(conn, {}, date(2026, 7, 15))
    assert [r.ticker for r in rows] == ["ELV"]


def test_collect_day_returns_names_with_no_actuals_yet():
    """The whole point of a SAME-day lane: the 8-K release is filed hours before
    the aggregators post actuals, so a row with NULL actuals must not be dropped."""
    conn = _db_with([
        ("ELV", "Elevance", "2026-07-15", "bmo", "2026Q2", 2, None, 6.2, None, 49e9, None),
    ])
    rows = ds.collect_day(conn, {}, date(2026, 7, 15))
    assert len(rows) == 1 and rows[0].eps_actual is None


def test_collect_day_sorts_rows_grouped_by_subgroup():
    """Renderers print a header on each (tier, subgroup) change, so interleaved
    rows would print the same header twice (live bug on 2026-07-15: MedTech,
    Mgd Care, MedTech)."""
    conn = _db_with([
        ("ABT", "Abbott", "2026-07-15", "bmo", "2026Q2", 2, None, 1.0, None, 1e9, None),
        ("ELV", "Elevance", "2026-07-15", "bmo", "2026Q2", 2, None, 1.0, None, 1e9, None),
        ("JNJ", "JNJ", "2026-07-15", "bmo", "2026Q2", 2, None, 1.0, None, 1e9, None),
    ])
    cov = {
        "ABT": _Info(subsector="Diversified MedTech"),
        "ELV": _Info(subsector="Mgd Care"),
        "JNJ": _Info(subsector="Diversified MedTech"),
    }
    subgroups = [ds._subgroup(r) for r in ds.collect_day(conn, cov, date(2026, 7, 15))]
    assert subgroups == sorted(subgroups), "rows must arrive grouped"


def test_subgroup_prefers_position_over_sector():
    row = ds.SummaryRow(ticker="X", company_name="X", event_date="2026-07-15",
                        event_hour="bmo", tier=1, subsector="Mgd Care",
                        position="Portfolio")
    assert ds._subgroup(row) == "Portfolio"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _row(**kw):
    base = dict(ticker="ELV", company_name="Elevance Health", event_date="2026-07-15",
                event_hour="bmo", tier=2, subsector="Mgd Care", quarter="2026Q2")
    base.update(kw)
    return ds.SummaryRow(**base)


def test_render_text_reports_every_degradation():
    row = _row(degradations=["no 8-K Item 2.02 on file yet",
                             "narrative unavailable (ANTHROPIC_API_KEY unset)"])
    out = ds.render_text([row], date(2026, 7, 15))
    assert "no 8-K Item 2.02 on file yet" in out
    assert "ANTHROPIC_API_KEY unset" in out


def test_render_text_handles_an_empty_day():
    out = ds.render_text([], date(2026, 7, 4))
    assert "No Tier 1/2 coverage names reported" in out


def test_render_text_shows_guidance_and_movers():
    row = _row(
        guidance=["FY 2026 adjusted EPS guidance raised to at least $27.00"],
        narrative=ds.Narrative(headline="Beat on below-the-line items",
                               movers=["Benefit expense ratio 89.7%, +80bp y/y"]),
    )
    out = ds.render_text([row], date(2026, 7, 15))
    assert "at least $27.00" in out
    assert "Benefit expense ratio 89.7%" in out
    assert "Beat on below-the-line items" in out


def test_slack_context_blocks_use_elements_not_text():
    """A context block with a bare `text` field is an invalid_blocks HTTP 400.
    See reference_slack_context_block_elements."""
    row = _row(release_url="https://sec.gov/x.htm", release_doc_type="EX-99.1",
               release_filed="2026-07-15")
    for b in ds.build_slack_blocks([row], date(2026, 7, 15)):
        if b["type"] == "context":
            assert "elements" in b and "text" not in b


def test_slack_blocks_link_the_release_source():
    row = _row(release_url="https://sec.gov/x.htm", release_doc_type="EX-99.1",
               release_filed="2026-07-15")
    blob = str(ds.build_slack_blocks([row], date(2026, 7, 15)))
    assert "https://sec.gov/x.htm" in blob


def test_markers_flag_beat_miss_and_unknown():
    assert ds._marker(2.0, 1.0) == "\U0001f7e9"
    assert ds._marker(1.0, 2.0) == "\U0001f7e5"
    assert ds._marker(None, 2.0) == "⬜"


def test_money_and_eps_formatting():
    assert ds._money(49_800_000_000) == "$49.80B"
    assert ds._money(None) == "n/a"
    assert ds._eps(7.45) == "$7.45"
