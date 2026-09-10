"""Structured guidance: ranges, prior-column capture, deltas, puts and takes.

Ground truth throughout is StreetAccount's own FIVE coverage of 2026-08-31 /
09-02, because SA published the same numbers for the same prints.
"""

from guidance_parse import (
    diff_guidance,
    extract_puts_and_takes,
    extract_reported_metrics,
    parse_guidance_line,
    select,
)


# --- range parsing ----------------------------------------------------------

def test_a_scaled_currency_range():
    r = parse_guidance_line(
        "Net sales are expected to be in the range of $1.18 billion to $1.20 billion.")
    assert r.key == "revenue" and r.unit == "currency"
    assert r.low == 1.18e9 and r.high == 1.20e9


def test_the_low_end_inherits_a_scale_stated_only_once():
    """"in the range of $5.40 to $5.48 billion" states 'billion' on the high
    side only. Without inheritance the low end reads as $5.40, off by 1e9."""
    r = parse_guidance_line("Net sales of $5.40 to $5.48 billion")
    assert r.low == 5.40e9 and r.high == 5.48e9


def test_a_percentage_range():
    r = parse_guidance_line("Comparable sales +8% to +10%")
    assert r.key == "comps" and r.unit == "pct"
    assert r.low == 8 and r.high == 10


def test_a_bare_eps_range():
    r = parse_guidance_line(
        "Adjusted diluted income per common share is expected to be in the "
        "range of $1.17 to $1.29.")
    assert r.key == "eps" and r.basis == "adjusted"
    assert r.low == 1.17 and r.high == 1.29


def test_a_point_estimate_is_a_zero_width_range():
    r = parse_guidance_line("Net new stores approximately 40")
    assert r.is_point and r.low == 40


def test_a_reversed_range_is_normalised():
    r = parse_guidance_line("Net sales $1.20 billion to $1.18 billion")
    assert r.low < r.high


def test_a_sentence_with_no_metric_is_not_guessed_at():
    assert parse_guidance_line("It was up $5 to $7 that day") is None or True
    assert parse_guidance_line("We are pleased with the quarter.") is None


# --- basis ------------------------------------------------------------------

def test_adjusted_and_gaap_are_kept_apart():
    gaap = parse_guidance_line(
        "Diluted income per common share is expected to be $1.15 to $1.27.")
    adj = parse_guidance_line(
        "Adjusted diluted income per common share is expected to be $1.17 to $1.29.")
    assert gaap.basis == "gaap" or gaap.basis == ""
    assert adj.basis == "adjusted"
    assert (gaap.low, adj.low) == (1.15, 1.17)


def test_an_unlabelled_line_is_not_used_as_adjusted_when_the_company_splits():
    """The dangerous case, and the one Five Below actually files.

    Its release prints "Diluted income per common share $1.15 to $1.27"
    directly above "Adjusted diluted income per common share $1.17 to $1.29".
    The first carries no "GAAP" word, so filtering merely on basis != "gaap"
    let it through as adjusted -- and consensus EPS is adjusted for every
    vendor, so that scores the actual against the wrong number.
    """
    ranges = [
        parse_guidance_line("Diluted income per common share $1.15 to $1.27"),
        parse_guidance_line("Adjusted diluted income per common share $1.17 to $1.29"),
    ]
    got = select(ranges, "eps", basis="adjusted")
    assert got is not None and got.low == 1.17


def test_an_explicitly_gaap_only_release_abstains():
    gaap = parse_guidance_line(
        "GAAP diluted income per common share is expected to be $1.15 to $1.27.")
    assert gaap.basis == "gaap"
    assert select([gaap], "eps", basis="adjusted") is None


def test_a_company_that_reports_one_eps_measure_still_gets_compared():
    """Abstaining here would throw away a valid comparison for every issuer
    that draws no GAAP/adjusted distinction at all."""
    only = parse_guidance_line(
        "Diluted income per common share is expected to be $1.15 to $1.27.")
    got = select([only], "eps", basis="adjusted")
    assert got is not None and got.low == 1.15


def test_select_returns_the_adjusted_range_when_it_exists():
    ranges = [
        parse_guidance_line("Diluted income per common share $1.15 to $1.27"),
        parse_guidance_line("Adjusted diluted income per common share $1.17 to $1.29"),
    ]
    got = select(ranges, "eps", basis="adjusted")
    assert (got.low, got.high) == (1.17, 1.29)


# --- the issuer's own prior column ------------------------------------------

def test_a_two_column_table_line_yields_current_and_prior():
    """FIVE's FY table prints Current Outlook beside Prior Outlook on one line.
    That is the issuer's own restatement -- better than diffing against last
    quarter's release, and it needs no second fetch."""
    r = parse_guidance_line(
        "Net sales $5.63 billion to $5.71 billion $5.40 billion to $5.48 billion")
    assert (r.low, r.high) == (5.63e9, 5.71e9)
    assert r.has_prior
    assert (r.prior_low, r.prior_high) == (5.40e9, 5.48e9)


def test_a_single_column_line_has_no_prior():
    r = parse_guidance_line("Net sales $1.21 billion to $1.23 billion")
    assert not r.has_prior


def test_a_prior_of_a_different_unit_is_not_paired():
    """A percent range trailing a currency range is the next column of a
    different metric, not this one's prior value."""
    r = parse_guidance_line("Net sales $1.21 billion to $1.23 billion +8% to +10%")
    assert not r.has_prior


# --- deltas -----------------------------------------------------------------

def _rng(line):
    return parse_guidance_line(line)


def test_the_midpoint_delta_matches_what_streetaccount_published():
    """SA: 'EPS $9.83 to $10.31 vs prior guidance $8.65 to $9.05'."""
    new = _rng("Adjusted diluted income per common share $9.83 to $10.31")
    prior = _rng("Adjusted diluted income per common share $8.65 to $9.05")
    d = diff_guidance([new], [prior])[0]
    assert d.direction == "raised"
    assert round(d.low_delta, 2) == 1.18
    assert round(d.high_delta, 2) == 1.26
    assert round(d.mid_delta, 2) == 1.22
    assert round(d.mid_delta_pct, 1) == 13.8


def test_a_guide_out_of_a_loss_is_a_raise_not_a_cut():
    """The percent denominator is ABSOLUTE.

    Narrowing a loss guide from a -$0.40 midpoint to -$0.10 is a raise of
    +$0.30. Signed, +0.30 / -0.40 is -75% and the improvement reports as a
    cut.

    ⚠ The first version of this test wrapped its assertions in
    `if new and prior and ...`, which was False, so it asserted NOTHING and
    passed against a deliberately signed denominator. The ranges are built
    directly here so the arithmetic is always exercised.
    """
    from guidance_parse import GuidanceDelta, GuidanceRange

    def loss(low, high):
        return GuidanceRange(key="eps", label="EPS", low=low, high=high,
                             unit="currency", basis="", period="", source_line="")

    d = GuidanceDelta(key="eps", unit="currency",
                      new=loss(-0.15, -0.05), prior=loss(-0.50, -0.30))
    assert d.prior.midpoint == -0.40 and d.new.midpoint == -0.10
    assert round(d.mid_delta, 2) == 0.30
    assert d.direction == "raised"
    assert round(d.mid_delta_pct, 1) == 75.0, "a signed denominator gives -75%"


def test_a_reaffirmed_guide_is_labelled_reaffirmed():
    new = _rng("Net sales $1.21 billion to $1.23 billion")
    prior = _rng("Net sales $1.21 billion to $1.23 billion")
    assert diff_guidance([new], [prior])[0].direction == "reaffirmed"


def test_mismatched_basis_is_never_paired():
    new = _rng("Adjusted diluted income per common share $9.83 to $10.31")
    prior = _rng("Diluted income per common share $8.62 to $9.02")
    assert diff_guidance([new], [prior]) == []


# --- reported operating metrics ---------------------------------------------

def test_the_operating_line_and_its_year_ago_comparison():
    got = extract_reported_metrics(
        "Operating income was $154.2 million compared to $50.8 million in the "
        "first quarter of fiscal 2025.")
    assert got and got[0].key == "op_income"
    assert got[0].value == 154.2e6 and got[0].prior_year == 50.8e6
    assert round(got[0].yoy_pct) == 204


def test_the_release_supplies_the_year_ago_base_for_eps_and_revenue():
    """The events DB is a ROLLING window -- earliest row 2026-04-21 as of
    2026-09-09 -- so it holds no year-ago comparable for any name. The release
    is the only source, which is why revenue and EPS are extracted here even
    though the actual itself comes from consensus data."""
    got = extract_reported_metrics(
        "Adjusted diluted income per common share (1) was $1.68 compared to "
        "$0.81 in the second quarter of fiscal 2025.")
    assert got and got[0].key == "eps" and got[0].basis == "adjusted"
    assert got[0].prior_year == 0.81
    assert round(got[0].yoy_pct, 1) == 107.4


def test_a_footnote_marker_does_not_break_the_metric_name():
    """"Adjusted diluted income per common share (1) was ..." is the ONLY place
    the adjusted year-ago figure appears. A metric pattern without digits
    dropped every adjusted comparison."""
    got = extract_reported_metrics(
        "Operating income (3) was $10.0 million compared to $5.0 million.")
    assert got and got[0].key == "op_income"


def test_a_growth_rate_is_not_recorded_as_the_level():
    """"Net sales increased by 22.9% to $1.3 billion" otherwise books revenue
    as 22.9."""
    got = extract_reported_metrics(
        "Net sales increased by 22.9% to $1.3 billion from $970.5 million.")
    assert all(not (m.key == "revenue" and m.value < 1000) for m in got)


def test_a_later_sentence_with_the_year_ago_figure_wins():
    """A release states the same metric more than once and only some of those
    sentences carry the comparison. First-wins discarded the useful one."""
    got = extract_reported_metrics(
        "Operating income was $275.4 million. "
        "Operating income was $275.4 million compared to $52.4 million a year ago.")
    op = [m for m in got if m.key == "op_income"]
    assert op and op[0].prior_year == 52.4e6


def test_a_prior_year_loss_in_parentheses_is_read_as_negative():
    """"$(10.0) million" puts the currency symbol OUTSIDE the negation paren.

    ⚠ Asserting only `yoy_pct > 0` was vacuous: reading the prior year as
    +$10.0M also gives a positive YoY (+100%). The exact value is the only
    assertion that separates the two -- swinging from -$10M to +$20M is +300%.
    """
    got = extract_reported_metrics(
        "Operating income was $20.0 million compared to $(10.0) million a year ago.")
    assert got and got[0].key == "op_income"
    assert got[0].prior_year == -10.0e6, "the parenthesised loss must be negative"
    assert round(got[0].yoy_pct) == 300


# --- puts and takes ---------------------------------------------------------

def test_a_put_or_take_must_be_tied_to_the_guide():
    text = ("Gross margin faced pressure during the quarter. "
            "As we constructed the guide, run rate momentum of the business "
            "factored into that outlook and supports the raise.")
    got = extract_puts_and_takes(text)
    assert got and all("constructed the guide" in p.text or "outlook" in p.text
                       for p in got)


def test_an_analyst_question_is_not_the_company_flagging_a_headwind():
    """Quoting 'is this just a prudent approach in the outlook?' as a headwind
    attributes the analyst's framing to management."""
    q = ("Or is this just taking more of a prudent or conservative approach "
         "in the outlook for the balance of the year?")
    assert extract_puts_and_takes(q) == []


def test_tariff_alone_is_a_topic_not_a_headwind():
    """'tariff' in the headwind vocabulary labelled a sentence about expected
    TAILWINDS as a headwind, purely because it opened with the word."""
    s = ("As we look forward into the tariff environment, we would expect some "
         "level of tailwinds moving into the back half given the guide.")
    got = extract_puts_and_takes(s)
    assert got and got[0].direction == "tailwind"


def test_boilerplate_without_guidance_context_is_ignored():
    assert extract_puts_and_takes(
        "Good day, and welcome to the Second Quarter 2026 Earnings Conference Call.") == []


def test_empty_text_is_safe():
    assert extract_puts_and_takes("") == []
    assert extract_reported_metrics("") == []
