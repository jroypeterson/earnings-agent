"""Sectioned guidance extraction, and the release-document API it reads.

Both were added 2026-09-09 when JP asked that every preview and review carry a
metrics section and a guidance section. The reference case throughout is Five
Below's Q1 FY26 release, because StreetAccount published a preview for the
same print and its figures are the ground truth to match.
"""

import re

import pytest

import edgar_client
from daily_summary import extract_guidance_blocks, extract_guidance_lines


# The shape of a well-structured release: a period-naming heading, a preamble
# with no figures, then bullets that carry the figures but NOT the period.
FIVE_Q1 = """
Five Below, Inc. Announces First Quarter Fiscal 2026 Financial Results

Second Quarter and Fiscal 2026 Outlook:
The Company expects the following results for the second quarter and full year of fiscal 2026.

For the second quarter of Fiscal 2026:
 - Net sales are expected to be in the range of $1.18 billion to $1.20 billion based on opening approximately 50 new stores and assumes an approximate 7% to 9% increase in comparable sales.
 - Net income is expected to be in the range of $64 million to $71 million.
 - Adjusted diluted income per common share is expected to be in the range of $1.17 to $1.29.

For the full year of Fiscal 2026:
 - Net sales are expected to be in the range of $5.40 billion to $5.48 billion based on opening approximately 150 net new stores.
 - Adjusted diluted income per common share is expected to be in the range of $8.65 to $9.05.

Non-GAAP Information:
The Company reports adjusted figures alongside GAAP results.

This news release includes forward-looking statements within the meaning of the Private Securities Litigation Reform Act, and the Company expects fiscal 2026 risks to be material.
"""


def test_the_figures_are_extracted_not_just_the_heading():
    """The defect this function was written for.

    extract_guidance_lines requires the period AND the verb in one sentence.
    Here the period is in the heading and the figures are in the bullets, so it
    returned the headings and dropped every number -- while StreetAccount's own
    preview for this print carried 'guidance $1.18-1.20B' and '$1.17-1.29'.
    """
    blocks = extract_guidance_blocks(FIVE_Q1)
    joined = " ".join(line for b in blocks for line in b.lines)
    assert "$1.18 billion to $1.20 billion" in joined
    assert "7% to 9%" in joined
    assert "$1.17 to $1.29" in joined


def test_a_period_sub_heading_opens_a_block_rather_than_ending_the_outlook():
    """'For the second quarter of Fiscal 2026:' sits INSIDE the outlook.

    Treating it as a terminator dropped every figure -- the first version of
    this extractor returned zero blocks on the real filing for exactly that
    reason.
    """
    labels = [b.label for b in extract_guidance_blocks(FIVE_Q1)]
    assert any("second quarter" in l.lower() for l in labels)
    assert any("full year" in l.lower() for l in labels)


def test_both_periods_survive_the_block_cap():
    """Heading-only blocks must not spend the budget.

    Filtering empties AFTER slicing returned the quarter and silently dropped
    the full-year block, which is the more important of the two.
    """
    blocks = extract_guidance_blocks(FIVE_Q1, max_blocks=2)
    assert len(blocks) == 2
    assert all(b.lines for b in blocks)


def test_the_preamble_is_not_reported_as_guidance():
    """'The Company expects the following results...' names a period and a verb
    but carries no figure, so it is a heading, not guidance."""
    for b in extract_guidance_blocks(FIVE_Q1):
        for line in b.lines:
            assert "the following results" not in line.lower()


def test_forward_looking_boilerplate_never_becomes_guidance():
    for b in extract_guidance_blocks(FIVE_Q1):
        for line in b.lines:
            assert "forward-looking" not in line.lower()
            assert "Private Securities Litigation" not in line


def test_an_unrelated_heading_closes_the_block():
    """Text under 'Non-GAAP Information:' is not an outlook."""
    for b in extract_guidance_blocks(FIVE_Q1):
        for line in b.lines:
            assert "reports adjusted figures" not in line


def test_empty_text_yields_no_blocks():
    assert extract_guidance_blocks("") == []
    assert extract_guidance_blocks("No outlook here at all.") == []


def test_the_sentence_extractor_is_still_the_fallback():
    """Issuers that guide in one prose sentence under no heading are covered by
    the older extractor, so the two are additive and both stay wired."""
    prose = ("The Company now expects full-year 2026 revenue of $5.40 billion "
             "to $5.48 billion, raising its prior outlook.")
    assert extract_guidance_blocks(prose) == []
    assert extract_guidance_lines(prose)


# --- the release document API ----------------------------------------------


def test_release_exhibit_ranking_prefers_ex991():
    rank = edgar_client._rank_release_exhibit
    assert rank("EX-99.1", "Press Release", "pr.htm") < rank("EX-99.2", "", "x.htm")


def test_a_slide_deck_sinks_below_a_plain_release():
    """A supplement flattens to a table of numbers with no guidance sentence."""
    rank = edgar_client._rank_release_exhibit
    deck = rank("EX-99.1", "Q2 Earnings Slide Presentation", "slides.htm")
    release = rank("EX-99.2", "Press Release", "pr.htm")
    assert release < deck


def test_a_bare_ex99_ranks_with_ex991():
    rank = edgar_client._rank_release_exhibit
    assert rank("EX-99", "", "a.htm")[1] == rank("EX-99.1", "", "b.htm")[1]


def test_html_to_text_breaks_blocks_before_stripping_tags():
    """Without block-level newlines the whole release becomes one sentence and
    the sentence splitter downstream can never separate the guidance."""
    out = edgar_client._html_to_text(
        "<p>Net sales guidance</p><p>$1.18 billion to $1.20 billion</p>")
    assert "\n" in out
    assert "guidance$1.18" not in out.replace(" ", "")


def test_html_to_text_drops_script_and_style():
    out = edgar_client._html_to_text(
        "<style>.a{color:red}</style><script>var x=1;</script><p>Outlook</p>")
    assert "color:red" not in out and "var x" not in out
    assert "Outlook" in out


def test_the_release_doc_reports_its_own_size():
    doc = edgar_client.ReleaseDoc(
        ticker="FIVE", accession="x", url="u", doc_type="EX-99.1",
        filing_date="2026-06-03", text="abcd")
    assert doc.char_count == 4


def test_daily_summary_calls_a_function_that_exists():
    """The seam that had no test.

    daily_summary.attach_release calls edgar_client.fetch_release_document; the
    name existed in no commit of edgar_client, so --daily-summary raised
    AttributeError for every name with a filing. Both halves were tested; the
    seam was not.
    """
    assert callable(getattr(edgar_client, "fetch_release_document", None))
