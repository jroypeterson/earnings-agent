"""IR distribution-list coverage (#246).

The whole tally rests on attributing a message to a company, and the two ways to get
that wrong are the two things tested hardest: matching too loosely (crediting the wrong
ticker) and matching too tightly (losing a covered name silently).
"""

from __future__ import annotations

import ir_coverage as ic


# --------------------------------------------------------------------------
# Name normalisation — exact after stripping, NEVER containment
# --------------------------------------------------------------------------

def test_corporate_suffixes_are_stripped_from_both_sides():
    assert ic.norm_name("The Ensign Group, Inc.") == ic.norm_name("Ensign Group Inc")
    assert ic.norm_name("Thermo Fisher Scientific Inc.") == \
        ic.norm_name("Thermo Fisher Scientific Inc")


def test_a_mail_product_suffix_is_stripped():
    """Found live: BSX arrives as 'Boston Scientific Corporation Alerting Service'.
    Without this the name matches nothing and a covered company drops out of the
    tally silently."""
    assert ic.norm_name("Boston Scientific Corporation Alerting Service") == \
        ic.norm_name("Boston Scientific Corp")


def test_a_PARENT_does_not_match_its_SUBSIDIARY():
    """The mistake this fleet has already paid for: a subset name-check let
    `Siemens AG` match `Siemens Healthineers`, screening the parent conglomerate as
    the subsidiary. Exact-after-normalisation gets it right."""
    assert ic.norm_name("Siemens AG") != ic.norm_name("Siemens Healthineers AG")


def test_normalisation_does_not_collapse_two_different_companies():
    assert ic.norm_name("Ardent Health, Inc.") != ic.norm_name("Ardelyx, Inc.")


# --------------------------------------------------------------------------
# Header parsing
# --------------------------------------------------------------------------

def test_display_name_is_extracted_from_a_quoted_header():
    assert ic.display_name('"The Ensign Group, Inc." <no-reply@q4inc.com>') == \
        "The Ensign Group, Inc."


def test_display_name_is_extracted_from_an_unquoted_header():
    assert ic.display_name("Eli Lilly and Company <no-reply@notification.gcs-web.com>") \
        == "Eli Lilly and Company"


def test_a_bare_address_has_no_display_name():
    assert ic.display_name("<news@hcahealthcare.com>") == ""


def test_mail_subdomains_are_folded_to_the_root_domain():
    assert ic._root(ic._domain("alerts@email.abbvie.com")) == "abbvie.com"
    assert ic._root(ic._domain("no-reply@notification.gcs-web.com")) == "gcs-web.com"


# --------------------------------------------------------------------------
# The platform problem
# --------------------------------------------------------------------------

def test_the_ir_platforms_are_treated_as_bulk_senders():
    """223 of 259 live messages came from IR-platform domains. Matching on domain
    would have credited Q4 Inc with covering 40 companies."""
    for d in ("q4inc.com", "gcs-web.com", "equisolve.com", "investis.com"):
        assert d in ic.BULK_SENDERS


# --------------------------------------------------------------------------
# What the report must refuse to claim
# --------------------------------------------------------------------------

def test_a_gmail_failure_renders_as_INCONCLUSIVE_not_zero_coverage():
    """Silence because the mailbox could not be read is not silence because you are
    on no lists. Rendering the first as the second would send JP re-subscribing to
    everything."""
    j = {"generated": "2026-08-04", "window_days": 365,
         "error": "gmail query failed: RefreshError", "messages": 0,
         "core_total": 256, "covered": [], "missing": ["AAA"], "non_core_hits": [],
         "hits": {}, "bulk": [], "unmatched": [], "core_rows": {}}
    out = ic.render(j)
    assert "INCONCLUSIVE" in out
    assert "could not check" in out


def test_the_report_separates_no_mail_from_not_subscribed():
    j = {"generated": "2026-08-04", "window_days": 365, "error": "", "messages": 5,
         "core_total": 2, "covered": ["AAA"], "missing": ["BBB"], "non_core_hits": [],
         "hits": {"AAA": ["2026-08-01"]}, "bulk": [], "unmatched": [],
         "core_rows": {"AAA": "A Co", "BBB": "B Co"}}
    out = ic.render(j)
    assert "does not mean unsubscribed" in out
    assert "No mail in window" in out


def test_unmatched_senders_are_reported_not_dropped():
    j = {"generated": "2026-08-04", "window_days": 365, "error": "", "messages": 1,
         "core_total": 1, "covered": [], "missing": ["AAA"], "non_core_hits": [],
         "hits": {}, "bulk": [], "unmatched": [("mystery.com", 4)],
         "core_rows": {"AAA": "A Co"}}
    out = ic.render(j)
    assert "mystery.com" in out
