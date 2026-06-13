"""Tests for foreign-filer 6-K earnings detection (ICLR-class phantom-date fix).

The 8-K Item 2.02 path is blind to foreign private issuers, who furnish results
on a 6-K with no item codes — so an earnings 6-K must be identified from its
primaryDocument filename. These tests pin the heuristic against real ICON (ICLR)
filenames and verify the window search.
"""
from datetime import date

import edgar_client as ec


# Real ICON (ICLR) 6-K primaryDocument filenames pulled from data.sec.gov,
# with the intended classification.
ICLR_EARNINGS_DOCS = [
    "iconplc6kq425.htm",                 # Q4'25 results
    "iconplc6kq325.htm",                 # Q3'25 results
    "iconearningscallanddeficie.htm",    # earnings-call 6-K (same event)
]
ICLR_NON_EARNINGS_DOCS = [
    "a202504292non-reliance6k.htm",      # non-reliance / restatement notice
    "a6kofpr-iconplcannounces.htm",      # generic "announces" PR
    "a6k-iconplc.htm",                   # bare 6-K
    "a6kofpr-iconplcjefferieslo.htm",    # Jefferies conference
    "a6kofpr-iconplcschedulesth.htm",    # "schedules the…" notice
    "a6kofpr-iconplcprovidesupd.htm",    # "provides update"
    "a6kofpr-iconplctoparticipa.htm",    # "to participate" (conference)
]


def test_heuristic_flags_real_earnings_docs():
    for doc in ICLR_EARNINGS_DOCS:
        assert ec.is_likely_earnings_6k_doc(doc), f"should flag {doc}"


def test_heuristic_rejects_non_earnings_docs():
    for doc in ICLR_NON_EARNINGS_DOCS:
        assert not ec.is_likely_earnings_6k_doc(doc), f"should NOT flag {doc}"


def test_heuristic_handles_empty():
    assert ec.is_likely_earnings_6k_doc("", "") is False
    assert ec.is_likely_earnings_6k_doc(None, None) is False


def test_heuristic_description_fallback():
    # Some issuers leave a useful description even with a bare filename.
    assert ec.is_likely_earnings_6k_doc("ex99-1.htm", "Q1 2026 Results") is True
    assert ec.is_likely_earnings_6k_doc("ex99-1.htm", "Notice of AGM") is False


def _canned_submissions():
    return {
        "filings": {"recent": {
            "form":        ["6-K",                "6-K",               "6-K",          "8-K"],
            "filingDate":  ["2026-05-27",         "2026-05-26",        "2026-04-29",   "2026-05-27"],
            "accessionNumber": ["0-1", "0-2", "0-3", "0-4"],
            "primaryDocument": ["iconplc6kq425.htm", "iconearningscallanddeficie.htm",
                                 "a202504292non-reliance6k.htm", "something8k.htm"],
            "primaryDocDescription": ["6-K", "6-K", "6-K", "8-K"],
        }}
    }


def test_fetch_6k_filings_filters_to_earnings(monkeypatch):
    monkeypatch.setattr(ec, "get_cik", lambda t: "0001060955")
    monkeypatch.setattr(ec, "_get_json", lambda url: _canned_submissions())
    out = ec.fetch_6k_filings("ICLR", days_back=400)
    dates = [f.filing_date for f in out]
    # Both earnings 6-Ks kept (newest first); the non-reliance 6-K + the 8-K dropped.
    assert dates == ["2026-05-27", "2026-05-26"]
    assert all(f.form == "6-K" for f in out)


def test_find_results_6k_returns_most_recent_in_window(monkeypatch):
    monkeypatch.setattr(ec, "get_cik", lambda t: "0001060955")
    monkeypatch.setattr(ec, "_get_json", lambda url: _canned_submissions())
    f = ec.find_results_6k("ICLR", date(2026, 5, 20), date(2026, 5, 31))
    assert f is not None and f.filing_date == "2026-05-27" and f.form == "6-K"


def test_find_results_6k_none_outside_window(monkeypatch):
    monkeypatch.setattr(ec, "get_cik", lambda t: "0001060955")
    monkeypatch.setattr(ec, "_get_json", lambda url: _canned_submissions())
    assert ec.find_results_6k("ICLR", date(2026, 1, 1), date(2026, 3, 1)) is None
