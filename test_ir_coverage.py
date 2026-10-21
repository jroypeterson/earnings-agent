"""IR distribution-list coverage (#246).

The whole tally rests on attributing a message to a company, and the two ways to get
that wrong are the two things tested hardest: matching too loosely (crediting the wrong
ticker) and matching too tightly (losing a covered name silently).
"""

from __future__ import annotations

import json

import pytest

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


@pytest.fixture
def ownership_files(tmp_path, monkeypatch):
    """Exercise the canonical reader against a local miniature fleet, never live files."""
    exports = tmp_path / "Coverage Manager" / "exports"
    exports.mkdir(parents=True)
    universe = exports / "universe.csv"
    universe.write_text(
        "Ticker,Company Name,Core,Website\n"
        "HELD,Held Company,,\nNEW,New Company,,\n"
        "SOLD,Sold Company,,\nCORE,Core Company,Y,\n", encoding="utf-8-sig")
    (exports / "portfolio.json").write_text(
        json.dumps({"HELD": {}, "SOLD": {}}), encoding="utf-8")
    (exports / "ticker_aliases.json").write_text(
        json.dumps({"alias_to_canonical": {"NEW.BROKER": "NEW"}}), encoding="utf-8")
    (exports / "universe_metadata.json").write_text(
        json.dumps({t: {} for t in ("HELD", "NEW", "SOLD", "CORE")}), encoding="utf-8")
    held = tmp_path / "portfolio_daily" / "exports" / "held.json"
    held.parent.mkdir(parents=True)
    today = ic.date(2026, 10, 20)
    held.write_text(json.dumps({
        "schema": 1, "generated_at": today.isoformat(),
        "brokers": [{"broker": "Test", "as_of": today.isoformat(), "age_source": "as_of"}],
        "held": [{"ticker": t, "shares": 1, "brokers": ["Test"]}
                 for t in ("HELD", "NEW.BROKER")],
    }), encoding="utf-8")
    monkeypatch.setattr(ic, "ROOT", tmp_path)
    monkeypatch.setattr(ic, "UNIVERSE", universe)
    monkeypatch.setattr(ic.held_feed, "today_et", lambda: today)
    monkeypatch.delenv("PORTFOLIO_HELD_JSON", raising=False)
    return exports, held


def test_broker_held_name_absent_from_cm_portfolio_enters_scope(ownership_files):
    _, _, rows = ic.load_universe()
    assert {r["Ticker"]: r["_scope"] for r in rows} == {
        "HELD": "portfolio", "NEW": "portfolio", "CORE": "core",
    }


def test_sold_non_core_cm_portfolio_name_leaves_scope(ownership_files):
    assert ic._portfolio_tickers() == {"HELD", "NEW"}
    _, _, rows = ic.load_universe()
    assert "SOLD" not in {r["Ticker"] for r in rows}


@pytest.mark.parametrize("bad_content", [None, "not json"])
def test_unreadable_held_feed_falls_back_loudly(ownership_files, capsys, bad_content):
    _, held = ownership_files
    if bad_content is None:
        held.unlink()
    else:
        held.write_text(bad_content, encoding="utf-8")
    assert ic._portfolio_tickers() == {"HELD", "SOLD"}
    err = capsys.readouterr().err
    assert "warning:" in err and "held.json" in err
    assert "Coverage Manager's copy stands" in err


@pytest.mark.parametrize("bad_content", [None, "not json", "[]"])
def test_unreadable_cm_portfolio_warns_and_still_promotes(ownership_files, capsys, bad_content):
    exports, _ = ownership_files
    portfolio = exports / "portfolio.json"
    if bad_content is None:
        portfolio.unlink()
    else:
        portfolio.write_text(bad_content, encoding="utf-8")
    assert ic._portfolio_tickers() == {"HELD", "NEW"}
    err = capsys.readouterr().err
    assert "warning:" in err and str(portfolio) in err


def test_the_report_names_the_ownership_source_and_its_fallback(ownership_files):
    """A scheduled run's stderr reaches no one, so the report must say which source won."""
    ic._portfolio_tickers()
    assert ic.OWNERSHIP["source"] == "brokers' held.json" and ic.OWNERSHIP["warnings"] == []
    _, held = ownership_files
    held.unlink()
    ic._portfolio_tickers()
    assert ic.OWNERSHIP["source"].startswith("Coverage Manager") and ic.OWNERSHIP["warnings"]
    j = {"generated": "2026-10-20", "window_days": 1, "error": None, "messages": 0, "core_total": 0,
         "covered": [], "missing": [], "non_core_hits": [], "hits": {}, "matched_by_name": [],
         "bulk": [], "unmatched": [], "core_rows": {}, "scopes": {}, "ownership": dict(ic.OWNERSHIP)}
    out = ic.render(j)
    assert "Owned names from: **Coverage Manager" in out and "ownership warning:" in out
