"""Healthcare REITs are Tier 2, whatever sector Coverage Manager files them in.

JP 2026-10-21: "I want to update Tier 2 to include healthcare REITs ... I want
that to be the updated general rule." On 2026-09-02 CM moved ARE/DOC/VTR/WELL
from Healthcare Services to Real Estate (GICS alignment) and kept Subsector
(JP) = "Healthcare Real Estate"; by October every healthcare REIT was sector
Real Estate, so all 19 had dropped to Tier 3 — no calendar event, no TickTick
task (ARE's 3Q26 task was found orphaned). The rule now keys on the subsector.
"""
from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

COLS = ["Ticker", "Company Name", "Sector (JP)", "Subsector (JP)", "Core"]
ROWS = [
    # ticker, name,              sector,                subsector,                core
    ("ARE",  "Alexandria",       "Real Estate",         "Healthcare Real Estate", "Y"),
    ("XHR",  "Non-core HC REIT", "Real Estate",         "Healthcare Real Estate", ""),
    ("SPG",  "Simon Property",   "Real Estate",         "Retail REIT",            "Y"),
    ("UNH",  "UnitedHealth",     "Healthcare Services", "Mgd Care",               "Y"),
]


@pytest.fixture()
def exports(tmp_path, monkeypatch):
    root = tmp_path / "cm"
    ex = root / "exports"
    ex.mkdir(parents=True)
    with open(ex / "universe.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for t, n, s, sub, c in ROWS:
            w.writerow(dict(zip(COLS, (t, n, s, sub, c))))
    meta = {t: {"name": n, "sector": s, "subsector": sub, "core": c}
            for t, n, s, sub, c in ROWS}
    (ex / "universe_metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    for fname in ("portfolio.json", "researching.json", "following_for_interest.json",
                  "ready_to_buy.json", "ready_to_short.json"):
        (ex / fname).write_text("{}", encoding="utf-8")
    import coverage as cov
    monkeypatch.setattr(cov, "COVERAGE_MANAGER_PATH", str(root), raising=False)
    return ex


def _tiers():
    import coverage as cov
    return {t.ticker: t.tier for t in cov.load_coverage()}


def test_a_healthcare_reit_filed_under_real_estate_is_tier_2(exports):
    assert _tiers()["ARE"] == 2


def test_the_rule_is_the_subsector_not_core(exports):
    """Like HC Services and MedTech, the subsector qualifies on its own."""
    assert _tiers()["XHR"] == 2


def test_other_real_estate_stays_tier_3(exports):
    """The subsector admits healthcare REITs, not the Real Estate sector."""
    assert _tiers()["SPG"] == 3


def test_existing_tier_2_is_unchanged(exports):
    assert _tiers()["UNH"] == 2


def test_every_healthcare_reit_in_the_real_coverage_manager_is_tracked():
    """Run the rule against the PRODUCTION shape, not only a fixture: every
    ticker CM files under Healthcare Real Estate must come out Tier 1 or 2."""
    import coverage as cov
    universe = Path(cov.COVERAGE_MANAGER_PATH) / "exports" / "universe.csv"
    if not universe.exists():
        pytest.skip("Coverage Manager exports not checked out here")
    with open(universe, encoding="utf-8-sig") as f:
        reits = {r["Ticker"] for r in csv.DictReader(f)
                 if r.get("Subsector (JP)") == "Healthcare Real Estate"}
    assert reits, "CM has no Healthcare Real Estate rows - the subsector was renamed?"
    tiers = _tiers()
    untracked = sorted(t for t in reits if tiers.get(t, 3) > 2)
    assert untracked == []
