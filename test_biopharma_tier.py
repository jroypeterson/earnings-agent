"""Biopharma reaches TickTick only for its Core names.

JP asked (2026-08-06) for a `Biopharma` tag on TickTick earnings tasks, "just
like you do for MedTech and healthcare services". The tag alone was inert:
TickTick syncs `tier <= 2` only, and every biopharma name outside a position
list was Tier 3, so no biopharma task existed to carry a tag.

The fix admits biopharma to Tier 2 — but **gated on Core=Y**, because the two
existing Tier-2 sectors are covered almost end to end (82 of 103 Healthcare
Services, 136 of 139 MedTech) while Biopharma is 38 of 687. Sector alone would
have enrolled every clinical-stage shell in the universe in quarterly TickTick
tasks. These tests pin both halves: the gate, and that the gate is what makes
the tag reachable.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

UNIVERSE_COLS = ["Ticker", "Company Name", "Sector (JP)", "Subsector (JP)", "Core"]

ROWS = [
    # ticker, name,          sector,                subsector,      core
    ("LLY",  "Eli Lilly",    "Biopharma",           "Large Pharma", "Y"),
    ("XOMA", "Xoma Shell",   "Biopharma",           "Biotech",      ""),
    ("ISRG", "Intuitive",    "MedTech",             "Surgery",      ""),
    ("UNH",  "UnitedHealth", "Healthcare Services", "Mgd Care",     "Y"),
    ("NVDA", "Nvidia",       "Tech",                "Semis",        "Y"),
]


@pytest.fixture()
def exports(tmp_path, monkeypatch):
    """A minimal Coverage Manager exports/ dir with no position lists."""
    root = tmp_path / "cm"
    ex = root / "exports"
    ex.mkdir(parents=True)

    with open(ex / "universe.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNIVERSE_COLS)
        w.writeheader()
        for t, name, sector, sub, core in ROWS:
            w.writerow({"Ticker": t, "Company Name": name, "Sector (JP)": sector,
                        "Subsector (JP)": sub, "Core": core})

    meta = {t: {"name": name, "sector": sector, "subsector": sub, "core": core}
            for t, name, sector, sub, core in ROWS}
    (ex / "universe_metadata.json").write_text(json.dumps(meta), encoding="utf-8")

    for fname in ("portfolio.json", "researching.json", "following_for_interest.json",
                  "ready_to_buy.json", "ready_to_short.json"):
        (ex / fname).write_text("{}", encoding="utf-8")

    monkeypatch.setattr("config.COVERAGE_MANAGER_PATH", str(root), raising=False)
    import coverage as cov
    monkeypatch.setattr(cov, "COVERAGE_MANAGER_PATH", str(root), raising=False)
    return ex


def _tiers(exports):
    import coverage as cov
    return {t.ticker: t.tier for t in cov.load_coverage()}


def test_core_biopharma_reaches_tier_2(exports):
    """Without this, LLY is Tier 3 and never gets a TickTick task at all."""
    assert _tiers(exports)["LLY"] == 2


def test_non_core_biopharma_stays_tier_3(exports):
    """The whole point of the Core gate: 687 biopharma rows must not flood in."""
    assert _tiers(exports)["XOMA"] == 3


def test_existing_tier_2_sectors_are_unchanged_and_still_need_no_core(exports):
    """MedTech/HC Services qualify on sector alone — ISRG has a blank Core."""
    tiers = _tiers(exports)
    assert tiers["ISRG"] == 2
    assert tiers["UNH"] == 2


def test_core_alone_does_not_promote_an_unrelated_sector(exports):
    """A Core=Y Tech name must not ride the biopharma gate into Tier 2."""
    assert _tiers(exports)["NVDA"] == 3


def test_biopharma_is_a_tagged_sector():
    from ticktick import sector_tag
    assert sector_tag("Biopharma") == "Biopharma"
    assert sector_tag("MedTech") == "MedTech"
    assert sector_tag("Healthcare Services") == "Healthcare Services"


def test_untagged_sectors_are_still_left_alone_rather_than_guessed():
    from ticktick import sector_tag
    assert sector_tag("Tech") is None
    assert sector_tag("") is None
    assert sector_tag(None) is None


def test_the_tag_is_only_reachable_because_of_the_tier_gate(exports):
    """Ties the two halves together.

    A future edit that reverts the Tier-2 gate would leave `sector_tag` happily
    returning "Biopharma" for a name that never gets a task — the tag would pass
    its own test while delivering nothing. Assert the reachable set instead.
    """
    import coverage as cov
    from ticktick import sector_tag
    synced = {t.ticker: t for t in cov.load_coverage() if t.tier <= 2}
    tagged = {tk: sector_tag(i.sector) for tk, i in synced.items()}
    assert tagged.get("LLY") == "Biopharma"
    assert "XOMA" not in synced
