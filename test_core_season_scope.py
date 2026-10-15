"""Core-coverage season page scope (JP 2026-10-15).

Every Core=Y name is in scope; non-Core names are not, even when held, because
the page is the coverage schedule, not the book.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

from coverage import TickerInfo  # noqa: E402
import build_season_page as bsp  # noqa: E402


def _ti(t, position="", sector="MedTech"):
    return TickerInfo(ticker=t, tier=2, company_name=t, sector=sector,
                      subsector="", position=position)


def test_core_scope_keeps_only_core_and_labels_each_name():
    cov = [_ti("AAA", "Portfolio"), _ti("BBB"), _ti("CCC", sector=""),
           _ti("HELD", "Portfolio")]
    m = bsp.core_scope_map(cov, {"AAA", "BBB", "CCC"})
    assert set(m) == {"AAA", "BBB", "CCC"}          # HELD is not Core
    assert m["AAA"].position == "Portfolio"          # Position list wins
    assert m["BBB"].position == "MedTech"            # else sector
    assert m["CCC"].position == bsp.CORE_LABEL       # else "Core"
    assert cov[1].position == ""                     # input not mutated


def test_core_page_title_names_its_scope():
    from season_progress import SeasonProgress
    from datetime import date
    p = SeasonProgress(season="2026Q3", as_of=date(2026, 10, 15))
    out = bsp.render(p, generated_at="x", db_asof=None, scope="core")
    assert "2026Q3 Earnings Season Progress &mdash; Core Coverage Companies" in out
    assert "Portfolio &amp; Researching Companies" not in out
