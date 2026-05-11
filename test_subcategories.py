"""Tests for digest subgroup routing after the 2026-05-11 expansion of
Coverage Manager's Position taxonomy from {Portfolio, Researching} to
five values."""

from notifications import (
    _SUBCATEGORY_ORDER,
    _results_subcategory,
    ResultRow,
)


def _row(ticker="X", position="", sector="", subsector=""):
    return ResultRow(
        ticker=ticker,
        company_name=f"{ticker} Inc",
        event_date="2026-05-11",
        event_hour=None,
        eps_actual=None,
        eps_estimate=None,
        rev_actual=None,
        rev_estimate=None,
        tier=1,
        sector=sector,
        subsector=subsector,
        position=position,
    )


class TestNewPositionSubgroups:
    """The three Position values added 2026-05-11 each route to their own
    subgroup, and rank above sector-derived buckets."""

    def test_ready_to_buy_routes_to_own_subgroup(self):
        # Sector intentionally non-HC to prove Position wins over sector.
        assert _results_subcategory(_row(position="Ready to Buy", sector="Tech")) == "Ready to Buy"

    def test_ready_to_short_routes_to_own_subgroup(self):
        assert _results_subcategory(_row(position="Ready to Short", sector="Biopharma")) == "Ready to Short"

    def test_following_for_interest_routes_to_own_subgroup(self):
        # Following-for-Interest names are typically in non-HC sectors
        # (Tech / Consumer / Industrials). Verify they don't fall to "Other".
        assert _results_subcategory(_row(position="Following for Interest", sector="Consumer")) == "Following for Interest"

    def test_subcategory_order_includes_new_buckets_in_correct_position(self):
        # Position-derived subgroups all rank above sector-derived ones.
        order = _SUBCATEGORY_ORDER
        i_p = order.index("Portfolio")
        i_r = order.index("Researching")
        i_rb = order.index("Ready to Buy")
        i_rs = order.index("Ready to Short")
        i_fi = order.index("Following for Interest")
        i_hc = order.index("Healthcare Services")
        assert i_p < i_r < i_rb < i_rs < i_fi < i_hc

    def test_legacy_subgroups_still_work(self):
        # Pre-2026-05-11 behavior preserved.
        assert _results_subcategory(_row(position="Portfolio")) == "Portfolio"
        assert _results_subcategory(_row(position="Researching")) == "Researching"
        assert _results_subcategory(_row(sector="Healthcare Services")) == "Healthcare Services"
        assert _results_subcategory(_row(sector="MedTech")) == "MedTech"
        assert _results_subcategory(_row(sector="Biopharma", subsector="Large Pharma")) == "Large Pharma"
        assert _results_subcategory(_row(sector="Tech")) == "Other"
