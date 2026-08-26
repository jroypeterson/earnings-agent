"""Forward ANNUAL Street consensus from FMP — the input half of board #298's
guidance-vs-consensus square.

Offline: every test injects a recorded FMP payload, so nothing here spends a
metered call. Fixtures are real values observed 2026-08-26.
"""

import consensus_preview as cp
from consensus_preview import (
    AnnualConsensus,
    fetch_fmp_annual_estimates,
    fetch_fmp_reported_currency,
)

# Real UNH rows, trimmed to the fields the fetcher reads.
UNH_ROWS = [
    {"date": "2027-12-31", "revenueAvg": 458616419994, "revenueLow": 442248000000,
     "revenueHigh": 476309000000, "numAnalystsRevenue": 19, "epsAvg": 22.4478,
     "epsLow": 19.92, "epsHigh": 24.5, "numAnalystsEps": 19},
    {"date": "2026-12-31", "revenueAvg": 445724878984, "revenueLow": 438519000000,
     "revenueHigh": 453000000000, "numAnalystsRevenue": 16, "epsAvg": 19.75996,
     "epsLow": 18.4, "epsHigh": 20.11, "numAnalystsEps": 18},
]


def _patch(monkeypatch, payload):
    monkeypatch.setattr(cp, "_fmp_get_json", lambda *a, **k: payload)


class TestAnnualEstimates:
    def test_parses_and_sorts_by_fiscal_period_end(self, monkeypatch):
        _patch(monkeypatch, UNH_ROWS)
        out = fetch_fmp_annual_estimates("UNH", "KEY")
        assert [r.fiscal_period_end for r in out] == ["2026-12-31", "2027-12-31"]
        assert out[0].revenue_avg == 445724878984
        assert out[0].eps_analysts == 18

    def test_currency_is_carried_not_assumed(self, monkeypatch):
        _patch(monkeypatch, UNH_ROWS)
        out = fetch_fmp_annual_estimates("NVO", "KEY", currency="DKK")
        assert all(r.currency == "DKK" for r in out)

    def test_currency_defaults_to_none_not_usd(self, monkeypatch):
        """Assuming USD is how a DKK reporter gets silently mis-compared."""
        _patch(monkeypatch, UNH_ROWS)
        out = fetch_fmp_annual_estimates("NVO", "KEY")
        assert all(r.currency is None for r in out)

    def test_zero_analysts_is_no_coverage_not_a_measured_zero(self, monkeypatch):
        _patch(monkeypatch, [{"date": "2026-12-31", "revenueAvg": 1.0,
                              "numAnalystsRevenue": 0, "numAnalystsEps": 0}])
        out = fetch_fmp_annual_estimates("X", "KEY")
        assert out[0].revenue_analysts is None
        assert out[0].eps_analysts is None

    def test_thin_coverage_is_preserved_for_the_caller_to_gate_on(self, monkeypatch):
        """QDEL really returns nEps=1. The fetcher must not hide it; the square
        is what refuses to draw a conclusion from a one-analyst consensus."""
        _patch(monkeypatch, [{"date": "2026-12-28", "revenueAvg": 2544427335,
                              "numAnalystsRevenue": 2, "epsAvg": 0.77264,
                              "numAnalystsEps": 1}])
        out = fetch_fmp_annual_estimates("QDEL", "KEY")
        assert out[0].eps_analysts == 1 and out[0].revenue_analysts == 2

    def test_row_without_a_period_end_is_dropped(self, monkeypatch):
        _patch(monkeypatch, [{"revenueAvg": 1.0}, {"date": "2026-12-31"}])
        out = fetch_fmp_annual_estimates("X", "KEY")
        assert len(out) == 1 and out[0].fiscal_period_end == "2026-12-31"

    def test_missing_metrics_are_none_never_zero(self, monkeypatch):
        _patch(monkeypatch, [{"date": "2026-12-31"}])
        r = fetch_fmp_annual_estimates("X", "KEY")[0]
        assert r.revenue_avg is None and r.eps_avg is None
        assert r.revenue_avg != 0

    def test_never_raises_on_transport_failure(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("network down")
        monkeypatch.setattr(cp, "_fmp_get_json", boom)
        assert fetch_fmp_annual_estimates("X", "KEY") == []

    def test_never_raises_on_unexpected_shape(self, monkeypatch):
        _patch(monkeypatch, {"Error Message": "gated"})
        assert fetch_fmp_annual_estimates("X", "KEY") == []

    def test_no_calendar_year_field_is_exposed(self):
        """MCK's fiscal year ends 2029-03-31. Offering a calendar stamp invites
        exactly the comparison that mislabels it, so the dataclass has none."""
        fields = AnnualConsensus.__dataclass_fields__
        assert not any("calendar" in f.lower() for f in fields)
        assert "fiscal_period_end" in fields


class TestReportedCurrency:
    def test_reads_reported_currency_not_trading_currency(self, monkeypatch):
        """`profile.currency` returns USD for NVO — the ADR's trading currency
        — while the company reports in DKK. This is why the income statement
        is the source and the obvious field is not."""
        _patch(monkeypatch, [{"reportedCurrency": "DKK", "revenue": 309064000000}])
        assert fetch_fmp_reported_currency("NVO", "KEY") == "DKK"

    def test_missing_currency_is_none_not_a_default(self, monkeypatch):
        _patch(monkeypatch, [{"revenue": 1}])
        assert fetch_fmp_reported_currency("X", "KEY") is None

    def test_empty_payload_is_none(self, monkeypatch):
        _patch(monkeypatch, [])
        assert fetch_fmp_reported_currency("X", "KEY") is None

    def test_never_raises(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("boom")
        monkeypatch.setattr(cp, "_fmp_get_json", boom)
        assert fetch_fmp_reported_currency("X", "KEY") is None
