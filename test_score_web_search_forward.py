"""Board #522: the forward-test scorer (scripts/score_web_search_forward.py).

Offline by construction: EDGAR fetchers are injected. The differential test
re-scores the 2026-10-01 replay data and must reproduce the numbers that
diagnostics/web_search_tool_replay_2026-10-01.md recorded with the replay's
own scorer - two implementations, one answer.
"""
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent / "scripts"))

import score_web_search_forward as sf  # noqa: E402

REPLAY = Path(__file__).parent / "diagnostics" / "web_search_tool_replay_2026-10-01.jsonl"


def _f(d):
    return SimpleNamespace(filing_date=d)


def _row(t, tool, got, conf="high", fh="2026-10-29", yf=("2026-11-05",),
         asked="2026-10-20", prod=True, **meta):
    return {"ticker": t, "tool": tool, "announced_date": got, "confidence": conf,
            "finnhub": fh, "yfinance": list(yf), "asked_on": asked, "cost": 0.1,
            "production_eligible": prod, "meta": dict({"tool_type": tool}, **meta)}


def test_reproduces_the_replay_numbers():
    rows = [json.loads(l) for l in REPLAY.read_text(encoding="utf-8").splitlines() if l]
    truth = {r["ticker"]: {"date": r["truth"], "source": "replay"} for r in rows}
    res = sf.score(rows, truth)
    P = res["cohorts"]["production"]
    L, D = P["tools"][sf.LEGACY], P["tools"][sf.DYNAMIC]
    assert P["n"] == 40 and res["final"]
    assert (L["exact"], D["exact"]) == (38, 38)
    assert (L["wrong"], D["wrong"]) == (2, 2)
    assert (L["high"], L["high_wrong"], D["high"], D["high_wrong"]) == (33, 1, 29, 1)
    assert (L["auto_lock"], L["auto_lock_wrong"]) == (32, 0)
    assert (D["auto_lock"], D["auto_lock_wrong"]) == (28, 0)


def test_truth_is_the_earliest_filing_inside_the_window():
    f8 = lambda t: [_f("2026-12-20"), _f("2026-11-04"), _f("2026-10-30"), _f("2026-10-01")]
    hit = sf.edgar_truth("X", date(2026, 10, 9), date(2026, 12, 1), f8, lambda t: [])
    assert hit == ("2026-10-30", "8-K 2.02")


def test_6k_is_a_labelled_fallback_and_none_is_pending():
    f6 = lambda t: [_f("2026-10-30")]
    assert sf.edgar_truth("X", date(2026, 10, 9), date(2026, 12, 1),
                          lambda t: [], f6) == ("2026-10-30", "6-K (filename heuristic)")
    assert sf.edgar_truth("X", date(2026, 10, 9), date(2026, 12, 1),
                          lambda t: [], lambda t: []) is None


def test_window_starts_after_the_question_and_reaches_past_the_latest_candidate():
    def f8(t):
        return [_f("2026-10-08"), _f("2026-12-02")]
    rows = [_row("A", sf.LEGACY, None), _row("A", sf.DYNAMIC, None)]
    truth = sf.resolve_truth(rows, {}, 30, f8, lambda t: [])
    # 2026-10-08 is the day asked -> excluded; 12-02 <= 11-05 + 30d -> kept.
    assert truth == {"A": {"date": "2026-12-02", "source": "8-K 2.02"}}


def test_override_wins_and_pending_marks_not_final():
    rows = [_row("A", sf.LEGACY, "2026-10-29"), _row("A", sf.DYNAMIC, "2026-10-29"),
            _row("B", sf.LEGACY, None), _row("B", sf.DYNAMIC, None)]
    truth = sf.resolve_truth(rows, {"A": {"date": "2026-10-29", "source": "manual"}},
                             30, lambda t: [], lambda t: [])
    assert truth == {"A": {"date": "2026-10-29", "source": "manual"}}
    res = sf.score(rows, truth)
    assert res["pending"] == ["B"] and not res["final"]
    assert "NOT FINAL" in sf.render(res, truth)


def test_auto_lock_counts_only_a_high_answer_equal_to_a_candidate():
    rows = [
        _row("A", sf.LEGACY, "2026-10-29"),                 # high, = finnhub -> lock
        _row("A", sf.DYNAMIC, "2026-10-30"),                # high, third date -> no lock
        _row("B", sf.LEGACY, "2026-11-05", conf="medium"),  # medium -> no lock
        _row("B", sf.DYNAMIC, "2026-11-05"),                # high, = yf, WRONG -> wrong lock
    ]
    truth = {"A": {"date": "2026-10-29", "source": "t"},
             "B": {"date": "2026-10-29", "source": "t"}}
    res = sf.score(rows, truth)
    P = res["cohorts"]["production"]
    L, D = P["tools"][sf.LEGACY], P["tools"][sf.DYNAMIC]
    assert (L["auto_lock"], L["auto_lock_wrong"]) == (1, 0)
    assert (D["auto_lock"], D["auto_lock_wrong"]) == (1, 1)
    assert (D["high"], D["high_wrong"]) == (2, 2)
    assert P["only_legacy_right"] == ["A"] and P["only_dynamic_right"] == []
    assert not res["switch"]


def test_switch_requires_ge_exact_and_no_more_wrong_locks():
    rows = [_row("A", sf.LEGACY, None), _row("A", sf.DYNAMIC, "2026-10-29")]
    truth = {"A": {"date": "2026-10-29", "source": "t"}}
    assert sf.score(rows, truth)["switch"]
    # Tied exact, but the new tool auto-locks a wrong date the old one did not.
    rows = [_row("A", sf.LEGACY, "2026-10-29"), _row("A", sf.DYNAMIC, "2026-10-29"),
            _row("B", sf.LEGACY, None), _row("B", sf.DYNAMIC, "2026-11-05")]
    truth["B"] = {"date": "2026-11-04", "source": "t"}
    res = sf.score(rows, truth)
    T = res["cohorts"]["production"]["tools"]
    assert T[sf.LEGACY]["exact"] == T[sf.DYNAMIC]["exact"]
    assert not res["switch"]


def test_a_fallback_row_is_excluded_and_blocks_final():
    rows = [_row("A", sf.LEGACY, "2026-10-29"),
            _row("A", sf.DYNAMIC, "2026-10-29", fell_back=True)]
    res = sf.score(rows, {"A": {"date": "2026-10-29", "source": "t"}})
    assert res["excluded"] == ["A/" + sf.DYNAMIC]
    assert res["cohorts"]["all"]["n"] == 0 and not res["final"]


def test_sign_test():
    assert abs(sf.sign_test_p(5, 1) - 0.21875) < 1e-9
    assert sf.sign_test_p(0, 0) == 1.0


def test_empty_or_partial_input_is_never_final():
    assert not sf.score([], {})["final"]
    expected = [{"ticker": "A", "tier": 2, "providers_disagree": True, "finnhub": "2026-10-29", "yfinance": ["2026-11-05"]},
                {"ticker": "B", "tier": 2, "providers_disagree": True, "finnhub": "2026-10-29", "yfinance": ["2026-11-05"]},
                {"ticker": "C", "tier": 3, "providers_disagree": True, "finnhub": "2026-10-29", "yfinance": ["2026-11-05"]}]
    rows = [_row("A", sf.LEGACY, "2026-10-29"), _row("A", sf.DYNAMIC, "2026-10-29"),
            _row("B", sf.LEGACY, "2026-10-29")]
    truth = {t: {"date": "2026-10-29", "source": "manual"} for t in "ABC"}
    res = sf.score(rows, truth, expected)
    assert res["half_pairs"] == ["B"] and res["unrun_production"] == ["B"]
    assert res["unrun"] == ["B", "C"] and not res["final"]
    # B completed: final, although control C never ran (a budget stop drops controls).
    rows.append(_row("B", sf.DYNAMIC, "2026-10-29"))
    res = sf.score(rows, truth, expected)
    assert res["final"] and res["unrun"] == ["C"]


def test_a_row_with_no_api_call_is_excluded():
    rows = [_row("A", sf.LEGACY, None), _row("A", sf.DYNAMIC, None)]
    rows[1]["meta"] = {}  # resolve_with_meta without a key returns (None, {})
    res = sf.score(rows, {"A": {"date": "2026-10-29", "source": "manual"}})
    assert res["excluded"] == ["A/" + sf.DYNAMIC] and not res["final"]


def test_any_answer_contradicting_an_8k_date_and_any_6k_need_adjudication():
    rows = [_row("A", sf.LEGACY, "2026-10-29"), _row("A", sf.DYNAMIC, "2026-10-29"),
            _row("B", sf.LEGACY, "2026-10-29"), _row("B", sf.DYNAMIC, None),
            _row("C", sf.LEGACY, "2026-11-04"), _row("C", sf.DYNAMIC, "2026-11-04"),
            _row("D", sf.LEGACY, "2026-10-29"), _row("D", sf.DYNAMIC, None)]
    truth = {"A": {"date": "2026-10-30", "source": "8-K 2.02"},
             "B": {"date": "2026-10-29", "source": "6-K (filename heuristic)"},
             # a late 8-K two days after the release both tools named (codex r2)
             "C": {"date": "2026-11-06", "source": "8-K 2.02"},
             "D": {"date": "2026-10-29", "source": "8-K 2.02"}}
    res = sf.score(rows, truth)
    assert sorted(res["adjudicate"]) == ["A", "B", "C"] and not res["final"]
    assert res["cohorts"]["all"]["n"] == 1  # only D (8-K agrees, null is no claim)
    for t, d in (("A", "2026-10-29"), ("B", "2026-10-29"), ("C", "2026-11-04")):
        truth[t] = {"date": d, "source": "manual"}  # the hand overrides
    res = sf.score(rows, truth)
    assert res["final"] and res["cohorts"]["all"]["n"] == 4
    assert res["cohorts"]["all"]["tools"][sf.LEGACY]["exact"] == 4


def test_arms_asked_apart_or_after_the_event_are_quarantined():
    rows = [_row("A", sf.LEGACY, "2026-10-29"), _row("A", sf.DYNAMIC, "2026-10-29", asked="2026-10-21"),
            _row("B", sf.LEGACY, "2026-10-29", asked="2026-10-29"),
            _row("B", sf.DYNAMIC, "2026-10-29", asked="2026-10-29")]
    truth = {t: {"date": "2026-10-29", "source": "manual"} for t in "AB"}
    res = sf.score(rows, truth)
    assert sorted(res["adjudicate"]) == ["A", "B"] and not res["final"]


def test_lead_time_against_the_production_window_is_reported():
    k = dict(asked="2026-10-08")
    rows = [_row("A", sf.LEGACY, None, fh="2026-10-29", **k), _row("A", sf.DYNAMIC, None, fh="2026-10-29", **k),
            _row("B", sf.LEGACY, None, fh="2026-10-15", **k), _row("B", sf.DYNAMIC, None, fh="2026-10-15", **k)]
    truth = {t: {"date": d, "source": "manual"} for t, d in (("A", "2026-10-29"), ("B", "2026-10-15"))}
    res = sf.score(rows, truth)
    assert res["production_lead_days"] == [7, 21] and res["production_in_window"] == 1
    out = sf.render(res, truth)
    assert "CAVEAT" in out and "INDICATIVE ONLY" in out
    # Gating: complete, adjudicated data asked outside the window is never final.
    assert not res["production_matched"] and not res["final"]


def test_only_the_production_cohort_decides():
    # Dynamic wins two controls, legacy wins the one production case: keep legacy.
    rows = [_row("P", sf.LEGACY, "2026-10-29"), _row("P", sf.DYNAMIC, None),
            _row("C1", sf.LEGACY, None, prod=False), _row("C1", sf.DYNAMIC, "2026-10-29", prod=False),
            _row("C2", sf.LEGACY, None, prod=False), _row("C2", sf.DYNAMIC, "2026-10-29", prod=False)]
    truth = {t: {"date": "2026-10-29", "source": "manual"} for t in ("P", "C1", "C2")}
    res = sf.score(rows, truth)
    A = res["cohorts"]["all"]["tools"]
    assert A[sf.DYNAMIC]["exact"] > A[sf.LEGACY]["exact"]
    assert not res["switch"]


def test_an_entirely_unrun_production_case_blocks_final():
    expected = [{"ticker": "A", "tier": 1, "providers_disagree": True, "finnhub": "2026-10-29", "yfinance": ["2026-11-05"]},
                {"ticker": "D", "tier": 2, "providers_disagree": True, "finnhub": "2026-10-29", "yfinance": ["2026-11-05"]}]
    rows = [_row("A", sf.LEGACY, "2026-10-29"), _row("A", sf.DYNAMIC, "2026-10-29")]
    res = sf.score(rows, {"A": {"date": "2026-10-29", "source": "manual"}}, expected)
    assert res["unrun_production"] == ["D"] and res["half_pairs"] == []
    assert not res["final"]


def test_a_failed_call_and_a_foreign_row_are_excluded():
    expected = [{"ticker": "A", "tier": 2, "providers_disagree": True,
                 "finnhub": "2026-10-29", "yfinance": ["2026-11-05"]}]
    rows = [_row("A", sf.LEGACY, None), _row("A", sf.DYNAMIC, "2026-10-29"),
            _row("Z", sf.LEGACY, None), _row("Z", sf.DYNAMIC, None)]
    rows[0]["verdict"] = False  # resolve_with_meta returned None after a call
    truth = {t: {"date": "2026-10-29", "source": "manual"} for t in "AZ"}
    res = sf.score(rows, truth, expected)
    assert sorted(res["excluded"]) == sorted(["A/" + sf.LEGACY, "Z/" + sf.DYNAMIC, "Z/" + sf.LEGACY])
    assert not res["final"]
    # Same ticker, different frozen payload (another sample) is foreign too.
    rows = [_row("A", sf.LEGACY, None, fh="2026-10-30"), _row("A", sf.DYNAMIC, None)]
    assert sf.score(rows, truth, expected)["excluded"] == ["A/" + sf.LEGACY]


def test_arms_one_day_apart_are_quarantined():
    rows = [_row("A", sf.LEGACY, None), _row("A", sf.DYNAMIC, "2026-10-29", asked="2026-10-21")]
    res = sf.score(rows, {"A": {"date": "2026-10-29", "source": "manual"}})
    assert list(res["adjudicate"]) == ["A"]


def test_no_scored_production_pair_is_undecided():
    res = sf.score([], {})
    assert res["switch"] is None and "UNDECIDED" in sf.render(res, {})


def test_a_control_gap_does_not_block_final():
    rows = [_row("P", sf.LEGACY, "2026-10-29"), _row("P", sf.DYNAMIC, "2026-10-29"),
            _row("C", sf.LEGACY, None, prod=False), _row("C", sf.DYNAMIC, None, prod=False),
            _row("H", sf.LEGACY, None, prod=False)]
    res = sf.score(rows, {"P": {"date": "2026-10-29", "source": "manual"}})
    assert res["pending"] == ["C"] and res["half_pairs"] == ["H"]
    assert res["final"]
    rows.append(_row("Q", sf.LEGACY, None))  # a production half pair does block
    assert not sf.score(rows, {"P": {"date": "2026-10-29", "source": "manual"}})["final"]


def test_an_8k_date_needs_independent_corroboration():
    rows = [_row("A", sf.LEGACY, "2026-10-30"), _row("A", sf.DYNAMIC, None)]
    truth = {"A": {"date": "2026-10-30", "source": "8-K 2.02"}}
    res = sf.score(rows, truth)  # finnhub 10-29, yf 11-05: nothing else says 10-30
    assert list(res["adjudicate"]) == ["A"]
    rows[0]["source_verified"] = True  # the company's own release said 10-30
    assert sf.score(rows, truth)["adjudicate"] == {}


def test_a_row_bound_to_another_sample_is_excluded():
    rows = [_row("A", sf.LEGACY, None), _row("A", sf.DYNAMIC, None)]
    rows[0]["sample_sha256"] = "other"
    rows[1]["sample_sha256"] = "mine"
    res = sf.score(rows, {}, None, "mine")
    assert res["excluded"] == ["A/" + sf.LEGACY]
    assert sf.score(rows[1:], {}, None, "mine")["unbound"] == 0
