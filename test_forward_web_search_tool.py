"""Board #522: the forward harness's run loop, offline (resolver stubbed)."""
import json
import sys
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "scripts"))

import forward_web_search_tool as fw  # noqa: E402
import web_resolver  # noqa: E402


def _case(t, tier=2, disagree=True):
    return {"ticker": t, "company_name": t, "quarter": "2026Q3", "tier": tier,
            "date_confirmed": False, "finnhub": "2026-11-04",
            "yfinance": ["2026-11-09"], "providers_disagree": disagree}


def _setup(tmp_path, monkeypatch, cases, cost_tokens=10_000):
    sample = tmp_path / "s.json"
    sample.write_text(json.dumps({"cases": cases}), encoding="utf-8")
    calls = []

    def fake(ticker, name, fh, yf, today, tool_type=None):
        calls.append((ticker, tool_type))
        return None, {"tool_type": tool_type, "input_tokens": cost_tokens,
                      "output_tokens": 0, "web_search_requests": 0}
    monkeypatch.setattr(web_resolver, "resolve_with_meta", fake)
    monkeypatch.setattr(web_resolver, "ANTHROPIC_API_KEY", "k")
    monkeypatch.setattr(fw, "_install_capture", lambda: None)
    return sample, calls


def _args(sample, out, budget=100.0, resume=False):
    return Namespace(sample=str(sample), out=str(out), budget=budget, resume=resume,
                     prior_unrecorded=0.0, max_cases=0, seed=522)


def test_refuses_without_a_key(tmp_path, monkeypatch):
    sample, calls = _setup(tmp_path, monkeypatch, [_case("A")])
    monkeypatch.setattr(web_resolver, "ANTHROPIC_API_KEY", "")
    assert fw.run(_args(sample, tmp_path / "o.jsonl")) == 2
    assert calls == [] and not (tmp_path / "o.jsonl").exists()


def test_production_cases_run_first_and_a_budget_stop_leaves_no_half_pair(tmp_path, monkeypatch):
    cases = [_case("CTRL", tier=3), _case("P1"), _case("P2")]
    sample, calls = _setup(tmp_path, monkeypatch, cases)
    # Budget admits one case (both arms reserved), not a second: the stop
    # falls between cases, and the first case run is production-eligible.
    out = tmp_path / "o.jsonl"
    assert fw.run(_args(sample, out, budget=2 * fw._WORST_CALL + 0.05)) == 0
    assert sorted(calls) == sorted([("P1", fw.TOOLS[0]), ("P1", fw.TOOLS[1])])
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert len(rows) == 2 and all(r["sample_sha256"] for r in rows)
    assert fw.run_order(cases)[-1]["ticker"] == "CTRL"


def test_resume_skips_done_arms_and_refuses_a_foreign_sample(tmp_path, monkeypatch):
    sample, calls = _setup(tmp_path, monkeypatch, [_case("A"), _case("B")])
    out = tmp_path / "o.jsonl"
    assert fw.run(_args(sample, out, budget=2 * fw._WORST_CALL + 0.01)) == 0
    assert {c[0] for c in calls} == {"A"}
    assert fw.run(_args(sample, out)) == 2  # exists, no --resume
    calls.clear()
    assert fw.run(_args(sample, out, resume=True)) == 0
    assert {c[0] for c in calls} == {"B"}
    other = tmp_path / "other.json"
    other.write_text(json.dumps({"cases": [_case("A"), _case("B"), _case("C")]}), encoding="utf-8")
    assert fw.run(_args(other, out, resume=True)) == 2


def test_freeze_refuses_a_stale_local_db(tmp_path):
    import sqlite3
    db = tmp_path / "e.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE kv_store (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO kv_store VALUES ('last_sync_completed', '2026-01-01')")
    conn.commit()
    conn.close()
    args = Namespace(db=str(db), after="2026-10-09", until="2026-11-20", seed=522,
                     out=str(tmp_path / "s.json"), max_db_age_days=1)
    assert fw.freeze(args) == 2 and not (tmp_path / "s.json").exists()
