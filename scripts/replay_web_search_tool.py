"""Board #409 replay: web_search_20250305 vs web_search_20260209 on the
production resolver, against earnings dates with known answers.

Ground truth = a reported Tier 1/2 event whose date SEC EDGAR independently
confirms (an 8-K Item 2.02 filed ON that date). Each case is replayed as the
cross-check would have seen it ~8 days earlier: the true date on one side, a
plausible wrong date (+-1 / +-7 days, weekday) on the other, sides randomised.
Both tool versions get the identical prompt through `resolve_with_meta`, so
the measurement includes the production parser and trust gate.

    python scripts/replay_web_search_tool.py --n 40 --budget 8 --out diagnostics/x.jsonl

Spends real money (~$0.04-0.10 per call). Stops submitting once the measured
spend plus the in-flight worst case would pass --budget.
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import edgar_client  # noqa: E402
import web_resolver  # noqa: E402

# Sonnet 4.6, USD (platform.claude.com/docs/en/about-claude/pricing, read
# 2026-10-01): $3 / $15 per MTok, web search $10 per 1,000 searches, code
# execution free when used with web_search_20260209.
_IN, _OUT, _SEARCH = 3.0 / 1e6, 15.0 / 1e6, 10.0 / 1000
# In-flight reservation per call when checking the budget: every request a
# resolution may make (1 + _MAX_CONTINUATIONS), each at a generous 100k input
# tokens (2026-10-01 peak: 60,295) and the full _MAX_TOKENS of output, plus a
# search budget of _MAX_SEARCHES + one floor search per continuation. Codex r2:
# the first value ($0.25) sat below a legal 3-request resolution.
# NOT a hard API maximum (codex r5): input per request is unbounded in
# principle, so --budget is a cap ESTIMATE accurate to that 100k assumption.
# Run with --workers 1 near the cap if a hard bound matters.
_REQUESTS = 1 + web_resolver._MAX_CONTINUATIONS
_WORST_CALL = (_REQUESTS * (100_000 * _IN + web_resolver._MAX_TOKENS * _OUT)
               + (web_resolver._MAX_SEARCHES + web_resolver._MAX_CONTINUATIONS)
               * _SEARCH)


def call_cost(meta: dict) -> float:
    return (meta.get("input_tokens", 0) * _IN
            + meta.get("output_tokens", 0) * _OUT
            + meta.get("web_search_requests", 0) * _SEARCH)


def _weekday(d: date, step: int) -> date:
    while d.weekday() >= 5:
        d += timedelta(days=step)
    return d


def build_cases(db: str, n: int, seed: int, start: str, end: str) -> list[dict]:
    rng = random.Random(seed)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT DISTINCT ticker, company_name, event_date FROM events "
        "WHERE reported = 1 AND eps_actual IS NOT NULL AND tier <= 2 "
        "AND date_confirmed = 1 AND event_date BETWEEN ? AND ? "
        "AND ticker NOT LIKE '% %' AND ticker NOT LIKE '%.%' "
        "ORDER BY ticker, event_date", (start, end)).fetchall()
    conn.close()
    rng.shuffle(rows)
    cases, seen = [], set()
    for ticker, name, ev in rows:
        if len(cases) >= n or ticker in seen:
            continue
        truth = date.fromisoformat(ev)
        filing = edgar_client.find_earnings_release_filing(ticker, truth, truth)
        if filing is None:
            continue  # EDGAR does not independently confirm this date
        seen.add(ticker)
        off = rng.choice([-7, -1, 1, 7])
        wrong = _weekday(truth + timedelta(days=off), 1 if off > 0 else -1)
        if wrong == truth:
            wrong = _weekday(truth + timedelta(days=7), 1)
        truth_side = rng.choice(["finnhub", "yfinance"])
        fh, yf = (truth, wrong) if truth_side == "finnhub" else (wrong, truth)
        cases.append({
            "ticker": ticker, "company_name": name or ticker,
            "truth": truth.isoformat(), "wrong": wrong.isoformat(),
            "truth_side": truth_side, "finnhub": fh.isoformat(),
            "yfinance": yf.isoformat(),
            "today": (truth - timedelta(days=8)).isoformat(),
            "edgar_accession": filing.accession,
        })
    return cases


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "earnings_events.db"))
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--seed", type=int, default=409)
    ap.add_argument("--start", default="2026-07-08")
    ap.add_argument("--end", default="2026-09-15")
    ap.add_argument("--budget", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    cases = build_cases(args.db, args.n, args.seed, args.start, args.end)
    print(f"{len(cases)} EDGAR-confirmed cases")
    tools = [web_resolver._LEGACY_WEB_SEARCH_TOOL,
             web_resolver._DYNAMIC_WEB_SEARCH_TOOL]
    jobs = []
    rng = random.Random(args.seed + 1)
    for c in cases:
        order = tools[:]
        rng.shuffle(order)
        jobs += [(c, t) for t in order]

    lock = threading.Lock()
    state = {"spent": 0.0, "inflight": 0, "stopped": False}
    out = open(args.out, "w", encoding="utf-8")

    def run(job):
        c, tool = job
        with lock:
            if state["stopped"] or (state["spent"] + (state["inflight"] + 1)
                                    * _WORST_CALL > args.budget):
                state["stopped"] = True
                return
            state["inflight"] += 1
        try:
            v, meta = web_resolver.resolve_with_meta(
                c["ticker"], c["company_name"], c["finnhub"],
                [date.fromisoformat(c["yfinance"])],
                date.fromisoformat(c["today"]), tool_type=tool)
        finally:
            with lock:
                state["inflight"] -= 1
        cost = call_cost(meta)
        rec = dict(c, tool=tool, cost=round(cost, 5), meta=meta,
                   announced_date=v.announced_date if v else None,
                   confidence=v.confidence if v else None,
                   source_url=v.source_url if v else None,
                   source_verified=v.source_verified if v else None,
                   note=v.note if v else None, verdict=v is not None)
        with lock:
            state["spent"] += cost
            out.write(json.dumps(rec) + "\n")
            out.flush()
            print(f"{c['ticker']:6} {tool[-8:]} got={rec['announced_date']} "
                  f"truth={c['truth']} conf={rec['confidence']} "
                  f"${cost:.3f} total=${state['spent']:.2f}", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        list(ex.map(run, jobs))
    out.close()
    print(f"spent ${state['spent']:.2f}; stopped_on_budget={state['stopped']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
