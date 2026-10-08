"""Score the board #522 FORWARD test (scripts/forward_web_search_tool.py).

Run it once the sampled 3Q26 dates have passed (target: 2026-11-20):

    python scripts/score_web_search_forward.py diagnostics/web_search_tool_forward_2026-10-08.jsonl

Ground truth per ticker, highest authority first:
  1. --truth CSV (ticker,date[,source]) - a hand adjudication, e.g. a company
     whose release and 8-K were filed on different days.
  2. SEC EDGAR 8-K Item 2.02: the EARLIEST one filed after the question was
     asked (asked_on + 1 .. --horizon days after the latest candidate date).
     Earliest, not latest: a later 2.02 in the window is a different event.
  3. An earnings-looking 6-K (foreign private issuers - CNI, GRFS class). A
     filename heuristic, so its rows are labelled and listed for review.
  No filing yet -> PENDING, and the summary says the score is not final.

A mechanical truth is NOT used where it is weakest: a 6-K, or an 8-K date
one day off either tool's answer (a release after the close can carry a
next-day 8-K). Those are listed as ADJUDICATE and need a --truth row.

Two cohorts. PRODUCTION = Tier 1/2 names whose providers disagree - exactly
what `main.run_cross_check` sends to the resolver - and only it DECIDES.
CONTROLS (Tier 3, or providers agree) are context. Per tool: exact / wrong /
null, high-confidence (and wrong), would-auto-lock (high AND equal to a
provider candidate - what production would lock) and its wrong count, the
provider baselines, and a sign test on the names where exactly one tool was
right. Every page either tool read was published before the call, so there is
no look-ahead to adjudicate - the reason this test exists.

Decision rule (the replay's): switch `_WEB_SEARCH_TOOL` to web_search_20260209
only if, on the production cohort, its exact count is >= legacy's AND its
wrong-auto-lock count is <= legacy's. FINAL only when every production case
has both arms, a truth, and nothing awaits adjudication.

--save-truth writes the resolved truth table so a re-score is reproducible
without EDGAR.
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

LEGACY, DYNAMIC = "web_search_20250305", "web_search_20260209"


def _yf_list(r: dict) -> list[str]:
    y = r.get("yfinance")
    return list(y) if isinstance(y, (list, tuple)) else ([y] if y else [])


def edgar_truth(ticker: str, start: date, end: date,
                fetch_8k=None, fetch_6k=None) -> tuple[str, str] | None:
    """(date, source) of the earliest results filing in [start, end], or None.
    Fetchers are injectable so tests never touch the network."""
    if fetch_8k is None or fetch_6k is None:
        import edgar_client
        days = max(7, (date.today() - start).days + 7)
        fetch_8k = fetch_8k or (lambda t: edgar_client.fetch_8k_filings(
            t, days_back=days, earnings_only=True))
        fetch_6k = fetch_6k or (lambda t: edgar_client.fetch_6k_filings(
            t, days_back=days))
    for fetch, label in ((fetch_8k, "8-K 2.02"), (fetch_6k, "6-K (filename heuristic)")):
        hits = sorted(f.filing_date for f in fetch(ticker)
                      if start.isoformat() <= f.filing_date <= end.isoformat())
        if hits:
            return hits[0], label
    return None


def resolve_truth(rows: list[dict], overrides: dict, horizon: int,
                  fetch_8k=None, fetch_6k=None) -> dict:
    """ticker -> {"date", "source"} for every ticker with a known answer."""
    truth = {}
    for t in sorted({r["ticker"] for r in rows}):
        if t in overrides:
            truth[t] = overrides[t]
            continue
        rs = [r for r in rows if r["ticker"] == t]
        asked = max(date.fromisoformat(r["asked_on"]) for r in rs)
        cands = [date.fromisoformat(d) for r in rs
                 for d in [r["finnhub"], *_yf_list(r), r.get("announced_date")] if d]
        end = max(cands + [asked]) + timedelta(days=horizon)
        hit = edgar_truth(t, asked + timedelta(days=1), end, fetch_8k, fetch_6k)
        if hit:
            truth[t] = {"date": hit[0], "source": hit[1]}
    return truth


def sign_test_p(a: int, b: int) -> float:
    """Two-sided exact binomial p for a vs b discordant pairs."""
    n, k = a + b, min(a, b)
    if n == 0:
        return 1.0
    p = sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n
    return min(1.0, 2 * p)


def is_production(r: dict) -> bool:
    """Would the production cross-check have sent this case to the resolver?
    (Tier 1/2 AND providers disagree.) Replay rows carry no tier: every replay
    case was a Tier 1/2 disagreement by construction."""
    if "production_eligible" in r:
        return bool(r["production_eligible"])
    return int(r.get("tier", 1)) <= 2 and bool(r.get("providers_disagree", True))


def _metrics(scored: dict, truth: dict) -> dict:
    out = {"n": len(scored), "tools": {}}
    for tool in (LEGACY, DYNAMIC):
        rs = [(d[tool], truth[t]["date"]) for t, d in scored.items()]
        hi = [(r, tr) for r, tr in rs if r["confidence"] == "high"]
        lock = [(r, tr) for r, tr in hi
                if r["announced_date"] in [r["finnhub"], *_yf_list(r)]]
        out["tools"][tool] = {
            "exact": sum(r["announced_date"] == tr for r, tr in rs),
            "wrong": sum(r["announced_date"] not in (None, tr) for r, tr in rs),
            "null": sum(r["announced_date"] is None for r, _ in rs),
            "high": len(hi), "high_wrong": sum(r["announced_date"] != tr for r, tr in hi),
            "auto_lock": len(lock),
            "auto_lock_wrong": sum(r["announced_date"] != tr for r, tr in lock),
        }
    out["baselines"] = {
        "db_date (finnhub-led)": sum(d[LEGACY]["finnhub"] == truth[t]["date"]
                                     for t, d in scored.items()),
        "yfinance (any)": sum(truth[t]["date"] in _yf_list(d[LEGACY])
                              for t, d in scored.items()),
    }
    right = lambda d, t, tool: d[tool]["announced_date"] == truth[t]["date"]
    out["only_legacy_right"] = sorted(t for t, d in scored.items()
                                      if right(d, t, LEGACY) and not right(d, t, DYNAMIC))
    out["only_dynamic_right"] = sorted(t for t, d in scored.items()
                                       if right(d, t, DYNAMIC) and not right(d, t, LEGACY))
    out["sign_test_p"] = round(sign_test_p(len(out["only_legacy_right"]),
                                           len(out["only_dynamic_right"])), 3)
    return out


def _needs_adjudication(d: dict, tr: dict) -> str:
    """Why a mechanical truth must not score this pair without a --truth row
    ('' if it may). A 6-K is a filename heuristic, never proof (CLAUDE.md
    source hierarchy). An 8-K filing date is not a guaranteed release-date
    proxy - a late 8-K can trail the release by days (codex r2) - so an 8-K
    date only stands where neither tool contradicts it: every disagreeing
    answer is checked by hand. Agreement corroborates; nulls are not claims."""
    if tr["source"].startswith("6-K"):
        return "truth is a 6-K filename heuristic"
    if tr["source"] == "8-K 2.02":
        for tool in (LEGACY, DYNAMIC):
            a = d[tool]["announced_date"]
            if a and a != tr["date"]:
                return f"{tool} answered {a}, 8-K filed {tr['date']}"
        # The filing date must also be CORROBORATED by an independent read of
        # the release date - a provider candidate, or a tool answer resting on
        # a verified company/wire source (codex r5). A late 8-K that only a
        # tool's unverified answer happens to equal must not score it exact.
        r = d[LEGACY]
        support = {r["finnhub"], *_yf_list(r)} | {
            d[t]["announced_date"] for t in (LEGACY, DYNAMIC) if d[t].get("source_verified")}
        if tr["date"] not in support:
            return f"8-K date {tr['date']} matches no provider candidate or verified answer"
    return ""


def _unfair_pair(d: dict, tr: dict) -> str:
    """Both arms must have been asked on (about) the same day, before the
    event: a later-asked arm can read evidence its pair could not (codex r2)."""
    asked = [d[t].get("asked_on") for t in (LEGACY, DYNAMIC)]
    if not all(asked):
        return ""  # replay rows: no ask date, scored as the replay was
    a0, a1 = sorted(date.fromisoformat(a) for a in asked)
    if a1 != a0:  # same day only (codex r3): a day is enough for an announcement
        return f"arms asked {a0} and {a1}"
    if a1 >= date.fromisoformat(tr["date"]):
        return f"asked {a1}, on/after the event {tr['date']}"
    return ""


# main.run_cross_check(days_ahead=14): production only searches names whose
# stored date is at most this far ahead.
PRODUCTION_WINDOW_DAYS = 14


def lead_days(r: dict) -> int | None:
    if not r.get("asked_on"):
        return None
    return (date.fromisoformat(r["finnhub"]) - date.fromisoformat(r["asked_on"])).days


def score(rows: list[dict], truth: dict, expected: list[dict] | None = None,
          sample_sha: str | None = None) -> dict:
    """Pure scoring. rows: forward JSONL rows; truth: ticker -> {date, source};
    expected: the frozen sample's cases (so an unrun or half-run case is
    visible instead of silently vanishing - codex r1)."""
    exp_by = {c["ticker"]: c for c in (expected or [])}

    def _bad(r):
        m = r.get("meta") or {}
        # No tool_type = no API call was made (e.g. key unset): not a result.
        # verdict False = the call failed or returned no parseable answer: a
        # failure, not a "found nothing" null (codex r3).
        if m.get("fell_back") or m.get("tool_type") != r["tool"] or r.get("verdict") is False:
            return True
        if sample_sha and r.get("sample_sha256") not in (None, sample_sha):
            return True  # bound to a different frozen sample (codex r5)
        if exp_by:  # the row must be THIS frozen sample's case (codex r3)
            c = exp_by.get(r["ticker"])
            return c is None or (c.get("finnhub"), c.get("yfinance")) != (
                r.get("finnhub"), r.get("yfinance"))
        return False
    bad = [r for r in rows if _bad(r)]
    rows = [r for r in rows if not _bad(r)]
    by = collections.defaultdict(dict)
    for r in rows:
        by[r["ticker"]][r["tool"]] = r
    pairs = {t: d for t, d in by.items() if LEGACY in d and DYNAMIC in d}
    half = sorted(t for t in by if t not in pairs)
    pending = sorted(t for t in pairs if t not in truth)
    adjudicate = {t: why for t, d in pairs.items() if t in truth
                  for why in [_unfair_pair(d, truth[t]) or _needs_adjudication(d, truth[t])]
                  if why}
    scored = {t: d for t, d in pairs.items() if t in truth and t not in adjudicate}
    prod = {t: d for t, d in scored.items() if is_production(d[LEGACY])}
    ctrl = {t: d for t, d in scored.items() if t not in prod}
    exp = expected or []
    unrun = sorted(c["ticker"] for c in exp if c["ticker"] not in pairs)
    unrun_prod = sorted(c["ticker"] for c in exp
                        if c["ticker"] not in pairs and is_production(c))
    out = {"excluded": [f"{r['ticker']}/{r['tool']}" for r in bad],
           # Rows from before the harness stamped sample_sha256: admitted only
           # because their case payload equals the frozen sample's (codex r5).
           "unbound": sum(1 for r in rows if not r.get("sample_sha256")),
           "pairs": len(pairs), "half_pairs": half, "pending": pending,
           "adjudicate": adjudicate, "unrun": unrun, "unrun_production": unrun_prod,
           "cohorts": {"production": _metrics(prod, truth),
                       "controls": _metrics(ctrl, truth),
                       "all": _metrics(scored, truth)},
           "cost": {tool: round(sum(r.get("cost", 0) for r in rows if r["tool"] == tool), 4)
                    for tool in (LEGACY, DYNAMIC)},
           "calls": {tool: sum(r["tool"] == tool for r in rows) for tool in (LEGACY, DYNAMIC)}}
    P = out["cohorts"]["production"]["tools"]
    L, D = P[LEGACY], P[DYNAMIC]
    # The decision rests on the PRODUCTION cohort only: controls (Tier 3, or
    # providers agree) never reach the resolver in production (codex r1).
    # None = undecided: with no scored production pair both comparisons are
    # vacuously true, and "switch" must never come out of zero evidence (codex r4).
    out["switch"] = (None if not prod else
                     D["exact"] >= L["exact"] and D["auto_lock_wrong"] <= L["auto_lock_wrong"])
    leads = [lead_days(d[LEGACY]) for d in prod.values()]
    out["production_in_window"] = sum(1 for x in leads
                                      if x is None or x <= PRODUCTION_WINDOW_DAYS)
    out["production_lead_days"] = [min(x for x in leads if x is not None),
                                   max(x for x in leads if x is not None)]         if any(x is not None for x in leads) else None
    # Gating, not informational (codex r3): a cohort asked further ahead than
    # production ever asks cannot make the switch decision final on its own.
    out["production_matched"] = bool(prod) and out["production_in_window"] == len(prod)
    # Only PRODUCTION-cohort gaps block finality; a control is context (codex r4).
    prod_t = {c["ticker"] for c in exp if is_production(c)} | {
        r["ticker"] for r in rows + bad if is_production(r)}
    blockers = ([t for t in pending if t in prod_t] + [t for t in adjudicate if t in prod_t]
                + [t for t in half if t in prod_t]
                + [r["ticker"] for r in bad if r["ticker"] in prod_t] + unrun_prod)
    out["final"] = out["production_matched"] and not blockers
    return out


def load_overrides(path: str | None) -> dict:
    if not path:
        return {}
    out = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            date.fromisoformat(row["date"])  # refuse a malformed date loudly
            out[row["ticker"].strip()] = {"date": row["date"].strip(),
                                          "source": (row.get("source") or "manual").strip()}
    return out


def render(res: dict, truth: dict) -> str:
    lines = [f"pairs={res['pairs']} scored: production={res['cohorts']['production']['n']} "
             f"controls={res['cohorts']['controls']['n']}"
             + ("" if res["final"] else "  ** NOT FINAL **")]
    for key, label in (("excluded", "EXCLUDED (no call / wrong tool)"),
                       ("half_pairs", "HALF PAIRS (one arm only, unscorable)"),
                       ("pending", "PENDING (no results filing found yet)"),
                       ("unrun_production", "UNRUN production-eligible cases"),
                       ("unrun", "unrun cases (all)")):
        if res[key]:
            lines.append(f"{label}: " + " ".join(res[key]))
    if res.get("unbound"):
        lines.append(f"note: {res['unbound']} row(s) carry no sample_sha256 (pre-binding harness); "
                     "admitted by exact case-payload match against the frozen sample")
    for t, why in sorted(res["adjudicate"].items()):
        lines.append(f"ADJUDICATE {t}: truth {truth[t]['date']} ({truth[t]['source']}) - {why}; "
                     "add a --truth row")
    for name in ("production", "controls"):
        c = res["cohorts"][name]
        lines.append(f"-- {name} cohort (n={c['n']})"
                     + (" - DECIDES" if name == "production" else " - context only"))
        for tool, m in c["tools"].items():
            lines.append(f"  {tool}: exact {m['exact']}/{c['n']} | wrong {m['wrong']} | "
                         f"null {m['null']} | high {m['high']} (wrong {m['high_wrong']}) | "
                         f"would auto-lock {m['auto_lock']} (wrong {m['auto_lock_wrong']})")
        lines.append("  baselines (exact): " + ", ".join(
            f"{k} {v}/{c['n']}" for k, v in c["baselines"].items()))
        lines.append(f"  only legacy right: {c['only_legacy_right']} | only dynamic right: "
                     f"{c['only_dynamic_right']} | sign test p={c['sign_test_p']}")
    lines.append("cost: " + ", ".join(f"{t} ${v:.2f} over {res['calls'][t]} calls"
                                      for t, v in res["cost"].items()))
    n_prod = res["cohorts"]["production"]["n"]
    if res.get("production_lead_days"):
        lo, hi = res["production_lead_days"]
        lines.append(f"production cohort asked {lo}-{hi} days ahead; "
                     f"{res['production_in_window']}/{n_prod} inside production's "
                     f"{PRODUCTION_WINDOW_DAYS}-day cross-check window")
        if res["production_in_window"] < n_prod:
            lines.append("  CAVEAT: cases asked earlier than production would ask measure "
                         "EARLY resolution (often before any announcement exists), not the "
                         "exact production moment")
    lines.append("DECISION: " + ("UNDECIDED - no scored production pair" if res["switch"] is None
                                  else "switch to web_search_20260209" if res["switch"]
                                  else "keep web_search_20250305")
                 + ("" if res["final"] else
                    " (PROVISIONAL - resolve the items above first)" if res.get("production_matched")
                    else " (INDICATIVE ONLY - the deciding cohort was not asked inside "
                         "production's window; not sufficient on its own to switch)"))
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl")
    ap.add_argument("--sample", help="frozen sample JSON (default: <jsonl stem>_sample.json)")
    ap.add_argument("--truth", help="CSV ticker,date[,source] overriding EDGAR")
    ap.add_argument("--horizon", type=int, default=30,
                    help="days past the latest candidate date to look for a filing")
    ap.add_argument("--save-truth", help="write the resolved truth table (JSON)")
    ap.add_argument("--load-truth", help="use a saved truth table instead of EDGAR")
    a = ap.parse_args(argv)
    rows = [json.loads(l) for l in open(a.jsonl, encoding="utf-8") if l.strip()]
    sample_path = Path(a.sample) if a.sample else Path(a.jsonl).with_name(
        Path(a.jsonl).stem + "_sample.json")
    expected = sample_sha = None
    if sample_path.exists():
        raw = sample_path.read_bytes()
        sample_sha = hashlib.sha256(raw).hexdigest()
        expected = json.loads(raw.decode("utf-8"))["cases"]
    if expected is None:
        print(f"WARNING: no frozen sample at {sample_path}; unrun cases are invisible")
    if a.load_truth:
        truth = json.loads(Path(a.load_truth).read_text(encoding="utf-8"))
        truth.update(load_overrides(a.truth))
    else:
        truth = resolve_truth(rows, load_overrides(a.truth), a.horizon)
    if a.save_truth:
        Path(a.save_truth).write_text(json.dumps(truth, indent=1), encoding="utf-8")
    res = score(rows, truth, expected, sample_sha)
    print(render(res, truth))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
