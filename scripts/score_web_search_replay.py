"""Score a #409 replay JSONL (scripts/replay_web_search_tool.py output).

Two accuracies per tool:
  raw        - announced_date == EDGAR-confirmed truth.
  leak-free  - the same, AND the verdict does not visibly rest on evidence
               published after the simulated "today". The replay searches the
               CURRENT web, so a model can "find" the date in the results
               release itself or in an announcement made after the simulated
               day (codex r1, 2026-10-01). A hit flagged here was not
               available to the production resolver at that moment.

The leak flag reads the SOURCE URL only - a publication stamp in it
(Business Wire /YYYYMMDD..., GlobeNewswire /YYYY/MM/DD/, IR /YYYY-MM-DD-)
later than `today`, or a results-release slug ("-reports-...-quarter",
an EDGAR exhibit). A first draft also parsed the model's NOTE and flagged 25
of 38 hits per arm, nearly all false ("announced August 6" names the answer
itself; "will report Q2" is future tense), so notes are not read. This is a
LOWER bound on leakage: a clean-looking URL can still have been read
alongside a results page the model did not cite.

    python scripts/score_web_search_replay.py diagnostics/x.jsonl
"""
from __future__ import annotations

import collections
import json
import re
import sys
from datetime import date

_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}
_RESULTS_SLUG = re.compile(
    r"(-reports-|reports-(first|second|third|fourth|fiscal|q[1-4])|"
    r"/archives/edgar/|exhibitx?99|ex-?99)", re.I)


def _dates(text: str, year: int) -> list[date]:
    out = []
    for y, m, d in re.findall(r"(20\d\d)[/-]?(\d\d)[/-]?(\d\d)", text):
        try:
            out.append(date(int(y), int(m), int(d)))
        except ValueError:
            pass
    for mon, d in re.findall(r"\b([A-Z][a-z]+)\.? (\d{1,2})\b", text):
        if mon.lower() in _MONTHS:
            try:
                out.append(date(year, _MONTHS[mon.lower()], int(d)))
            except ValueError:
                pass
    return out


def leak_reason(r: dict) -> str:
    """Why this verdict's cited source visibly post-dates `today` or is the
    results release itself ('' if neither is visible in the URL)."""
    today = date.fromisoformat(r["today"])
    url = r.get("source_url") or ""
    for d in _dates(url, today.year):
        if d > today:
            return f"url dated {d}"
    m = _RESULTS_SLUG.search(url)
    if m:
        return f"results-release url ({m.group(0)})"
    return ""


def main(path: str) -> int:
    rows = [json.loads(l) for l in open(path, encoding="utf-8")]
    # Score only rows that ran on the tool they claim (codex r3): a dynamic
    # request that fell back to legacy must not count for the dynamic arm.
    bad = [r for r in rows if r["meta"].get("fell_back")
           or r["meta"].get("tool_type", r["tool"]) != r["tool"]]
    if bad:
        print(f"EXCLUDED {len(bad)} row(s) that did not run on their tool: "
              + ", ".join(f"{r['ticker']}/{r['tool']}" for r in bad))
    rows = [r for r in rows if r not in bad]
    by = collections.defaultdict(dict)
    for r in rows:
        by[r["ticker"]][r["tool"]] = r
    pairs = {t: d for t, d in by.items() if len(d) == 2}
    tools = sorted({r["tool"] for r in rows})
    print(f"rows={len(rows)} complete pairs={len(pairs)} "
          f"spent=${sum(r['cost'] for r in rows):.2f}")
    for tool in tools:
        rs = [d[tool] for d in pairs.values()]
        exact = [r for r in rs if r["announced_date"] == r["truth"]]
        leaks = {r["ticker"]: leak_reason(r) for r in rs}
        clean = [r for r in exact if not leaks[r["ticker"]]]
        hi = [r for r in rs if r["confidence"] == "high"]
        lock = [r for r in hi if r["announced_date"] in (r["finnhub"], r["yfinance"])]
        m = collections.Counter()
        for r in rs:
            for k in ("input_tokens", "output_tokens", "web_search_requests"):
                m[k] += r["meta"].get(k, 0)
        cost = sum(r["cost"] for r in rs)
        print(f"\n{tool}: exact {len(exact)}/{len(rs)} | leak-free exact "
              f"{len(clean)}/{len(rs)} (URL-visible leaks excluded) | wrong {sum(r['announced_date'] not in (None, r['truth']) for r in rs)}"
              f" | null {sum(r['announced_date'] is None for r in rs)}"
              f" | high {len(hi)} (wrong {sum(r['announced_date'] != r['truth'] for r in hi)})"
              f" | would auto-lock {len(lock)} (wrong {sum(r['announced_date'] != r['truth'] for r in lock)})")
        print(f"  tokens in {m['input_tokens']:,} out {m['output_tokens']:,} "
              f"searches {m['web_search_requests']} | ${cost:.2f} "
              f"(${cost / max(len(rs), 1):.3f}/call) | stop_reasons "
              f"{dict(collections.Counter(r['meta'].get('stop_reason') for r in rs))}")
        for t, why in sorted(leaks.items()):
            if why:
                ok = by[t][tool]["announced_date"] == by[t][tool]["truth"]
                print(f"  leak? {t:5} exact={ok} {why}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
