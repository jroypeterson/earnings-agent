"""Board #522: FORWARD re-measure of web_search_20250305 vs web_search_20260209.

The 2026-10-01 replay (#409, scripts/replay_web_search_tool.py) could not rule
out look-ahead: it searched today's web while pretending to be 8 days earlier.
A forward test cannot leak - every page either tool can read was published
before the call - so this asks both tools NOW about 3Q26 dates that have not
happened yet, freezes the answers, and scores them after the dates pass
(scripts/score_web_search_forward.py).

Two steps, so the sample is on disk before a cent is spent:

    python scripts/forward_web_search_tool.py freeze --out diagnostics/<x>_sample.json
    python scripts/forward_web_search_tool.py run --sample <x>_sample.json --out <x>.jsonl --budget 10.5

Calls run one at a time, production-eligible cases (Tier 1/2 AND providers
disagree - what the cross-check would actually send) first, and both arms of a
case are reserved before it starts, so a budget stop drops whole control cases
rather than half pairs. `--resume` continues an interrupted run.

Both arms go through the production resolver, `web_resolver.resolve_with_meta`
(same prompt, model, parser, trust gate); only `tool_type` differs. The one
addition is a pass-through wrapper on `web_resolver._run_search` that copies
every retrieved search result's url / title / page_age into the row, so the
evidence each answer could rest on is recorded with its publish date. The
production module is not edited and nothing writes to the events DB, the
calendar, exports or Slack: the DB is opened read-only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import web_resolver  # noqa: E402
from storage import OPEN_EVENT_SQL  # noqa: E402
from replay_web_search_tool import _WORST_CALL, call_cost  # noqa: E402

TOOLS = [web_resolver._LEGACY_WEB_SEARCH_TOOL,
         web_resolver._DYNAMIC_WEB_SEARCH_TOOL]

# Per tier: (names wanted). Within a tier, names are taken in this order:
# date NOT confirmed by the company AND providers disagree (the population the
# production resolver actually sees), then unconfirmed+agree, then confirmed
# (controls: an announcement exists, both tools should find it).
_TIER_QUOTA = {1: 10, 2: 20, 3: 10}


def _yf_dates(ticker: str):
    from market_data import fetch_yfinance_earnings_date
    return fetch_yfinance_earnings_date(ticker)


def _disagree(db_date: str, yf: list[date] | None) -> bool:
    if not yf:
        return False
    d = date.fromisoformat(db_date)
    return not any(abs((d - y).days) <= 1 for y in yf)


def freeze(args) -> int:
    import edgar_client
    rng = random.Random(args.seed)
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    # The DB is a CI artifact; a local copy is only as fresh as its last
    # restore. Refuse one whose daily-sync heartbeat is stale (codex r4).
    from storage import get_last_sync_completed
    beat = get_last_sync_completed(conn)
    if not beat or (date.today() - date.fromisoformat(beat[:10])).days > args.max_db_age_days:
        print(f"refusing to freeze: last_sync_completed={beat!r} is older than "
              f"{args.max_db_age_days}d - restore the earnings-db artifact first")
        conn.close()
        return 2
    rows = conn.execute(
        "SELECT ticker, company_name, event_date, tier, date_confirmed, quarter "
        f"FROM events WHERE {OPEN_EVENT_SQL} "
        "AND event_date > ? AND event_date <= ? "
        "AND ticker NOT LIKE '% %' AND ticker NOT LIKE '%.%' "
        "ORDER BY ticker, event_date", (args.after, args.until)).fetchall()
    conn.close()
    by_tier: dict[int, list] = {}
    seen = set()
    for r in rows:
        if r[0] in seen:
            continue  # one event per ticker (earliest)
        seen.add(r[0])
        by_tier.setdefault(min(int(r[3]), 3), []).append(r)
    sample = []
    for tier, quota in _TIER_QUOTA.items():
        pool = by_tier.get(tier, [])
        rng.shuffle(pool)
        unconf = [r for r in pool if not r[4]]
        conf = [r for r in pool if r[4]]
        buckets = {"unconfirmed_disagree": [], "unconfirmed_agree": [],
                   "confirmed": []}
        want_conf = max(1, quota // 5)
        # Fetch yfinance lazily: stop once each bucket can fill the quota.
        for r in unconf + conf:
            if (len(buckets["unconfirmed_disagree"]) >= quota
                    and len(buckets["unconfirmed_agree"]) >= quota
                    and len(buckets["confirmed"]) >= want_conf):
                break
            if not edgar_client.get_cik(r[0]):
                continue  # scoring needs an EDGAR 8-K Item 2.02
            yf = _yf_dates(r[0])
            if yf is None:
                continue  # production cross-check skips yf-missing rows too
            kind = ("confirmed" if r[4] else
                    "unconfirmed_disagree" if _disagree(r[2], yf)
                    else "unconfirmed_agree")
            buckets[kind].append((r, yf))
        picked = buckets["confirmed"][:want_conf]
        for k in ("unconfirmed_disagree", "unconfirmed_agree", "confirmed"):
            for item in buckets[k]:
                if len(picked) >= quota:
                    break
                if item not in picked:
                    picked.append(item)
        for (t, name, ev, tr, conf_flag, q), yf in picked:
            sample.append({
                "ticker": t, "company_name": name or t, "quarter": q,
                "tier": int(tr), "date_confirmed": bool(conf_flag),
                # The cross-check passes the stored (Finnhub-led) date as
                # "Finnhub says"; yfinance's own read is the second side.
                "finnhub": ev, "yfinance": [d.isoformat() for d in yf],
                "providers_disagree": _disagree(ev, yf),
            })
    out = {"frozen_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "seed": args.seed, "db": Path(args.db).name, "after": args.after,
           "until": args.until, "n": len(sample), "cases": sample}
    Path(args.out).write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(f"froze {len(sample)} cases -> {args.out}")
    for c in sample:
        print(f"  T{c['tier']} {c['ticker']:6} db={c['finnhub']} yf={c['yfinance']} "
              f"confirmed={c['date_confirmed']} disagree={c['providers_disagree']}")
    return 0


def _search_results(blocks) -> list[dict]:
    """url / title / page_age of every result the search tool retrieved."""
    f = web_resolver._field
    out = []
    for b in blocks or []:
        if f(b, "type", "") != "web_search_tool_result":
            continue
        res = f(b, "content", None)
        if not isinstance(res, (list, tuple)):
            continue
        for r in res:
            u = f(r, "url", None)
            if isinstance(u, str) and u:
                out.append({"url": u, "title": f(r, "title", None),
                            "page_age": f(r, "page_age", None)})
    return out


def _install_capture():
    """Pass-through wrapper: production _run_search, plus the retrieved
    results copied into meta. Behaviour is unchanged (same blocks back)."""
    orig = web_resolver._run_search

    def capture(client, prompt, tool_type, meta=None):
        blocks, meta = orig(client, prompt, tool_type, meta)
        meta["search_results"] = _search_results(blocks)
        meta["prompt"] = prompt
        return blocks, meta
    web_resolver._run_search = capture


def production_eligible(c: dict) -> bool:
    """Would the production cross-check send this name to the resolver?
    It reads only Tier 1/2 (main.run_cross_check) and only rows where
    yfinance disagrees with the stored date by more than a day."""
    return c["tier"] <= 2 and bool(c["providers_disagree"])


def run_order(cases: list[dict]) -> list[dict]:
    """Production-eligible cases first (they decide the switch), controls after,
    each group in frozen order - so a budget stop costs controls, not the
    decision cohort."""
    return ([c for c in cases if production_eligible(c)]
            + [c for c in cases if not production_eligible(c)])


def run(args) -> int:
    if not web_resolver.ANTHROPIC_API_KEY:
        # resolve_with_meta returns (None, {}) without a key; recording that
        # as a null answer would score as a real call (codex r1).
        print("ANTHROPIC_API_KEY unset - refusing to record fake calls")
        return 2
    raw = Path(args.sample).read_bytes()
    sample_sha = hashlib.sha256(raw).hexdigest()
    sample = json.loads(raw.decode("utf-8"))
    cases = run_order(sample["cases"])
    if args.max_cases:
        cases = cases[:args.max_cases]
    done, spent = set(), args.prior_unrecorded
    out_path = Path(args.out)
    if out_path.exists():
        if not args.resume:
            print(f"{out_path} exists; pass --resume to append")
            return 2
        first_asked = None
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                done.add((r["ticker"], r["tool"]))
                spent += r.get("cost", 0)
                if r.get("sample_sha256") not in (None, sample_sha):
                    # Rows from a different frozen sample (codex r3).
                    print(f"refusing --resume: {out_path} holds rows from another sample")
                    return 2
                d = date.fromisoformat(r["asked_on"])
                first_asked = d if first_asked is None else min(first_asked, d)
        # A resumed arm asked later than its pair can read evidence the first
        # arm could not (codex r2): resume only on the SAME day as the first
        # ask, which is what the scorer requires of a pair (codex r4).
        if first_asked and date.today() != first_asked:
            print(f"refusing --resume: first ask {first_asked}, today {date.today()}; "
                  "start a new --out instead")
            return 2
    asked_on = date.today()
    _install_capture()
    rng = random.Random(args.seed + 1)
    stopped = False
    with open(out_path, "a", encoding="utf-8") as out:
        for c in cases:
            order = TOOLS[:]
            rng.shuffle(order)  # drawn for every case so the order is stable on resume
            todo = [t for t in order if (c["ticker"], t) not in done]
            if not todo:
                continue
            # Reserve BOTH arms before starting a case, so a budget stop never
            # leaves a half pair (the scorer cannot use one).
            if spent + len(todo) * _WORST_CALL > args.budget:
                stopped = True
                print(f"budget stop before {c['ticker']}: spent ${spent:.2f}")
                break
            for tool in todo:
                v, meta = web_resolver.resolve_with_meta(
                    c["ticker"], c["company_name"], c["finnhub"],
                    [date.fromisoformat(d) for d in c["yfinance"]],
                    asked_on, tool_type=tool)
                cost = call_cost(meta)
                spent += cost
                src = v.source_url if v else None
                src_age = next((r["page_age"] for r in meta.get("search_results", [])
                                if src and r["url"] == src), None)
                rec = dict(c, tool=tool, asked_on=asked_on.isoformat(),
                           sample_sha256=sample_sha,
                           asked_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                           production_eligible=production_eligible(c),
                           cost=round(cost, 5), meta=meta,
                           announced_date=v.announced_date if v else None,
                           confidence=v.confidence if v else None,
                           source_url=src, source_page_age=src_age,
                           source_verified=v.source_verified if v else None,
                           note=v.note if v else None, verdict=v is not None)
                out.write(json.dumps(rec, default=str) + "\n")
                out.flush()
                print(f"{c['ticker']:6} {tool[-8:]} got={rec['announced_date']} "
                      f"db={c['finnhub']} conf={rec['confidence']} "
                      f"${cost:.3f} total=${spent:.2f}", flush=True)
    print(f"spent ${spent:.2f} (incl. ${args.prior_unrecorded:.2f} prior unrecorded); "
          f"stopped_on_budget={stopped}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("freeze")
    f.add_argument("--db", default=str(ROOT / "earnings_events.db"))
    f.add_argument("--after", default="2026-10-09",
                   help="only events strictly after this date")
    f.add_argument("--until", default="2026-11-20")
    f.add_argument("--seed", type=int, default=522)
    f.add_argument("--max-db-age-days", type=int, default=1)
    f.add_argument("--out", required=True)
    r = sub.add_parser("run")
    r.add_argument("--sample", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--budget", type=float, default=10.5,
                   help="USD cap ESTIMATE incl. rows already in --out (see _WORST_CALL)")
    r.add_argument("--resume", action="store_true",
                   help="append to an existing --out, skipping (ticker, tool) done")
    r.add_argument("--prior-unrecorded", type=float, default=0.0,
                   help="USD billed by calls killed in flight (not in --out)")
    r.add_argument("--max-cases", type=int, default=0)
    r.add_argument("--seed", type=int, default=522)
    args = ap.parse_args()
    return freeze(args) if args.cmd == "freeze" else run(args)


if __name__ == "__main__":
    raise SystemExit(main())
