"""
FMP vs Finnhub earnings-calendar comparison across the earnings_agent universe.

Purpose: decide whether FMP (already paid, Starter) is accurate/broad enough to
become the PRIMARY earnings source, with Finnhub demoted to fallback.

Methodology (kept deliberately simple + auditable):
  - Universe + tiers come from the SAME source the agent uses
    (main.load_coverage(), i.e. Coverage Manager exports).
  - Two windows:
      PAST     : reported earnings — tests date accuracy + actuals presence.
      UPCOMING : forward coverage — tests confirmed/scheduled date breadth.
  - Finnhub via the agent's own finnhub_client.fetch_earnings.
  - FMP via the stable bulk /earnings-calendar endpoint, chunked weekly.
  - Comparison is per (ticker) within the window. We report:
      * breadth  : which names each source has (Venn, by tier)
      * accuracy : date agreement among shared names (exact / ±1d / >1d)
      * timeliness: who has actuals the other lacks
      * phantoms : per source, tickers with >1 row mapping to the same
                   reporting quarter (the ICLR double-listing class)
  - For PAST-window Tier 1/2 date disagreements (>1d), EDGAR 8-K Item 2.02 is
    used as tiebreaker (filing date ~= release date; not perfect, flagged).

Run:
  FMP_API_KEY=... COVERAGE_MANAGER_PATH=... python scripts/compare_providers.py
"""
import os
import sys
import json
import time
import urllib.request
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from main import load_coverage
from storage import date_to_quarter
from finnhub_client import get_client, fetch_earnings
from edgar_client import find_earnings_release_filing

FMP_KEY = os.getenv("FMP_API_KEY")
if not FMP_KEY:
    print("FMP_API_KEY not set"); sys.exit(1)

TODAY = date.today()
PAST_FROM = (TODAY - timedelta(days=35)).isoformat()
PAST_TO = TODAY.isoformat()
UP_FROM = (TODAY + timedelta(days=1)).isoformat()
UP_TO = (TODAY + timedelta(days=45)).isoformat()


def fmp_calendar(frm: str, to: str) -> list[dict]:
    """Bulk FMP earnings calendar, chunked weekly to dodge any range cap."""
    out = []
    start = date.fromisoformat(frm)
    end = date.fromisoformat(to)
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=6), end)
        url = (
            f"https://financialmodelingprep.com/stable/earnings-calendar"
            f"?from={cur.isoformat()}&to={chunk_end.isoformat()}&apikey={FMP_KEY}"
        )
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
            if isinstance(data, list):
                out.extend(data)
            else:
                print(f"  FMP chunk {cur}..{chunk_end} non-list: {str(data)[:120]}")
        except Exception as exc:
            print(f"  FMP chunk {cur}..{chunk_end} error: {exc}")
        cur = chunk_end + timedelta(days=1)
        time.sleep(0.3)
    return out


def has_actual(eps, rev) -> bool:
    return eps is not None or rev is not None


def index_rows(rows, sym_key, date_key, eps_key, rev_key, universe):
    """ticker -> list of (date, has_actuals); only universe names."""
    by_ticker = defaultdict(list)
    for r in rows:
        sym = (r.get(sym_key) or "").upper()
        if sym not in universe:
            continue
        d = r.get(date_key)
        if not d:
            continue
        by_ticker[sym].append((d[:10], has_actual(r.get(eps_key), r.get(rev_key))))
    return by_ticker


def main():
    cov = load_coverage()
    tier = {t.ticker.upper(): t.tier for t in cov}
    universe = set(tier)
    print(f"Universe: {len(universe)} tickers "
          f"(T1={sum(1 for v in tier.values() if v==1)}, "
          f"T2={sum(1 for v in tier.values() if v==2)}, "
          f"T3={sum(1 for v in tier.values() if v==3)})")

    # ---- PAST window ----
    print(f"\n=== PAST window {PAST_FROM} .. {PAST_TO} (reported) ===")
    fh_client = get_client()
    fh_rows = fetch_earnings(fh_client, list(universe), PAST_FROM, PAST_TO)
    fmp_rows = fmp_calendar(PAST_FROM, PAST_TO)
    print(f"raw rows — Finnhub: {len(fh_rows)}  FMP: {len(fmp_rows)}")

    fh = index_rows(fh_rows, "symbol", "date", "epsActual", "revenueActual", universe)
    fmp = index_rows(fmp_rows, "symbol", "date", "epsActual", "revenueActual", universe)

    def tier_counts(by_ticker):
        c = defaultdict(int); a = defaultdict(int)
        for tk, evs in by_ticker.items():
            c[tier[tk]] += 1
            if any(act for _, act in evs):
                a[tier[tk]] += 1
        return c, a

    fhc, fha = tier_counts(fh)
    fmc, fma = tier_counts(fmp)
    print("\nNames with >=1 event in window (and with actuals), by tier:")
    for tt in (1, 2, 3):
        print(f"  T{tt}: Finnhub {fhc[tt]:>4} names / {fha[tt]:>4} w-actuals "
              f"| FMP {fmc[tt]:>4} names / {fma[tt]:>4} w-actuals")

    fh_set, fmp_set = set(fh), set(fmp)
    only_fmp = fmp_set - fh_set
    only_fh = fh_set - fmp_set
    both = fh_set & fmp_set
    print(f"\nCoverage Venn (names in window): both={len(both)} "
          f"FMP-only={len(only_fmp)} Finnhub-only={len(only_fh)}")
    for label, s in (("FMP-only", only_fmp), ("Finnhub-only", only_fh)):
        bytier = defaultdict(list)
        for tk in s:
            bytier[tier[tk]].append(tk)
        print(f"  {label}: " + ", ".join(
            f"T{tt}={len(bytier[tt])}" for tt in (1, 2, 3)))
        # show Tier 1/2 specifics (the ones that matter)
        t12 = sorted([tk for tk in s if tier[tk] <= 2])
        if t12:
            print(f"    T1/2 {label}: {', '.join(t12)}")

    # date agreement among shared names (use each ticker's most-recent in-window date)
    def latest_date(evs):
        return max(d for d, _ in evs)
    exact = pm1 = gt1 = 0
    disagreements = []  # (ticker, tier, fh_date, fmp_date)
    for tk in both:
        fd = latest_date(fh[tk]); md = latest_date(fmp[tk])
        delta = abs((date.fromisoformat(fd) - date.fromisoformat(md)).days)
        if delta == 0:
            exact += 1
        elif delta == 1:
            pm1 += 1
        else:
            gt1 += 1
            disagreements.append((tk, tier[tk], fd, md))
    print(f"\nDate agreement among {len(both)} shared names: "
          f"exact={exact}  within-1d={pm1}  >1d={gt1}")

    # actuals timeliness among shared names
    fmp_has_fh_not = []
    fh_has_fmp_not = []
    for tk in both:
        fh_act = any(a for _, a in fh[tk])
        fmp_act = any(a for _, a in fmp[tk])
        if fmp_act and not fh_act:
            fmp_has_fh_not.append(tk)
        if fh_act and not fmp_act:
            fh_has_fmp_not.append(tk)
    print(f"\nActuals present in one but not the other (shared names):")
    print(f"  FMP has actuals, Finnhub doesn't: {len(fmp_has_fh_not)} "
          f"(T1/2: {sorted(t for t in fmp_has_fh_not if tier[t]<=2)})")
    print(f"  Finnhub has actuals, FMP doesn't: {len(fh_has_fmp_not)} "
          f"(T1/2: {sorted(t for t in fh_has_fmp_not if tier[t]<=2)})")

    # phantom/duplicate class: >1 distinct date mapping to same reporting quarter
    def phantom_tickers(by_ticker):
        out = []
        for tk, evs in by_ticker.items():
            byq = defaultdict(set)
            for d, _ in evs:
                byq[date_to_quarter(d)].add(d)
            if any(len(v) > 1 for v in byq.values()):
                out.append(tk)
        return out
    fh_ph = phantom_tickers(fh)
    fmp_ph = phantom_tickers(fmp)
    print(f"\nPhantom/double-listing (>1 date, same reporting quarter, in window):")
    print(f"  Finnhub: {len(fh_ph)} tickers  (T1/2: {sorted(t for t in fh_ph if tier[t]<=2)})")
    print(f"  FMP:     {len(fmp_ph)} tickers  (T1/2: {sorted(t for t in fmp_ph if tier[t]<=2)})")

    # EDGAR adjudication of Tier 1/2 >1d disagreements
    t12_dis = [d for d in disagreements if d[1] <= 2]
    if t12_dis:
        print(f"\nEDGAR adjudication of {len(t12_dis)} Tier-1/2 date disagreement(s):")
        for tk, tt, fd, md in t12_dis:
            lo = min(date.fromisoformat(fd), date.fromisoformat(md)) - timedelta(days=3)
            hi = min(max(date.fromisoformat(fd), date.fromisoformat(md)) + timedelta(days=1), TODAY)
            verdict = "?"
            try:
                f = find_earnings_release_filing(tk, lo, hi)
                if f:
                    ed = f.filing_date
                    if ed == fd: verdict = f"EDGAR={ed} -> Finnhub"
                    elif ed == md: verdict = f"EDGAR={ed} -> FMP"
                    else: verdict = f"EDGAR={ed} -> neither"
                else:
                    verdict = "no 8-K 2.02 found"
            except Exception as exc:
                verdict = f"edgar err {exc}"
            print(f"  T{tt} {tk}: Finnhub {fd} vs FMP {md}  =>  {verdict}")

    # ---- UPCOMING window (forward coverage) ----
    print(f"\n=== UPCOMING window {UP_FROM} .. {UP_TO} (forward coverage) ===")
    fh_up = index_rows(fetch_earnings(fh_client, list(universe), UP_FROM, UP_TO),
                       "symbol", "date", "epsActual", "revenueActual", universe)
    fmp_up = index_rows(fmp_calendar(UP_FROM, UP_TO),
                        "symbol", "date", "epsActual", "revenueActual", universe)
    fhc_u, _ = tier_counts(fh_up)
    fmc_u, _ = tier_counts(fmp_up)
    print("Names with a scheduled event ahead, by tier:")
    for tt in (1, 2, 3):
        print(f"  T{tt}: Finnhub {fhc_u[tt]:>4} | FMP {fmc_u[tt]:>4}")
    up_only_fmp = sorted(t for t in (set(fmp_up)-set(fh_up)) if tier[t] <= 2)
    up_only_fh = sorted(t for t in (set(fh_up)-set(fmp_up)) if tier[t] <= 2)
    print(f"  T1/2 upcoming only-FMP: {up_only_fmp}")
    print(f"  T1/2 upcoming only-Finnhub: {up_only_fh}")


if __name__ == "__main__":
    main()
