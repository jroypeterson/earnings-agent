"""
FMP earnings-calendar client + Finnhub/FMP merge.

Why this exists: a universe-wide comparison (scripts/compare_providers.py) showed
FMP — which we already pay for via Coverage Manager's Starter plan — covers far
more of our names per window than Finnhub's free tier (≈+14 Tier-1 / +70 Tier-2
reporters, almost all WITH actuals) and is never behind on actuals (it had
actuals for 30 names Finnhub lacked, incl. FIVE; the reverse was 0). Date
accuracy between the two is a wash, and FMP actually carries MORE phantom
double-listings, so the design is deliberately a *merge*, not a replacement:

  - Finnhub stays the date authority on names BOTH sources have (its date is
    what the calendar is anchored to; date conflicts are arbitrated by the
    existing yfinance/EDGAR cross-check, not here) — this sidesteps FMP's
    occasional date errors on shared names.
  - FMP fills the gaps: names Finnhub never lists (breadth) and actuals
    Finnhub hasn't ingested yet (timeliness).
  - The same-quarter phantom guard downstream (storage/main) still applies, so
    FMP's extra phantoms can't reintroduce the re-post bug.

Output is normalized to Finnhub's event shape so the rest of the agent is
unchanged: dicts with keys symbol, date, hour, epsEstimate, epsActual,
revenueEstimate, revenueActual (+ a 'source' tag for observability).
"""
import json
import logging
import time
import urllib.request
import urllib.error
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from config import FMP_API_KEY
from storage import date_to_quarter

logger = logging.getLogger(__name__)


@dataclass
class FMPFetch:
    """Result of a chunked FMP fetch, carrying partial-failure counts so the
    caller can alarm on silent coverage loss (some chunks failed but the run
    still looks healthy)."""
    events: list
    failed_chunks: int
    total_chunks: int

_FMP_BASE = "https://financialmodelingprep.com/stable/earnings-calendar"
_CHUNK_DAYS = 7          # weekly chunks dodge any server-side range cap
_CHUNK_SLEEP = 0.25


def _normalize(row: dict) -> dict | None:
    """FMP bulk row -> Finnhub-shaped event dict (or None if unusable)."""
    sym = (row.get("symbol") or "").upper()
    d = row.get("date")
    if not sym or not d:
        return None
    return {
        "symbol": sym,
        "date": d[:10],
        "hour": None,  # FMP bulk carries no BMO/AMC; yfinance fallback fills it
        "epsEstimate": row.get("epsEstimated"),
        "epsActual": row.get("epsActual"),
        "revenueEstimate": row.get("revenueEstimated"),
        "revenueActual": row.get("revenueActual"),
        "source": "fmp",
    }


def fetch_fmp_earnings(
    tickers, from_iso: str, to_iso: str, *, timeout: int = 30
) -> FMPFetch:
    """Fetch the FMP earnings calendar over [from_iso, to_iso], filtered to
    `tickers`, normalized to Finnhub's event shape. Chunked weekly.

    Returns an FMPFetch carrying the events plus failed/total chunk counts so a
    PARTIAL outage (some chunks failed) is observable rather than silently
    shrinking the merge. Raises only on a hard/auth failure (401/403, or an
    error payload), which the caller treats as a full FMP outage and degrades
    to Finnhub-only.
    """
    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY not configured")

    universe = {t.upper() for t in tickers}
    out: list[dict] = []
    start = date.fromisoformat(from_iso)
    end = date.fromisoformat(to_iso)
    cur = start
    total_chunks = 0
    failed_chunks = 0
    while cur <= end:
        chunk_end = min(cur + timedelta(days=_CHUNK_DAYS - 1), end)
        total_chunks += 1
        url = (
            f"{_FMP_BASE}?from={cur.isoformat()}&to={chunk_end.isoformat()}"
            f"&apikey={FMP_API_KEY}"
        )
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                data = json.load(resp)
        except urllib.error.HTTPError as exc:
            # 401/403 = auth/plan problem: hard-fail so the caller degrades
            # loudly rather than silently dropping FMP coverage every run.
            if exc.code in (401, 403):
                raise RuntimeError(f"FMP auth/plan error {exc.code}: {exc.reason}")
            logger.warning(f"FMP chunk {cur}..{chunk_end} HTTP {exc.code}; skipping")
            failed_chunks += 1
            cur = chunk_end + timedelta(days=1)
            continue
        except Exception as exc:
            logger.warning(f"FMP chunk {cur}..{chunk_end} error: {exc}; skipping")
            failed_chunks += 1
            cur = chunk_end + timedelta(days=1)
            continue

        if isinstance(data, dict):
            # FMP returns an {"Error Message": ...} dict on plan/endpoint issues.
            raise RuntimeError(f"FMP returned error payload: {str(data)[:160]}")
        for row in data:
            if (row.get("symbol") or "").upper() not in universe:
                continue
            ev = _normalize(row)
            if ev:
                out.append(ev)
        cur = chunk_end + timedelta(days=1)
        time.sleep(_CHUNK_SLEEP)

    level = logger.warning if failed_chunks else logger.info
    level(
        f"FMP earnings: {len(out)} universe events over "
        f"{total_chunks - failed_chunks}/{total_chunks} chunk(s)"
        + (f" ({failed_chunks} FAILED)" if failed_chunks else "")
    )
    return FMPFetch(events=out, failed_chunks=failed_chunks, total_chunks=total_chunks)


def _has_actuals(ev: dict) -> bool:
    return ev.get("epsActual") is not None or ev.get("revenueActual") is not None


def _fill_from(primary: dict, donor: dict) -> dict:
    """Return primary with missing est/actual fields — and the earnings hour —
    filled from donor.

    Preserving `hour` matters: FMP rows carry hour=None, so when an FMP actuals
    row WINS over a Finnhub row that knew the session (e.g. Finnhub had `amc`
    but no actuals yet), dropping the hour would mislead
    `fetch_post_earnings_move` (which treats amc vs bmo/unknown differently for
    the move window + deferral). So inherit the donor's hour when the chosen
    row lacks one.
    """
    merged = dict(primary)
    for k in ("epsEstimate", "epsActual", "revenueEstimate", "revenueActual"):
        if merged.get(k) is None and donor.get(k) is not None:
            merged[k] = donor[k]
    if not merged.get("hour") and donor.get("hour"):
        merged["hour"] = donor["hour"]
    # Note both contributors for observability.
    srcs = {primary.get("source", "finnhub"), donor.get("source", "finnhub")}
    merged["source"] = "+".join(sorted(srcs))
    return merged


def merge_earnings(fh_events: list[dict], fmp_events: list[dict]) -> list[dict]:
    """Merge Finnhub + FMP events into one Finnhub-shaped list.

    Policy, per (ticker, reporting-quarter):
      1. Finnhub has actuals          -> use Finnhub row, fill gaps from FMP.
      2. Finnhub has it but NO actuals
         and FMP HAS actuals          -> use FMP row (its date is the real
                                          report date; fixes the FIVE lag),
                                          fill estimates from Finnhub.
      3. Both upcoming (no actuals)    -> use Finnhub row (date authority),
                                          fill estimates from FMP.
      4. Only one source has it        -> take that one (breadth win, ~all FMP).

    Row-count behaviour, to be precise:
      - Single-source (ticker, quarter): ALL of that source's rows pass through
        intact (so e.g. a Finnhub same-quarter phantom + real row both reach
        the downstream same-quarter phantom guard, which collapses them).
      - Both-source (ticker, quarter): collapses to ONE representative row
        (picked per the policy above, gaps filled from the other source). The
        downstream guard still protects against phantoms that surface across
        runs / against persisted DB state, so cross-source extras don't need to
        pass through here.
    """
    for e in fh_events:
        e.setdefault("source", "finnhub")

    fh_by_key: dict[tuple, list[dict]] = defaultdict(list)
    fmp_by_key: dict[tuple, list[dict]] = defaultdict(list)
    for e in fh_events:
        fh_by_key[(e["symbol"], date_to_quarter(e["date"]))].append(e)
    for e in fmp_events:
        fmp_by_key[(e["symbol"], date_to_quarter(e["date"]))].append(e)

    out: list[dict] = []
    for key in set(fh_by_key) | set(fmp_by_key):
        fh_rows = fh_by_key.get(key, [])
        fmp_rows = fmp_by_key.get(key, [])

        if fh_rows and not fmp_rows:
            out.extend(fh_rows)
            continue
        if fmp_rows and not fh_rows:
            out.extend(fmp_rows)
            continue

        # Both sources have this (ticker, quarter). Pick representative rows.
        fh_act = next((r for r in fh_rows if _has_actuals(r)), None)
        fmp_act = next((r for r in fmp_rows if _has_actuals(r)), None)

        if fh_act:                                   # (1) Finnhub reported
            out.append(_fill_from(fh_act, fmp_act or fmp_rows[0]))
        elif fmp_act:                                # (2) only FMP reported
            out.append(_fill_from(fmp_act, fh_rows[0]))
        else:                                        # (3) both upcoming
            out.append(_fill_from(fh_rows[0], fmp_rows[0]))

    return out
