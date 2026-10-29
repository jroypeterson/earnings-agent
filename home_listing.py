"""Board #712 follow-up: dates for foreign names refused at ingest, asked by their HOME listing.

`coverage.find_symbol_collisions` drops CSL Ltd, UCB SA, Medartis and Medacta because every US
vendor answers their bare symbol for Carlisle, United Community Banks, Medifast and Corvex. That
left them with no date anywhere. JP 2026-10-29: "Do A" -- ask by the home-exchange symbol.

Measured 2026-10-29: Finnhub's free tier refuses CSL.AX / UCB.BR / MED.SW / MOVE.SW (403) and our
FMP plan refuses them too (402). Yahoo Finance answers all four (next: 2027-02-09, 2027-02-25,
2027-03-16, 2027-03-10), so Yahoo it is -- one source, never company-confirmed, and said so.

⚠ These names are deliberately NOT written to the events DB. Lanes there act on a row by its bare
ticker -- results checks, consensus previews, the stock-move z -- and would fetch Carlisle again.
So this lane keeps its own state (`kv_store` key `home_listing:<TICKER>`), its own calendar event,
its own TickTick task, and publishes `exports/home_listing_dates.json` for earnings_review.
Only the DATE is used: Yahoo's estimates are in the home currency and are never merged.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger("earnings_agent")

EXPORT = Path(__file__).parent / "exports" / "home_listing_dates.json"
KV_PREFIX = "home_listing:"

# Coverage Manager `Exchange` -> Yahoo suffix. Only exchanges CM uses; an unmapped one is logged.
YAHOO_SUFFIX = {
    "ASX": ".AX", "Euronext Brussels": ".BR", "SIX": ".SW", "LSE": ".L", "XETRA": ".DE",
    "Frankfurt": ".F", "Euronext Paris": ".PA", "OMX Stockholm": ".ST", "OMX Copenhagen": ".CO",
    "OMX Helsinki": ".HE", "Oslo Bors": ".OL", "Borsa Italiana": ".MI", "BME Madrid": ".MC",
    "TSE": ".T", "HKEX": ".HK", "TSX": ".TO", "NZX": ".NZ", "KRX": ".KS", "KOSDAQ": ".KQ",
    "NSE": ".NS", "TWSE": ".TW", "JSE": ".JO", "B3": ".SA", "WSE": ".WA", "SSE": ".SS",
    "SZSE": ".SZ", "Tadawul": ".SR",
}


def home_symbol(ticker: str, exchange: str) -> str | None:
    if "." in ticker:
        return ticker
    suffix = YAHOO_SUFFIX.get((exchange or "").strip())
    return f"{ticker}{suffix}" if suffix else None


def _default_next_date(symbol: str, today: date) -> str | None:
    """The first future report date Yahoo lists for `symbol`, or None."""
    import yfinance as yf
    t = yf.Ticker(symbol)
    dates = []
    try:
        ed = t.get_earnings_dates(limit=8)
        if ed is not None:
            dates += [d.date() for d in ed.index]
    except Exception as exc:  # noqa: BLE001 - Yahoo is best-effort
        logger.info(f"home listing: {symbol} earnings dates unavailable: {exc}")
    try:
        cal = t.calendar or {}
        dates += [d for d in (cal.get("Earnings Date") or []) if isinstance(d, date)]
    except Exception as exc:  # noqa: BLE001
        logger.info(f"home listing: {symbol} calendar unavailable: {exc}")
    future = sorted(d for d in dates if d >= today)
    return future[0].isoformat() if future else None


def lookup(refused: dict[str, dict], today: date, next_date=None) -> dict[str, dict]:
    """{ticker: {company, home_symbol, next_date|None, note}} for each refused ticker.

    `refused` is `coverage.LAST_REFUSED_INFO`: {ticker: {company, exchange, tier, ...}}.
    """
    next_date = next_date or _default_next_date
    out = {}
    for ticker, info in sorted(refused.items()):
        sym = home_symbol(ticker, info.get("exchange", ""))
        rec = {"company": info.get("company", ""), "home_symbol": sym, "next_date": None,
               "tier": info.get("tier", 3), "sector": info.get("sector", ""),
               "position": info.get("position", ""), "reason": info.get("reason", "")}
        if not sym:
            rec["note"] = f"no Yahoo suffix for exchange {info.get('exchange')!r}"
            logger.warning(f"home listing: {ticker}: {rec['note']}")
        else:
            try:
                rec["next_date"] = next_date(sym, today)
            except Exception as exc:  # noqa: BLE001 - one name must not stop the rest
                rec["note"] = f"Yahoo lookup failed: {exc}"
                logger.warning(f"home listing: {ticker} ({sym}): {rec['note']}")
            if rec["next_date"] is None and "note" not in rec:
                rec["note"] = "Yahoo lists no future date"
        out[ticker] = rec
    return out


def write_export(found: dict[str, dict], prior: dict | None = None) -> dict:
    """Publish dates for earnings_review. A failed lookup keeps the last known date, marked stale."""
    prior = prior or {}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    names = {}
    for t, rec in found.items():
        old = (prior.get("names") or {}).get(t, {})
        if rec["next_date"]:
            names[t] = {"company": rec["company"], "home_symbol": rec["home_symbol"],
                        "next_date": rec["next_date"], "checked": now}
        elif old.get("next_date"):
            names[t] = {**old, "stale_since": old.get("stale_since") or now, "note": rec.get("note", "")}
        else:
            names[t] = {"company": rec["company"], "home_symbol": rec["home_symbol"],
                        "next_date": None, "checked": now, "note": rec.get("note", "")}
    doc = {"source": "Yahoo Finance, by home-exchange symbol (never company-confirmed)",
           "why": "board #712: SEC gives these bare symbols to US companies, so US vendors are refused",
           "names": names}
    EXPORT.parent.mkdir(parents=True, exist_ok=True)
    EXPORT.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return doc


def _task_content(ticker: str, rec: dict) -> str:
    return (f"{rec['company']} ({rec['home_symbol']}). Next results per Yahoo Finance: "
            f"{rec['next_date']} -- NOT company-confirmed.\n"
            f"US earnings vendors are refused for this name: {rec['reason']} (board #712).")


def sync(conn, found: dict[str, dict], *, cal_service=None, calendar_id: str = "",
         ticktick_token: str | None = None, dry_run: bool = False) -> dict:
    """Converge one calendar event and one TickTick task per (ticker, next date). Returns counts."""
    from calendar_sync import create_calendar_event, delete_calendar_event
    from storage import kv_get, kv_set
    stats = {"calendar_created": 0, "calendar_moved": 0, "tasks_created": 0, "errors": 0}
    for ticker, rec in found.items():
        d = rec["next_date"]
        # Tier 3 gets no calendar event or TickTick task anywhere in this agent; same here.
        if not d or rec["tier"] > 2:
            continue
        key = KV_PREFIX + ticker
        state = json.loads(kv_get(conn, key) or "{}")
        if dry_run:
            if state.get("date") != d:
                logger.info(f"[dry-run] home listing: {ticker} would be dated {d} ({rec['home_symbol']})")
            continue
        if cal_service is not None and (state.get("date") != d or not state.get("gcal_id")):
            try:
                new_id = create_calendar_event(cal_service, calendar_id, ticker, d, None,
                                               tier=rec["tier"],
                                               source_fingerprint=f"home:{rec['home_symbol']}:{d}")
                if state.get("gcal_id") and state.get("date", "") >= date.today().isoformat():
                    delete_calendar_event(cal_service, calendar_id, state["gcal_id"])
                    stats["calendar_moved"] += 1
                else:
                    stats["calendar_created"] += 1
                state["gcal_id"] = new_id
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                logger.warning(f"home listing: {ticker} calendar event failed: {exc}")
        if ticktick_token:
            import ticktick as tt
            list_name = tt._quarter_list_name(d, rec["tier"], position=rec["position"])
            if state.get("task_list") != list_name:
                try:
                    list_id = tt.find_or_create_list(ticktick_token, list_name)
                    title = tt.build_task_title(ticker, d, None, confirmed=False)
                    tags = [x for x in (tt.sector_tag(rec["sector"]), tt.position_tag(rec["position"])) if x]
                    # Undated: JP 2026-07-24, no date in TickTick until the company confirms it.
                    task_id = tt.create_task(ticktick_token, list_id, title, _task_content(ticker, rec),
                                             None, tags=tags or None) if list_id else None
                    if task_id:
                        state.update(task_id=task_id, task_list=list_name)
                        stats["tasks_created"] += 1
                    else:
                        stats["errors"] += 1
                except tt.TickTickTokenExpired:
                    raise
                except Exception as exc:  # noqa: BLE001
                    stats["errors"] += 1
                    logger.warning(f"home listing: {ticker} TickTick task failed: {exc}")
        state.update(date=d, home_symbol=rec["home_symbol"])
        kv_set(conn, key, json.dumps(state, sort_keys=True))
    return stats
