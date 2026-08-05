"""Resolve a VERIFIED investor-relations URL per company, for the #ir-emails audit.

    python ir_links.py --refresh          # probe every core+portfolio name, cache results
    python ir_links.py --show TICKER      # print one

WHY THIS IS PROBED AND NOT GUESSED. Coverage Manager carries a `Website` (the corporate
homepage), not an IR page. Constructing `investors.<domain>` and trusting it would put
dead links in JP's task list, which is worse than no link at all. So every candidate is
fetched and kept only if it returns 200 AND does not land back on the bare homepage.

That second condition is not theoretical. On 2026-08-05, bioMerieux's published IR
address `biomerieux-finance.com` returned 200 and silently redirected to the general
corporate site; Coloplast's search-reported IR URL 404'd outright. A status-code-only
check would have accepted the first and a no-check approach would have shipped both.

CURATED holds the 11 names Coverage Manager has no `Website` for at all -- all European
or Canadian issuers. They were web-searched and then verified by fetch on 2026-08-05.
When CM gains a Website for one of these, the curated entry still wins: it points at the
IR page, which is what the task needs, not the homepage.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import json
import re
import sys
from datetime import date
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
UNIVERSE = ROOT / "Coverage Manager" / "exports" / "universe.csv"
PORTFOLIO = ROOT / "Coverage Manager" / "exports" / "portfolio.json"
CACHE = Path(__file__).resolve().parent / "data" / "ir_links.json"

_UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/128.0 Safari/537.36")}

# Web-searched then fetch-verified 2026-08-05. CM has no Website for any of these.
CURATED: dict[str, str] = {
    "Ambush DC": "https://www.ambu.com/corporate-info/investors/investors",
    "BIM": "https://www.biomerieux.com/corp/en/investors.html",
    "COLOB DC": "https://www.coloplast.com/investor-relations/",
    "CSU": "https://www.csisoftware.com/investor-relations/",
    "CVSG.L": "https://www.cvsukltd.co.uk/investor-centre/",
    "DAE": "https://datwyler.com/investors/",
    "FRE": "https://www.fresenius.com/investors",
    "GETIB SS": "https://www.getinge.com/int/company/investors/overview/",
    "GXI": "https://www.gerresheimer.com/en/investors",
    "SOON": "https://www.sonova.com/en/investors",
    "YPSN": "https://www.ypsomed.com/en/investors",
}

# Sites behind bot protection: every path returns 403, INCLUDING nonsense ones. Tested
# 2026-08-05 -- `/investors` and `/zzz-not-a-page-9137` both 403 on all five, so a 403
# is not evidence the page exists and these CANNOT be fetch-verified from here. The URLs
# below come from web search and are recorded as UNVERIFIED rather than dropped: a link
# JP's browser opens fine is more useful than a blank, but the provenance has to be
# honest. Re-check by hand if one ever 404s in a browser.
CURATED_UNVERIFIED: dict[str, str] = {
    "TSLA": "https://ir.tesla.com/",
    "LONN CH": "https://www.lonza.com/investor-relations",
    "4543.T": "https://www.terumo.com/investors",
    "CTEC LN": "https://www.convatecgroup.com/investors/",
    "STMN.SW": "https://www.straumann.com/group/en/home/investors.html",
}

# The DIRECT signup page, which is what JP actually needs when doing these by hand --
# the IR landing page still costs a hunt on every name. `resources/investor-email-alerts`
# is the Q4/gcs-web convention, found live on EHC during the 2026-08-05 signup spike;
# the rest are the common variants. A hit must LOOK like a signup page (the final URL
# mentions email/alert) so a site that 200s everything cannot pass its homepage off as
# a subscribe form.
_SIGNUP_PATHS = (
    "resources/investor-email-alerts/default.aspx",
    "resources/email-alerts/default.aspx",
    "shareholder-services/email-alerts/default.aspx",
    "resources/email-alerts",
    "investors/email-alerts",
    "email-alerts",
    "investor-email-alerts",
)


def resolve_signup(ir_url: str) -> str | None:
    """Direct email-alerts signup page for an IR site, or None if not found."""
    if not ir_url:
        return None
    m = re.match(r"(https?://[^/]+)", ir_url)
    if not m:
        return None
    base = m.group(1)
    for path in _SIGNUP_PATHS:
        try:
            r = requests.get(f"{base}/{path}", headers=_UA, timeout=12, allow_redirects=True)
        except requests.RequestException:
            continue
        low = (r.url or "").lower()
        if r.status_code == 200 and ("email" in low or "alert" in low):
            return r.url
    return None


# Ordered by how often they are the real IR page; first verified hit wins.
_PATHS = ("/investors", "/investor-relations", "/en/investors", "/investors/overview",
          "/company/investors", "/en/investor-relations", "/investor")
_SUBS = ("investors.", "ir.", "investor.")


def _host(website: str) -> str:
    h = re.sub(r"^https?://", "", (website or "").strip()).split("/")[0].strip().lower()
    return h[4:] if h.startswith("www.") else h


def _candidates(website: str) -> list[str]:
    host = _host(website)
    if not host or "." not in host:
        return []
    out = [f"https://www.{host}{p}" for p in _PATHS]
    out += [f"https://{s}{host}" for s in _SUBS]
    return out


def _is_bare_home(url: str, host: str) -> bool:
    """Did we land back on the homepage? Then it is not an IR link."""
    tail = re.sub(r"^https?://(www\.)?", "", (url or "").lower()).rstrip("/")
    return tail in (host, f"{host}/en", f"{host}/index.html")


def verify(url: str, host: str) -> str | None:
    """Return the FINAL url if it is a live, non-homepage page; else None."""
    try:
        r = requests.get(url, headers=_UA, timeout=15, allow_redirects=True)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    if _is_bare_home(r.url, host):
        return None
    return r.url


def resolve(ticker: str, website: str) -> tuple[str | None, str]:
    """(url, how) -- how in {curated, probed, homepage, none}. Never raises."""
    if ticker in CURATED:
        return CURATED[ticker], "curated"
    if ticker in CURATED_UNVERIFIED:
        return CURATED_UNVERIFIED[ticker], "unverified"
    host = _host(website)
    if not host:
        return None, "none"
    for cand in _candidates(website):
        got = verify(cand, host)
        if got:
            return got, "probed"
    home = website if website.startswith("http") else f"https://{host}"
    try:
        r = requests.get(home, headers=_UA, timeout=15, allow_redirects=True)
        if r.status_code == 200:
            return r.url, "homepage"      # loud fallback: named as a homepage, not an IR page
    except requests.RequestException:
        pass
    return None, "none"


def audit_universe() -> dict[str, dict]:
    """{ticker: {name, website, scope}} for Core=Y UNION Portfolio."""
    rows: dict[str, dict] = {}
    core: set[str] = set()
    with open(UNIVERSE, encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            tk = (r.get("Ticker") or "").strip()
            if not tk:
                continue
            rows[tk] = {"name": (r.get("Company Name") or "").strip(),
                        "website": (r.get("Website") or "").strip()}
            if (r.get("Core") or "").strip().upper() in ("Y", "YES", "TRUE", "1"):
                core.add(tk)
    port: set[str] = set()
    if PORTFOLIO.exists():
        try:
            port = set(json.loads(PORTFOLIO.read_text(encoding="utf-8")).keys())
        except ValueError:
            port = set()
    out: dict[str, dict] = {}
    for tk in sorted(core | port):
        base = rows.get(tk, {"name": tk, "website": ""})
        out[tk] = {**base, "scope": "portfolio" if tk in port else "core"}
    return out


def load_cache() -> dict:
    if CACHE.exists():
        try:
            return json.loads(CACHE.read_text(encoding="utf-8"))
        except ValueError:
            return {}
    return {}


def save_cache(d: dict) -> None:
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(d, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(CACHE)


def refresh(only: set[str] | None = None, workers: int = 12) -> dict:
    uni = audit_universe()
    cache = load_cache()
    todo = [(tk, v) for tk, v in uni.items()
            if (only is None or tk in only) and tk not in cache.get("links", {})]
    links = dict(cache.get("links", {}))

    def one(item):
        tk, v = item
        url, how = resolve(tk, v["website"])
        return tk, {"url": url, "how": how, "name": v["name"], "scope": v["scope"]}

    if todo:
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for tk, rec in ex.map(one, todo):
                links[tk] = rec
    out = {"as_of": date.today().isoformat(), "links": links}
    save_cache(out)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--refresh", action="store_true", help="probe uncached names")
    ap.add_argument("--force", action="store_true", help="ignore the cache entirely")
    ap.add_argument("--show", metavar="TICKER")
    a = ap.parse_args(argv)

    if a.show:
        rec = load_cache().get("links", {}).get(a.show)
        print(json.dumps(rec, indent=2) if rec else f"{a.show}: not cached")
        return 0
    if a.force and CACHE.exists():
        CACHE.unlink()
    if a.refresh or a.force:
        out = refresh()
        links = out["links"]
        by = {}
        for r in links.values():
            by[r["how"]] = by.get(r["how"], 0) + 1
        print(f"{len(links)} names -> " + " · ".join(f"{k} {v}" for k, v in sorted(by.items())))
        bad = sorted(t for t, r in links.items() if not r["url"])
        if bad:
            print(f"NO LINK ({len(bad)}): {', '.join(bad)}")
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
