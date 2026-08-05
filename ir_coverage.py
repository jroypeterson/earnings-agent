"""Which covered companies is JP actually on the IR distribution list for? (#246)

    python ir_coverage.py                  # report to stdout
    python ir_coverage.py --days 365       # window
    python ir_coverage.py --out FILE.md    # write the readable artifact

JP created `#ir-emails` to track which IR emails he receives directly, measured against
his core coverage. This is the **measurement** half of the older auto-subscribe idea
(#131), which is CAPTCHA-blocked — and measurement is the cheap half, because the
readonly Gmail token already exists and the join target is Coverage Manager's own list.

THE JOIN IS DERIVED, NOT GUESSED. Signups use the `floridabusinessman+IR@gmail.com`
alias, so every IR list lands in one bucket. Sender domain is matched against CM's
`Website` column — 245 of 256 core names carry one — rather than against company names,
because a name match on "Ardent" or "Cochlear" is exactly the ticker-is-not-an-identity
mistake this fleet keeps paying for. A sender that matches nothing is REPORTED, never
silently dropped: it is usually a name outside core coverage, and that is information.

WHAT IT CANNOT TELL YOU. Receiving mail proves you are ON a list; NOT receiving it does
not prove you are off one — a company may simply not have sent anything in the window.
So "no mail in window" is reported as its own bucket rather than folded into "not
subscribed", because acting on the difference is the entire point.
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UNIVERSE = ROOT / "Coverage Manager" / "exports" / "universe.csv"
IR_ALIAS = "floridabusinessman+IR@gmail.com"

# Sender domains that carry IR mail for MANY issuers -- a hit tells you nothing about
# which company you are subscribed to, so they are excluded from the match and counted
# separately. Without this, one Q4 filing agent would "cover" 40 names at once.
BULK_SENDERS = {
    "q4inc.com", "q4websystems.com", "notify.q4inc.com", "globenewswire.com",
    "prnewswire.com", "businesswire.com", "issuerdirect.com", "nasdaq.com",
    "investorroom.com", "sharpspring.com", "mailchimpapp.net", "list-manage.com",
    # Added after the first live run: 223 of 259 messages came from IR-PLATFORM
    # domains, not issuer domains. Domain alone attributes none of them.
    "gcs-web.com", "equisolve.com", "em.equisolve.com", "investis.com",
    "investisdigital.com",
}

# ...but a platform message still names its issuer, in the From DISPLAY NAME:
#   "Eli Lilly and Company <no-reply@notification.gcs-web.com>"
#   "The Ensign Group, Inc." <no-reply@q4inc.com>
# So the display name is the second join key. It is matched by EXACT equality after
# stripping corporate suffixes -- never by containment.
#
# Containment is precisely the mistake this fleet has already paid for: a subset
# name-check let `Siemens AG` match `Siemens Healthineers`, screening the parent
# conglomerate as the subsidiary. Under exact-after-normalisation, "siemens" and
# "siemenshealthineers" simply do not match, which is the correct answer.
_SUFFIXES = (
    "incorporated", "corporation", "company", "holdings", "holding", "group",
    "limited", "plc", "inc", "corp", "co", "ltd", "ag", "sa", "nv", "se", "ab",
    "asa", "spa", "kgaa", "llc", "lp", "the",
)


# Mail-PRODUCT phrases an IR platform appends to the issuer's name. These are not part
# of any company's legal name, so stripping them is safe in a way that containment
# matching never is. Found live: `BSX` arrives as "Boston Scientific Corporation
# Alerting Service", which normalises to `bostonscientificalertingservice` and matches
# nothing, silently losing a covered name from the tally.
_MAIL_PRODUCT = re.compile(
    r"\b(alerting service|investor relations|investor alerts?|email alerts?|"
    r"news alerts?|ir alerts?|alerts?|newsroom|news|investor centre|investor center)\b",
    re.I)


def norm_name(s: str) -> str:
    """`The Ensign Group, Inc.` -> `ensign`. Aggressive, and applied to BOTH sides."""
    s = _MAIL_PRODUCT.sub(" ", s or "")
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    words = [w for w in s.split() if w]
    # Strip suffix words from either end, repeatedly.
    changed = True
    while changed and words:
        changed = False
        if words[0] in _SUFFIXES:
            words.pop(0)
            changed = True
        if words and words[-1] in _SUFFIXES:
            words.pop()
            changed = True
    return "".join(words)


def display_name(raw_from: str) -> str:
    """The human part of a From header, unquoted."""
    m = re.match(r'\s*"?([^"<]+?)"?\s*<', raw_from or "")
    return (m.group(1) if m else "").strip()


def _domain(addr: str) -> str:
    m = re.search(r"@([A-Za-z0-9.\-]+)", addr or "")
    if not m:
        return ""
    d = m.group(1).lower().strip(".")
    # Strip common mail subdomains so `email.abbvie.com` matches `abbvie.com`.
    for pre in ("email.", "mail.", "e.", "news.", "ir.", "investor.", "notify.",
                "info.", "updates.", "links."):
        if d.startswith(pre):
            d = d[len(pre):]
    return d


def _root(domain: str) -> str:
    """`ir.abbvie.co.uk` -> `abbvie.co.uk`; good enough to join on, never to trust alone."""
    parts = domain.split(".")
    if len(parts) <= 2:
        return domain
    if parts[-2] in {"co", "com", "org", "net", "gov"} and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _portfolio_tickers() -> set[str]:
    """Held names, from CM's position export. Empty set on any failure -- never raises."""
    p = ROOT / "Coverage Manager" / "exports" / "portfolio.json"
    if not p.exists():
        return set()
    try:
        return set(json.loads(p.read_text(encoding="utf-8")).keys())
    except (ValueError, OSError):
        return set()


def load_universe() -> tuple[dict, dict, list[dict]]:
    """({root domain: row}, {normalised company name: row}, audited rows).

    Audited = Core=Y **UNION Portfolio** (JP 2026-08-05). Core alone was the original
    scope and it silently omitted 11 held names -- ADSK, BE, BRO, CPRT, FI, KRC, LLY, Q,
    ROIV, SPCX, ULS -- none of which carry Core=Y. The audit exists to decide where to
    sign up, and "I own it" is the strongest possible reason to be on a distribution
    list, so a held name must never be invisible to it. Each row is tagged `_scope`
    (portfolio > core) for the two output lists.
    """
    by_domain, by_name, audited = {}, {}, []
    port = _portfolio_tickers()
    with open(UNIVERSE, encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            site = (r.get("Website") or "").strip()
            if site:
                d = _root(_domain("@" + re.sub(r"^https?://(www\.)?", "", site).split("/")[0]))
                if d:
                    by_domain.setdefault(d, r)
            nn = norm_name(r.get("Company Name", ""))
            if nn and nn not in by_name:
                by_name[nn] = r
            tk = (r.get("Ticker") or "").strip()
            is_core = (r.get("Core") or "").strip().upper() in ("Y", "YES", "TRUE", "1")
            if is_core or tk in port:
                r["_scope"] = "portfolio" if tk in port else "core"
                audited.append(r)
    return by_domain, by_name, audited


def fetch_senders(days: int, max_results: int = 500) -> tuple[list[tuple[str, str]], str]:
    """[(sender, iso date)] for mail to the IR alias. Returns ([], reason) on failure."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from gmail_client import get_gmail_service, list_message_ids
    except Exception as e:  # noqa: BLE001
        return [], f"gmail client unavailable: {type(e).__name__}: {e}"
    try:
        svc = get_gmail_service()
        q = f"to:{IR_ALIAS} newer_than:{days}d"
        ids = list_message_ids(svc, q, max_results=max_results)
        out = []
        for mid in ids:
            msg = svc.users().messages().get(
                userId="me", id=mid, format="metadata",
                metadataHeaders=["From", "Date"]).execute()
            hdrs = {h["name"]: h["value"]
                    for h in msg.get("payload", {}).get("headers", [])}
            ts = int(msg.get("internalDate", "0")) / 1000
            out.append((hdrs.get("From", ""),
                        datetime.fromtimestamp(ts, timezone.utc).date().isoformat()))
        return out, ""
    except Exception as e:  # noqa: BLE001
        # A failure here must be reported, never rendered as "you are on no lists".
        return [], f"gmail query failed: {type(e).__name__}: {e}"


def build(days: int = 365) -> dict:
    by_domain, by_name, core = load_universe()
    senders, err = fetch_senders(days)

    hits = collections.defaultdict(list)      # ticker -> [dates]
    matched_by_name = collections.Counter()   # attributed via display name, not domain
    bulk = collections.Counter()
    unmatched = collections.Counter()
    for frm, d in senders:
        dom = _root(_domain(frm))
        if not dom:
            continue
        row = None if dom in BULK_SENDERS else by_domain.get(dom)
        if row is None:
            # Platform sender (or an unknown domain): fall back to the DISPLAY NAME,
            # matched by exact equality after suffix-stripping. Never containment.
            row = by_name.get(norm_name(display_name(frm)))
            if row is not None:
                matched_by_name[row["Ticker"]] += 1
        if row is not None:
            hits[row["Ticker"]].append(d)
        elif dom in BULK_SENDERS:
            bulk[dom] += 1
        else:
            unmatched[dom] += 1

    core_tickers = [r["Ticker"] for r in core]
    covered = sorted(t for t in core_tickers if t in hits)
    missing = sorted(t for t in core_tickers if t not in hits)
    non_core = sorted(t for t in hits if t not in set(core_tickers))
    return {
        "generated": date.today().isoformat(),
        "window_days": days,
        "error": err,
        "messages": len(senders),
        "core_total": len(core_tickers),
        "covered": covered,
        "missing": missing,
        "non_core_hits": non_core,
        "hits": {k: sorted(v) for k, v in hits.items()},
        "matched_by_name": matched_by_name.most_common(),
        "bulk": bulk.most_common(),
        "unmatched": unmatched.most_common(30),
        "core_rows": {r["Ticker"]: r.get("Company Name", "") for r in core},
        "scopes": {r["Ticker"]: r.get("_scope", "core") for r in core},
    }


def render(j: dict) -> str:
    pct = (len(j["covered"]) / j["core_total"] * 100) if j["core_total"] else 0.0
    L = [f"# IR distribution-list coverage — {j['generated']}", "",
         f"_Mail to `{IR_ALIAS}` over the last {j['window_days']} days, "
         f"joined to Coverage Manager's core list on the `Website` domain._", ""]
    if j["error"]:
        L += [f"> **INCONCLUSIVE — {j['error']}**", ">",
              "> No mail could be read, so nothing below distinguishes "
              "'not subscribed' from 'could not check'.", ""]
    L += [f"- Core names: **{j['core_total']}**",
          f"- Receiving IR mail: **{len(j['covered'])}** ({pct:.0f}%)",
          f"- No mail in window: **{len(j['missing'])}**",
          f"- Messages scanned: **{j['messages']}**", "",
          "**Receiving IR mail does not mean subscribed-and-current, and silence does "
          "not mean unsubscribed** — a company may simply not have sent anything in "
          "the window. The two are separated below rather than merged.", ""]
    L += ["## Receiving IR mail", ""]
    if j["covered"]:
        L += ["| Ticker | Company | Messages | Most recent |", "|---|---|---:|---|"]
        for t in j["covered"]:
            ds = j["hits"][t]
            L.append(f"| `{t}` | {j['core_rows'].get(t,'')} | {len(ds)} | {ds[-1]} |")
    else:
        L.append("_None matched._")
    L += ["", "## No mail in window (signup candidates)", "",
          "Sorted alphabetically; this is the working list for name-by-name signup.", ""]
    L.append(", ".join(f"`{t}`" for t in j["missing"]) or "_None._")
    if j["non_core_hits"]:
        L += ["", "## Receiving mail, but NOT core coverage", "",
              ", ".join(f"`{t}`" for t in j["non_core_hits"])]
    if j.get("matched_by_name"):
        L += ["", "## Attributed via the sender DISPLAY NAME", "",
              "Most IR mail arrives from an IR-platform domain (Q4, gcs-web, Equisolve, "
              "Investis) which names no issuer. These were matched on the From display "
              "name by EXACT equality after stripping corporate suffixes -- never by "
              "containment, which is what let `Siemens AG` match `Siemens Healthineers` "
              "elsewhere in this fleet.", "",
              "| Ticker | Messages |", "|---|---:|"]
        L += [f"| `{t}` | {n} |" for t, n in j["matched_by_name"]]
    if j["bulk"]:
        L += ["", "## Bulk senders that named no issuer (excluded)", "",
              "A hit here says nothing about WHICH issuer you are subscribed to, so "
              "they are counted rather than matched.", "",
              "| Sender domain | Messages |", "|---|---:|"]
        L += [f"| {d} | {n} |" for d, n in j["bulk"]]
    if j["unmatched"]:
        L += ["", "## Senders matching no covered name", "",
              "Reported rather than dropped — usually a name outside core coverage, "
              "which is itself information.", "",
              "| Sender domain | Messages |", "|---|---:|"]
        L += [f"| {d} | {n} |" for d, n in j["unmatched"]]
    return "\n".join(L) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ir_coverage")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError, ValueError):
        pass

    j = build(days=args.days)
    if j["error"]:
        print(f"INCONCLUSIVE: {j['error']}", file=sys.stderr)
    print(f"core {j['core_total']} · receiving IR mail {len(j['covered'])} · "
          f"no mail in window {len(j['missing'])} · messages {j['messages']} · "
          f"bulk senders {len(j['bulk'])} · unmatched domains {len(j['unmatched'])}")
    for t in j["covered"][:40]:
        print(f"   {t:<8} {len(j['hits'][t]):>3} msg  last {j['hits'][t][-1]}")

    out = Path(args.out) if args.out else (
        Path(__file__).resolve().parent / "readable" /
        f"ir_coverage_{date.today().isoformat()}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(j), encoding="utf-8")
    print(f"\nwrote {out}")
    return 1 if j["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
