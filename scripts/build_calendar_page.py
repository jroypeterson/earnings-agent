"""Render the public earnings-calendar GitHub Pages site to docs/index.html.

JP 2026-07-19: "a GitHub page that has all of the confirmed earnings on it,
including past as well as the future."

Pattern: in-repo `docs/` Pages (same as the daily-reads project) rather than a
separate Pages repo (sector_chart_pack / hc_macro_policy style) -- `earnings-agent`
is already a PUBLIC repo and already commits `exports/*.json` back from CI, so the
page rides the exact same commit-back step in `daily_earnings_check.yml`.

Regenerates from `earnings_events.db` on every run. The DB is opened READ-ONLY
(`mode=ro`) -- it is a shared CI artifact and this is a pure consumer.

## What "confirmed" means here

The DB does not carry a single confirmed boolean, so this module derives one from
the three independent signals the agent actually maintains (see CLAUDE.md
"Date-correctness safeguards"). Highest-authority signal wins the badge:

  LOCKED     `date_locked = 1`  -- pinned by `--lock TICKER:DATE`, a Slack `lock`
                                  reply, or the EDGAR 8-K Item 2.02 / 6-K
                                  auto-correction (corroborated). Strongest: the
                                  date is frozen against provider drift.
  ANNOUNCED  `date_confirmed = 1` -- the COMPANY announced the date. Set from
                                  Finnhub's `hour` field being bmo/amc/dmh
                                  (empty hour = Finnhub projecting from cadence),
                                  or by --check-announcements (IR RSS),
                                  --check-ir-emails (Gmail IR alert), or the
                                  high-confidence web-search resolver.
  REPORTED   `reported = 1` or actual EPS/revenue present -- the event already
                                  happened; actuals are the evidence.
  ESTIMATED  none of the above -- a provider's cadence projection. NOT confirmed.

CONFIRMED = LOCKED | ANNOUNCED | REPORTED. ESTIMATED rows are rendered only
behind an explicitly-labelled, off-by-default toggle so an estimate can never be
mistaken for a confirmed date.

Usage (from the repo root):
    python scripts/build_calendar_page.py
    python scripts/build_calendar_page.py --db path.db --out docs/index.html
"""
from __future__ import annotations

import argparse
import html
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "earnings_events.db"
DEFAULT_OUT = REPO_ROOT / "docs" / "index.html"

# Status keys, most-authoritative first. Order matters: _status() returns the first hit.
STATUS_LOCKED = "locked"
STATUS_ANNOUNCED = "announced"
STATUS_REPORTED = "reported"
STATUS_ESTIMATED = "estimated"

STATUS_LABEL = {
    STATUS_LOCKED: "Locked",
    STATUS_ANNOUNCED: "Announced",
    STATUS_REPORTED: "Reported",
    STATUS_ESTIMATED: "Estimated",
}

SESSION_LABEL = {
    "bmo": "Before Market Open (BMO)",
    "amc": "After Market Close (AMC)",
    "dmh": "During Market Hours (DMH)",
}

TIER_LABEL = {
    1: "Tier 1 (core watchlist)",
    2: "Tier 2 (Healthcare Services & MedTech)",
    3: "Tier 3 (wider universe)",
}


class Row:
    __slots__ = ("ticker", "company", "date", "hour", "tier", "quarter", "status",
                 "eps_est", "eps_act", "rev_est", "rev_act", "url")

    def __init__(self, ticker, company, date, hour, tier, quarter, status,
                 eps_est, eps_act, rev_est, rev_act, url):
        self.ticker = ticker
        self.company = company
        self.date = date
        self.hour = hour
        self.tier = tier
        self.quarter = quarter
        self.status = status
        self.eps_est = eps_est
        self.eps_act = eps_act
        self.rev_est = rev_est
        self.rev_act = rev_act
        self.url = url


def _status(date_locked, date_confirmed, reported, eps_actual, rev_actual) -> str:
    """Derive the confirmation status. See module docstring for the full rationale."""
    if date_locked:
        return STATUS_LOCKED
    if date_confirmed:
        return STATUS_ANNOUNCED
    if reported or eps_actual is not None or rev_actual is not None:
        return STATUS_REPORTED
    return STATUS_ESTIMATED


# Hosts whose URLs must NEVER reach the public page. `announcement_url` is
# populated from several sources, and `--check-ir-emails` stores a
# https://mail.google.com/.../{thread_id} link pointing into JP's private
# mailbox (codex 2026-07-20). This repo is public and the page is committed to
# GitHub Pages, so publishing one would expose a private mailbox URL. Such a
# link is also useless to anyone but the mailbox owner, so dropping it costs
# nothing.
_PRIVATE_URL_HOSTS = {
    "mail.google.com",
    "drive.google.com",
    "docs.google.com",
}


def _publishable_url(url: str | None) -> str:
    """Return `url` if it is safe to publish on a public page, else "".

    Deliberately applied at load time rather than at render, so a future
    renderer can't reintroduce the leak by reading the raw column.
    """
    if not url:
        return ""
    url = url.strip()
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    # Scheme allowlist — also blocks javascript:/data: from ever reaching href.
    if parts.scheme not in ("http", "https"):
        return ""
    host = (parts.hostname or "").lower()
    if not host:
        return ""
    if host in _PRIVATE_URL_HOSTS:
        return ""
    return url


def load_rows(db_path: Path) -> list[Row]:
    """Read every event from the DB (read-only) and derive its confirmation status."""
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found at {db_path}")
    # mode=ro: the DB is a shared CI artifact; this consumer must never mutate it.
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        raw = con.execute(
            """
            SELECT ticker, company_name, event_date, event_hour, tier, quarter,
                   date_locked, date_confirmed, reported,
                   eps_estimate, eps_actual, rev_estimate, rev_actual,
                   announcement_url
            FROM events
            WHERE event_date IS NOT NULL AND event_date != ''
            """
        ).fetchall()
    finally:
        con.close()

    rows = []
    for (ticker, company, date, hour, tier, quarter, locked, confirmed, reported,
         eps_est, eps_act, rev_est, rev_act, url) in raw:
        ticker = (ticker or "").upper()
        if not ticker:
            continue
        rows.append(Row(
            ticker=ticker,
            # Full name alongside the ticker is a standing convention; fall back to
            # the ticker itself rather than rendering a blank cell.
            company=(company or "").strip() or ticker,
            date=date,
            hour=(hour or "").strip().lower(),
            tier=int(tier or 0),
            quarter=(quarter or "").strip(),
            status=_status(locked, confirmed, reported, eps_act, rev_act),
            eps_est=eps_est, eps_act=eps_act, rev_est=rev_est, rev_act=rev_act,
            url=_publishable_url(url),
        ))
    return rows


def _fmt_date(iso: str) -> str:
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%a %b %d, %Y")
    except (ValueError, TypeError):
        return iso or "-"


def _fmt_money(v) -> str:
    if v is None:
        return ""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(f) >= 1_000_000_000:
        return f"${f / 1_000_000_000:.2f}B"
    if abs(f) >= 1_000_000:
        return f"${f / 1_000_000:.1f}M"
    return f"${f:,.2f}"


def _actuals_cell(r: Row) -> str:
    """`actual / estimate` for EPS and revenue, blank when nothing has landed."""
    parts = []
    if r.eps_act is not None:
        parts.append("EPS " + _fmt_money(r.eps_act) +
                     (" vs " + _fmt_money(r.eps_est) if r.eps_est is not None else ""))
    if r.rev_act is not None:
        parts.append("Rev " + _fmt_money(r.rev_act) +
                     (" vs " + _fmt_money(r.rev_est) if r.rev_est is not None else ""))
    return " &middot; ".join(html.escape(p) for p in parts)


def _session_cell(hour: str) -> str:
    if hour in SESSION_LABEL:
        return f'<abbr title="{SESSION_LABEL[hour]}">{hour.upper()}</abbr>'
    return '<span class="dim">TBD</span>'


def _row_html(r: Row) -> str:
    name = html.escape(r.company)
    ticker = html.escape(r.ticker)
    if r.url:
        name = f'<a href="{html.escape(r.url, quote=True)}" rel="nofollow noopener">{name}</a>'
    tier_title = TIER_LABEL.get(r.tier, "Tier unknown")
    return (
        f'<tr data-status="{r.status}" data-tier="{r.tier}" '
        f'data-q="{html.escape((r.ticker + " " + r.company).lower(), quote=True)}">'
        # _fmt_date returns the raw string when the date doesn't parse, so this
        # is the one cell that can carry unmodified DB content into the public
        # page — escape it like every other field (codex 2026-07-20).
        f'<td class="date">{html.escape(_fmt_date(r.date))}</td>'
        f'<td class="sess">{_session_cell(r.hour)}</td>'
        f'<td class="tick"><code>{ticker}</code></td>'
        f'<td class="name">{name}</td>'
        f'<td class="tier"><abbr title="{html.escape(tier_title)}">T{r.tier or "?"}</abbr></td>'
        f'<td class="qtr">{html.escape(r.quarter)}</td>'
        f'<td class="stat"><span class="badge {r.status}">{STATUS_LABEL[r.status]}</span></td>'
        f'<td class="act">{_actuals_cell(r)}</td>'
        f'</tr>'
    )


def _table(rows: list[Row], empty_msg: str) -> str:
    if not rows:
        return f'<p class="empty">{html.escape(empty_msg)}</p>'
    body = "\n".join(_row_html(r) for r in rows)
    return (
        '<div class="tablewrap"><table>'
        '<thead><tr><th>Date</th><th>Session</th><th>Ticker</th><th>Company</th>'
        '<th>Tier</th><th>Quarter</th><th>Status</th><th>Reported figures</th></tr></thead>'
        f'<tbody>{body}</tbody></table></div>'
    )


CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0f0f23; color: #e0e0e0; padding: 24px; max-width: 1180px; margin: 0 auto; }
h1 { color: #e94560; font-size: 28px; margin-bottom: 6px; }
h2 { font-size: 20px; margin: 32px 0 6px; color: #fff; }
h2 .count { color: #a8a8b3; font-size: 14px; font-weight: 400; }
.updated { color: #666; font-size: 13px; margin-bottom: 18px; }
.intro { background: #151530; border: 1px solid #2a2a50; border-radius: 12px;
         padding: 16px; margin-bottom: 20px; line-height: 1.6; font-size: 14px; }
.intro p { margin-bottom: 8px; }
.intro p:last-child { margin-bottom: 0; }
.legend { list-style: none; margin: 10px 0 0; }
.legend li { margin-bottom: 6px; font-size: 13px; color: #c9c9d4; }
.controls { display: flex; gap: 12px; flex-wrap: wrap; align-items: center;
            background: #16213e; border-radius: 10px; padding: 12px 14px; margin-bottom: 8px;
            position: sticky; top: 0; z-index: 5; }
.controls input[type=search], .controls select {
  background: #0f0f23; border: 1px solid #333; border-radius: 6px; color: #eee;
  padding: 7px 10px; font-size: 13px; }
.controls label { font-size: 13px; color: #c9c9d4; display: flex; align-items: center; gap: 6px; }
.jump { font-size: 13px; margin-bottom: 18px; }
.jump a { color: #0fbcf9; text-decoration: none; margin-right: 14px; }
.jump a:hover { text-decoration: underline; }
.tablewrap { overflow-x: auto; border-radius: 10px; border: 1px solid #23234a; }
table { border-collapse: collapse; width: 100%; font-size: 13px; background: #16213e; }
/* Deliberately NOT position:sticky -- the controls bar above is sticky and its height
   varies with wrapping, so a fixed sticky offset here floats the header over the rows. */
th { text-align: left; padding: 9px 10px; background: #1b2a4a; color: #a8a8b3;
     text-transform: uppercase; font-size: 11px; letter-spacing: 0.6px; font-weight: 600; }
td { padding: 8px 10px; border-top: 1px solid #21214a; vertical-align: top; }
tbody tr:hover { background: #1c2748; }
td.date { white-space: nowrap; color: #dcdce6; }
td.tick code { background: #0f0f23; border: 1px solid #2a2a50; border-radius: 4px;
               padding: 1px 6px; color: #0fbcf9; font-size: 12px; }
td.name a { color: #0fbcf9; text-decoration: none; }
td.name a:hover { text-decoration: underline; }
td.act { color: #a8a8b3; white-space: nowrap; }
td.sess abbr, td.tier abbr { text-decoration: none; border-bottom: 1px dotted #555; cursor: help; }
.dim { color: #666; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px;
         font-weight: 600; white-space: nowrap; }
.badge.locked    { background: #10352a; color: #4ade80; border: 1px solid #1e6b4f; }
.badge.announced { background: #10352a; color: #4ade80; border: 1px solid #1e6b4f; }
.badge.reported  { background: #14243f; color: #7dc4ff; border: 1px solid #2b4f80; }
.badge.estimated { background: #3a2412; color: #fbbf24; border: 1px solid #7a4c11; }
.warn { background: #2a1a10; border: 1px solid #7a4c11; color: #fbbf24; border-radius: 10px;
        padding: 12px 14px; margin: 14px 0; font-size: 13px; line-height: 1.5; }
.empty { color: #666; padding: 18px 4px; font-size: 14px; }
footer { color: #555; font-size: 12px; margin-top: 40px; line-height: 1.6; }
"""

JS = """
(function () {
  var q = document.getElementById('q');
  var tier = document.getElementById('tier');
  var showEst = document.getElementById('showest');
  function apply() {
    var term = (q.value || '').trim().toLowerCase();
    var t = tier.value;
    var est = showEst.checked;
    document.querySelectorAll('tbody tr').forEach(function (tr) {
      var ok = true;
      if (!est && tr.dataset.status === 'estimated') ok = false;
      if (ok && t !== 'all' && tr.dataset.tier !== t) ok = false;
      if (ok && term && tr.dataset.q.indexOf(term) === -1) ok = false;
      tr.style.display = ok ? '' : 'none';
    });
    document.querySelectorAll('section[data-sec]').forEach(function (sec) {
      var n = sec.querySelectorAll('tbody tr:not([style*="none"])').length;
      var el = sec.querySelector('.count');
      if (el) el.textContent = '(' + n + ' shown)';
    });
  }
  q.addEventListener('input', apply);
  tier.addEventListener('change', apply);
  showEst.addEventListener('change', apply);
  apply();
})();
"""


def render(rows: list[Row], *, today: str, generated_at: str, db_asof: Optional[str]) -> str:
    upcoming = [r for r in rows if r.date >= today]
    past = [r for r in rows if r.date < today]
    # Next-upcoming first (soonest at the top of the upcoming table); past newest-first
    # per the standing "latest at top, older still reachable" convention.
    upcoming.sort(key=lambda r: (r.date, r.ticker))
    past.sort(key=lambda r: (r.date, r.ticker), reverse=True)

    def n_conf(rs):
        return sum(1 for r in rs if r.status != STATUS_ESTIMATED)

    conf_up, conf_past = n_conf(upcoming), n_conf(past)
    est_up = len(upcoming) - conf_up
    est_past = len(past) - conf_past

    stale = ""
    if db_asof:
        stale = (f'<p class="updated">Underlying database last written: '
                 f'{html.escape(db_asof)} UTC.</p>')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Earnings Calendar &mdash; Confirmed Dates</title>
<style>{CSS}</style>
</head>
<body>
<h1>Earnings Calendar</h1>
<p class="updated">Regenerated {html.escape(generated_at)} &middot; {conf_past} confirmed past &middot; {conf_up} confirmed upcoming</p>
{stale}

<div class="intro">
  <p>Every earnings date this agent tracks, past and upcoming. <strong>Only dates with
  positive confirmation evidence are shown by default</strong> &mdash; estimated dates are
  hidden behind the toggle below and are badged distinctly wherever they appear.</p>
  <ul class="legend">
    <li><span class="badge locked">Locked</span> Date pinned against provider drift &mdash;
      operator lock, or an SEC EDGAR 8-K Item&nbsp;2.02 / 6-K auto-correction. Strongest evidence.</li>
    <li><span class="badge announced">Announced</span> The company itself announced the date
      (confirmed session timing, an investor-relations release/email, or a high-confidence
      web-search resolution).</li>
    <li><span class="badge reported">Reported</span> The event already happened &mdash; actual
      earnings figures have landed against it.</li>
    <li><span class="badge estimated">Estimated</span> <strong>Not confirmed.</strong> A data
      provider's projection from historical reporting cadence. Treat as a guess.</li>
  </ul>
</div>

<div class="controls">
  <input type="search" id="q" placeholder="Filter by ticker or company name&hellip;" size="34">
  <label>Tier
    <select id="tier">
      <option value="all">All tiers</option>
      <option value="1">Tier 1 &mdash; core watchlist</option>
      <option value="2">Tier 2 &mdash; Healthcare Services &amp; MedTech</option>
      <option value="3">Tier 3 &mdash; wider universe</option>
    </select>
  </label>
  <label><input type="checkbox" id="showest"> Show estimated (unconfirmed) dates
    &mdash; {est_up} upcoming, {est_past} past</label>
</div>
<p class="jump"><a href="#upcoming">Upcoming</a><a href="#past">Past</a></p>

<section data-sec="upcoming" id="upcoming">
  <h2>Upcoming &mdash; next first <span class="count"></span></h2>
  <p class="updated">{conf_up} confirmed upcoming date(s) as of {html.escape(today)}.</p>
  {_table(upcoming, "No upcoming events in the database.")}
</section>

<section data-sec="past" id="past">
  <h2>Past &mdash; most recent first <span class="count"></span></h2>
  <p class="updated">{conf_past} confirmed past date(s). History reaches back only as far as
  the agent's rolling database window.</p>
  {_table(past, "No past events in the database.")}
</section>

<footer>
  Generated by <code>scripts/build_calendar_page.py</code> in the earnings-agent project;
  rebuilt from the events database on every daily run. Dates are press-release dates.
  Not investment advice.
</footer>
<script>{JS}</script>
</body>
</html>
"""


def build(db_path: Path, out_path: Path) -> dict:
    rows = load_rows(db_path)
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()

    db_asof = None
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        db_asof = con.execute("SELECT MAX(updated_at) FROM events").fetchone()[0]
        con.close()
    except sqlite3.Error:
        pass

    page = render(
        rows,
        today=today,
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        db_asof=db_asof,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # .nojekyll makes Pages serve the static HTML directly and skip the Jekyll build
    # (Jekyll chokes on large generated HTML -- bit sector_chart_pack). Idempotent.
    nojekyll = out_path.parent / ".nojekyll"
    if not nojekyll.exists():
        nojekyll.write_text("", encoding="utf-8")
    # Atomic write: a half-written index.html would be served as-is by Pages.
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(page, encoding="utf-8")
    tmp.replace(out_path)

    upcoming = [r for r in rows if r.date >= today]
    past = [r for r in rows if r.date < today]
    return {
        "total": len(rows),
        "confirmed_upcoming": sum(1 for r in upcoming if r.status != STATUS_ESTIMATED),
        "confirmed_past": sum(1 for r in past if r.status != STATUS_ESTIMATED),
        "estimated_upcoming": sum(1 for r in upcoming if r.status == STATUS_ESTIMATED),
        "estimated_past": sum(1 for r in past if r.status == STATUS_ESTIMATED),
        "out": str(out_path),
    }


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Build the earnings-calendar GitHub Pages site")
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"default: {DEFAULT_DB}")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"default: {DEFAULT_OUT}")
    args = p.parse_args(argv)

    try:
        stats = build(args.db, args.out)
    except FileNotFoundError as exc:
        # Fail loud: a missing DB must not leave a stale page silently published.
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    # ASCII-only stdout: CI and Windows Task Scheduler consoles are cp1252.
    print(f"wrote {stats['out']}: {stats['confirmed_past']} confirmed past, "
          f"{stats['confirmed_upcoming']} confirmed upcoming "
          f"({stats['estimated_past']} + {stats['estimated_upcoming']} estimated hidden by default; "
          f"{stats['total']} rows total)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
