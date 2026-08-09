"""
Build the portfolio earnings-season page -> `docs/season.html`.

The browsable companion to the Slack season card: every in-scope name, every
reaction, untruncated and sortable, at a stable URL:

    https://jroypeterson.github.io/earnings-agent/season.html

Slack is the push surface and is necessarily lossy — a peak week is 26 prints
and a fenced table gets chunked. This page is the pull surface: nothing is
dropped, nothing is collapsed, and the reaction columns can be sorted so
"which of my prints did the market react hardest to" is one click rather than
a scan.

**Scope is Portfolio + Researching**, so this page publishes JP's position
list. That is deliberate and cleared (JP 2026-08-09: *"Its fine to have my
portfolio public"*); the same book is already public in Coverage-Manager and
sigma-alert. Cost basis, share counts and P&L are NOT here and must not be
added — what is cleared is the *composition*, not the position sizes.

Read-only consumer: the DB is opened `mode=ro` because it is a shared CI
artifact.
"""

from __future__ import annotations

import argparse
import html
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config import DB_PATH                                    # noqa: E402
from coverage import load_coverage                            # noqa: E402
from season_progress import (                                 # noqa: E402
    SCOPE_POSITIONS, SeasonProgress, SeasonRow,
    attach_reactions, collect_season,
)

DEFAULT_OUT = REPO_ROOT / "docs" / "season.html"

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
a { color: #0fbcf9; }
.funnel { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
.stat { background: #16213e; border: 1px solid #2a2a50; border-radius: 10px;
        padding: 12px 16px; min-width: 128px; }
.stat .n { font-size: 24px; font-weight: 600; color: #fff; display: block; }
.stat .l { font-size: 12px; color: #a8a8b3; }
.bar { height: 10px; background: #16213e; border: 1px solid #2a2a50; border-radius: 6px;
       overflow: hidden; margin-bottom: 20px; }
.bar > span { display: block; height: 100%; background: #2ecc71; }
.pin { font-size: 11px; opacity: .75; cursor: help; }
table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 8px; }
th, td { padding: 7px 9px; text-align: left; border-bottom: 1px solid #23233f; white-space: nowrap; }
th { color: #a8a8b3; font-weight: 600; font-size: 12px; text-transform: uppercase;
     letter-spacing: .03em; background: #151530; position: sticky; top: 0; }
tbody tr:hover { background: #16213e; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
.tick { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color: #fff; font-weight: 600; }
.up { color: #2ecc71; } .down { color: #ff6b6b; }
.big { font-weight: 700; }
.muted { color: #6a6a80; }
.badge { display: inline-block; padding: 1px 7px; border-radius: 999px; font-size: 11px;
         font-weight: 600; border: 1px solid; }
.badge.confirmed { color: #7ee787; border-color: #2c5a35; background: #14261a; }
.badge.reported { color: #d2a8ff; border-color: #4a3060; background: #1d1428; }
.badge.nodate { color: #a8a8b3; border-color: #3a3a5c; background: #1a1a30; }
.badge.estimated { color: #ffcc66; border-color: #6b551f; background: #2a2210; }
.badge.p { color: #fff; border-color: #e94560; background: #3a1420; }
.badge.r { color: #c9c9d4; border-color: #3a3a5c; background: #1a1a30; }
.note { font-size: 12.5px; color: #a8a8b3; margin: 8px 0 0; line-height: 1.6; }
footer { margin-top: 40px; font-size: 12px; color: #555; line-height: 1.7; }
"""


def _pct(v: float | None, decimals: int = 1) -> str:
    """Blank when unmeasurable — never 0.0, and never a signed zero."""
    if v is None:
        return '<span class="muted">&mdash;</span>'
    r = round(v, decimals)
    cls = "up" if r > 0 else ("down" if r < 0 else "")
    txt = f"{abs(r):.{decimals}f}%" if r == 0 else f"{r:+.{decimals}f}%"
    return f'<span class="{cls}">{txt}</span>' if cls else txt


def _sigma(v: float | None) -> str:
    if v is None:
        return '<span class="muted">&mdash;</span>'
    r = round(v, 1)
    cls = "up" if r > 0 else ("down" if r < 0 else "")
    big = " big" if abs(r) >= 2 else ""
    txt = f"{abs(r):.1f}" if r == 0 else f"{r:+.1f}"
    return f'<span class="{cls}{big}">{txt}</span>'


def _when(r: SeasonRow) -> str:
    if not r.event_date:
        return '<span class="muted">no date</span>'
    d = date.fromisoformat(r.event_date)
    hour = {"bmo": "BMO", "amc": "AMC", "dmh": "DMH"}.get((r.event_hour or "").lower(), "")
    return f"{d.strftime('%a %b')} {d.day}" + (f' <span class="muted">{hour}</span>' if hour else "")


def _list_badge(pos: str) -> str:
    cls = "p" if pos == "Portfolio" else "r"
    return f'<span class="badge {cls}">{html.escape(pos)}</span>'


def _status_badge(r: SeasonRow) -> str:
    """Trust is binary: Confirmed or Estimated.

    A pinned date gets a small marker rather than a third status — the lock is
    a *mechanism* (this date will not be moved by a provider sync), not a higher
    grade of evidence, and rendering it as one made a row that was both look
    like some alternative kind of certainty. See SeasonRow.status.
    """
    s = r.status
    badge = f'<span class="badge {s.lower().replace(" ", "")}">{html.escape(s)}</span>'
    if r.pinned:
        badge += (
            ' <span class="pin" title="Date pinned against provider drift '
            '(operator lock or a corroborated SEC filing) — it will not be '
            'moved by a calendar sync.">&#128204;</span>'
        )
    return badge


def _reported_table(rows: list[SeasonRow]) -> str:
    if not rows:
        return '<p class="note">No in-scope company has reported yet this season.</p>'
    body = []
    for r in rows:
        sort_move = "" if r.move_pct is None else f"{r.move_pct:.4f}"
        sort_sig = "" if r.sigma is None else f"{r.sigma:.4f}"
        gap = ""
        if r.move_pct is None and r.reaction_note:
            gap = f' <span class="muted" title="{html.escape(r.reaction_note)}">&#9432;</span>'
        sort_ytd = "" if r.ytd_pct is None else f"{r.ytd_pct:.4f}"
        body.append(
            "<tr>"
            f'<td class="tick">{html.escape(r.ticker)}</td>'
            f"<td>{html.escape(r.company_name)}</td>"
            f'<td data-sort="{html.escape(r.event_date or "")}">{_when(r)}</td>'
            f'<td class="num">{_pct(r.eps_surprise_pct, 0)}</td>'
            f'<td class="num">{_pct(r.rev_surprise_pct, 0)}</td>'
            f'<td class="num" data-sort="{sort_move}">{_pct(r.move_pct)}{gap}</td>'
            f'<td class="num" data-sort="{sort_sig}">{_sigma(r.sigma)}</td>'
            f'<td class="num">{_pct(r.rel_pct)}</td>'
            f'<td class="num" data-sort="{sort_ytd}">{_pct(r.ytd_pct, 0)}</td>'
            f"<td>{_list_badge(r.position)}</td>"
            "</tr>"
        )
    return f"""<table id="reported">
<thead><tr>
<th>Ticker</th><th>Company</th><th>Reported</th>
<th class="num">EPS surp.</th><th class="num">Rev surp.</th><th class="num">Move</th>
<th class="num">Sigma</th><th class="num">vs SPY</th><th class="num">YTD</th><th>List</th>
</tr></thead>
<tbody>{''.join(body)}</tbody></table>"""


def _upcoming_table(rows: list[SeasonRow], today: date) -> str:
    if not rows:
        return '<p class="note">Every in-scope company with a date has reported.</p>'
    body = []
    for r in rows:
        days = (date.fromisoformat(r.event_date) - today).days if r.event_date else None
        in_ = "today" if days == 0 else (f"{days}d" if days else "")
        sort_ytd = "" if r.ytd_pct is None else f"{r.ytd_pct:.4f}"
        body.append(
            "<tr>"
            f'<td class="tick">{html.escape(r.ticker)}</td>'
            f"<td>{html.escape(r.company_name)}</td>"
            f'<td data-sort="{html.escape(r.event_date or "")}">{_when(r)}</td>'
            f"<td>{_status_badge(r)}</td>"
            f'<td class="num" data-sort="{days if days is not None else ""}">{in_}</td>'
            f'<td class="num" data-sort="{sort_ytd}">{_pct(r.ytd_pct, 0)}</td>'
            f"<td>{_list_badge(r.position)}</td>"
            "</tr>"
        )
    return f"""<table id="upcoming">
<thead><tr>
<th>Ticker</th><th>Company</th><th>Expected</th>
<th>Status</th><th class="num">In</th><th class="num">YTD</th><th>List</th>
</tr></thead>
<tbody>{''.join(body)}</tbody></table>"""


# Sorting only. The search box was removed 2026-08-09 (JP: "the ticker or
# company filter is useless") — at 50 and 15 rows the whole table already fits
# a couple of screens, so filtering solved a problem the page does not have,
# while sorting is what actually answers a question.
JS = """
function wire(tableId){
  const t=document.getElementById(tableId); if(!t) return;
  t.querySelectorAll('th').forEach((th,i)=>{
    th.style.cursor='pointer';
    th.addEventListener('click',()=>{
      const dir = th.dataset.dir==='asc' ? 'desc' : 'asc';
      t.querySelectorAll('th').forEach(o=>delete o.dataset.dir);
      th.dataset.dir=dir;
      const rows=[...t.tBodies[0].rows];
      rows.sort((a,b)=>{
        const cell=r=>{const c=r.cells[i]; return c.dataset.sort!==undefined?c.dataset.sort:c.innerText.trim();};
        const A=cell(a),B=cell(b);
        // Blank means NOT MEASURABLE. Park those at the bottom in both
        // directions rather than letting them read as the smallest value.
        if(A===''&&B==='') return 0;
        if(A==='') return 1;
        if(B==='') return -1;
        const nA=parseFloat(A),nB=parseFloat(B);
        const cmp = (!isNaN(nA)&&!isNaN(nB)) ? nA-nB : A.localeCompare(B);
        return dir==='asc'?cmp:-cmp;
      });
      rows.forEach(r=>t.tBodies[0].appendChild(r));
    });
  });
}
wire('reported'); wire('upcoming');
"""


def render(p: SeasonProgress, *, generated_at: str, db_asof: str | None) -> str:
    reported = sorted(p.reported, key=lambda r: (r.event_date or "", r.ticker), reverse=True)
    upcoming = sorted(p.upcoming, key=lambda r: (r.event_date or "", r.ticker))

    pct = p.pct_reported
    bar = f'<div class="bar"><span style="width:{pct or 0}%"></span></div>'

    nodate = ""
    if p.no_date:
        names = ", ".join(f'<span class="tick">{html.escape(r.ticker)}</span>'
                          for r in p.no_date)
        nodate = (
            f'<p class="note">No {html.escape(p.season)} date yet for {names} &mdash; not '
            f"scheduled by any provider. They are listed here rather than counted as "
            f"upcoming, and are <strong>excluded from the percentage</strong> rather than "
            f"counted as unreported.</p>"
        )

    overdue = ""
    if p.overdue:
        overdue = (
            f'<h2>Past due, no results <span class="count">{len(p.overdue)}</span></h2>'
            + _upcoming_table(p.overdue, p.as_of)
        )

    measured = sum(1 for r in reported if r.move_pct is not None)
    gap_note = ""
    if measured < len(reported):
        gap_note = (
            f'<p class="note">{len(reported) - measured} reported name(s) have no '
            f"measurable reaction yet &mdash; an after-the-close print has no next "
            f"session to react in until the following close. A blank cell means "
            f"<em>not measurable</em>, never zero.</p>"
        )

    stale = ""
    if db_asof:
        stale = (f'<p class="updated">Underlying database last written: '
                 f'{html.escape(db_asof)} UTC.</p>')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Earnings Season Progress &mdash; {html.escape(p.season)}</title>
<style>{CSS}</style>
</head>
<body>
<h1>Earnings Season Progress</h1>
<p class="updated">{html.escape(p.season)} &middot; regenerated {html.escape(generated_at)}
 &middot; <a href="./index.html">full earnings calendar &rarr;</a></p>
{stale}

<div class="funnel">
  <div class="stat"><span class="n">{len(reported)}</span><span class="l">reported</span></div>
  <div class="stat"><span class="n">{len(upcoming)}</span><span class="l">still to report</span></div>
  <div class="stat"><span class="n">{pct if pct is not None else '&mdash;'}%</span><span class="l">of the season done</span></div>
  <div class="stat"><span class="n">{p.in_scope}</span><span class="l">names in scope</span></div>
</div>
{bar}

<div class="intro">
  <p>Every company in <strong>Portfolio</strong> and <strong>Researching</strong>, and where
  it stands in the {html.escape(p.season)} reporting season. Click any column heading to sort;
  a blank cell means <em>not measurable</em>, never zero.</p>
  <p><strong>Move</strong> is the post-earnings reaction: a BMO print is measured from the
  prior session's close to the report-day close, an AMC print from the report-day close to the
  next session's close. <strong>Sigma</strong> puts that move in the stock's own trailing
  252-day daily standard deviation, with the reaction day excluded. <strong>vs SPY</strong> is
  the same move net of the index over the identical window &mdash; which is what separates a
  real reaction from a tape move. <strong>EPS surp.</strong> is surprise against consensus,
  not growth, and is blank where consensus is missing or too near zero for a percentage to
  mean anything. <strong>Rev surp.</strong> is the same against revenue consensus.</p>
  <p><strong>YTD</strong> is total return from the last close of {p.as_of.year - 1} to the
  <em>latest</em> close &mdash; so for a company that has already reported it
  <strong>includes that post-earnings reaction</strong>. It answers &ldquo;where does this
  stock stand now&rdquo;, which is the only definition that also works for the names that
  have not reported yet. It is deliberately <em>not</em> the post-earnings-movers
  <code>YTD</code>, which stops at the print so it never includes the move it is explaining.</p>
  <p><strong>Status</strong> is binary on purpose. <span class="badge confirmed">Confirmed</span>
  means the date has positive evidence behind it &mdash; the company announced it, or an SEC
  filing corroborated it. <span class="badge estimated">Estimated</span> means a provider
  projected it from historical cadence; treat it as a guess. A &#128204; marks a date
  additionally <em>pinned</em> against provider drift, which is a note about the plumbing
  rather than a third grade of confidence.</p>
</div>

<h2>Reported this season <span class="count">{len(reported)}</span></h2>
{_reported_table(reported)}
{gap_note}

<h2>Still to report <span class="count">{len(upcoming)}</span></h2>
{_upcoming_table(upcoming, p.as_of)}
{nodate}
{overdue}

<footer>
  Denominator is the {p.scheduled} name(s) carrying a scheduled {html.escape(p.season)} date;
  names with no date at all are listed but never folded into the percentage.
  Position composition only &mdash; no share counts, cost basis or P&amp;L are published here.
  Generated from <code>earnings_events.db</code>, opened read-only.
</footer>
<script>{JS}</script>
</body>
</html>
"""


def build(db_path: Path, out_path: Path, as_of: date | None = None) -> dict:
    as_of = as_of or datetime.now(timezone.utc).date()

    coverage = load_coverage()
    if not coverage:
        raise SystemExit("build_season_page: no coverage loaded — refusing to build")
    coverage_map = {t.ticker.upper(): t for t in coverage}

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        progress = collect_season(con, coverage_map, as_of, SCOPE_POSITIONS)
        db_asof = con.execute("SELECT MAX(updated_at) FROM events").fetchone()[0]
    finally:
        con.close()

    if progress.in_scope == 0:
        # An empty roster renders as a finished season. Refuse, exactly as the
        # Slack lane does.
        raise SystemExit(
            "build_season_page: empty in-scope roster — refusing to publish a "
            "page that would read as a completed season"
        )

    # with_ytd covers EVERY in-scope row (reported and not) off the same single
    # download, so the page never issues two overlapping price fetches.
    attach_reactions(
        progress.reported + progress.upcoming + progress.overdue,
        as_of, with_ytd=True,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    (out_path.parent / ".nojekyll").touch()
    out_path.write_text(
        render(
            progress,
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            db_asof=db_asof,
        ),
        encoding="utf-8",
    )
    return {
        "out": str(out_path),
        "season": progress.season,
        "reported": len(progress.reported),
        "upcoming": len(progress.upcoming),
        "scheduled": progress.scheduled,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=DB_PATH)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT,
                    help=f"default: {DEFAULT_OUT}")
    ap.add_argument("--date", help="as-of date (YYYY-MM-DD); defaults to today")
    args = ap.parse_args(argv)

    stats = build(
        args.db, args.out,
        date.fromisoformat(args.date) if args.date else None,
    )
    print(
        f"season page: {stats['season']} — {stats['reported']} reported / "
        f"{stats['upcoming']} to come (of {stats['scheduled']} scheduled) "
        f"-> {stats['out']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
