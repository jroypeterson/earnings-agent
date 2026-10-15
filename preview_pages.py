"""
Public per-company earnings-preview pages -> docs/preview/<TICKER>.html.

JP 2026-10-15: for Core coverage names, a more robust earnings preview with a
page linked from the TickTick earnings task's note, on the public calendar site.

Each page carries the same sections as the Slack preview card (When, Metrics,
Guidance, Setup), rendered from the same `_render_row_lines` so the two can
never disagree, plus what a Slack card has no room for:

  * the reported history the events DB holds for the name (EPS and revenue,
    actual vs consensus, per quarter, as far back as the DB reaches);
  * an explicit list of every field that was unavailable and why.

`docs/preview/index.html` lists every page built this season from a small
manifest (`docs/preview/manifest.json`) so a page survives the run that built
it: the preview window is three days, the season is six weeks.

Public by design, like the calendar: nothing here is a position size, and the
names are the Coverage Manager universe, already public.
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from consensus_preview import (
    PreviewRow,
    _fmt_eps,
    _fmt_rev,
    _render_row_lines,
)

PAGES_BASE_URL = "https://jroypeterson.github.io/earnings-agent/preview/"

_LINK = re.compile(r"&lt;(https?://[^|&]+?)\|([^&]+?)&gt;")
_BOLD = re.compile(r"\*([^*]+)\*")
_ITAL = re.compile(r"(?<![\w/])_([^_]+)_(?![\w])")
_CODE = re.compile(r"`([^`]+)`")


def page_url(ticker: str) -> str:
    return PAGES_BASE_URL + _safe_name(ticker) + ".html"


def _safe_name(ticker: str) -> str:
    """A filename that cannot escape docs/preview/ (tickers carry dots, e.g. ROG.SW)."""
    return re.sub(r"[^A-Za-z0-9.\-]", "_", ticker.upper()).lstrip(".") or "_"


def mrkdwn_to_html(line: str) -> str:
    """Slack mrkdwn -> HTML for the subset `_render_row_lines` emits.

    Escape FIRST, then rewrite markup, so no DB- or vendor-supplied text can
    inject HTML into a public page.
    """
    s = html.escape(line, quote=False)
    s = _LINK.sub(lambda m: f'<a href="{m.group(1)}" rel="nofollow noopener">'
                            f'{m.group(2)}</a>', s)
    s = _CODE.sub(r"<code>\1</code>", s)
    s = _BOLD.sub(r"<strong>\1</strong>", s)
    s = _ITAL.sub(r"<em>\1</em>", s)
    return s


def _lines_to_html(lines: list[str]) -> str:
    """Section labels become headings; bullets become list items."""
    out: list[str] = []
    open_list = False
    for raw in lines[1:]:            # line 0 is the header, rendered separately
        stripped = raw.strip()
        is_bullet = stripped.startswith(("•", "–"))
        if not is_bullet:
            if open_list:
                out.append("</ul>")
                open_list = False
            out.append(f"<h2>{mrkdwn_to_html(stripped)}</h2>")
            continue
        if not open_list:
            out.append("<ul>")
            open_list = True
        cls = ' class="sub"' if stripped.startswith("–") else ""
        out.append(f"<li{cls}>{mrkdwn_to_html(stripped.lstrip('•– ').strip())}</li>")
    if open_list:
        out.append("</ul>")
    return "\n".join(out)


def load_history(conn: sqlite3.Connection, ticker: str, before: str,
                 limit: int = 8) -> list[tuple]:
    """(quarter, event_date, eps_est, eps_act, rev_est, rev_act), newest first."""
    return conn.execute(
        "SELECT quarter, event_date, eps_estimate, eps_actual, rev_estimate, rev_actual "
        "FROM events WHERE ticker = ? AND event_date < ? "
        "AND (eps_actual IS NOT NULL OR rev_actual IS NOT NULL) "
        "ORDER BY event_date DESC LIMIT ?",
        (ticker, before, limit),
    ).fetchall()


def _surprise(act: Optional[float], est: Optional[float]) -> str:
    """Surprise against an ABSOLUTE denominator: a signed one renders a
    deepening loss as a beat (NNOX, -0.79 vs -0.17)."""
    if act is None or est is None or abs(est) < 0.10:
        return "&mdash;"
    pct = (act - est) / abs(est) * 100
    cls = "up" if pct > 0 else ("down" if pct < 0 else "")
    return f'<span class="{cls}">{pct:+.1f}%</span>'


def _history_html(hist: list[tuple]) -> str:
    if not hist:
        return ('<p class="muted">No reported quarters in the events database for '
                'this name yet.</p>')
    rows = []
    for q, d, ee, ea, re_, ra in hist:
        rows.append(
            f"<tr><td>{html.escape(q or '')}</td><td>{html.escape(d or '')}</td>"
            f"<td>{_fmt_eps(ea)}</td><td>{_fmt_eps(ee)}</td><td>{_surprise(ea, ee)}</td>"
            f"<td>{_fmt_rev(ra)}</td><td>{_fmt_rev(re_)}</td><td>{_surprise(ra, re_)}</td></tr>"
        )
    return (
        "<table><thead><tr><th>Quarter</th><th>Reported</th><th>EPS</th><th>Cons.</th>"
        "<th>Surprise</th><th>Revenue</th><th>Cons.</th><th>Surprise</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        '<p class="muted">As far back as the events database reaches. Consensus is the '
        "adjusted figure the Street models, not GAAP.</p>"
    )


CSS = """
:root { --bg:#fff; --fg:#1b1b1f; --muted:#6b6b76; --line:#e3e3e8; --up:#1a7f37; --down:#c62828; --accent:#3b5bdb; }
@media (prefers-color-scheme: dark) { :root { --bg:#111216; --fg:#e7e7ea; --muted:#9a9aa6; --line:#2a2b31; --up:#4cc76b; --down:#ff6b6b; --accent:#8ea2ff; } }
* { box-sizing: border-box; }
body { margin:0 auto; max-width:860px; padding:24px 16px 48px; background:var(--bg); color:var(--fg);
       font:15px/1.55 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
h1 { font-size:24px; margin:0 0 4px; }
h2 { font-size:15px; margin:22px 0 6px; text-transform:none; color:var(--accent); }
ul { margin:0; padding-left:20px; } li { margin:2px 0; } li.sub { margin-left:18px; list-style:circle; }
.muted { color:var(--muted); font-size:13px; }
a { color:var(--accent); }
code { font-size:13px; }
table { width:100%; border-collapse:collapse; font-size:13px; font-variant-numeric:tabular-nums; }
th, td { text-align:right; padding:5px 6px; border-bottom:1px solid var(--line); white-space:nowrap; }
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align:left; }
.tablewrap { overflow-x:auto; }
.up { color:var(--up); } .down { color:var(--down); }
"""


def render_page(row: PreviewRow, hist: list[tuple], generated_at: str) -> str:
    lines = _render_row_lines(row)
    name = html.escape(row.company_name or row.ticker)
    tk = html.escape(row.ticker)
    na = ""
    if row.na_notes:
        na = "<h2>Unavailable</h2><ul>" + "".join(
            f"<li>{html.escape(n)}</li>" for n in row.na_notes) + "</ul>"
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{tk} Earnings Preview</title><style>{CSS}</style></head><body>
<p class="muted"><a href="./index.html">&larr; all previews</a> &middot;
<a href="../core_season.html">core-coverage schedule</a></p>
<h1>{tk} &middot; {name}</h1>
<p class="muted">Earnings preview for the {html.escape(row.event_date)} report &middot;
generated {html.escape(generated_at)}</p>
{_lines_to_html(lines)}
<h2>Reported history <em>(actual vs Street consensus)</em></h2>
<div class="tablewrap">{_history_html(hist)}</div>
{na}
<p class="muted">Built by earnings_agent from free and cached data (yfinance, the
events database, SEC 8-K exhibits, FMP where noted). Not investment advice.</p>
</body></html>
"""


def _load_manifest(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def render_index(manifest: dict, generated_at: str) -> str:
    items = sorted(manifest.items(), key=lambda kv: (kv[1].get("event_date", ""), kv[0]))
    rows = "".join(
        f'<tr><td>{html.escape(m.get("event_date", ""))}</td>'
        f'<td><a href="./{html.escape(_safe_name(t))}.html"><code>{html.escape(t)}</code></a></td>'
        f'<td style="text-align:left">{html.escape(m.get("company", ""))}</td></tr>'
        for t, m in items
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Earnings Previews</title><style>{CSS}</style></head><body>
<p class="muted"><a href="../core_season.html">core-coverage schedule</a> &middot;
<a href="../index.html">full earnings calendar</a></p>
<h1>Earnings Previews</h1>
<p class="muted">One page per upcoming report, built about three days ahead.
{len(items)} page(s) &middot; regenerated {html.escape(generated_at)}</p>
<div class="tablewrap"><table><thead><tr><th>Reports</th><th>Ticker</th>
<th style="text-align:left">Company</th></tr></thead><tbody>{rows}</tbody></table></div>
</body></html>
"""


def write_preview_pages(rows: list[PreviewRow], conn: sqlite3.Connection,
                        out_dir: Path) -> list[str]:
    """Write one page per row plus the index. Returns the tickers written."""
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    manifest_path = out_dir / "manifest.json"
    manifest = _load_manifest(manifest_path)
    written = []
    for row in rows:
        hist = load_history(conn, row.ticker, row.event_date)
        (out_dir / f"{_safe_name(row.ticker)}.html").write_text(
            render_page(row, hist, now), encoding="utf-8")
        manifest[row.ticker.upper()] = {
            "event_date": row.event_date,
            "company": row.company_name or row.ticker,
            "generated": now,
        }
        written.append(row.ticker)
    manifest_path.write_text(json.dumps(manifest, indent=1, sort_keys=True),
                             encoding="utf-8")
    (out_dir / "index.html").write_text(render_index(manifest, now), encoding="utf-8")
    return written


def built_on(out_dir: Path, day: str) -> dict[str, str]:
    """{TICKER: event_date} for pages already built on `day` (YYYY-MM-DD)."""
    m = _load_manifest(out_dir / "manifest.json")
    return {t: v.get("event_date", "") for t, v in m.items()
            if (v.get("generated") or "").startswith(day)}


def links_context_block(rows: list[PreviewRow], cap: int = 30) -> dict:
    """A Slack context block linking each previewed name to its page."""
    links = [f"<{page_url(r.ticker)}|{r.ticker}>" for r in rows[:cap]]
    more = f" +{len(rows) - cap} more" if len(rows) > cap else ""
    text = (":page_facing_up: Full preview pages: " + " · ".join(links) + more
            + f" · <{PAGES_BASE_URL}index.html|all previews>")
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text[:2990]}]}
