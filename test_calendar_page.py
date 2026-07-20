"""Tests for the earnings-calendar Pages builder (scripts/build_calendar_page.py).

The load-bearing invariant: an ESTIMATED date must never be presented as confirmed.
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "scripts"))

import build_calendar_page as bcp  # noqa: E402


def test_status_precedence_locked_wins():
    # A locked row is locked even if it is also confirmed/reported.
    assert bcp._status(1, 1, 1, 1.0, 2.0) == bcp.STATUS_LOCKED
    assert bcp._status(1, 0, 0, None, None) == bcp.STATUS_LOCKED


def test_status_announced_from_date_confirmed():
    assert bcp._status(0, 1, 0, None, None) == bcp.STATUS_ANNOUNCED


def test_status_reported_from_flag_or_actuals():
    assert bcp._status(0, 0, 1, None, None) == bcp.STATUS_REPORTED
    # Actuals landed but the reported flag hasn't flipped yet (deferred-post path).
    assert bcp._status(0, 0, 0, 1.23, None) == bcp.STATUS_REPORTED
    assert bcp._status(0, 0, 0, None, 4.5e8) == bcp.STATUS_REPORTED


def test_status_estimated_when_no_evidence():
    # Empty Finnhub hour + no lock + no actuals = a cadence projection, NOT confirmed.
    assert bcp._status(0, 0, 0, None, None) == bcp.STATUS_ESTIMATED


def _mkdb(tmp_path, rows):
    db = tmp_path / "t.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE events (ticker TEXT, company_name TEXT, event_date TEXT, "
        "event_hour TEXT, tier INT, quarter TEXT, date_locked INT, date_confirmed INT, "
        "reported INT, eps_estimate REAL, eps_actual REAL, rev_estimate REAL, "
        "rev_actual REAL, announcement_url TEXT, updated_at TEXT)"
    )
    con.executemany(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    con.close()
    return db


def test_estimated_rows_are_badged_and_hidden_by_default(tmp_path):
    db = _mkdb(tmp_path, [
        ("AAA", "Alpha Corp", "2099-01-02", "", 1, "2099Q1", 0, 0, 0,
         None, None, None, None, None, "2026-01-01"),
        ("BBB", "Beta Inc", "2099-01-03", "amc", 2, "2099Q1", 0, 1, 0,
         None, None, None, None, None, "2026-01-01"),
    ])
    out = tmp_path / "docs" / "index.html"
    stats = bcp.build(db, out)
    assert stats["confirmed_upcoming"] == 1
    assert stats["estimated_upcoming"] == 1

    page = out.read_text(encoding="utf-8")
    # The estimated row carries the estimated badge and the toggle is unchecked,
    # so it is filtered out on load.
    assert 'data-status="estimated"' in page
    assert '<input type="checkbox" id="showest">' in page
    assert "Alpha Corp" in page and "Beta Inc" in page


def test_full_company_name_falls_back_to_ticker(tmp_path):
    db = _mkdb(tmp_path, [
        ("CCC", "", "2099-01-02", "bmo", 1, "2099Q1", 0, 1, 0,
         None, None, None, None, None, "2026-01-01"),
    ])
    rows = bcp.load_rows(db)
    assert rows[0].company == "CCC"


def test_build_does_not_mutate_the_db(tmp_path):
    db = _mkdb(tmp_path, [
        ("DDD", "Delta Ltd", "2099-01-02", "bmo", 1, "2099Q1", 0, 1, 0,
         None, None, None, None, None, "2026-01-01"),
    ])
    before = db.read_bytes()
    bcp.build(db, tmp_path / "docs" / "index.html")
    assert db.read_bytes() == before


def test_past_is_newest_first_and_upcoming_is_soonest_first(tmp_path):
    rows = [
        bcp.Row("OLD", "Old Co", "2000-01-01", "bmo", 1, "", bcp.STATUS_ANNOUNCED,
                None, None, None, None, ""),
        bcp.Row("MID", "Mid Co", "2000-06-01", "bmo", 1, "", bcp.STATUS_ANNOUNCED,
                None, None, None, None, ""),
        bcp.Row("SOON", "Soon Co", "2099-01-01", "bmo", 1, "", bcp.STATUS_ANNOUNCED,
                None, None, None, None, ""),
        bcp.Row("LATER", "Later Co", "2099-06-01", "bmo", 1, "", bcp.STATUS_ANNOUNCED,
                None, None, None, None, ""),
    ]
    page = bcp.render(rows, today="2026-01-01",
                      generated_at="2026-01-01 00:00 UTC", db_asof=None)
    upcoming = page.split('id="upcoming"')[1].split("</section>")[0]
    past = page.split('id="past"')[1].split("</section>")[0]
    assert upcoming.index("SOON") < upcoming.index("LATER")
    assert past.index("MID") < past.index("OLD")


# --- announcement_url can hold a Gmail thread URL from --check-ir-emails. This
# repo is public and docs/index.html is committed to Pages, so a private mailbox
# link must never reach the rendered page (codex 2026-07-20). ---

def test_gmail_thread_url_is_dropped():
    assert bcp._publishable_url(
        "https://mail.google.com/mail/u/0/#inbox/thread-f:12345") == ""


def test_other_private_google_hosts_dropped():
    assert bcp._publishable_url("https://drive.google.com/file/d/abc/view") == ""
    assert bcp._publishable_url("https://docs.google.com/document/d/abc/edit") == ""


def test_legitimate_ir_url_survives():
    for u in ("https://ir.irhythmtech.com/news/press-release",
              "http://www.globenewswire.com/news-release/2026/abc.html"):
        assert bcp._publishable_url(u) == u


def test_non_http_schemes_are_dropped():
    """Also closes the javascript:/data: href vector."""
    for u in ("javascript:alert(1)", "data:text/html,<script>x</script>",
              "file:///C:/secret.txt", "mailto:jp@example.com"):
        assert bcp._publishable_url(u) == "", u


def test_empty_and_malformed_are_safe():
    for u in (None, "", "   ", "not a url", "https://"):
        assert bcp._publishable_url(u) == "", repr(u)


def test_host_match_is_case_insensitive():
    assert bcp._publishable_url("https://MAIL.GOOGLE.COM/mail/u/0/#x") == ""


def test_rendered_page_contains_no_gmail_url():
    """End-to-end: a row carrying a Gmail URL renders without it."""
    r = bcp.Row(ticker="IRTC", company="iRhythm Holdings, Inc.",
                date="2026-08-06", hour="amc", tier=2, quarter="2026Q2",
                status=bcp.STATUS_ANNOUNCED, eps_est=None, eps_act=None,
                rev_est=None, rev_act=None,
                url=bcp._publishable_url(
                    "https://mail.google.com/mail/u/0/#inbox/thread-f:99"))
    out = bcp._row_html(r)
    assert "mail.google.com" not in out
    assert "<a href" not in out
    assert "iRhythm" in out


# --- Every value that can reach the public page must be escaped. _fmt_date
# returns its input verbatim when the date doesn't parse, which made the date
# cell the one XSS path into docs/index.html (codex 2026-07-20). ---

def test_malformed_date_is_escaped_not_injected():
    r = bcp.Row(ticker="XYZ", company="Test Co", hour="bmo", tier=3,
                quarter="2026Q2", status=bcp.STATUS_ESTIMATED,
                date='<img src=x onerror=alert(1)>',
                eps_est=None, eps_act=None, rev_est=None, rev_act=None, url="")
    out = bcp._row_html(r)
    assert "<img src=x" not in out
    assert "&lt;img src=x" in out


def test_every_row_field_survives_a_hostile_value():
    """Belt-and-braces sweep over the other attacker-reachable columns."""
    payload = '"><script>alert(1)</script>'
    r = bcp.Row(ticker=payload, company=payload, hour=payload, tier=1,
                quarter=payload, status=bcp.STATUS_ANNOUNCED, date=payload,
                eps_est=None, eps_act=None, rev_est=None, rev_act=None,
                url="https://example.com/ok")
    out = bcp._row_html(r)
    assert "<script>" not in out
    assert "</script>" not in out


def test_normal_date_still_renders_readably():
    r = bcp.Row(ticker="IRTC", company="iRhythm Holdings, Inc.",
                date="2026-08-06", hour="amc", tier=2, quarter="2026Q2",
                status=bcp.STATUS_ANNOUNCED, eps_est=None, eps_act=None,
                rev_est=None, rev_act=None, url="")
    out = bcp._row_html(r)
    assert "Thu Aug 06, 2026" in out
