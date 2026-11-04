"""Public preview pages + the TickTick note link (JP 2026-10-15)."""
from datetime import date

import preview_pages as pp
import ticktick as tt
from consensus_preview import PreviewRow
import pytest


@pytest.fixture(autouse=True)
def _no_price_download(monkeypatch):
    monkeypatch.setattr(pp, "_fetch_ytd", lambda tickers, today: {})


def _row(ticker="ISRG", name="Intuitive Surgical"):
    return PreviewRow(ticker=ticker, company_name=name, tier=1,
                      event_date="2026-10-20", event_hour="amc",
                      call_datetime_utc=None, eps_mean=2.69)


def test_vendor_text_cannot_inject_html_into_a_public_page():
    r = _row(name='<script>alert(1)</script>')
    r.guidance_lines = ['FY26: <img src=x onerror=alert(1)> growth 5%']
    out = pp.render_page(r, [], "now")
    assert "<script>alert" not in out and "<img src=x" not in out


def test_link_markup_becomes_an_anchor_and_bold_becomes_strong():
    s = pp.mrkdwn_to_html("*Metrics* <https://sec.gov/x|source>")
    assert '<a href="https://sec.gov/x"' in s and "<strong>Metrics</strong>" in s


def test_filename_cannot_escape_the_preview_dir():
    assert "/" not in pp._safe_name("../../etc") and not pp._safe_name("../x").startswith(".")


def test_windows_reserved_stems_get_a_suffix_on_the_stem():
    """CI committed docs/preview/CON.html and `git pull` broke on Windows."""
    assert pp._safe_name("CON") == "CON_"
    assert pp._safe_name("con") == "CON_"
    assert pp._safe_name("CON.TO") == "CON_.TO"
    assert pp._safe_name("NUL.SW") == "NUL_.SW"
    for t in ("PRN", "AUX", "COM0", "COM9", "LPT1"):
        assert pp._safe_name(t) == t + "_"
    # Not reserved: the stem must match exactly, not just start with one.
    for t in ("CONE", "COMP", "NULL", "AUXO", "COM10", "ROG.SW", "XCON"):
        assert pp._safe_name(t) == t
    assert pp.page_url("CON").endswith("/preview/CON_.html")


def test_ticktick_link_and_page_writer_agree_on_the_url():
    for t in ("ISRG", "ROG.SW", "2715.HK", "BRK/B"):
        assert tt.preview_url(t) == pp.page_url(t)


def test_manifest_survives_runs_and_built_on_reads_it(tmp_path):
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE events (ticker TEXT, quarter TEXT, event_date TEXT, "
                 "eps_estimate REAL, eps_actual REAL, rev_estimate REAL, rev_actual REAL)")
    pp.write_preview_pages([_row("AAA")], conn, tmp_path)
    pp.write_preview_pages([_row("BBB")], conn, tmp_path)
    idx = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "AAA" in idx and "BBB" in idx          # first run's page still listed
    today = __import__("datetime").datetime.utcnow().date().isoformat()
    assert pp.built_on(tmp_path, today) == {"AAA": "2026-10-20", "BBB": "2026-10-20"}
    assert pp.built_on(tmp_path, "1999-01-01") == {}


def test_task_body_carries_the_preview_link_so_a_report_rewrite_keeps_it():
    body = tt.build_task_content(ticker="ISRG", hour="amc", eps_actual=2.9,
                                 eps_estimate=2.69, tier=1)
    assert tt.preview_line("ISRG") in body


class _Resp:
    status_code = 200


def test_note_line_appends_to_desc_without_erasing_jps_notes(monkeypatch):
    task = {"content": "old body", "desc": "JP typed this", "items": [{"title": "x"}],
            "status": 0}
    posted = {}
    monkeypatch.setattr(tt, "_find_task_in_project", lambda *a: dict(task))
    monkeypatch.setattr(tt.requests, "post",
                        lambda url, headers, json, timeout: posted.update(json) or _Resp())
    assert tt.ensure_note_line("tok", "L", "T", "Earnings preview: u") is True
    assert posted["desc"].startswith("JP typed this")
    assert posted["desc"].endswith("Earnings preview: u")
    assert posted["content"].endswith("Earnings preview: u")


def test_note_line_is_idempotent(monkeypatch):
    task = {"content": "a\n\nEarnings preview: u", "desc": "", "status": 0}
    monkeypatch.setattr(tt, "_find_task_in_project", lambda *a: dict(task))
    called = []
    monkeypatch.setattr(tt.requests, "post", lambda *a, **k: called.append(1))
    assert tt.ensure_note_line("tok", "L", "T", "Earnings preview: u") is None
    assert not called


def test_links_block_caps_and_links_index():
    rows = [_row(f"T{i}") for i in range(40)]
    txt = pp.links_context_block(rows)["elements"][0]["text"]
    assert "+10 more" in txt and "index.html|all previews" in txt and len(txt) < 3000


def _h(q, eps_est, eps_act, rev_est=100.0, rev_act=101.0):
    return (q, "2026-01-01", eps_est, eps_act, rev_est, rev_act)


def test_actionable_notes_flag_a_big_gap_to_the_market_and_repeated_recent_misses():
    # JP 2026-11-04: note previews that may be more actionable.
    quiet = [_h("Q2", 1.0, 1.1), _h("Q1", 1.0, 1.1), _h("Q4", 1.0, 0.9), _h("Q3", 1.0, 1.2)]
    assert pp.actionable_notes(quiet, 55.0, 10.0) == []                  # 45pp gap, 1 old miss: nothing
    assert pp.actionable_notes(quiet, 60.0, 10.0) == ["Strong this year: +60% vs S&P 500 +10%"]
    assert pp.actionable_notes(quiet, -40.0, 10.0) == ["Weak this year: -40% vs S&P 500 +10%"]
    assert pp.actionable_notes(quiet, None, 10.0) == []                  # no price: no claim
    last = [_h("Q2", 1.0, 0.9)] + quiet[1:]
    assert pp.actionable_notes(last, 0, 0) == [
        "Missed EPS consensus by more than 2% in 2 of the last 4 reported, the latest included"]
    assert pp.actionable_notes([_h("Q2", 1.0, 0.9)] + [_h("Q1", 1.0, 1.1)] * 3, 0, 0) == []   # one miss: no pattern
    assert pp.actionable_notes([_h("Q2", 1.0, 0.99), _h("Q1", 1.0, 0.9)], 0, 0) == []        # -1% is noise
    old = [_h("Q2", 1.0, 1.1), _h("Q1", 1.0, 0.9), _h("Q4", 1.0, 0.9)]
    assert pp.actionable_notes(old, 0, 0) == []                          # beat the latest: not "recently"
    rev = [_h("Q2", 1.0, 1.1, 100, 97), _h("Q1", 1.0, 1.1, 100, 97), _h("Q4", 1.0, 1.1), _h("Q3", 1.0, 1.1),
           _h("Q2old", 1.0, 1.1, 100, 50)]                              # a 5th quarter is out of the window
    assert pp.actionable_notes(rev, 0, 0) == [
        "Missed revenue consensus by more than 2% in 2 of the last 4 reported, the latest included"]
    loss = [_h("Q2", -0.17, -0.79), _h("Q1", -0.17, -0.79)]              # a deepening loss is a miss
    assert pp.actionable_notes(loss, 0, 0)[0].startswith("Missed EPS consensus")


def test_the_index_carries_the_note_and_says_what_a_blank_means(tmp_path):
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE events (ticker TEXT, quarter TEXT, event_date TEXT, "
                 "eps_estimate REAL, eps_actual REAL, rev_estimate REAL, rev_actual REAL)")
    conn.execute("INSERT INTO events VALUES ('AAA','Q2','2026-07-20',1.0,0.8,100,101)")
    conn.execute("INSERT INTO events VALUES ('AAA','Q1','2026-04-20',1.0,0.8,100,101)")
    (tmp_path / "manifest.json").write_text('{"OLD": {"event_date": "2026-10-01", "company": "Old Co"}}')
    pp.write_preview_pages([_row("AAA"), _row("BBB")], conn, tmp_path, ytd={"AAA": 70.0, "BBB": 5.0, "SPY": 10.0})
    idx = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "Strong this year: +70% vs S&amp;P 500 +10%<br>Missed EPS consensus by more than 2% in 2 of the last 2" in idx
    assert "(built before notes)" in idx and "Not yet included: catalysts" in idx
    m = __import__("json").loads((tmp_path / "manifest.json").read_text())
    assert m["BBB"]["notes"] == [] and "notes" not in m["OLD"]
