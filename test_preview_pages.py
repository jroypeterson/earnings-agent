"""Public preview pages + the TickTick note link (JP 2026-10-15)."""
from datetime import date

import preview_pages as pp
import ticktick as tt
from consensus_preview import PreviewRow


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
