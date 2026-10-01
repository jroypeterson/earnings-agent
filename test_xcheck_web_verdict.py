"""Board #515: the company's own announced date must outrank the vendors.

2026-09-30, ABT Q3: Finnhub said Tue Oct 13 (with a `bmo` session, so
`finnhub_confirmed`), yfinance said Wed Oct 21, and the web resolver found
Abbott's own PR Newswire release saying Oct 21 - but downgraded it to medium.
The card's web line quoted the release; its Read line said
"Finnhub likely right (company-confirmed + on cadence)", because
`_xcheck_verdict` never looked at the web result and the "company-confirmed"
label was really Finnhub's own `hour` field. And because the resolver only
ever ran at first detection, the stored date stayed on Oct 13 until a human
replied.

These pin: (1) the Read line follows a medium/high company date, (2) a low one
does not, (3) the Finnhub-hour branch no longer claims company confirmation,
(4) an already-alerted, unanswered row is re-sent to the resolver (once a
day) and a high-confidence match moves the stored date, (5) the trust gate
tolerates a scheme-less URL.
"""
from datetime import date, timedelta

import main
import storage
import web_resolver
from notifications import (
    DisagreementRow,
    _xcheck_verdict,
    build_crosscheck_thread_blocks,
)
from web_resolver import WebVerdict


def _abt_row(**kw):
    base = dict(
        ticker="ABT", company_name="Abbott Laboratories",
        finnhub_date="2026-10-13", yf_dates=[date(2026, 10, 21)], tier=2,
        finnhub_confirmed=True, edgar_ref_date="2025-10-15",
        edgar_finnhub_offset=-2, edgar_yf_offset=6,
    )
    base.update(kw)
    return DisagreementRow(**base)


# --- verdict text ---------------------------------------------------------

def test_abt_medium_company_date_beats_finnhub_session_and_cadence():
    """The exact 2026-09-30 ABT inputs: Finnhub has a session AND is closer to
    cadence, but the company's own release (medium after the trust gate) says
    yfinance's date. The Read line must recommend yfinance."""
    r = _abt_row(web_announced_date="2026-10-21", web_confidence="medium", web_source_verified=True,
                 web_note="medium confidence - Abbott's own release says Oct 21")
    v = _xcheck_verdict(r)
    assert v.startswith("yfinance likely right"), v
    assert "Finnhub likely right" not in v
    assert "lock 2026-10-21" in v
    # And it is what the posted card actually renders.
    text = str(build_crosscheck_thread_blocks(r, date(2026, 9, 30)))
    assert "yfinance likely right" in text
    assert "Finnhub likely right" not in text


def test_high_company_date_matching_finnhub_recommends_finnhub():
    r = _abt_row(finnhub_confirmed=False, edgar_finnhub_offset=9,
                 edgar_yf_offset=1, web_announced_date="2026-10-13",
                 web_confidence="high", web_source_verified=True)
    v = _xcheck_verdict(r)
    assert v.startswith("Finnhub likely right"), v
    assert "company's own announcement" in v


def test_company_date_matching_neither_source_says_so():
    r = _abt_row(web_announced_date="2026-10-15", web_confidence="medium",
                 web_source_verified=True)
    v = _xcheck_verdict(r)
    assert v.startswith("Neither source"), v
    assert "lock 2026-10-15" in v


def test_low_confidence_web_date_does_not_drive_the_read():
    """Low = inferred from aggregators/cadence, the class that is disagreeing."""
    r = _abt_row(web_announced_date="2026-10-21", web_confidence="low")
    v = _xcheck_verdict(r)
    assert v.startswith("Finnhub likely right"), v


def test_finnhub_session_is_not_labelled_company_confirmed():
    v = _xcheck_verdict(_abt_row())
    assert "company-confirmed" not in v
    assert "release session" in v


def test_edgar_still_outranks_web():
    r = _abt_row(edgar_release_date="2026-10-13",
                 web_announced_date="2026-10-21", web_confidence="high")
    assert "EDGAR confirms 2026-10-13" in _xcheck_verdict(r)


# --- trust gate -----------------------------------------------------------

def test_trusted_source_accepts_scheme_less_wire_url():
    assert web_resolver._is_trusted_source(
        "www.prnewswire.com/news-releases/abbott-q3.html")
    assert not web_resolver._is_trusted_source("www.zacks.com/stock/ABT")


# --- re-resolution of an already-alerted, unanswered row -----------------

def _wire(monkeypatch, db_path, fh_date, yf_date, verdict, calls, moves):
    monkeypatch.setattr(main, "init_db", lambda *a, **k: storage.init_db(db_path))
    monkeypatch.setattr(main, "fetch_yfinance_earnings_date",
                        lambda t: [date.fromisoformat(yf_date)])
    monkeypatch.setattr(main, "fetch_yfinance_call_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "infer_cadence_signal", lambda *a, **k: None)
    monkeypatch.setattr(main, "find_earnings_release_filing", lambda *a, **k: None)
    monkeypatch.setattr(main, "find_results_6k", lambda *a, **k: None)
    monkeypatch.setattr(main, "ANTHROPIC_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", None)
    monkeypatch.setattr(main, "SLACK_BOT_TOKEN", None)
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", None)
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "post_slack", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_thread_replies", lambda *a, **k: [])

    def fake_resolve(ticker, *a, **k):
        calls.append(ticker)
        return verdict

    monkeypatch.setattr(main, "resolve_disagreement", fake_resolve)

    def fake_move(conn, cal, ticker, old, new, reason=""):
        moves.append((ticker, old, new))
        # Mirror the real DB effect (calendar is out of scope here).
        conn.execute("UPDATE events SET event_date = ?, date_locked = 1 "
                     "WHERE ticker = ? AND event_date = ?", (new, ticker, old))
        conn.commit()
        return True

    monkeypatch.setattr(main, "_apply_edgar_auto_correction", fake_move)


def _seed_alerted(db_path, fh_date, yf_date, state="open"):
    c = storage.init_db(db_path)
    storage.upsert_event(c, ticker="ABT", event_date=fh_date, event_hour="bmo",
                         gcal_id=None, quarter=storage.date_to_quarter(fh_date),
                         reported=False, tier=2, company_name="Abbott Laboratories")
    # Already alerted with these yfinance dates -> dedup-suppressed.
    c.execute("UPDATE events SET last_xcheck_yf_dates = ?, date_confirmed = 1 "
              "WHERE ticker = 'ABT'",
              (main._yf_dates_signature([date.fromisoformat(yf_date)]),))
    c.commit()
    storage.open_question(c, "ABT", fh_date, thread_ts="1.1", kind="xcheck",
                          first_seen_iso=date.today().isoformat(), channel_id="C1")
    if state != "open":
        storage.update_question_state(c, "ABT", fh_date, state,
                                      snooze_until_iso="2099-01-01")
    c.close()


def _dates(db_path):
    c = storage.init_db(db_path)
    try:
        return c.execute("SELECT event_date, date_locked FROM events "
                         "WHERE ticker = 'ABT'").fetchall()
    finally:
        c.close()


def test_open_question_is_re_resolved_and_high_match_moves_the_date(
        monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "high", "https://www.prnewswire.com/x", "own release", True),
          calls, moves)

    main.run_cross_check(dry_run=False)
    assert calls == ["ABT"]
    assert moves == [("ABT", fh, yf)]
    assert _dates(db) == [(yf, 1)]


def test_re_resolution_runs_at_most_once_a_day(monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "medium", "https://abbott.example/x", "maybe"),
          calls, moves)

    main.run_cross_check(dry_run=False)
    main.run_cross_check(dry_run=False)
    assert calls == ["ABT"]          # second run same day: not re-sent
    assert moves == []               # medium never moves the stored date
    assert _dates(db) == [(fh, 0)]


def test_snoozed_question_is_not_re_resolved(monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf, state="snoozed")
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "high", "https://www.prnewswire.com/x", "own", True),
          calls, moves)

    main.run_cross_check(dry_run=False)
    assert calls == []
    assert _dates(db) == [(fh, 0)]


def test_re_resolve_matching_finnhub_locks_and_closes_question(
        monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(fh, "high", "https://www.prnewswire.com/x", "own", True),
          calls, moves)

    main.run_cross_check(dry_run=False)
    assert _dates(db) == [(fh, 1)]
    c = storage.init_db(db)
    try:
        snap = storage.get_question_snapshot(c, "ABT", fh)
    finally:
        c.close()
    assert snap["question_state"] == "resolved"


def test_new_disagreement_card_reads_the_web_verdict(monkeypatch, tmp_path):
    """End to end: a first-time disagreement with a MEDIUM company date posts
    a card whose Read line recommends the company's date (the live ABT card)."""
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    c = storage.init_db(db)
    storage.upsert_event(c, ticker="ABT", event_date=fh, event_hour="bmo",
                         gcal_id=None, quarter=storage.date_to_quarter(fh),
                         reported=False, tier=2, company_name="Abbott Laboratories")
    c.execute("UPDATE events SET date_confirmed = 1")
    c.commit()
    c.close()
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "medium", "https://ir.abbott.example/x",
                     "Abbott's own release says it", True),
          calls, moves)
    posted = []
    monkeypatch.setattr(main, "SLACK_BOT_TOKEN", "t")
    monkeypatch.setattr(main, "SLACK_STATUS_CHANNEL_ID", "C1")

    def fake_post(token, channel, blocks=None, text=None, **k):
        posted.append(str(blocks))
        return f"ts{len(posted)}"

    monkeypatch.setattr(main, "slack_post_message", fake_post)
    main.run_cross_check(dry_run=False)
    cards = [p for p in posted if "Read:" in p]
    assert cards, posted
    assert "yfinance likely right" in cards[0]
    assert "Finnhub likely right" not in cards[0]


# --- Codex round 1 findings ------------------------------------------------

def test_recommends_the_exact_date_when_yfinance_has_several_candidates():
    """`lock yf` takes the EARLIEST yfinance candidate; the company date is
    the later one, so the advice must name the date itself."""
    r = _abt_row(yf_dates=[date(2026, 10, 20), date(2026, 10, 21)],
                 web_announced_date="2026-10-21", web_confidence="medium",
                 web_source_verified=True)
    v = _xcheck_verdict(r)
    assert "lock 2026-10-21" in v
    assert "lock yf" not in v


def test_web_evidence_outranks_the_split_day_pattern():
    r = _abt_row(split_day_call_date="2026-10-13",
                 web_announced_date="2026-10-15", web_confidence="high",
                 web_source_verified=True)
    v = _xcheck_verdict(r)
    assert v.startswith("Neither source"), v
    assert "no action needed" not in v
    # ...and the summary header must agree that it needs action.
    from notifications import build_crosscheck_summary_blocks
    hdr = str(build_crosscheck_summary_blocks([r], date(2026, 9, 30)))
    assert "no action needed" not in hdr
    assert "Source disagreement" in hdr


def test_citation_gate_accepts_scheme_less_url():
    assert web_resolver._url_was_cited(
        "www.prnewswire.com/news-releases/x.html",
        {"https://www.prnewswire.com/news-releases/x.html"})


def test_yfinance_move_on_re_resolve_closes_the_carried_question(
        monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "high", "https://www.prnewswire.com/x", "own", True),
          calls, moves)
    main.run_cross_check(dry_run=False)
    c = storage.init_db(db)
    try:
        snap = storage.get_question_snapshot(c, "ABT", yf)
    finally:
        c.close()
    assert snap["slack_thread_ts"] == "1.1"   # fixture really carried it
    assert snap["question_state"] == "resolved"


def _capture_thread_posts(monkeypatch):
    posts = []
    monkeypatch.setattr(main, "SLACK_BOT_TOKEN", "t")

    def fake_post(token, channel, blocks=None, text=None, thread_ts=None, **k):
        posts.append((channel, thread_ts, text))
        return "ts"

    monkeypatch.setattr(main, "slack_post_message", fake_post)
    return posts


def test_re_resolve_third_date_is_replied_into_the_thread_once(
        monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    third = (date.today() + timedelta(days=9)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(third, "high", "https://www.prnewswire.com/x", "own", True),
          calls, moves)
    posts = _capture_thread_posts(monkeypatch)
    main.run_cross_check(dry_run=False)
    assert moves == [] and _dates(db) == [(fh, 0)]     # never locked
    assert len(posts) == 1
    channel, thread_ts, text = posts[0]
    assert (channel, thread_ts) == ("C1", "1.1")
    assert third in text and "Neither source" in text
    # Next day, same evidence: re-resolved again but NOT re-posted.
    c = storage.init_db(db)
    storage.kv_set(c, main._web_reresolve_key("ABT", fh), "1999-01-01")
    c.close()
    main.run_cross_check(dry_run=False)
    assert len(calls) == 2
    assert len(posts) == 1


def test_re_resolve_medium_company_date_is_replied_into_the_thread(
        monkeypatch, tmp_path):
    """The live ABT shape on a later day: medium confidence, yfinance's date."""
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "medium", "https://ir.abbott.example/x", "own", True),
          calls, moves)
    posts = _capture_thread_posts(monkeypatch)
    main.run_cross_check(dry_run=False)
    assert len(posts) == 1 and "yfinance likely right" in posts[0][2]
    assert moves == []


# --- Codex round 2 findings ------------------------------------------------

def test_unverified_web_date_is_a_hint_not_lock_advice():
    """The real ABT card: the trust gate downgraded the source. The Read line
    must neither back Finnhub nor advise a lock on unverified evidence."""
    r = _abt_row(web_announced_date="2026-10-21", web_confidence="medium",
                 web_source_verified=False)
    v = _xcheck_verdict(r)
    assert v.startswith("Unresolved"), v
    assert "matches yfinance" in v
    assert "Finnhub likely right" not in v
    assert "lock" not in v


def test_split_day_stays_informational_without_web_evidence():
    from notifications import build_crosscheck_summary_blocks
    r = _abt_row(split_day_call_date="2026-10-13")
    hdr = str(build_crosscheck_summary_blocks([r], date(2026, 9, 30)))
    assert "no action needed" in hdr


def test_failed_card_does_not_gag_later_re_resolution(monkeypatch, tmp_path):
    """Run 1 finds evidence but the card post fails: nothing may be marked
    delivered, so the evidence is still surfaced later."""
    import slack_api
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    c = storage.init_db(db)
    storage.upsert_event(c, ticker="ABT", event_date=fh, event_hour="bmo",
                         gcal_id=None, quarter=storage.date_to_quarter(fh),
                         reported=False, tier=2, company_name="Abbott Laboratories")
    c.close()
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "medium", "https://ir.abbott.example/x", "own", True),
          calls, moves)
    monkeypatch.setattr(main, "SLACK_BOT_TOKEN", "t")
    monkeypatch.setattr(main, "SLACK_STATUS_CHANNEL_ID", "C1")

    def boom(*a, **k):
        raise slack_api.SlackAPIError("down")

    monkeypatch.setattr(main, "slack_post_message", boom)
    import pytest
    with pytest.raises(RuntimeError):
        main.run_cross_check(dry_run=False)
    c = storage.init_db(db)
    try:
        assert storage.kv_get(c, main._web_reresolve_note_key("ABT", fh)) is None
    finally:
        c.close()


def _stub_resolver(monkeypatch, confidence, source_url, cited_urls):
    import anthropic

    class _Text:
        type = "text"
        text = ('{"announced_date": "2026-10-21", "confidence": "%s", '
                '"source_url": "%s", "note": "n"}' % (confidence, source_url))

    class _Hit:
        def __init__(self, u):
            self.url = u

    class _Search:
        type = "web_search_tool_result"
        content = [_Hit(u) for u in cited_urls]

    class _Resp:
        content = [_Search(), _Text()]

    class _Client:
        def __init__(self, **kw):
            self.messages = type("M", (), {"create": lambda self, **k: _Resp()})()

    monkeypatch.setattr(web_resolver, "ANTHROPIC_API_KEY", "k")
    monkeypatch.setattr(anthropic, "Anthropic", _Client)
    return web_resolver.resolve_disagreement(
        "ABT", "Abbott", "2026-10-13", [date(2026, 10, 21)], date(2026, 9, 30))


def test_resolver_marks_a_cited_wire_source_verified_at_medium(monkeypatch):
    u = "https://www.prnewswire.com/news-releases/abbott-q3.html"
    v = _stub_resolver(monkeypatch, "medium", u, [u])
    assert v.source_verified is True


def test_resolver_leaves_an_aggregator_source_unverified(monkeypatch):
    u = "https://www.zacks.com/stock/ABT"
    v = _stub_resolver(monkeypatch, "high", u, [u])
    assert v.confidence == "medium"
    assert v.source_verified is False


def test_resolver_leaves_an_uncited_wire_source_unverified(monkeypatch):
    v = _stub_resolver(monkeypatch, "medium",
                       "https://www.prnewswire.com/x", ["https://other.example/y"])
    assert v.source_verified is False


def test_delivered_card_evidence_is_not_repeated_by_re_resolution(
        monkeypatch, tmp_path):
    """Day 1 posts the card WITH the company date; day 2's re-resolve finds the
    same evidence and must not reply it into the thread a second time."""
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    c = storage.init_db(db)
    storage.upsert_event(c, ticker="ABT", event_date=fh, event_hour="bmo",
                         gcal_id=None, quarter=storage.date_to_quarter(fh),
                         reported=False, tier=2, company_name="Abbott Laboratories")
    c.close()
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "medium", "https://ir.abbott.example/x", "own", True),
          calls, moves)
    monkeypatch.setattr(main, "SLACK_STATUS_CHANNEL_ID", "C1")
    posts = _capture_thread_posts(monkeypatch)
    main.run_cross_check(dry_run=False)
    n_day1 = len(posts)
    assert n_day1 == 2                       # summary + card
    c = storage.init_db(db)
    storage.kv_set(c, main._web_reresolve_key("ABT", fh), "1999-01-01")
    c.close()
    main.run_cross_check(dry_run=False)
    assert len(calls) == 2                   # really re-resolved on day 2
    assert len(posts) == n_day1              # ...but nothing re-posted


# --- Codex round 3 findings ------------------------------------------------

def test_userinfo_url_is_not_trusted_as_the_wire_host():
    u = "https://prnewswire.com:443@evil.example/fake"
    assert not web_resolver._is_trusted_source(u)
    assert not web_resolver._is_trusted_source("ftp://www.prnewswire.com/x")
    # Credentials on a URL are refused outright, even to a trusted host.
    assert not web_resolver._is_trusted_source("https://u:p@www.prnewswire.com/x")
    assert web_resolver._is_trusted_source("https://www.prnewswire.com:443/x")


def test_capped_re_resolution_rotates_instead_of_starving(monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    sig = main._yf_dates_signature([date.fromisoformat(yf)])
    c = storage.init_db(db)
    for tk in ("AAA", "BBB", "CCC"):
        storage.upsert_event(c, ticker=tk, event_date=fh, event_hour="bmo",
                             gcal_id=None, quarter=storage.date_to_quarter(fh),
                             reported=False, tier=2, company_name=tk)
        c.execute("UPDATE events SET last_xcheck_yf_dates = ? WHERE ticker = ?",
                  (sig, tk))
        c.commit()
        storage.open_question(c, tk, fh, thread_ts=f"t{tk}", kind="xcheck",
                              first_seen_iso=date.today().isoformat(),
                              channel_id="C1")
    c.close()
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf, None, calls, moves)
    monkeypatch.setattr(main, "_WEB_RESOLVE_MAX", 2)
    main.run_cross_check(dry_run=False)
    assert calls == ["AAA", "BBB"]
    # Next day: the never-tried CCC must go first.
    c = storage.init_db(db)
    for tk in ("AAA", "BBB"):
        storage.kv_set(c, main._web_reresolve_key(tk, fh), "1999-01-01")
    c.close()
    main.run_cross_check(dry_run=False)
    assert calls[2] == "CCC"


# --- Codex round 4 findings ------------------------------------------------

def test_citation_gate_rejects_a_lookalike_host():
    assert not web_resolver._url_was_cited(
        "www.prnewswire.com",
        {"https://www.prnewswire.com.evil.example/fake"})


def _seed_and_wire_with_replies(monkeypatch, tmp_path, replies_fn):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "high", "https://www.prnewswire.com/x", "own", True),
          calls, moves)
    monkeypatch.setattr(main, "SLACK_BOT_TOKEN", "t")
    monkeypatch.setattr(main, "slack_post_message", lambda *a, **k: "ts")
    monkeypatch.setattr(main, "fetch_thread_replies", replies_fn)
    main.run_cross_check(dry_run=False)
    return db, fh, calls, moves


def test_unread_operator_reply_blocks_re_resolution(monkeypatch, tmp_path):
    from slack_api import SlackReply
    db, fh, calls, moves = _seed_and_wire_with_replies(
        monkeypatch, tmp_path,
        lambda *a, **k: [SlackReply(user="U1", text="lock fh", ts="2.0",
                                    is_bot=False)])
    assert calls == [] and moves == []
    assert _dates(db) == [(fh, 0)]


def test_bot_only_replies_do_not_block_re_resolution(monkeypatch, tmp_path):
    from slack_api import SlackReply
    db, fh, calls, moves = _seed_and_wire_with_replies(
        monkeypatch, tmp_path,
        lambda *a, **k: [SlackReply(user="B", text=":mag:", ts="2.0",
                                    is_bot=True)])
    assert calls == ["ABT"] and len(moves) == 1


def test_unreadable_thread_fails_closed(monkeypatch, tmp_path):
    from slack_api import SlackAPIError

    def boom(*a, **k):
        raise SlackAPIError("ratelimited")

    db, fh, calls, moves = _seed_and_wire_with_replies(monkeypatch, tmp_path, boom)
    assert calls == [] and moves == []


# --- Codex round 5 findings ------------------------------------------------

def test_undeliverable_re_resolution_evidence_fails_the_run_and_retries(
        monkeypatch, tmp_path):
    import pytest
    from slack_api import SlackAPIError
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "medium", "https://ir.abbott.example/x", "own", True),
          calls, moves)
    monkeypatch.setattr(main, "SLACK_BOT_TOKEN", "t")

    def boom(*a, **k):
        raise SlackAPIError("down")

    monkeypatch.setattr(main, "slack_post_message", boom)
    with pytest.raises(RuntimeError, match="re-resolution"):
        main.run_cross_check(dry_run=False)
    c = storage.init_db(db)
    try:
        # Attempt stamp cleared -> eligible again the same day.
        assert main._web_reresolve_eligible(c, "ABT", fh, date.today())
        assert storage.kv_get(c, main._web_reresolve_note_key("ABT", fh)) is None
    finally:
        c.close()


def test_reply_posted_during_the_lookup_blocks_the_move(monkeypatch, tmp_path):
    from slack_api import SlackReply
    seen = {"n": 0}

    def replies(*a, **k):
        seen["n"] += 1          # empty before the lookup, a reply after it
        return [] if seen["n"] == 1 else [
            SlackReply(user="U1", text="lock fh", ts="2.0", is_bot=False)]

    db, fh, calls, moves = _seed_and_wire_with_replies(
        monkeypatch, tmp_path, replies)
    assert calls == ["ABT"]
    assert moves == [] and _dates(db) == [(fh, 0)]


def test_failed_move_on_re_resolve_still_replies_the_evidence(
        monkeypatch, tmp_path):
    db = str(tmp_path / "x.db")
    fh = (date.today() + timedelta(days=5)).isoformat()
    yf = (date.today() + timedelta(days=13)).isoformat()
    _seed_alerted(db, fh, yf)
    calls, moves = [], []
    _wire(monkeypatch, db, fh, yf,
          WebVerdict(yf, "high", "https://www.prnewswire.com/x", "own", True),
          calls, moves)
    monkeypatch.setattr(main, "_apply_edgar_auto_correction",
                        lambda *a, **k: False)
    posts = _capture_thread_posts(monkeypatch)
    main.run_cross_check(dry_run=False)
    assert _dates(db) == [(fh, 0)]
    assert len(posts) == 1 and "yfinance likely right" in posts[0][2]
