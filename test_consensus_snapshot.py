"""Board #298 Phase A: the pre-print ANNUAL consensus snapshot.

The guidance-vs-consensus square (Phase B) must compare a guide against the
consensus as it stood BEFORE the release. FMP keeps no history, so a snapshot
that was not taken is lost for good -- this phase takes and stores them. No
rendering changes; nothing here touches the Slack output.

Every test is offline. Fetchers are stubs and FMP transport is stubbed, so
nothing here can spend a metered call.
"""

import sqlite3
import urllib.error
from types import SimpleNamespace
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

TODAY = date(2026, 9, 17)
NOW = datetime(2026, 9, 17, 11, 20, tzinfo=timezone.utc)


def _mod():
    # Imported per test so each test fails on its own (not as one collection
    # error) against code that predates the module.
    import consensus_snapshot
    return consensus_snapshot


def _db(path=":memory:"):
    from storage import init_db
    return init_db(path)


def _event(conn, ticker, event_date, *, hour="bmo", confirmed=1, reported=0,
           closed=None, hour_yf=None):
    conn.execute(
        "INSERT INTO events (ticker, event_date, event_hour, date_confirmed, "
        "reported, closed_reason, event_hour_yf, tier) VALUES (?,?,?,?,?,?,?,1)",
        (ticker, event_date, hour, confirmed, reported, closed, hour_yf),
    )
    conn.commit()


def _consensus(ticker, fpe, rev=1.0e9, eps=2.5, n_rev=5, n_eps=6, cur="USD"):
    from consensus_preview import AnnualConsensus
    return AnnualConsensus(
        ticker=ticker, fiscal_period_end=fpe, currency=cur,
        revenue_avg=rev, revenue_low=None, revenue_high=None,
        revenue_analysts=n_rev, eps_avg=eps, eps_low=None, eps_high=None,
        eps_analysts=n_eps,
    )


class RecordingFetcher:
    def __init__(self, by_ticker=None):
        self.calls = []
        self.by_ticker = by_ticker or {}

    def __call__(self, ticker):
        self.calls.append(ticker)
        if ticker in self.by_ticker:
            return self.by_ticker[ticker]
        return ([_consensus(ticker, "2026-12-31"),
                 _consensus(ticker, "2027-12-31")], "ok")


# ---------------------------------------------------------------------------
# 1. Schema v14 -- fresh CREATE and migration path produce the same table
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {
    "ticker", "fiscal_period_end", "taken_at", "event_date", "revenue_avg",
    "eps_avg", "num_analysts_revenue", "num_analysts_eps", "currency",
    "fetch_status",
}


def _table_shape(conn):
    return sorted(
        (r[1], r[2].upper(), r[5])  # name, declared type, pk position
        for r in conn.execute("PRAGMA table_info(consensus_snapshot)")
    )


def test_schema_v14_fresh_and_migrated_paths_agree(tmp_path):
    import storage

    assert storage.CURRENT_SCHEMA_VERSION == 14

    # Path A: a fresh database.
    fresh = _db()
    fresh_shape = _table_shape(fresh)
    assert {c for c, _t, _pk in fresh_shape} == EXPECTED_COLUMNS
    # PK is (ticker, fiscal_period_end, taken_at) -- event_date is provenance.
    assert [c for c, _t, pk in sorted(fresh_shape, key=lambda r: r[2]) if pk] == [
        "ticker", "fiscal_period_end", "taken_at"]

    # Path B: a real v13 database on disk, migrated up by init_db.
    path = tmp_path / "v13.db"
    conn = _db(path)
    conn.execute("DROP TABLE IF EXISTS consensus_snapshot")
    conn.execute("DELETE FROM schema_version WHERE version >= 14")
    conn.execute("INSERT OR IGNORE INTO schema_version VALUES (13, datetime('now'))")
    conn.commit()
    conn.close()
    assert "consensus_snapshot" not in {
        r[0] for r in sqlite3.connect(path).execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}

    migrated = _db(path)
    assert _table_shape(migrated) == fresh_shape
    assert storage._get_schema_version(migrated) == 14


# ---------------------------------------------------------------------------
# 2. Window selection
# ---------------------------------------------------------------------------

def test_window_selects_only_pre_release_open_rows_and_fetches_each_ticker_once():
    conn = _db()
    _event(conn, "AAA", "2026-09-18")                        # +1  in
    _event(conn, "AAA", "2026-09-20")                        # dup ticker, fetch once
    _event(conn, "BBB", "2026-09-21")                        # +4  in (inclusive)
    _event(conn, "CCC", "2026-09-22")                        # +5  out
    _event(conn, "DDD", "2026-09-17", hour="amc", hour_yf="amc")  # today AMC, corroborated, in
    _event(conn, "DDX", "2026-09-17", hour="amc")            # today AMC, single-source: out (r18)
    _event(conn, "EEE", "2026-09-17", hour="bmo")            # today BMO out (released)
    _event(conn, "FFF", "2026-09-29", hour="", confirmed=0)  # +12 unconfirmed in (+10d)
    _event(conn, "GGG", "2026-10-02", hour="", confirmed=0)  # +15 unconfirmed out
    _event(conn, "HHH", "2026-09-19", reported=1)            # reported out
    _event(conn, "III", "2026-09-19", closed="delisted")     # closed out
    _event(conn, "JJJ", "2026-09-16")                        # past out

    fetcher = RecordingFetcher()
    summary = _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)

    assert sorted(fetcher.calls) == ["AAA", "BBB", "DDD", "FFF"]
    rows = conn.execute(
        "SELECT ticker, fiscal_period_end, taken_at, event_date, fetch_status "
        "FROM consensus_snapshot ORDER BY ticker, fiscal_period_end").fetchall()
    assert len(rows) == 8  # 4 tickers x 2 fiscal periods, one row each
    assert {r[4] for r in rows} == {"ok"}
    # event_date is the NEAREST open event in the window (provenance only).
    assert {(r[0], r[3]) for r in rows if r[0] == "AAA"} == {("AAA", "2026-09-18")}
    assert summary["tickers"] == 4 and summary["ok"] == 4


def test_empty_window_makes_zero_fetcher_calls():
    conn = _db()
    _event(conn, "CCC", "2026-09-22")                        # outside
    _event(conn, "EEE", "2026-09-17", hour="bmo")            # released

    def must_not_be_called(ticker):
        raise AssertionError(f"fetcher called for {ticker} on an empty window")

    summary = _mod().snapshot_annual_consensus(
        conn, TODAY, must_not_be_called, now=NOW)
    assert summary["tickers"] == 0
    assert conn.execute("SELECT COUNT(*) FROM consensus_snapshot").fetchone()[0] == 0


def test_today_amc_is_dropped_once_its_cutoff_has_passed():
    """A today-AMC name is fetched only while 16:00 ET is still ahead, and not
    at all when yfinance disputes the hour -- or, since Codex round 18, does
    not corroborate it (cutoff 00:00 ET). A snapshot the read side would
    reject is a wasted metered call."""
    conn = _db()
    _event(conn, "DDD", "2026-09-17", hour="amc", hour_yf="amc")
    _event(conn, "YYY", "2026-09-17", hour="amc", hour_yf="bmo")
    _event(conn, "ZZZ", "2026-09-17", hour="amc")

    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)  # 07:20 EDT
    assert fetcher.calls == ["DDD"]

    fetcher = RecordingFetcher()
    late = datetime(2026, 9, 17, 20, 30, tzinfo=timezone.utc)       # 16:30 EDT
    _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=late)
    assert fetcher.calls == []


# ---------------------------------------------------------------------------
# 3. A 429 and an empty list are DISTINGUISHABLE
# ---------------------------------------------------------------------------

def _http_error(code):
    return urllib.error.HTTPError("https://fmp/x", code, "err", {}, None)


def test_checked_fetcher_distinguishes_429_from_empty(monkeypatch):
    import consensus_preview as cp

    def raise_429(url, *a, **k):
        raise _http_error(429)

    rows, status = cp.fetch_fmp_annual_estimates_checked("X", "KEY", get_json=raise_429)
    assert (rows, status) == ([], "error:429")

    rows, status = cp.fetch_fmp_annual_estimates_checked(
        "X", "KEY", get_json=lambda url, *a, **k: [])
    assert (rows, status) == ([], "empty")

    rows, status = cp.fetch_fmp_annual_estimates_checked(
        "X", "KEY", get_json=lambda url, *a, **k: [{"date": "2026-12-31", "epsAvg": 1.0}])
    assert status == "ok" and rows[0].fiscal_period_end == "2026-12-31"

    # FMP answers some refusals with HTTP 200 + an error object. Not coverage.
    rows, status = cp.fetch_fmp_annual_estimates_checked(
        "X", "KEY", get_json=lambda url, *a, **k: {"Error Message": "Limit Reach"})
    assert (rows, status) == ([], "error:shape")

    # The never-raise legacy API is unchanged for its existing callers.
    monkeypatch.setattr(cp, "_fmp_get_json", raise_429)
    assert cp.fetch_fmp_annual_estimates("X", "KEY") == []


def test_snapshot_rows_record_429_and_empty_differently():
    conn = _db()
    _event(conn, "RATE", "2026-09-18")
    _event(conn, "NONE", "2026-09-18")
    _event(conn, "GOOD", "2026-09-18")
    fetcher = RecordingFetcher({
        "RATE": ([], "error:429"),
        "NONE": ([], "empty"),
    })
    summary = _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)

    status = dict(conn.execute(
        "SELECT ticker, fetch_status FROM consensus_snapshot "
        "WHERE ticker IN ('RATE','NONE')").fetchall())
    assert status == {"RATE": "error:429", "NONE": "empty"}
    assert summary["errors"] == 1 and summary["empty"] == 1 and summary["ok"] == 1
    # A status-only row can never be mistaken for a consensus figure.
    mod = _mod()
    assert mod.latest_pre_release_snapshot(
        conn, "RATE", "2026-12-31", datetime(2027, 1, 1, tzinfo=timezone.utc)) is None


def test_pacer_backs_off_5_15_30_on_429_then_gives_up():
    sleeps = []
    calls = []

    def always_429(url):
        calls.append(url)
        raise _http_error(429)

    pacer = _mod().FmpPacer(always_429, sleep=sleeps.append, clock=lambda: 1000.0)
    with pytest.raises(urllib.error.HTTPError) as exc:
        pacer("u")
    assert exc.value.code == 429
    assert len(calls) == 4
    assert [s for s in sleeps if s >= 1] == [5, 15, 30]


def test_pacer_recovers_after_a_429_and_does_not_retry_other_errors():
    sleeps = []
    seq = [_http_error(429), [{"ok": 1}]]

    def flaky(url):
        item = seq.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    pacer = _mod().FmpPacer(flaky, sleep=sleeps.append, clock=lambda: 1000.0)
    assert pacer("u") == [{"ok": 1}]
    assert [s for s in sleeps if s >= 1] == [5]

    def forbidden(url):
        raise _http_error(403)

    sleeps.clear()
    pacer = _mod().FmpPacer(forbidden, sleep=sleeps.append, clock=lambda: 1000.0)
    with pytest.raises(urllib.error.HTTPError):
        pacer("u")
    assert [s for s in sleeps if s >= 1] == []


def test_pacer_holds_four_requests_per_second():
    t = [0.0]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        t[0] += s

    pacer = _mod().FmpPacer(lambda url: [], sleep=sleep, clock=lambda: t[0])
    for _ in range(5):
        pacer("u")
    # 5 calls at 4/s span at least 1.0 s.
    assert t[0] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# 4. latest_pre_release_snapshot -- keyed on the fiscal period, not event_date
# ---------------------------------------------------------------------------

def _snap(conn, ticker, fpe, taken_at, event_date, eps, status="ok"):
    conn.execute(
        "INSERT INTO consensus_snapshot (ticker, fiscal_period_end, taken_at, "
        "event_date, revenue_avg, eps_avg, num_analysts_revenue, "
        "num_analysts_eps, currency, fetch_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ticker, fpe, taken_at, event_date, 1e9, eps, 5, 5, "USD", status),
    )
    conn.commit()


def test_latest_pre_release_ignores_post_cutoff_same_day_row_and_event_date():
    mod = _mod()
    conn = _db()
    # Taken before FMP moved the date (event_date 09-20 -> 09-17): still pre-print.
    _snap(conn, "XYZ", "2026-12-31", "2026-09-10T15:00:00Z", "2026-09-20", 1.00)
    # Same day, 10:00 EDT: the latest pre-print figure.
    _snap(conn, "XYZ", "2026-12-31", "2026-09-17T14:00:00Z", "2026-09-17", 1.10)
    # Same day, 17:00 EDT -- after an AMC release. Must be ignored.
    _snap(conn, "XYZ", "2026-12-31", "2026-09-17T21:00:00Z", "2026-09-17", 9.99)
    # Other fiscal period, same batch -- never returned for this one. (Was
    # 15:00Z, a batch of its own holding only FY2027; one fetch writes every
    # period at ONE taken_at, and Codex r3 makes the newest batch define the
    # period set, so the fixture now matches what the writer produces.)
    _snap(conn, "XYZ", "2027-12-31", "2026-09-17T14:00:00Z", "2026-09-17", 7.77)

    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", "amc", 1)
    got = mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)
    assert got["eps_avg"] == 1.10
    assert got["taken_at"] == "2026-09-17T14:00:00Z"

    # A BMO cutoff (00:00 ET) excludes both same-day rows, and the pre-move
    # row is returned although its event_date no longer matches anything.
    cutoff = mod.pre_release_cutoff("2026-09-17", "bmo", None, 1)
    got = mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)
    assert got["eps_avg"] == 1.00 and got["event_date"] == "2026-09-20"


def test_latest_pre_release_none_when_every_row_is_post_cutoff():
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-09-17T21:00:00Z", "2026-09-17", 9.99)
    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", "amc", 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None


# ---------------------------------------------------------------------------
# 5. The cutoff rule (H1-NEW)
# ---------------------------------------------------------------------------

EDT_MIDNIGHT = datetime(2026, 9, 17, 4, 0, tzinfo=timezone.utc)
EDT_1600 = datetime(2026, 9, 17, 20, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("hour, hour_yf, confirmed, expected", [
    ("amc", None, 1, EDT_MIDNIGHT),  # single-source AMC label: not enough (r18)
    ("amc", "", 1, EDT_MIDNIGHT),    # empty second opinion: same
    ("amc", "amc", 1, EDT_1600),   # agreement
    ("AMC", "amc", 1, EDT_1600),   # case is not a disagreement
    ("amc", "bmo", 1, EDT_MIDNIGHT),  # both non-empty AND differ
    ("amc", None, 0, EDT_MIDNIGHT),   # unconfirmed date
    ("bmo", None, 1, EDT_MIDNIGHT),
    ("dmh", None, 1, EDT_MIDNIGHT),
    ("", "amc", 1, EDT_MIDNIGHT),     # unknown Finnhub hour is not AMC
    (None, None, 0, EDT_MIDNIGHT),
])
def test_cutoff_rule(hour, hour_yf, confirmed, expected):
    got = _mod().pre_release_cutoff("2026-09-17", hour, hour_yf, confirmed)
    assert got == expected
    assert got.tzinfo is not None


def test_a_single_source_AMC_label_does_not_admit_a_day_of_snapshot():
    """Codex round 18, and a deliberate REVERSAL of the rule this test used to
    pin ("a NULL event_hour_yf keeps the AMC cutoff").

    `date_confirmed` confirms the DATE, not the session. With one AMC label and
    no second opinion, a company that actually printed before the open has a
    07:13 ET capture that may already carry the post-print consensus (FIVE's
    moved 10% onto the guide) -- stored and served as pre-print, with nothing
    to betray it. The prior day's capture is what must be returned instead.
    """
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-09-16T19:23:00Z", "2026-09-17", 1.00)  # prior day
    _snap(conn, "XYZ", "2026-12-31", "2026-09-17T11:20:00Z", "2026-09-17", 10.25)  # day-of
    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", None, 1)
    assert cutoff == EDT_MIDNIGHT
    got = mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)
    assert got["eps_avg"] == 1.00, got
    # ...and a CORROBORATED AMC keeps the day-of capture, the freshest there is.
    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", "amc", 1)
    assert cutoff == EDT_1600
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)["eps_avg"] == 10.25


def test_cutoff_uses_zoneinfo_across_dst():
    # 2026-11-10 is EST (UTC-5): 16:00 ET = 21:00 UTC, not 20:00.
    got = _mod().pre_release_cutoff("2026-11-10", "amc", "amc", 1)
    assert got == datetime(2026, 11, 10, 21, 0, tzinfo=timezone.utc)
    got = _mod().pre_release_cutoff("2026-11-10", "bmo", None, 1)
    assert got == datetime(2026, 11, 10, 5, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 6. The CI step -- source scan of the workflow
# ---------------------------------------------------------------------------

WORKFLOW = Path(__file__).resolve().parent / ".github" / "workflows" / "daily_earnings_check.yml"


def _steps():
    """Split the job's steps on their `- name:` lines (no YAML dependency --
    CI installs requirements-dev.txt, which has no PyYAML)."""
    import re
    text = WORKFLOW.read_text(encoding="utf-8")
    parts = re.split(r"(?m)^      - name: ", text)[1:]
    return [(p.splitlines()[0].strip(), p) for p in parts]


def test_workflow_runs_the_snapshot_step_after_the_sync_with_an_alert():
    import re
    steps = _steps()
    names = [n for n, _ in steps]
    idx = [i for i, (_n, body) in enumerate(steps)
           if "main.py --snapshot-consensus" in body]
    assert len(idx) == 1, "exactly one step must run --snapshot-consensus"
    i = idx[0]
    body = steps[i][1]

    assert i > names.index("Run earnings agent")
    assert i < names.index("Save earnings database"), \
        "must run before the DB is saved, or the snapshots are never persisted"
    assert "FMP_API_KEY: ${{ secrets.FMP_API_KEY }}" in body
    assert re.search(r"(?m)^\s+continue-on-error: true\s*$", body)
    step_id = re.search(r"(?m)^\s+id: (\S+)\s*$", body)
    assert step_id, "the step needs an id so its alert can read its outcome"

    # continue-on-error turns the step's CONCLUSION into success, so a bare
    # `if: failure()` after it never fires. The alert must read the OUTCOME.
    alerts = [
        (n, b) for n, b in steps[i + 1:]
        # != success AND != skipped, so a step-timeout that GitHub reports as
        # `cancelled` still alerts (Fable code review).
        if f"steps.{step_id.group(1)}.outcome != 'success'" in b
        and f"steps.{step_id.group(1)}.outcome != 'skipped'" in b
    ]
    assert alerts, "no alert step fires on the snapshot step's failure"
    alert_text = "\n".join(b for _n, b in alerts)
    assert "SLACK_WEBHOOK" in alert_text and "curl" in alert_text
    # The out-of-band email is INLINED (runner python3 + stdlib), never a
    # call into scripts/ -- CLAUDE.md's failure-alert convention.
    assert "smtplib" in alert_text and "send_failure_email" not in alert_text
    # An alert that itself fails must not skip the DB save that follows.
    for _n, b in alerts:
        assert re.search(r"(?m)^\s+continue-on-error: true\s*$", b)
        assert names.index(_n) < names.index("Save earnings database")


# ---------------------------------------------------------------------------
# 7. The CLI runner -- offline, every FMP and Slack path stubbed
# ---------------------------------------------------------------------------

class _NoClose:
    """init_db stand-in whose close() is a no-op, so the test can inspect it."""
    def __init__(self, conn):
        self._c = conn

    def __getattr__(self, name):
        return getattr(self._c, name)

    def close(self):
        pass


def _runner_env(monkeypatch, fetcher=None, key="KEY"):
    import consensus_snapshot
    import main

    conn = _db()
    # A window that is non-empty whatever today's date is.
    far = date.today().toordinal() + 2
    _event(conn, "AAA", date.fromordinal(far).isoformat())
    _event(conn, "BBB", date.fromordinal(far).isoformat())
    monkeypatch.setattr(main, "init_db", lambda *a, **k: _NoClose(conn))
    monkeypatch.setattr(main, "FMP_API_KEY", key)
    posts = []
    monkeypatch.setattr(main, "post_slack", lambda *a, **k: posts.append(a))
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", "https://hooks.invalid/x")

    def no_real_fmp(*a, **k):
        if fetcher is None:
            raise AssertionError("a real FMP fetcher was built")
        return fetcher
    monkeypatch.setattr(consensus_snapshot, "make_fmp_fetcher", no_real_fmp)
    return main, conn, posts


def test_runner_dry_run_makes_zero_fmp_calls(monkeypatch):
    main, conn, posts = _runner_env(monkeypatch, fetcher=None)
    out = main.run_snapshot_consensus(dry_run=True)
    assert out["tickers"] == 2 and out["dry_run"]
    assert conn.execute("SELECT COUNT(*) FROM consensus_snapshot").fetchone()[0] == 0
    assert posts == []


def test_runner_exits_loudly_without_a_key(monkeypatch):
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=None, key="")
    with pytest.raises(SystemExit):
        main.run_snapshot_consensus()


def test_runner_partial_failure_posts_status_but_succeeds(monkeypatch):
    fetcher = RecordingFetcher({"BBB": ([], "error:429")})
    main, conn, posts = _runner_env(monkeypatch, fetcher=fetcher)
    out = main.run_snapshot_consensus()
    assert out["ok"] == 1 and out["errors"] == 1
    assert len(posts) == 1 and "error:429" in posts[0][2]


def test_runner_fails_when_a_real_window_returns_NO_consensus_at_all(monkeypatch):
    """Codex round 19. An HTTP-200 `[]` for every ticker (plan change, endpoint
    regression) counted as `empty`, never as an error, so a run that captured
    nothing passed as healthy -- no Slack, no email, green step."""
    from datetime import date
    names = ("AAA", "BBB", "CCC", "DDD", "EEE")
    fetcher = RecordingFetcher({t: ([], "empty") for t in names})
    main, conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    far = date.fromordinal(date.today().toordinal() + 2).isoformat()
    for t in names[2:]:
        _event(conn, t, far)
    with pytest.raises(RuntimeError, match="systemic"):
        main.run_snapshot_consensus()


def test_runner_a_SMALL_all_empty_window_is_not_an_alarm(monkeypatch):
    """The floor's other half: two genuinely uncovered names are a fact about
    those companies, and must not page anyone."""
    fetcher = RecordingFetcher({"AAA": ([], "empty"), "BBB": ([], "empty")})
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    out = main.run_snapshot_consensus()
    assert out["ok"] == 0 and out["empty"] == 2


def test_runner_an_UNDELIVERABLE_partial_failure_notice_fails_the_step(monkeypatch):
    """Codex round 19. The partial-failure notice was best-effort: a failed post
    was a log line and the step stayed green, so the workflow's own Slack and
    email steps (keyed on the step outcome) never ran either."""
    fetcher = RecordingFetcher({"BBB": ([], "error:429")})
    main, conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)

    def dead_webhook(*a, **k):
        raise RuntimeError("410 gone")
    monkeypatch.setattr(main, "post_slack", dead_webhook)
    with pytest.raises(RuntimeError, match="could not be delivered"):
        main.run_snapshot_consensus()
    # ...and the captured row is still kept (the export runs in `finally`).
    assert conn.execute(
        "SELECT COUNT(*) FROM consensus_snapshot WHERE fetch_status = 'ok'").fetchone()[0] > 0


def test_runner_raises_when_every_fetch_failed(monkeypatch):
    fetcher = RecordingFetcher({"AAA": ([], "error:401"), "BBB": ([], "error:401")})
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    with pytest.raises(RuntimeError, match="systemic"):
        main.run_snapshot_consensus()


# ---------------------------------------------------------------------------
# Codex round 1 (2026-09-17): per-fetch stamp, post-cutoff refusal, breaker
# ---------------------------------------------------------------------------

class _Clock:
    """Advances by `step` seconds on every call."""
    def __init__(self, start, step):
        self.t, self.step = start, step

    def __call__(self):
        from datetime import timedelta
        cur = self.t
        self.t = self.t + timedelta(seconds=self.step)
        return cur


def test_taken_at_is_the_fetch_time_and_a_post_cutoff_fetch_is_refused():
    """A run that starts before 16:00 ET and whose AMC fetch RETURNS after it
    must not store post-print consensus stamped with the batch start."""
    from datetime import datetime, timezone
    conn = _db()
    _event(conn, "DDD", "2026-09-17", hour="amc", hour_yf="amc")   # cutoff 20:00 UTC (16:00 EDT)
    # clock() calls: budget check 19:59:20, pre-fetch check 19:59:40 (before
    # the cutoff, so the call is made), fetch returns 20:00:00 (at the cutoff).
    start = datetime(2026, 9, 17, 19, 59, 20, tzinfo=timezone.utc)
    clock = _Clock(start, 20)
    summary = _mod().snapshot_annual_consensus(
        conn, TODAY, RecordingFetcher(), now=start, clock=clock)
    rows = conn.execute(
        "SELECT taken_at, fetch_status FROM consensus_snapshot").fetchall()
    # Crossed the cutoff during the call: refused, recorded as a SKIP, stamped
    # at the fetch -- and NOT an error (it must never feed the breaker/alarm).
    assert rows == [(rows[0][0], "skipped:post_cutoff")]
    assert rows[0][0] >= "2026-09-17T20:00:00Z"  # stamped at the fetch, not the start
    assert summary["ok"] == 0 and summary["errors"] == 0
    assert summary["skipped_post_cutoff"] == 1


def test_a_cutoff_passed_before_the_call_spends_no_request_and_is_no_error():
    """Fable code review H: a run straddling 16:00 ET must skip today-AMC names
    whose cutoff has passed WITHOUT a metered call, and without tripping the
    breaker for the future-dated names behind them."""
    from datetime import datetime, timezone
    conn = _db()
    for t in ("A1", "A2", "A3", "A4"):
        _event(conn, t, "2026-09-17", hour="amc", hour_yf="amc")   # cutoff 20:00 UTC
    _event(conn, "ZZZ", "2026-09-18")                # future: must still be fetched
    start = datetime(2026, 9, 17, 19, 59, 30, tzinfo=timezone.utc)
    fetcher = RecordingFetcher()
    summary = _mod().snapshot_annual_consensus(
        conn, TODAY, fetcher, now=start, clock=_Clock(start, 60),
        max_consecutive_errors=2)
    assert "A1" not in fetcher.calls or len([c for c in fetcher.calls if c.startswith("A")]) <= 1
    assert "ZZZ" in fetcher.calls
    assert summary["aborted"] is None and summary["errors"] == 0


def test_ok_rows_are_stamped_per_fetch_not_per_batch():
    from datetime import datetime, timezone
    conn = _db()
    _event(conn, "AAA", "2026-09-18")
    _event(conn, "BBB", "2026-09-19")
    clock = _Clock(NOW, 60)
    _mod().snapshot_annual_consensus(conn, TODAY, RecordingFetcher(), now=NOW, clock=clock)
    stamps = {t: ts for t, ts in conn.execute(
        "SELECT DISTINCT ticker, taken_at FROM consensus_snapshot")}
    assert stamps["AAA"] != stamps["BBB"]


def test_circuit_breaker_stops_after_consecutive_errors():
    conn = _db()
    for t in ("A1", "A2", "A3", "A4", "A5", "A6", "A7"):
        _event(conn, t, "2026-09-18")
    fetcher = RecordingFetcher({t: ([], "error:429")
                                for t in ("A1", "A2", "A3", "A4", "A5", "A6", "A7")})
    summary = _mod().snapshot_annual_consensus(
        conn, TODAY, fetcher, now=NOW, max_consecutive_errors=3)
    assert len(fetcher.calls) == 3
    assert summary["aborted"] and summary["skipped"] == 4


def test_a_success_resets_the_breaker():
    conn = _db()
    for t in ("A1", "A2", "A3", "A4"):
        _event(conn, t, "2026-09-18")
    fetcher = RecordingFetcher({"A1": ([], "error:429"), "A3": ([], "error:429")})
    summary = _mod().snapshot_annual_consensus(
        conn, TODAY, fetcher, now=NOW, max_consecutive_errors=2)
    assert len(fetcher.calls) == 4 and summary["aborted"] is None


def test_run_budget_stops_the_loop():
    conn = _db()
    for t in ("A1", "A2", "A3"):
        _event(conn, t, "2026-09-18")
    fetcher = RecordingFetcher()
    summary = _mod().snapshot_annual_consensus(
        conn, TODAY, fetcher, now=NOW, clock=_Clock(NOW, 400), budget_seconds=500)
    assert summary["aborted"] and "budget" in summary["aborted"]
    assert len(fetcher.calls) < 3


def test_runner_fails_the_step_when_the_breaker_trips(monkeypatch):
    import consensus_snapshot
    monkeypatch.setattr(consensus_snapshot, "MAX_CONSECUTIVE_ERRORS", 1)
    fetcher = RecordingFetcher({"AAA": ([], "error:429"), "BBB": ([], "error:429")})
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    with pytest.raises(RuntimeError, match="ABORTED"):
        main.run_snapshot_consensus()


def test_snapshot_alert_curl_is_bounded_and_the_step_has_a_ceiling():
    wf = (Path(__file__).parent / ".github" / "workflows" /
          "daily_earnings_check.yml").read_text(encoding="utf-8")
    step = wf[wf.index("id: snapshot_consensus"):wf.index("Email backup on consensus snapshot failure")]
    assert "timeout-minutes: 20" in step
    assert "--max-time" in step and "--connect-timeout" in step


# ---------------------------------------------------------------------------
# Codex round 2 (2026-09-22), finding 1: a read is bound to ITS reporting cycle
# ---------------------------------------------------------------------------

def test_a_previous_cycles_snapshot_is_not_served_for_this_event():
    """XYZ had an ok FY2026 snapshot before its Q1 print; the Q2 window was
    missed. The Q1 row is pre-cutoff for Q2 too -- but it is a QUARTER old,
    and serving it as Q2's pre-print consensus is the stale-as-current case."""
    mod = _mod()
    conn = _db()
    _event(conn, "XYZ", "2026-04-22", reported=1)            # Q1, printed
    _snap(conn, "XYZ", "2026-12-31", "2026-04-20T15:00:00Z", "2026-04-22", 1.00)
    cutoff = mod.pre_release_cutoff("2026-07-22", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None


def test_a_snapshot_taken_before_the_previous_print_is_out_of_cycle():
    """Even inside the age bound: a snapshot taken before the ticker's PREVIOUS
    reported release belongs to that release's cycle, not this one."""
    mod = _mod()
    conn = _db()
    _event(conn, "XYZ", "2026-09-01", reported=1)            # previous print
    _snap(conn, "XYZ", "2026-12-31", "2026-08-30T15:00:00Z", "2026-09-01", 1.00)
    cutoff = mod.pre_release_cutoff("2026-09-17", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None
    # A snapshot taken AFTER that print, before this cutoff, is this cycle's.
    _snap(conn, "XYZ", "2026-12-31", "2026-09-15T15:00:00Z", "2026-09-17", 1.20)
    got = mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)
    assert got["eps_avg"] == 1.20


def test_a_newer_empty_attempt_supersedes_an_older_ok_in_the_same_cycle():
    """The newest usable answer for this event is "FMP has no coverage": the
    older figure must not be served over it."""
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-09-14T15:00:00Z", "2026-09-17", 1.00)
    _snap(conn, "XYZ", "", "2026-09-16T15:00:00Z", "2026-09-17", None, status="empty")
    cutoff = mod.pre_release_cutoff("2026-09-17", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None


def test_a_newer_error_attempt_does_not_discard_an_older_ok_in_the_same_cycle():
    """A 429 says nothing about the consensus. An in-cycle, pre-cutoff figure
    from two days earlier is still the pre-print figure (adjudicated: the
    finding asked for ANY non-ok to mean none; an error does not qualify)."""
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-09-14T15:00:00Z", "2026-09-17", 1.00)
    _snap(conn, "XYZ", "", "2026-09-16T15:00:00Z", "2026-09-17", None, status="error:429")
    cutoff = mod.pre_release_cutoff("2026-09-17", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)["eps_avg"] == 1.00


# ---------------------------------------------------------------------------
# Codex round 2, finding 2: a past, still-open same-cycle event blocks a snapshot
# ---------------------------------------------------------------------------

def test_a_future_duplicate_behind_a_past_open_event_is_not_snapshotted():
    """Operator-locked 09-17 AMC row + a vendor duplicate at 09-20. On the 09-18
    run the locked row is past and drops out of the window; if XYZ printed on
    09-17, a fetch for the 09-20 duplicate is POST-print consensus stored as
    pre-print. Fail closed: no fetch, and the skip is on record."""
    conn = _db()
    _event(conn, "XYZ", "2026-09-17", hour="amc")
    conn.execute("UPDATE events SET date_locked = 1 WHERE ticker = 'XYZ'")
    conn.commit()
    _event(conn, "XYZ", "2026-09-20")
    _event(conn, "OK1", "2026-09-20")
    run = datetime(2026, 9, 18, 11, 20, tzinfo=timezone.utc)
    fetcher = RecordingFetcher()
    summary = _mod().snapshot_annual_consensus(conn, date(2026, 9, 18), fetcher, now=run)
    assert fetcher.calls == ["OK1"]
    assert conn.execute(
        "SELECT fetch_status FROM consensus_snapshot WHERE ticker = 'XYZ'"
    ).fetchall() == [("skipped:open_prior_event",)]
    assert summary["skipped_open_prior"] == ["XYZ"]
    assert summary["errors"] == 0
    # The dry-run / listing path applies the same rule.
    assert [t for t, _d in _mod().select_snapshot_window(
        conn, date(2026, 9, 18), run)] == ["OK1"]


def test_a_previous_quarters_unreported_row_does_not_block():
    """A past open row a full quarter back is a different cycle (measured: CTRE
    and HYPR carry a locked May row labelled with the August quarter)."""
    conn = _db()
    _event(conn, "XYZ", "2026-06-18")                  # ~92 days back, never reported
    _event(conn, "XYZ", "2026-09-18")
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)
    assert fetcher.calls == ["XYZ"]


def test_a_same_day_amc_sibling_before_its_cutoff_does_not_block():
    """Today's AMC row is still ahead at 07:20 ET, so it is not a past print."""
    conn = _db()
    _event(conn, "XYZ", "2026-09-17", hour="amc", hour_yf="amc")
    _event(conn, "XYZ", "2026-09-19")
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)
    assert fetcher.calls == ["XYZ"]


# ---------------------------------------------------------------------------
# Codex round 2, finding 3: a malformed HTTP-200 LIST is an error, not coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("body", [
    [{"Error Message": "Limit Reach . Please upgrade your plan"}],
    [{"symbol": "X", "epsAvg": 1.0}],          # rows lacking `date`
    ["not a row"],
])
def test_checked_fetcher_calls_a_malformed_list_an_error(body):
    import consensus_preview as cp
    rows, status = cp.fetch_fmp_annual_estimates_checked(
        "X", "KEY", get_json=lambda url, *a, **k: body)
    assert rows == [] and status.startswith("error:"), status


def test_checked_fetcher_still_calls_an_empty_list_empty():
    import consensus_preview as cp
    assert cp.fetch_fmp_annual_estimates_checked(
        "X", "KEY", get_json=lambda url, *a, **k: []) == ([], "empty")


def test_runner_fails_when_every_ticker_gets_a_quota_shaped_200(monkeypatch):
    """A quota failure across the whole run must fail the step (its alert
    fires), not read as a successful run over companies with no coverage."""
    import consensus_preview as cp

    def fetcher(ticker):
        return cp.fetch_fmp_annual_estimates_checked(
            ticker, "KEY",
            get_json=lambda url, *a, **k: [{"Error Message": "Limit Reach"}])

    main, _conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    with pytest.raises(RuntimeError, match="systemic"):
        main.run_snapshot_consensus()


# ---------------------------------------------------------------------------
# Codex round 2, finding 4: a currency that could not be FETCHED is not "ok"
# ---------------------------------------------------------------------------

_EST_BODY = [{"date": "2026-12-31", "epsAvg": 2.0, "revenueAvg": 1e9,
              "numAnalystsEps": 5, "numAnalystsRevenue": 5}]


def _routed(currency_behaviour):
    """A pacer stand-in: estimates succeed; the currency call does whatever
    `currency_behaviour` says (an exception to raise, or a body to return)."""
    def get_json(url, *a, **k):
        if "analyst-estimates" in url:
            return _EST_BODY
        assert "income-statement" in url, url
        if isinstance(currency_behaviour, Exception):
            raise currency_behaviour
        return currency_behaviour
    return get_json


@pytest.mark.parametrize("failure", [
    _http_error(403), TimeoutError("read timed out"), {"Error Message": "Limit Reach"},
])
def test_a_failed_currency_fetch_never_yields_an_ok_snapshot(failure):
    mod = _mod()
    conn = _db()
    _event(conn, "XYZ", "2026-09-18")
    fetch = mod.make_fmp_fetcher(conn, "KEY", TODAY, pacer=_routed(failure))
    summary = mod.snapshot_annual_consensus(conn, TODAY, fetch, now=NOW)
    statuses = {r[0] for r in conn.execute(
        "SELECT fetch_status FROM consensus_snapshot WHERE ticker = 'XYZ'")}
    assert "ok" not in statuses and len(statuses) == 1
    assert statuses.pop().startswith("partial:currency")
    # Seen by the error counter and the status post ...
    assert summary["ok"] == 0 and summary["errors"] == 1
    assert summary["failed"][0][0] == "XYZ"
    # ... and never served as a pre-print figure.
    cutoff = mod.pre_release_cutoff("2026-09-18", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None
    # The figures themselves are kept (a pre-print figure is unrecoverable).
    assert conn.execute(
        "SELECT eps_avg, currency FROM consensus_snapshot WHERE ticker='XYZ'"
    ).fetchone() == (2.0, None)


@pytest.mark.parametrize("body", [[], [{"symbol": "XYZ"}]])
def test_a_legitimately_absent_currency_is_ok_with_null(body):
    """FMP answered and has no reportedCurrency: a fact, not a failure. It is
    stored ok with currency NULL (Phase B abstains on None), distinguishable
    from the partial status above."""
    mod = _mod()
    conn = _db()
    _event(conn, "XYZ", "2026-09-18")
    fetch = mod.make_fmp_fetcher(conn, "KEY", TODAY, pacer=_routed(body))
    summary = mod.snapshot_annual_consensus(conn, TODAY, fetch, now=NOW)
    assert conn.execute(
        "SELECT fetch_status, currency FROM consensus_snapshot WHERE ticker='XYZ'"
    ).fetchall() == [("ok", None)]
    assert summary["ok"] == 1 and summary["errors"] == 0


def test_a_fetched_currency_is_stored():
    mod = _mod()
    conn = _db()
    _event(conn, "XYZ", "2026-09-18")
    fetch = mod.make_fmp_fetcher(
        conn, "KEY", TODAY, pacer=_routed([{"reportedCurrency": "DKK"}]))
    mod.snapshot_annual_consensus(conn, TODAY, fetch, now=NOW)
    assert conn.execute(
        "SELECT fetch_status, currency FROM consensus_snapshot WHERE ticker='XYZ'"
    ).fetchall() == [("ok", "DKK")]


def test_runner_fails_when_every_currency_fetch_failed(monkeypatch):
    fetcher = RecordingFetcher({
        "AAA": ([_consensus("AAA", "2026-12-31", cur=None)], "partial:currency_error:403"),
        "BBB": ([_consensus("BBB", "2026-12-31", cur=None)], "partial:currency_error:403"),
    })
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    with pytest.raises(RuntimeError, match="systemic"):
        main.run_snapshot_consensus()


# ---------------------------------------------------------------------------
# Codex round 3, B: the NEWEST batch for the event defines the period set
# ---------------------------------------------------------------------------

def test_a_period_missing_from_the_newest_batch_is_absent():
    """Oct 9 answered FY26+FY27; the valid Oct 12 answer has only FY27. FY26
    must not be served from the older batch."""
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 1.00)
    _snap(conn, "XYZ", "2027-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 2.00)
    _snap(conn, "XYZ", "2027-12-31", "2026-10-12T15:00:00Z", "2026-10-14", 2.10)
    cutoff = mod.pre_release_cutoff("2026-10-14", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2027-12-31", cutoff)["eps_avg"] == 2.10


def test_a_newer_partial_batch_is_not_backfilled_from_an_older_ok():
    """The newest answer's figures could not be currency-tagged; an older
    batch is not a substitute for it."""
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 1.00)
    _snap(conn, "XYZ", "2026-12-31", "2026-10-12T15:00:00Z", "2026-10-14", 1.10,
          status="partial:currency_error:403")
    cutoff = mod.pre_release_cutoff("2026-10-14", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None


# ---------------------------------------------------------------------------
# Codex round 3, C: an ordinary date move must not erase the guard's evidence
# ---------------------------------------------------------------------------

def _et_today():
    from zoneinfo import ZoneInfo
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date()


def test_a_vendor_move_later_after_the_old_date_blocks_the_snapshot():
    """Unlocked same-quarter row at D-1 moved by the vendor to D+2 AFTER D-1
    arrived: upsert_event DELETES the D-1 row, so the open-prior-event guard
    saw nothing and fetched possibly post-print consensus as ok."""
    from datetime import timedelta
    from storage import upsert_event
    today = _et_today()
    old, new = (today - timedelta(days=1)).isoformat(), (today + timedelta(days=2)).isoformat()
    conn = _db()
    upsert_event(conn, "XYZ", old, "bmo", None, quarter="2026Q3", tier=1)
    upsert_event(conn, "XYZ", new, "bmo", None, quarter="2026Q3", tier=1)
    assert [r[0] for r in conn.execute(
        "SELECT event_date FROM events WHERE ticker = 'XYZ'")] == [new]  # evidence gone
    fetcher = RecordingFetcher()
    summary = _mod().snapshot_annual_consensus(conn, today, fetcher)
    assert fetcher.calls == []
    assert summary["skipped_open_prior"] == ["XYZ"]


def test_a_move_made_before_the_old_date_arrived_does_not_block():
    """A reschedule announced ahead of the old date is not a possible print."""
    from datetime import timedelta
    from storage import upsert_event
    today = _et_today()
    old, new = (today + timedelta(days=1)).isoformat(), (today + timedelta(days=3)).isoformat()
    conn = _db()
    upsert_event(conn, "XYZ", old, "bmo", None, quarter="2026Q3", tier=1)
    upsert_event(conn, "XYZ", new, "bmo", None, quarter="2026Q3", tier=1)
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, today, fetcher)
    assert fetcher.calls == ["XYZ"]


# ---------------------------------------------------------------------------
# Codex round 3, A: snapshot AFTER the date corrections; persist ONLY the
# snapshot rows, in their own artifact, merged back on the next run
# (replaces round 2's mid-run earnings-db upload)
# ---------------------------------------------------------------------------

SNAP_FILE = "consensus_snapshots.db"


def _step(steps, pred):
    hits = [i for i, (_n, b) in enumerate(steps) if pred(b)]
    assert len(hits) == 1, hits
    return hits[0]


def test_snapshot_runs_after_every_date_correcting_step():
    """The cross-check (EDGAR auto-correction) and the Slack replies (operator
    locks / corrections) both move event dates. Snapshotting before them
    stores consensus against an uncorrected date / window."""
    steps = _steps()
    names = [n for n, _ in steps]
    snap = _step(steps, lambda b: "main.py --snapshot-consensus" in b)
    for fixer in ("Cross-check Finnhub vs yfinance (Tier 1/2)",
                  "Check IR-alert emails",
                  "Check Slack replies on open questions"):
        assert names.index(fixer) < snap, f"{fixer!r} must run before the snapshot"
    assert snap < names.index("Save earnings database")


def test_only_the_final_save_uploads_earnings_db():
    """Round 2's mid-run earnings-db upload persisted PRE-cross-check state,
    and overwrite:true DELETES the earlier copy before re-uploading."""
    import re
    steps = _steps()
    uploads = [n for n, b in steps
               if re.search(r"(?m)^\s+name: earnings-db\s*$", b)]
    assert uploads == ["Save earnings database"]


def test_snapshot_rows_have_their_own_artifact_restored_before_and_saved_after():
    import re
    steps = _steps()
    names = [n for n, _ in steps]
    snap = _step(steps, lambda b: "main.py --snapshot-consensus" in b)
    snap_body = steps[snap][1]
    assert f"CONSENSUS_SNAPSHOT_FILE: {SNAP_FILE}" in snap_body

    restore = _step(steps, lambda b: "ci_restore_db_artifact.sh" in b
                    and "EA_DB_ARTIFACT_NAME: consensus-snapshots" in b)
    rbody = steps[restore][1]
    assert restore < snap
    assert f"EA_DB_PATH: {SNAP_FILE}" in rbody
    assert "EA_DB_VERIFY_TABLE: consensus_snapshot" in rbody
    assert re.search(r"(?m)^\s+id: restore_snapshots\s*$", rbody)
    # A failed side-restore must never fail the critical sync.
    assert re.search(r"(?m)^\s+continue-on-error: true\s*$", rbody)

    save = _step(steps, lambda b: "actions/upload-artifact" in b
                 and re.search(r"(?m)^\s+name: consensus-snapshots\s*$", b))
    sbody = steps[save][1]
    assert snap < save < names.index("Save earnings database")
    assert f"path: {SNAP_FILE}" in sbody
    assert "overwrite: true" not in sbody
    assert re.search(r"(?m)^\s+id: persist_snapshots\s*$", sbody)
    assert re.search(r"(?m)^\s+continue-on-error: true\s*$", sbody)
    cond = re.search(r"(?m)^        if: (.+)$", sbody).group(1)
    # Runs whatever else failed, but only when the snapshot step actually ran
    # and exited cleanly (not `cancelled`: a killed step can leave a torn DB),
    # and only when the restore succeeded -- an upload built WITHOUT the
    # previous artifact merged in would become the newest, non-cumulative copy.
    assert "always()" in cond and "github.ref == 'refs/heads/main'" in cond
    assert "steps.restore_snapshots.outcome == 'success'" in cond
    # Codex r8: gated on this run's export marker, not the step outcome.
    assert "steps.snapshot_consensus.outputs.exported == 'true'" in cond

    # A failed restore or save is not silent: the snapshot alerts cover both,
    # and run after them.
    alerts = [i for i, (_n, b) in enumerate(steps)
              if "steps.snapshot_consensus.outcome != 'success'" in b]
    assert alerts and all(i > save for i in alerts)
    for i in alerts:
        a = steps[i][1]
        assert "steps.restore_snapshots.outcome == 'failure'" in a
        assert "steps.persist_snapshots.outcome == 'failure'" in a


def _rows(conn):
    return sorted(conn.execute(
        "SELECT ticker, fiscal_period_end, taken_at, event_date, eps_avg, "
        "fetch_status FROM consensus_snapshot"))


def test_export_then_merge_is_idempotent_and_adds_no_duplicates(tmp_path):
    mod = _mod()
    src = _db()
    _snap(src, "XYZ", "2026-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 1.00)
    _snap(src, "XYZ", "2026-12-31", "2026-10-12T15:00:00Z", "2026-10-14", 1.10)
    _snap(src, "ABC", "", "2026-10-12T15:00:00Z", "2026-10-14", None, status="error:429")
    f = tmp_path / SNAP_FILE
    assert mod.export_snapshot_file(src, f) == 3

    dst = _db()
    # dst already holds one of the rows (the DB save of that run succeeded).
    _snap(dst, "XYZ", "2026-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 1.00)
    assert mod.merge_snapshot_file(dst, f) == 2
    assert mod.merge_snapshot_file(dst, f) == 0          # idempotent
    assert _rows(dst) == _rows(src)                       # no duplicates


def test_merge_makes_the_newest_batch_win_on_read(tmp_path):
    """The DB save failed after a run that captured a NEWER batch; that batch
    lives only in the side artifact. After the merge it is the one read."""
    mod = _mod()
    lost_run = _db()
    _snap(lost_run, "XYZ", "2026-12-31", "2026-10-12T15:00:00Z", "2026-10-14", 1.10)
    f = tmp_path / SNAP_FILE
    mod.export_snapshot_file(lost_run, f)

    restored = _db()                                      # the older earnings-db
    _snap(restored, "XYZ", "2026-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 1.00)
    cutoff = mod.pre_release_cutoff("2026-10-14", "bmo", None, 1)
    assert mod.latest_pre_release_snapshot(restored, "XYZ", "2026-12-31", cutoff)["eps_avg"] == 1.00
    mod.merge_snapshot_file(restored, f)
    assert mod.latest_pre_release_snapshot(restored, "XYZ", "2026-12-31", cutoff)["eps_avg"] == 1.10


def test_merge_refuses_a_file_without_the_table(tmp_path):
    mod = _mod()
    f = tmp_path / SNAP_FILE
    sqlite3.connect(f).execute("CREATE TABLE other (x)").connection.commit()
    with pytest.raises(ValueError):
        mod.merge_snapshot_file(_db(), f)


def test_runner_merges_the_side_file_first_and_exports_a_cumulative_one(monkeypatch, tmp_path):
    mod = _mod()
    f = tmp_path / SNAP_FILE
    prior = _db()
    _snap(prior, "OLD", "2026-12-31", "2026-09-01T15:00:00Z", "2026-09-03", 3.0)
    mod.export_snapshot_file(prior, f)

    main, conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    main.run_snapshot_consensus()
    tickers = {r[0] for r in _rows(conn)}
    assert tickers == {"OLD", "AAA", "BBB"}                # merged + this run
    out = sqlite3.connect(f)
    assert {r[0] for r in out.execute("SELECT ticker FROM consensus_snapshot")} == tickers


def test_runner_still_exports_when_the_step_fails(monkeypatch, tmp_path):
    """Every fetch failed -> the step raises; the error rows are still exported
    so the artifact stays cumulative."""
    f = tmp_path / SNAP_FILE
    fetcher = RecordingFetcher({"AAA": ([], "error:401"), "BBB": ([], "error:401")})
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=fetcher)
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    with pytest.raises(RuntimeError):
        main.run_snapshot_consensus()
    assert sqlite3.connect(f).execute(
        "SELECT COUNT(*) FROM consensus_snapshot").fetchone()[0] == 2


def test_runner_without_the_env_var_writes_no_side_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CONSENSUS_SNAPSHOT_FILE", raising=False)
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    main.run_snapshot_consensus()
    assert not (tmp_path / SNAP_FILE).exists()


# ---------------------------------------------------------------------------
# JP decision on round 3 C: only a CONFIRMED moved-from date blocks
# ---------------------------------------------------------------------------

def test_a_confirmed_date_moved_later_after_it_arrived_blocks():
    from datetime import timedelta
    from storage import upsert_event
    today = _et_today()
    old, new = (today - timedelta(days=1)).isoformat(), (today + timedelta(days=2)).isoformat()
    conn = _db()
    upsert_event(conn, "XYZ", old, "amc", None, quarter="2026Q3", tier=1)  # confirmed
    upsert_event(conn, "XYZ", new, "amc", None, quarter="2026Q3", tier=1)
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, today, fetcher)
    assert fetcher.calls == []


def test_an_unconfirmed_rolling_date_is_still_snapshotted():
    """The vendor rolling an unannounced estimate forward day by day is the
    no-print case (measured: 180 of 1,089 companies would otherwise skip)."""
    from datetime import timedelta
    from storage import upsert_event
    today = _et_today()
    conn = _db()
    for back in (3, 2, 1):
        upsert_event(conn, "XYZ", (today - timedelta(days=back)).isoformat(), "",
                     None, quarter="2026Q3", tier=1)                     # unconfirmed
    upsert_event(conn, "XYZ", (today + timedelta(days=2)).isoformat(), "",
                 None, quarter="2026Q3", tier=1)
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, today, fetcher)
    assert fetcher.calls == ["XYZ"]


# ---------------------------------------------------------------------------
# Codex round 4
# ---------------------------------------------------------------------------

def test_a_confirmed_move_survives_25_later_unconfirmed_rolls():
    """The move record list is capped. Unconfirmed rolls are ignored by the
    guard, so they must not be able to evict the confirmed move it needs."""
    from datetime import timedelta
    from storage import upsert_event
    today = _et_today()
    d = lambda n: (today + timedelta(days=n)).isoformat()  # noqa: E731
    conn = _db()
    upsert_event(conn, "XYZ", d(-1), "bmo", None, quarter="2026Q3", tier=1)  # confirmed
    upsert_event(conn, "XYZ", d(1), "", None, quarter="2026Q3", tier=1)      # moved later
    for _ in range(25):                                   # 25 unconfirmed rolls
        upsert_event(conn, "XYZ", d(2), "", None, quarter="2026Q3", tier=1)
        upsert_event(conn, "XYZ", d(1), "", None, quarter="2026Q3", tier=1)
    upsert_event(conn, "XYZ", d(2), "", None, quarter="2026Q3", tier=1)
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, today, fetcher)
    assert fetcher.calls == []


def test_an_export_failure_fails_the_step_and_leaves_no_stale_file(monkeypatch, tmp_path):
    """A suppressed export left the RESTORED old file on disk; the upload then
    published it as the newest artifact and nothing alerted."""
    import consensus_snapshot
    mod = _mod()
    f = tmp_path / SNAP_FILE
    old = _db()
    _snap(old, "OLD", "2026-12-31", "2026-09-01T15:00:00Z", "2026-09-03", 3.0)
    mod.export_snapshot_file(old, f)                       # the restored artifact

    main, _conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))

    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(consensus_snapshot, "export_snapshot_file", boom)
    with pytest.raises(RuntimeError, match="export"):
        main.run_snapshot_consensus()
    assert not f.exists(), "a stale side file would be uploaded as the newest"


def test_the_side_upload_fails_when_there_is_no_file():
    import re
    steps = _steps()
    save = _step(steps, lambda b: "actions/upload-artifact" in b
                 and re.search(r"(?m)^\s+name: consensus-snapshots\s*$", b))
    assert re.search(r"(?m)^\s+if-no-files-found: error\s*$", steps[save][1])


# ---------------------------------------------------------------------------
# Codex round 5
# ---------------------------------------------------------------------------

# DELETED 2026-09-22: test_a_confirmed_same_quarter_move_blocks_beyond_45_days.
#
# It asserted that a confirmed row moved 09-01 -> 10-20 still blocks at 49 days,
# and it passed — but only because it hand-assigned `quarter="2026Q3"` to BOTH
# `upsert_event` calls. Production derives the label from the release month
# (`main.py:972` -> `storage.date_to_quarter`), so it would label 09-01 `2026Q2`
# and 10-20 `2026Q3`. Under real labels the long arm of `_same_cycle` cannot
# fire, the 45-day arm has lapsed at 49 days, and the snapshot proceeds.
#
# The test described a world production cannot produce, and asserted the
# behaviour we want in it. That is `a-check-that-silently-matches-nothing` — and
# it survived eight rounds of adversarial review because a green test attracts no
# attention.
#
# It is DELETED rather than rewritten because there is nothing honest to rewrite
# it to: with production labels this scenario proceeds, and asserting that would
# pin a defect as though it were intended. The behaviour that IS pinned lives in
# `test_a_real_cross_quarter_consecutive_print_is_still_snapshotted` below, which
# names this case as the residual. The real fix needs a vendor fiscal period —
# see the board row.


def test_an_unmergeable_side_file_fails_loud_even_on_an_empty_window(monkeypatch, tmp_path):
    """Passes restore verification (table exists) but lacks v14 columns; with
    an empty window the command used to return before the merge-failure raise,
    and the workflow republished the unmergeable file as newest."""
    f = tmp_path / SNAP_FILE
    bad = sqlite3.connect(f)
    bad.execute("CREATE TABLE consensus_snapshot (ticker TEXT, taken_at TEXT)")
    bad.commit()
    bad.close()
    main, conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    conn.execute("DELETE FROM events")
    conn.commit()
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    with pytest.raises(RuntimeError, match="merge"):
        main.run_snapshot_consensus()
    assert not f.exists(), "an unmergeable file must not be re-uploaded as newest"


def test_pre_release_snapshot_set_three_outcomes():
    """Plan v4 H2: Phase B must tell FMP-`empty` ("no Street consensus", no
    alert) from no snapshot at all (a pipeline gap, alert) -- and run the FY
    match over the whole period SET of the newest batch."""
    mod = _mod()
    conn = _db()
    cutoff = mod.pre_release_cutoff("2026-10-14", "bmo", None, 1)

    assert mod.pre_release_snapshot_set(conn, "XYZ", cutoff) == ("missing", {})

    _snap(conn, "XYZ", "2026-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 1.00)
    _snap(conn, "XYZ", "2027-12-31", "2026-10-09T15:00:00Z", "2026-10-14", 2.00)
    status, periods = mod.pre_release_snapshot_set(conn, "XYZ", cutoff)
    assert status == "ok" and set(periods) == {"2026-12-31", "2027-12-31"}
    assert periods["2027-12-31"]["eps_avg"] == 2.00

    _snap(conn, "XYZ", "", "2026-10-12T15:00:00Z", "2026-10-14", None, status="empty")
    assert mod.pre_release_snapshot_set(conn, "XYZ", cutoff) == ("empty", {})
    # The per-period reader is built on the set reader, so they cannot disagree.
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2027-12-31", cutoff) is None

    _snap(conn, "XYZ", "2026-12-31", "2026-10-13T15:00:00Z", "2026-10-14", 1.1,
          status="partial:currency_error:403")
    assert mod.pre_release_snapshot_set(conn, "XYZ", cutoff) == (
        "partial:currency_error:403", {})


# ---------------------------------------------------------------------------
# Codex round 6: the surviving-row check uses the same quarter rule
# ---------------------------------------------------------------------------

def _qevent(conn, ticker, event_date, quarter=None, *, moved_from=None,
            hour="bmo", confirmed=1, locked=0, reported=0,
            eps_actual=None, rev_actual=None):
    """Insert an event whose `quarter` matches a shape production can produce.

    ⛑ **Production has THREE label shapes, not one** (Codex round 10, C4). An
    earlier version of this helper refused every explicit label on the stated
    invariant "production always derives the quarter from the date". That
    invariant is FALSE, and the refusal made a real row unrepresentable:

      1. DERIVE -- `main.py:972`, the sync. `date_to_quarter(event_date)`.
         This is the default here.
      2. CARRY, LOCKED -- `main.py:4269`, the operator / EDGAR move. Keeps the
         OLD label at the NEW date; `set_date_lock` always follows at `:4283`.
      3. CARRY, UNLOCKED -- `main.py:2797`, the `run_check_results` actuals
         migration. Keeps the old label at the new date and NEVER locks.

    So `moved_from=` expresses a carry, and `locked` is INDEPENDENT of it --
    forcing `locked=1` would re-create the same defect through the other door,
    because shape 3 is unlocked.

    ⚠ **A fourth thing to know: an unlocked carry is TRANSIENT.** `run()`
    skips a matched row only when `date_locked` (`main.py:1160-1167`), then
    upserts with the derived label at `:1243`, which `COALESCE(?, quarter)`
    takes. So shape 3's output survives only until the next sync re-emits that
    date. That is why the live DB's only two non-derived rows are both locked:
    the lock is what makes a carry PERSIST, not what produces one. The
    coupling tests below pin all of this against production.

    A bare `quarter=` override stays refused: an arbitrary label is still a
    world production cannot produce.

    ⛑ An arbitrary label is not a parameter, and that is the point. Before this,
    every call here hand-assigned one — and a sweep on 2026-09-22 found **9 of
    11 carrying a label `date_to_quarter` would never produce for that date**.
    Mostly harmless (both rows in a fixture shared the same wrong label, as
    production shares the same right one), but not always: one test asserted the
    snapshot PROCEEDS for a pair that production labels identically, so under
    real labels it blocks — and its neighbour asserted the opposite outcome for
    the same shape. Two tests, the same impossible world, contradicting each
    other, both green.

    A test that can express a world production cannot produce is not a test of
    production. Constraining the label to production's three shapes makes that
    whole class unrepresentable, which is why this refuses a free-form override
    rather than defaulting to one: a default is a suggestion, and the next round
    writes past it.

    (The `upsert_event` call sites elsewhere in this file are a separate case and
    are deliberately NOT forced — they are clock-relative because
    `_record_moves_later` stamps `datetime.now()`, so a fixed-date fixture cannot
    express "moved before the old date arrived". Forcing derivation there creates
    a real two-days-per-quarter boundary failure. That needs an injectable clock;
    it is on the board row, not here.)
    """
    if quarter is not None:
        raise AssertionError(
            "_qevent takes the label from the date, or from `moved_from=` for "
            "a carry. Passing a free-form one lets a fixture describe a world "
            "production cannot produce, which is the defect this exists to "
            "prevent. See the three shapes in the docstring.")
    from storage import date_to_quarter
    label = date_to_quarter(moved_from if moved_from else event_date)
    conn.execute(
        "INSERT INTO events (ticker, event_date, event_hour, date_confirmed, "
        "reported, tier, quarter, date_locked, eps_actual, rev_actual) "
        "VALUES (?,?,?,?,?,1,?,?,?,?)",
        (ticker, event_date, hour, confirmed, reported,
         label, locked, eps_actual, rev_actual))
    conn.commit()


def test_a_locked_same_quarter_event_54_days_back_blocks():
    """Locked, confirmed 2026Q3 row on 07-25 is never deleted (so no move is
    recorded); a new same-quarter row on 09-19 is 56 days later, past the
    45-day filter, and was snapshotted although the company may have printed."""
    conn = _db()
    _qevent(conn, "XYZ", "2026-07-25", locked=1)
    _qevent(conn, "XYZ", "2026-09-19")
    fetcher = RecordingFetcher()
    summary = _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)
    assert fetcher.calls == [] and summary["skipped_open_prior"] == ["XYZ"]


def test_a_real_cross_quarter_consecutive_print_is_still_snapshotted():
    """The REPL case, with REPL's real dates, pinning what the code does TODAY.

    This replaces a test that hand-labelled 07-25 as `2026Q2` and 09-19 as
    `2026Q3` and asserted the snapshot proceeds. Production labels BOTH dates
    `2026Q2`, so under real labels that scenario BLOCKS — and its neighbour
    asserted the opposite for the same shape. Deriving the label in `_qevent`
    made that contradiction visible: the whole file went 84 pass / 1 fail, and
    the 1 was this test.

    REPL really did report on 2026-06-29 and again on 2026-08-14 — 46 days, and a
    genuine cross-label pair (`2026Q1` -> `2026Q2`). It MUST be snapshotted: it is
    two consecutive prints, not one that moved. Measured over 700 consecutive
    reported-print pairs in the live DB, **9 are under 60 days apart and 3 under
    50** (REPL 46d, OKYO 47d, NBP 30d), so this shape is ordinary, not exotic.

    ⚑ THE RESIDUAL, named here because this test is exactly where a future reader
    will look: a report that SLIPS across a quarter boundary produces the same
    observable — strong prior, >45 days, differing labels — and must NOT be
    snapshotted. Nothing in the current columns separates the two. So this test
    pins today's behaviour and the hole at once, and it is deliberately a plain
    green test rather than an xfail: an xfail would assert a correct expectation
    that does not exist yet, and the day it XPASSed it would be certifying a rule
    that also breaks REPL. The fix must key on a VENDOR FISCAL PERIOD (both
    providers send one and the code discards it) — see the board row.
    """
    from datetime import date as _date, datetime as _dt, timedelta as _td
    from storage import date_to_quarter
    mod = _mod()

    prior, upcoming = "2026-06-29", "2026-08-14"
    today = _date(2026, 8, 10)
    now = _dt(2026, 8, 10, 11, 20, tzinfo=timezone.utc)

    # Preconditions, so a pass here means what the docstring says rather than
    # passing for some incidental reason (an out-of-window date would also make
    # `calls` empty, for instance).
    q_prior, q_up = date_to_quarter(prior), date_to_quarter(upcoming)
    assert q_prior != q_up, "the point of the fixture is that the LABELS differ"
    gap = (_date.fromisoformat(upcoming) - _date.fromisoformat(prior)).days
    assert gap > mod.SAME_CYCLE_DAYS, "must be past the 45-day arm to be interesting"
    assert gap < mod.MOVED_SAME_QUARTER_MAX_DAYS, "and inside the long arm's reach"
    # The counterfactual is what proves the prior WAS evidence and only the label
    # let it through: same shape, labels forced equal, and it blocks.
    assert mod._same_cycle(prior, q_up, True, upcoming, q_up) is True
    assert mod._same_cycle(prior, q_prior, True, upcoming, q_up) is False

    conn = _db()
    _qevent(conn, "REPL", prior, reported=1)
    _qevent(conn, "REPL", upcoming)
    fetcher = RecordingFetcher()
    mod.snapshot_annual_consensus(conn, today, fetcher, now=now)
    assert fetcher.calls == ["REPL"]


# ---------------------------------------------------------------------------
# Codex round 7: a REPORTED same-quarter prior event blocks too
# ---------------------------------------------------------------------------

def test_a_reported_same_quarter_event_blocks_a_surviving_duplicate():
    """XYZ 09-17 (2026Q3) has reported; a provider duplicate stays open on
    09-20. The print already happened, so the 09-20 row's consensus is
    post-print."""
    conn = _db()
    _qevent(conn, "XYZ", "2026-09-17", hour="amc", locked=1)
    conn.execute("UPDATE events SET reported = 1 WHERE event_date = '2026-09-17'")
    conn.commit()
    _qevent(conn, "XYZ", "2026-09-20")
    fetcher = RecordingFetcher()
    run = datetime(2026, 9, 18, 11, 20, tzinfo=timezone.utc)
    summary = _mod().snapshot_annual_consensus(conn, date(2026, 9, 18), fetcher, now=run)
    assert fetcher.calls == [] and summary["skipped_open_prior"] == ["XYZ"]


def test_last_quarters_reported_event_does_not_block():
    conn = _db()
    _qevent(conn, "XYZ", "2026-06-18")
    conn.execute("UPDATE events SET reported = 1 WHERE event_date = '2026-06-18'")
    conn.commit()
    _qevent(conn, "XYZ", "2026-09-19")
    fetcher = RecordingFetcher()
    _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=NOW)
    assert fetcher.calls == ["XYZ"]


# ---------------------------------------------------------------------------
# Codex round 8 (NOT yet reviewed by Codex)
# ---------------------------------------------------------------------------

def test_a_reported_same_quarter_row_blocks_before_its_scheduled_cutoff():
    """09-17 AMC row marked reported=1 after an EARLY release; at 15:30 ET its
    scheduled 16:00 cutoff has not passed, but the print has happened."""
    conn = _db()
    _qevent(conn, "XYZ", "2026-09-17", hour="amc")
    conn.execute("UPDATE events SET reported = 1, event_hour_yf = 'amc' WHERE event_date = '2026-09-17'")
    conn.commit()
    _qevent(conn, "XYZ", "2026-09-20")
    fetcher = RecordingFetcher()
    run = datetime(2026, 9, 17, 19, 30, tzinfo=timezone.utc)   # 15:30 EDT
    _mod().snapshot_annual_consensus(conn, TODAY, fetcher, now=run)
    assert fetcher.calls == []


def test_runner_passes_a_clock_read_after_the_merge(monkeypatch, tmp_path):
    """`now` captured before a slow side-DB merge backdated every cutoff check
    and taken_at; the snapshot must start from the clock as it is then."""
    import time
    import consensus_snapshot
    mod = _mod()
    f = tmp_path / SNAP_FILE
    mod.export_snapshot_file(_db(), f)
    stamps = {}
    real_merge = consensus_snapshot.merge_snapshot_file

    def slow_merge(conn, path):
        time.sleep(0.2)
        stamps["merged"] = datetime.now(timezone.utc)
        return real_merge(conn, path)

    def spy(conn, today, fetcher, **kw):
        stamps["now"] = kw.get("now")
        return {"tickers": 0, "ok": 0, "empty": 0, "errors": 0, "failed": [],
                "aborted": None, "skipped": 0}

    main, _conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    monkeypatch.setattr(consensus_snapshot, "merge_snapshot_file", slow_merge)
    monkeypatch.setattr(consensus_snapshot, "snapshot_annual_consensus", spy)
    main.run_snapshot_consensus()
    assert stamps["now"] is None or stamps["now"] >= stamps["merged"]


def test_runner_marks_a_completed_export_in_github_output(monkeypatch, tmp_path):
    f = tmp_path / SNAP_FILE
    out = tmp_path / "gh_output"
    out.write_text("", encoding="utf-8")
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    main.run_snapshot_consensus()
    assert "exported=true" in out.read_text(encoding="utf-8").splitlines()


def test_runner_does_not_mark_a_failed_export(monkeypatch, tmp_path):
    import consensus_snapshot
    f = tmp_path / SNAP_FILE
    out = tmp_path / "gh_output"
    out.write_text("", encoding="utf-8")
    main, _conn, _posts = _runner_env(monkeypatch, fetcher=RecordingFetcher())
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))

    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(consensus_snapshot, "export_snapshot_file", boom)
    with pytest.raises(RuntimeError):
        main.run_snapshot_consensus()
    assert "exported=true" not in out.read_text(encoding="utf-8")


def test_side_upload_is_gated_on_this_runs_export_not_the_step_outcome():
    """A step TIMEOUT reports `failure`; gating on outcome republished the
    untouched restored file."""
    import re
    steps = _steps()
    save = _step(steps, lambda b: "actions/upload-artifact" in b
                 and re.search(r"(?m)^\s+name: consensus-snapshots\s*$", b))
    cond = re.search(r"(?m)^        if: (.+)$", steps[save][1]).group(1)
    assert "steps.snapshot_consensus.outputs.exported == 'true'" in cond
    assert "steps.snapshot_consensus.outcome" not in cond


# ---------------------------------------------------------------------------
# Codex round 9 / Fable: MEASURE the residual's incidence instead of guessing it
# ---------------------------------------------------------------------------

def _residual_hits(mod, conn, caplog):
    """Run the snapshot on the shared 2026-08-10 clock; return residual lines."""
    import logging
    from datetime import date as _date, datetime as _dt
    with caplog.at_level(logging.WARNING):
        mod.snapshot_annual_consensus(
            conn, _date(2026, 8, 10), RecordingFetcher(),
            now=_dt(2026, 8, 10, 11, 20, tzinfo=timezone.utc))
    return [r.getMessage() for r in caplog.records
            if "calendar-quarter labels differ" in r.getMessage()]


def test_the_residual_fires_on_a_strong_prior_that_has_NOT_printed(caplog):
    """The measurement. A prior that is strong, past the 45-day arm and carries
    NO actuals is the genuine ambiguity: we cannot tell whether it printed, so a
    snapshot taken after it may be post-print.

    Both fixtures are shapes measured on the live DB on 2026-09-23 (CLSD
    2026-05-12 -> 09-14 at 125d; SGMO 2026-08-11 -> 11-04 at 85d). **85 days is
    deliberate**: it is outside the 46-75d window an earlier draft of this fix
    proposed, and that draft would have discarded 4 of the 9 real cases.
    """
    mod = _mod()
    for label, prior_date, gap in (("CLSD", "2026-04-11", 125),
                                   ("SGMO", "2026-05-21", 85)):
        caplog.clear()
        conn = _db()
        _qevent(conn, label, prior_date, confirmed=1)   # strong, no actuals
        _qevent(conn, label, "2026-08-14")
        hits = _residual_hits(mod, conn, caplog)
        assert len(hits) == 1, f"{label} ({gap}d): {caplog.text}"
        assert f"{label}@2026-08-14" in hits[0]
        assert f"prior {prior_date}" in hits[0]


def test_the_residual_reads_the_KV_evidence_path_and_fails_CLOSED(caplog):
    """⛑ The second evidence path, and the one a mutation proved untested.

    `prior` is built from TWO sources that must feed one rule (round 6): DB rows,
    and `EVENT_MOVED_LATER_KV` records. The KV path exists precisely because
    `upsert_event` DELETED the row a move came from -- so there are no actuals to
    read, and `printed` cannot be derived. It is hard-coded `False`, i.e. FIRES.

    That default was written with nothing exercising it: the mutation "KV path
    supplies printed=True" SURVIVED the first run of this suite while all 88
    tests stayed green. A confirmed date moved later after it arrived is exactly
    the possible-print case this record exists to remember, so `True` would have
    silently deleted the whole path from the measurement -- the third
    `a-check-that-silently-matches-nothing` on this branch, inside the fix for
    the second one.

    The KV payload is built by the REAL producer (`upsert_event` ->
    `_record_moves_later`), not hand-written, so the shape cannot drift away
    from what production writes. Only the cross-label state is then applied by
    hand -- and that is itself a real production dynamic: an unlocked row's
    label is re-derived by the next sync (`main.py:1243`), which is how a KV
    record and its row come to disagree.
    """
    from storage import upsert_event, kv_get, EVENT_MOVED_LATER_KV
    import json
    mod = _mod()
    conn = _db()
    # A confirmed, open, unlocked prior 125 days before the upcoming event.
    _qevent(conn, "MOVD", "2026-04-11", confirmed=1)
    # The move: same quarter label, later date. upsert_event records the move
    # in KV and DELETES the old row, so KV is the ONLY surviving evidence.
    upsert_event(conn, "MOVD", "2026-08-14", "bmo", None,
                 quarter="2026Q1", tier=1)
    assert not conn.execute(
        "SELECT 1 FROM events WHERE ticker='MOVD' AND event_date='2026-04-11'"
    ).fetchone(), "the DB-row evidence path must be gone, or this tests nothing"
    moves = json.loads(kv_get(conn, EVENT_MOVED_LATER_KV + "MOVD"))
    assert [m["from"] for m in moves] == ["2026-04-11"], moves
    # The re-derive dynamic: the surviving row's label moves to its own date.
    conn.execute("UPDATE events SET quarter='2026Q2' WHERE ticker='MOVD'")
    conn.commit()

    hits = _residual_hits(mod, conn, caplog)
    assert len(hits) == 1, caplog.text
    assert "MOVD@2026-08-14" in hits[0]
    assert "prior 2026-04-11" in hits[0]


def test_the_residual_is_SILENT_when_the_prior_already_printed(caplog):
    """The half that keeps the line worth reading, and the defect Codex round 10
    found: without this term the warning fired on ORDINARY consecutive quarters.

    A normal print is ~91 days after its predecessor and ALWAYS carries a
    different calendar-quarter label, so the common case sat inside the 46-135d
    frame. Measured over the live DB 2026-09-23: **1,477 firings, of which 1,467
    were ordinary cadence** -- the fleet's own `a-flag-that-is-always-true`,
    inside a warning whose own docstring claimed it "can never become" one.

    Both halves of "printed" are covered, because they are different columns:
    `reported=1`, and actuals present on a row still flagged unreported (the
    FIVE-class lag, where the numbers are out before the flag flips).

    ⛑ The 85-day case is the SAME GAP as a firing case in the test above. That
    is the point: the instrument is the print status, not the day count.
    """
    mod = _mod()
    cases = (
        ("REPL", "2026-06-29", 46, dict(reported=1)),
        ("LATE", "2026-05-21", 85, dict(reported=0, eps_actual=-0.72)),
    )
    for label, prior_date, gap, flags in cases:
        caplog.clear()
        conn = _db()
        _qevent(conn, label, prior_date, confirmed=1, **flags)
        _qevent(conn, label, "2026-08-14")
        assert not _residual_hits(mod, conn, caplog), \
            f"{label} ({gap}d, printed) should be silent: {caplog.text}"


def test_the_residual_warning_stays_silent_when_there_is_nothing_to_report(caplog):
    """The other half, and the one that keeps the line worth reading: an ordinary
    window logs nothing. A warning that fires on every run is one nobody reads,
    and this fleet has paid for that shape before.
    """
    import logging
    from datetime import date as _date, datetime as _dt
    mod = _mod()
    conn = _db()
    _qevent(conn, "AAA", "2026-08-14")          # no prior at all

    with caplog.at_level(logging.WARNING):
        mod.snapshot_annual_consensus(
            conn, _date(2026, 8, 10), RecordingFetcher(),
            now=_dt(2026, 8, 10, 11, 20, tzinfo=timezone.utc))

    assert not [r for r in caplog.records
                if "calendar-quarter labels differ" in r.getMessage()], caplog.text


def test_qevent_refuses_a_FREE_FORM_quarter_but_allows_a_carry():
    """The guard that keeps this file from re-drifting. Nine of eleven `_qevent`
    calls used to carry a label production would never produce; one rewritten
    test beside them would drift back on the next round. A refusal is a property,
    not a convention -- it cannot be forgotten, only deliberately removed.

    But the refusal must admit the labels production DOES produce. Blanket-
    refusing every explicit label made the `main.py:4269` / `:2797` carry
    unrepresentable, which is the same defect pointed the other way.
    """
    from storage import date_to_quarter
    conn = _db()
    with pytest.raises(AssertionError, match="free-form"):
        _qevent(conn, "XYZ", "2026-09-01", "2026Q3")

    # A carry is allowed, takes the OLD date's label, and does NOT imply a lock.
    _qevent(conn, "CARRY", "2026-07-01", moved_from="2026-06-30", locked=0)
    row = conn.execute("SELECT quarter, date_locked FROM events "
                       "WHERE ticker='CARRY'").fetchone()
    assert row[0] == date_to_quarter("2026-06-30") == "2026Q1"
    assert date_to_quarter("2026-07-01") == "2026Q2", "the pair must cross a label"
    assert row[1] == 0, "locked must stay independent of the carry"


# ---------------------------------------------------------------------------
# Codex round 10 / C4: COUPLE the helper to production.
#
# The finding was not that the helper was wrong in isolation -- it was that
# NOTHING tied it to production, so `main.py` could stop producing a shape and
# every fixture here would silently describe a dead world. These four tests
# drive the real paths and assert the stored label is what `_qevent` writes.
# ---------------------------------------------------------------------------

def _label_of(db_path, ticker, ev_date):
    import sqlite3
    c = sqlite3.connect(db_path)
    try:
        r = c.execute("SELECT quarter, date_locked FROM events WHERE ticker=? "
                      "AND event_date=?", (ticker, ev_date)).fetchone()
        return (r[0], r[1]) if r else (None, None)
    finally:
        c.close()


def _results_harness(monkeypatch, tmp_path, *, stored_date, actuals_date,
                     locked=0):
    """Drive `run_check_results` over a stored row whose actuals land elsewhere.

    Lifted from `test_run_integration.test_check_results_fmp_corrected_date_
    moves_calendar_first`, which already exercises this path.
    """
    import main
    import storage as st
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = str(tmp_path / "c4.db")
    c = st.init_db(db_path)
    st.upsert_event(c, ticker="ZZZ", event_date=stored_date, event_hour="amc",
                    gcal_id="OLD", quarter=st.date_to_quarter(stored_date),
                    eps_estimate=1.0, reported=False, tier=1, company_name="Z")
    if locked:
        st.set_date_lock(c, "ZZZ", stored_date, locked=True)
    c.close()

    monkeypatch.setattr(main, "init_db", lambda *a, **k: st.init_db(db_path))
    monkeypatch.setattr(main, "load_coverage", lambda: [
        SimpleNamespace(ticker="ZZZ", tier=1, company_name="Z", sector=None,
                        subsector=None, position=None)])
    monkeypatch.setattr(main, "FINNHUB_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", "cal")
    monkeypatch.setattr(main, "get_finnhub_client", lambda: object())
    monkeypatch.setattr(main, "get_calendar_service", lambda: object())
    monkeypatch.setattr(main, "fetch_post_earnings_move",
                        lambda *a, **k: SimpleNamespace(move_pct=5.0,
                                                        window_label="1d"))
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", None)
    monkeypatch.setattr(main, "_fetch_earnings_source", lambda *a, **k: [
        {"symbol": "ZZZ", "date": actuals_date, "hour": "amc",
         "epsEstimate": 1.77, "epsActual": 2.22,
         "revenueEstimate": 1.2e9, "revenueActual": 1.285e9}])
    monkeypatch.setattr(main, "create_calendar_event",
                        lambda svc, cal, tk, d, h, **k: "NEW")
    monkeypatch.setattr(main, "delete_calendar_event", lambda svc, cal, gid: None)
    monkeypatch.setattr(main, "notify_results", lambda conn, rows, when: True)
    main.run_check_results(target_date=actuals_date, skip_heartbeat=True)
    return db_path


def test_production_CARRIES_a_label_across_an_unlocked_actuals_migration(
        monkeypatch, tmp_path):
    """Shape 3, and the one that killed the blanket refusal.

    `run_check_results` takes `quarter_for_event = existing["quarter"]`
    (`main.py:2797`), writes it at the NEW date (`:2839`) and never locks. So
    `2026-07-01 / 2026Q1 / locked=0` is a real row -- a boundary slip, which is
    exactly the shape the residual measurement exists to notice.

    Asserted IMMEDIATELY, before any `run()`: the label is transient and the
    next sync re-derives it (pinned by the lifetime test below).
    """
    from storage import date_to_quarter
    db = _results_harness(monkeypatch, tmp_path,
                          stored_date="2026-06-30", actuals_date="2026-07-01")
    label, locked = _label_of(db, "ZZZ", "2026-07-01")
    assert date_to_quarter("2026-06-30") != date_to_quarter("2026-07-01")
    assert label == "2026Q1", "production must CARRY the old label"
    assert locked in (0, None), "this path must NOT lock"

    # ...and `_qevent` reproduces exactly that row.
    conn = _db()
    _qevent(conn, "ZZZ", "2026-07-01", moved_from="2026-06-30", locked=0)
    assert conn.execute("SELECT quarter FROM events WHERE ticker='ZZZ'"
                        ).fetchone()[0] == label


def test_production_DERIVES_the_label_on_the_sync_path(monkeypatch, tmp_path):
    """Shape 1, and the coupling Codex named directly: *"changing main.py:972 so
    production no longer derives the quarter leaves the refusal test green."*

    It is not hypothetical -- the board's own stated direction for this hole is
    "keep the vendor fiscal period", which changes exactly that line. The day it
    lands, every derived `_qevent` fixture in this file describes a dead world.
    This test fails that day instead.
    """
    import storage as st
    from storage import date_to_quarter
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = str(tmp_path / "derive.db")
    st.init_db(db).close()
    _sync_once(monkeypatch, db, hour="amc")          # a brand-new event

    label, _ = _label_of(db, "ZZZ", "2026-07-01")
    assert label == date_to_quarter("2026-07-01") == "2026Q2", (
        "production no longer DERIVES on the sync path; every derived fixture "
        "in this file is now describing a world production does not produce")

    conn = _db()
    _qevent(conn, "ZZZ", "2026-07-01")               # helper default = derive
    assert conn.execute("SELECT quarter FROM events WHERE ticker='ZZZ'"
                        ).fetchone()[0] == label


def test_production_CARRIES_a_label_across_a_LOCKED_move(monkeypatch):
    """Shape 2: `_apply_edgar_auto_correction` keeps the old label at the new
    date (`main.py:4269`) and locks it (`:4283`). This is the row the blanket
    refusal made unrepresentable, and the one the live DB actually holds."""
    import main
    from storage import date_to_quarter, upsert_event, find_existing_event
    conn = _db()
    upsert_event(conn, "EDG", "2026-06-30", "amc", "OLD",
                 quarter=date_to_quarter("2026-06-30"), eps_estimate=1.0,
                 reported=False, tier=1, company_name="Edge")
    monkeypatch.setattr(main, "fetch_yfinance_hour_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "fetch_yfinance_call_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "create_calendar_event", lambda *a, **k: "NEW")
    monkeypatch.setattr(main, "delete_calendar_event", lambda *a, **k: None)

    assert main._apply_edgar_auto_correction(
        conn, object(), "EDG", "2026-06-30", "2026-07-01") is True

    moved = find_existing_event(conn, "EDG", "2026-07-01")
    assert moved["quarter"] == "2026Q1", "the OLD label must be carried"
    assert date_to_quarter("2026-07-01") == "2026Q2", "the pair must cross"
    assert moved["date_locked"] is True, "this path always locks"

    # `_qevent` reproduces it, lock stated explicitly rather than implied.
    c2 = _db()
    _qevent(c2, "EDG", "2026-07-01", moved_from="2026-06-30", locked=1)
    assert c2.execute("SELECT quarter, date_locked FROM events WHERE ticker='EDG'"
                      ).fetchone() == ("2026Q1", 1)


def _sync_once(monkeypatch, db_path, *, hour):
    """One `run()` over a single re-emitted ZZZ@2026-07-01 at `hour`."""
    import main
    import storage as st
    monkeypatch.setattr(main, "init_db", lambda *a, **k: st.init_db(db_path))
    monkeypatch.setattr(main, "load_coverage", lambda: [
        SimpleNamespace(ticker="ZZZ", tier=1, company_name="Z", sector=None,
                        subsector=None, position=None)])
    monkeypatch.setattr(main, "FINNHUB_API_KEY", "x")
    monkeypatch.setattr(main, "GOOGLE_CALENDAR_ID", "cal")
    monkeypatch.setattr(main, "get_finnhub_client", lambda: object())
    monkeypatch.setattr(main, "get_calendar_service", lambda: object())
    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", None)
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", None)
    monkeypatch.setattr(main, "fetch_yfinance_hour_for_date", lambda *a, **k: None)
    monkeypatch.setattr(main, "sync_ticktick_tasks", lambda *a, **k: {})
    monkeypatch.setattr(main, "reconcile_ticktick_tasks", lambda *a, **k: {})
    monkeypatch.setattr(main, "create_calendar_event", lambda *a, **k: "NEW")
    monkeypatch.setattr(main, "delete_calendar_event", lambda *a, **k: None)
    monkeypatch.setattr(main, "_fetch_earnings_source", lambda *a, **k: [
        {"symbol": "ZZZ", "date": "2026-07-01", "hour": hour,
         "epsEstimate": 1.5}])
    main.run(dry_run=True, skip_heartbeat=True)


def test_the_LIFETIME_of_an_unlocked_carry_is_the_next_row_UPDATE(
        monkeypatch, tmp_path):
    """⛑ The reviewer said "an unlocked carry lasts one sync". Exercising it
    showed that is imprecise, and the precision matters.

    `run()` only reaches the upsert when something about the row CHANGED. On an
    exact date+hour match it takes the unchanged branch ("1 unchanged") and
    never writes, so an unlocked carry survives indefinitely across any number
    of syncs. It is re-derived on the first sync that actually UPDATES the row
    (`main.py:1243`, whose `COALESCE(?, quarter)` takes the non-null derived
    label) -- here, a changed hour.

    So the live DB's two non-derived rows being locked is NOT evidence that
    locking is what preserves a carry: an untouched unlocked row keeps one too.

    Measured directly: same row, same date; hour unchanged -> 2026Q1 survives;
    hour changed -> 2026Q2.
    """
    import storage as st
    for tag, hour, expected in (("nothing changed", "amc", "2026Q1"),
                                ("hour changed", "bmo", "2026Q2")):
        d = tmp_path / tag.replace(" ", "_")
        d.mkdir(parents=True, exist_ok=True)
        db = str(d / "lt.db")
        c = st.init_db(db)
        # An unlocked, unreported carry: 07-01 wearing the 06-30 label.
        st.upsert_event(c, ticker="ZZZ", event_date="2026-07-01",
                        event_hour="amc", gcal_id="G", quarter="2026Q1",
                        eps_estimate=1.0, reported=False, tier=1,
                        company_name="Z")
        c.close()
        assert _label_of(db, "ZZZ", "2026-07-01")[0] == "2026Q1"

        _sync_once(monkeypatch, db, hour=hour)

        after, _ = _label_of(db, "ZZZ", "2026-07-01")
        assert after == expected, f"{tag}: expected {expected}, got {after}"
