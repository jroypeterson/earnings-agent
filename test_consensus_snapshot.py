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
    _event(conn, "DDD", "2026-09-17", hour="amc")            # today AMC in
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
    at all when yfinance disputes the hour (cutoff 00:00 ET). A snapshot the
    read side would reject is a wasted metered call."""
    conn = _db()
    _event(conn, "DDD", "2026-09-17", hour="amc")
    _event(conn, "YYY", "2026-09-17", hour="amc", hour_yf="bmo")

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

    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", None, 1)
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
    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", None, 1)
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff) is None


# ---------------------------------------------------------------------------
# 5. The cutoff rule (H1-NEW)
# ---------------------------------------------------------------------------

EDT_MIDNIGHT = datetime(2026, 9, 17, 4, 0, tzinfo=timezone.utc)
EDT_1600 = datetime(2026, 9, 17, 20, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("hour, hour_yf, confirmed, expected", [
    ("amc", None, 1, EDT_1600),    # NULL yfinance hour = no second opinion
    ("amc", "", 1, EDT_1600),      # empty yfinance hour = no second opinion
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


def test_null_event_hour_yf_keeps_the_amc_cutoff():
    """80% of rows have a NULL event_hour_yf. Reading NULL as a disagreement
    would drag every AMC cutoff to midnight and throw away the day-of
    snapshot -- the freshest pre-print figure there is."""
    mod = _mod()
    conn = _db()
    _snap(conn, "XYZ", "2026-12-31", "2026-09-17T14:00:00Z", "2026-09-17", 1.10)
    cutoff = mod.pre_release_cutoff("2026-09-17", "amc", None, 1)
    assert cutoff == EDT_1600
    assert mod.latest_pre_release_snapshot(conn, "XYZ", "2026-12-31", cutoff)["eps_avg"] == 1.10


def test_cutoff_uses_zoneinfo_across_dst():
    # 2026-11-10 is EST (UTC-5): 16:00 ET = 21:00 UTC, not 20:00.
    got = _mod().pre_release_cutoff("2026-11-10", "amc", None, 1)
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
    _event(conn, "DDD", "2026-09-17", hour="amc")   # cutoff 20:00 UTC (16:00 EDT)
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
        _event(conn, t, "2026-09-17", hour="amc")   # cutoff 20:00 UTC
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
    _event(conn, "XYZ", "2026-09-17", hour="amc")
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
    assert "steps.snapshot_consensus.outcome == 'success'" in cond
    assert "steps.snapshot_consensus.outcome == 'failure'" in cond

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
