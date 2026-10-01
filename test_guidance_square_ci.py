"""Board #298 Phase B go-live prerequisites (Codex 2026-10-01 round 5).

(a) A repo variable alone does nothing: every step that can post a results
    card must map `GUIDANCE_SQUARE: ${{ vars.GUIDANCE_SQUARE }}` into its env.
(b) Pre-print snapshots whose earnings-db save was lost live only in the
    `consensus-snapshots` side artifact, so BOTH workflows must restore AND
    merge it before any step that can post a results card.
(c) The #status-reports gap alert describes one card, so it is sent only once
    that card is delivered -- not repeated on every retry of a failing post.

Workflow checks parse the YAML (requirements-dev.txt, Codex round 10 C3):
a raw-text scan accepted a commented-out `# GUIDANCE_SQUARE: ...` as live
wiring (Codex 2026-10-01 round 3). Only keys Actions actually reads count.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pytest
import yaml  # declared in requirements-dev.txt; never importorskip

WF = Path(__file__).resolve().parent / ".github" / "workflows"
DAILY = WF / "daily_earnings_check.yml"
POST = WF / "post_earnings_check.yml"
SNAP_FILE = "consensus_snapshots.db"
FLAG_VALUE = "${{ vars.GUIDANCE_SQUARE }}"


def _steps(path):
    """(name, step-dict) for every step of the workflow's single job."""
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    jobs = doc["jobs"]
    assert len(jobs) == 1, list(jobs)
    steps = next(iter(jobs.values()))["steps"]
    return [(s.get("name", ""), s) for s in steps]


def _run(step) -> str:
    return str(step.get("run") or "")


def _env(step) -> dict:
    return step.get("env") or {}


def _if(step) -> str:
    return str(step.get("if") or "")


def _posts_results(step) -> bool:
    """A step that can post a results card: the bare daily sync (`run()` shares
    notify_results) or any --check-results sweep."""
    run = _run(step)
    return bool(re.search(r"(?m)^\s*python main\.py\s*$", run)) or "--check-results" in run


def _one(steps, pred):
    hits = [i for i, (_n, s) in enumerate(steps) if pred(s)]
    assert len(hits) == 1, hits
    return hits[0]


@pytest.mark.parametrize("path", [DAILY, POST], ids=["daily", "post"])
def test_every_results_step_maps_the_flag(path):
    steps = _steps(path)
    posting = [(n, s) for n, s in steps if _posts_results(s)]
    # Guard against a vacuous pass: the scan must actually find the steps.
    assert len(posting) == (1 if path == DAILY else 2), [n for n, _ in posting]
    for name, s in posting:
        env = _env(s)
        assert env.get("GUIDANCE_SQUARE") == FLAG_VALUE, f"{name!r} cannot see GUIDANCE_SQUARE"
        # Gap alerts route to #status-reports, EDGAR reads carry the UA.
        assert env.get("SLACK_WEBHOOK_STATUS") == "${{ secrets.SLACK_WEBHOOK_STATUS }}", name
        assert env.get("SEC_EDGAR_USER_AGENT") == "${{ secrets.SEC_EDGAR_USER_AGENT }}", name
    # Nothing else carries it: the flag only changes the results card.
    assert sum("GUIDANCE_SQUARE" in _env(s) for _n, s in steps) == len(posting)


@pytest.mark.parametrize("path", [DAILY, POST], ids=["daily", "post"])
def test_snapshots_are_restored_and_merged_before_any_results_step(path):
    steps = _steps(path)
    names = [n for n, _ in steps]
    restore = _one(steps, lambda s: "ci_restore_db_artifact.sh" in _run(s)
                   and _env(s).get("EA_DB_ARTIFACT_NAME") == "consensus-snapshots")
    merge = _one(steps, lambda s: _run(s).strip()
                 == "python main.py --merge-consensus-snapshots")
    first_post = min(i for i, (_n, s) in enumerate(steps) if _posts_results(s))
    guard = names.index("Guard against a rolled-back DB artifact")
    # After the rollback guard settles the DB (a self-heal replaces the file),
    # restore before merge, both before the first results card.
    assert guard < restore < merge < first_post

    rstep, mstep = steps[restore][1], steps[merge][1]
    renv, menv = _env(rstep), _env(mstep)
    assert renv.get("EA_DB_PATH") == SNAP_FILE
    assert renv.get("EA_DB_VERIFY_TABLE") == "consensus_snapshot"
    assert renv.get("EA_DB_REQUIRE_ARTIFACT") == "${{ vars.EA_CONSENSUS_BOOTSTRAPPED }}"
    assert rstep.get("id") == "restore_snapshots"
    assert menv.get("CONSENSUS_SNAPSHOT_FILE") == SNAP_FILE
    assert mstep.get("id") == "merge_snapshots"
    for s in (rstep, mstep):
        # A feature: it must never fail the critical sync / results sweep.
        assert s.get("continue-on-error") is True
        # Codex r2: the posting steps are ungated, so a branch dispatch posts a
        # card too. A prerequisite gated narrower than its consumer is skipped
        # exactly where the consumer still runs. Read-only, so: no gate.
        assert "if" not in s, "restore/merge must run wherever a results card can post"
    for name, s in steps:
        if _posts_results(s):
            assert "github.ref" not in _if(s), name


@pytest.mark.parametrize("path", [DAILY, POST], ids=["daily", "post"])
def test_a_failed_restore_or_merge_alerts_on_its_outcome(path):
    steps = _steps(path)
    # Every step that alerts on a restore failure must also alert on a merge
    # failure -- a merge that cannot read the file is the same lost-snapshot
    # outcome. (Daily has two: Slack and the email backup.)
    alerts = [s for _n, s in steps
              if "steps.restore_snapshots.outcome == 'failure'" in _if(s)
              and "upload-artifact" not in str(s.get("uses") or "")]
    assert len(alerts) == (2 if path == DAILY else 1), len(alerts)
    for a in alerts:
        cond = _if(a)
        assert "steps.merge_snapshots.outcome == 'failure'" in cond
        assert cond.startswith("always()")
        assert a.get("continue-on-error") is True


def test_post_workflow_never_uploads_the_side_artifact():
    """Only the daily snapshot step builds a cumulative export. An upload from
    the post job would publish the restored file unchanged as the newest."""
    steps = _steps(POST)
    assert not [n for n, s in steps
                if (s.get("with") or {}).get("name") == "consensus-snapshots"]
    # Positive control: the daily job DOES upload it, so the predicate can match.
    assert [n for n, s in _steps(DAILY)
            if (s.get("with") or {}).get("name") == "consensus-snapshots"]


# --- the merge command ------------------------------------------------------

def _snap(conn, ticker, taken_at):
    conn.execute(
        "INSERT INTO consensus_snapshot (ticker, fiscal_period_end, taken_at, "
        "event_date, revenue_avg, eps_avg, num_analysts_revenue, "
        "num_analysts_eps, currency, fetch_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ticker, "2026-12-31", taken_at, "2026-10-14", 1e9, 1.0, 5, 5, "USD", "ok"))
    conn.commit()


def _count(db_path):
    c = sqlite3.connect(db_path)
    try:
        return c.execute("SELECT COUNT(*) FROM consensus_snapshot").fetchone()[0]
    finally:
        c.close()


@pytest.fixture
def harness(monkeypatch, tmp_path):
    import main
    import storage as st
    db_path = str(tmp_path / "main.db")
    st.init_db(db_path).close()
    monkeypatch.setattr(main, "init_db", lambda *a, **k: st.init_db(db_path))
    return main, st, db_path


def _side_file(st, tmp_path, tickers):
    import consensus_snapshot as cs
    src = st.init_db(":memory:")
    for i, t in enumerate(tickers):
        _snap(src, t, f"2026-09-3{i}T15:00:00Z")
    f = tmp_path / SNAP_FILE
    cs.export_snapshot_file(src, f)
    return f


def test_cli_merges_the_side_file_into_the_db(harness, monkeypatch, tmp_path):
    """Through the real argparse dispatch, not the function alone -- a flag
    that parses but dispatches nowhere would leave the step green and inert."""
    main, st, db_path = harness
    # ⛑ A broken dispatch falls through to `run()` -- the REAL daily sync, with
    # whatever .env holds. That happened once while mutation-testing this file
    # (2026-10-01: it moved live Google Calendar events). Every other entry
    # point is inert by default, enumerated from the module so a new one is
    # covered without editing this list.
    def _forbidden(name):
        def _stub(*a, **k):
            raise AssertionError(f"dispatch reached {name}() instead of the merge")
        return _stub
    for name in dir(main):
        if (name.startswith("run") or name == "_run_safeguard") \
                and name != "run_merge_consensus_snapshots" \
                and callable(getattr(main, name)):
            monkeypatch.setattr(main, name, _forbidden(name))
    f = _side_file(st, tmp_path, ["AAA", "BBB"])
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(f))
    monkeypatch.setattr(sys, "argv", ["main.py", "--merge-consensus-snapshots"])
    main.main()
    assert _count(db_path) == 2
    main.main()                       # idempotent: the snapshot step re-merges
    assert _count(db_path) == 2
    assert f.exists(), "the merge step must never consume the side file"


def test_missing_or_unset_side_file_is_a_noop(harness, monkeypatch, tmp_path):
    main, _st, db_path = harness
    monkeypatch.delenv("CONSENSUS_SNAPSHOT_FILE", raising=False)
    assert main.run_merge_consensus_snapshots() == 0
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(tmp_path / "absent.db"))
    assert main.run_merge_consensus_snapshots() == 0
    assert _count(db_path) == 0


def test_unreadable_side_file_fails_loud_and_is_left_alone(harness, monkeypatch, tmp_path):
    main, _st, _db = harness
    bad = tmp_path / SNAP_FILE
    bad.write_bytes(b"not a sqlite file at all" * 100)
    monkeypatch.setenv("CONSENSUS_SNAPSHOT_FILE", str(bad))
    with pytest.raises(Exception):
        main.run_merge_consensus_snapshots()
    assert bad.exists(), "--snapshot-consensus owns removal of an unmergeable file"


# --- (c) gap alert only for a delivered card --------------------------------

def _notify(monkeypatch, *, card_fails):
    monkeypatch.setenv("GUIDANCE_SQUARE", "1")
    import main
    from notifications import NotificationError, ResultRow
    sent = []

    def fake_post(wh, blocks, fb):
        if wh.endswith("/e") and card_fails:
            raise NotificationError("webhook 500")
        sent.append(wh)

    monkeypatch.setattr(main, "SLACK_WEBHOOK_EARNINGS", "https://example.invalid/e")
    monkeypatch.setattr(main, "SLACK_WEBHOOK_STATUS", "https://example.invalid/s")
    monkeypatch.setattr(main, "post_slack", fake_post)
    monkeypatch.setattr(main, "load_coverage", lambda: [])
    monkeypatch.setattr(main, "compute_season_stats", lambda *a, **k: None)
    monkeypatch.setattr(main, "get_ticktick_config", lambda: None)
    monkeypatch.setattr(main, "attach_guidance_verdicts",
                        lambda conn, rows: ["JPX: no pre-print snapshot"])
    r = ResultRow(ticker="JPX", company_name="JPX", event_date="2026-08-05",
                  event_hour="bmo", eps_actual=1.0, eps_estimate=0.9,
                  rev_actual=None, rev_estimate=None, tier=1, sector="Other",
                  position="Portfolio")
    ok = main.notify_results(sqlite3.connect(":memory:"), [r], date(2026, 8, 5))
    return ok, sent


def test_gap_alert_is_not_sent_when_the_card_fails(monkeypatch):
    ok, sent = _notify(monkeypatch, card_fails=True)
    assert ok is False
    assert not [w for w in sent if w.endswith("/s")], \
        "a gap alert for an undelivered card repeats on every retry"


def test_gap_alert_is_sent_once_the_card_is_delivered(monkeypatch):
    ok, sent = _notify(monkeypatch, card_fails=False)
    assert ok is True
    assert sent == ["https://example.invalid/e", "https://example.invalid/s"]
