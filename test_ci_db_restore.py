"""Tests for the earnings-db restore + rollback guard shell scripts.

WHY THESE EXIST
---------------
The selector that chooses which `earnings-db` artifact to restore has now caused
data loss twice: a 28-day rollback in August 2026, and a 3.6-day one measured on
2026-09-08 (reconcile_calendar run 34259752925 restored a 2026-09-05 snapshot,
skipping one from 2026-09-08 15:06, and re-uploaded it as the newest artifact).
Both times CI was green throughout, because the damage is to state BETWEEN runs
and no test ever exercised the selection.

`gh` is stubbed with a script on PATH, so these run offline and assert on the
behaviour that actually failed rather than on the shape of the code.
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent
RESTORE = REPO / "scripts" / "ci_restore_db_artifact.sh"
GUARD = REPO / "scripts" / "ci_db_rollback_guard.sh"

BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")


def _make_db(path: Path, *, events: int = 10, actuals: int = 5, watermark: str) -> None:
    """A database shaped like the real one, as far as these scripts read it."""
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, event_date TEXT, reported INTEGER,"
        " eps_actual REAL, tier INTEGER, updated_at TEXT)"
    )
    con.execute("CREATE TABLE kv_store (k TEXT PRIMARY KEY, v TEXT)")
    con.execute("INSERT INTO kv_store VALUES ('watermark', '1')")
    future = (datetime.utcnow() + timedelta(days=30)).date().isoformat()
    for i in range(events):
        con.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            (i, future, 1 if i < actuals else 0, 1.0 if i < actuals else None, 1, watermark),
        )
    con.commit()
    con.close()


def _zip_of(db: Path, dest: Path) -> None:
    with zipfile.ZipFile(dest, "w") as z:
        z.write(db, "earnings_events.db")


def _install_fake_gh(tmp_path: Path, listing: list[dict], zips: dict[str, Path]) -> dict:
    """A `gh` stub that answers the two calls the scripts make."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    (tmp_path / "artifacts.json").write_text(json.dumps({"artifacts": listing}), encoding="utf-8")
    (tmp_path / "zips.json").write_text(
        json.dumps({k: str(v) for k, v in zips.items()}), encoding="utf-8"
    )

    # The stub emits the same FLAT PROJECTION the real `gh api --jq` produces:
    # "id created_at expired head_branch run_id" per artifact, deliberately in the
    # order the API returned them. Filtering and sorting are the script's job, and
    # that is precisely what these tests exist to exercise -- so the stub must not
    # do either. No jq dependency, so nothing here can silently skip.
    helper = tmp_path / "gh_stub.py"
    helper.write_text(
        # The list mode SLICES by page. A stub that ignores `page=` cannot prove
        # page-2 selection -- every page replays page 1, so a paginating script
        # and a single-request script are indistinguishable against it. The real
        # API pages; the stub must too, or the pagination test is vacuous.
        "import json, shutil, sys\n"
        "mode = sys.argv[1]\n"
        # The script asks for total_count in a SEPARATE call, so the row stream
        # stays positional (NF==5) and its line count stays a page-length
        # signal. The stub must answer that call too, or production parses a
        # shape the tests never produce -- the seam class this repo has already
        # paid for once.
        "if mode == 'count':\n"
        "    print(len(json.load(open(sys.argv[2]))['artifacts']))\n"
        "elif mode == 'list':\n"
        "    arts = json.load(open(sys.argv[2]))['artifacts']\n"
        "    per_page = int(sys.argv[3]) if len(sys.argv) > 3 else 50\n"
        "    page = int(sys.argv[4]) if len(sys.argv) > 4 else 1\n"
        "    for a in arts[(page - 1) * per_page: page * per_page]:\n"
        "        print(a['id'], a['created_at'], str(a['expired']).lower(),\n"
        "              a['workflow_run']['head_branch'], a['workflow_run']['id'])\n"
        "else:\n"
        "    path = json.load(open(sys.argv[2]))[sys.argv[3]]\n"
        "    shutil.copyfileobj(open(path, 'rb'), sys.stdout.buffer)\n",
        encoding="utf-8",
        newline="\n",
    )

    stub = bindir / "gh"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        "# args: api <path> [--jq <expr>]\n"
        'if [ "${GH_FAIL:-0}" = "1" ]; then exit 1; fi\n'
        'path="$2"\n'
        'case "$path" in\n'
        "  *artifacts/*/zip)\n"
        '    aid=$(printf "%s" "$path" | sed -E "s#.*/artifacts/([0-9]+)/zip#\\1#")\n'
        f'    python "{helper}" zip "{tmp_path / "zips.json"}" "$aid"\n'
        "    ;;\n"
        "  *)\n"
        # `--jq .total_count` is a SIZING call, not a page of the walk: answer
        # it and return BEFORE the call log, or every test that counts pages
        # sees a phantom request.
        '    if [ "$4" = ".total_count" ]; then\n'
        f'      python "{helper}" count "{tmp_path / "artifacts.json"}"\n'
        "      exit 0\n"
        "    fi\n"
        '    pp=$(printf "%s" "$path" | sed -nE "s/.*per_page=([0-9]+).*/\\1/p")\n'
        '    pg=$(printf "%s" "$path" | sed -nE "s/.*[?&]page=([0-9]+).*/\\1/p")\n'
        '    [ -n "$pp" ] || pp=50\n'
        '    [ -n "$pg" ] || pg=1\n'
        # Record every LIST call. Without this the pagination tests can only
        # assert WHICH artifact came back, never HOW MANY requests it took -- so
        # deleting the short-page `break` (making the script walk to the cap on
        # every run) leaves them green. Found by Codex round 10 reviewing round
        # 9's own fixes: a test that cannot see the thing it is about.
        f'    printf "%s\\n" "$pg" >> "{tmp_path / "gh_calls.log"}"\n'
        f'    python "{helper}" list "{tmp_path / "artifacts.json"}" "$pp" "$pg"\n'
        "    ;;\n"
        "esac\n",
        encoding="utf-8",
        newline="\n",
    )
    stub.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    env["GITHUB_REPOSITORY"] = "owner/repo"
    env["EA_DB_RESTORE_TRIES"] = "1"
    env.pop("GITHUB_OUTPUT", None)
    return env


def _list_calls(tmp_path: Path) -> list[str]:
    """Which list PAGES the script actually requested, in order.

    Asserting on this is what makes the pagination tests non-vacuous: the
    artifact that comes back is the same whether the script stopped correctly at
    a short page or walked every page to the cap.
    """
    log = tmp_path / "gh_calls.log"
    return log.read_text(encoding="utf-8").split() if log.exists() else []


def _artifact(aid: int, created: str, *, branch: str = "main", expired: bool = False) -> dict:
    return {
        "id": aid,
        "created_at": created,
        "expired": expired,
        "workflow_run": {"id": aid * 10, "head_branch": branch},
    }


def _run(script: Path, cwd: Path, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [BASH, str(script)], cwd=cwd, env=env, capture_output=True, text=True, timeout=120
    )


def test_picks_newest_even_when_the_api_lists_it_last(tmp_path):
    """THE REGRESSION. Selection must come from created_at, not list position.

    The listing is deliberately ordered oldest-first. A selector that trusts the
    API's ordering restores the 2026-09-05 artifact -- which is exactly what the
    dawidd6 run-enumeration did on 2026-09-08.
    """
    old, new = tmp_path / "old.db", tmp_path / "new.db"
    _make_db(old, events=10, actuals=5, watermark=_recent(hours=90))
    _make_db(new, events=20, actuals=15, watermark=_recent(hours=1))
    _zip_of(old, tmp_path / "old.zip")
    _zip_of(new, tmp_path / "new.zip")

    listing = [
        _artifact(111, "2026-09-05T00:22:41Z"),
        _artifact(222, "2026-09-08T15:06:39Z"),
    ]
    env = _install_fake_gh(
        tmp_path, listing, {"111": tmp_path / "old.zip", "222": tmp_path / "new.zip"}
    )
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "selected artifact 222" in res.stdout, res.stdout

    con = sqlite3.connect(work / "earnings_events.db")
    assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 20


def test_ignores_non_main_branches(tmp_path):
    old, new = tmp_path / "old.db", tmp_path / "new.db"
    _make_db(old, watermark=_recent(hours=1))
    _make_db(new, events=99, watermark=_recent(hours=1))
    _zip_of(old, tmp_path / "old.zip")
    _zip_of(new, tmp_path / "new.zip")

    listing = [
        _artifact(111, "2026-09-05T00:00:00Z"),
        _artifact(222, "2026-09-09T00:00:00Z", branch="feature/x"),
    ]
    env = _install_fake_gh(
        tmp_path, listing, {"111": tmp_path / "old.zip", "222": tmp_path / "new.zip"}
    )
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "selected artifact 111" in res.stdout


def test_expired_artifact_cannot_win_on_recency(tmp_path):
    old = tmp_path / "old.db"
    _make_db(old, watermark=_recent(hours=1))
    _zip_of(old, tmp_path / "old.zip")

    listing = [
        _artifact(111, "2026-09-05T00:00:00Z"),
        _artifact(999, "2026-09-09T00:00:00Z", expired=True),
    ]
    env = _install_fake_gh(tmp_path, listing, {"111": tmp_path / "old.zip"})
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "selected artifact 111" in res.stdout


def test_no_artifacts_is_bootstrap_not_failure(tmp_path):
    env = _install_fake_gh(tmp_path, [], {})
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert not (work / "earnings_events.db").exists()
    assert "bootstrap" in res.stdout


def test_unreachable_api_is_NOT_treated_as_bootstrap(tmp_path):
    """An outage must never look like 'no artifact exists'.

    Conflating them starts a fresh-DB run, which silently erases date locks,
    kv_store watermarks and Slack thread state.
    """
    env = _install_fake_gh(tmp_path, [], {})
    env["GH_FAIL"] = "1"
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 1
    assert not (work / "earnings_events.db").exists()
    assert "NOT an absent artifact" in res.stderr


def _recent(*, hours: float) -> str:
    return (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")


# --- the rollback guard: regression, not staleness --------------------------------
#
# These replace three tests that asserted the guard FAILS on an old watermark. That
# assertion is what took the pipeline down from 2026-09-11 15:00 to 2026-09-14: the
# guard runs before the agent writes, so once it failed nothing could advance the
# watermark, and every later run read an older one (47.91h, 52.43h, 73.94h, 76.29h --
# wall clock exactly).
#
# ⛑ The old test could not have caught that, and it is worth naming why, because the
# fixture looked convincing. `test_guard_refuses_a_rolled_back_db_that_the_overdue_
# metric_calls_healthy` built ONE database with an old watermark and asserted exit 1.
# A rolled-back database and a healthy off-season database are indistinguishable in
# that fixture -- there was no second, richer artifact for the first to be a rollback
# OF. The test named a property its input did not have, so it passed for a reason
# unrelated to the defect, and the guard shipped measuring recency instead of loss.
# Every test below therefore builds BOTH sides: what was restored and what exists to
# compare it against.


def _guard_env_with_artifacts(tmp_path, dbs: list[tuple[int, Path]]) -> dict:
    """`dbs` is [(artifact_id, db_path)], newest first -- the order the API returns."""
    listing, zips = [], {}
    for n, (aid, db) in enumerate(dbs):
        z = tmp_path / f"a{aid}.zip"
        _zip_of(db, z)
        zips[str(aid)] = z
        listing.append(_artifact(aid, (datetime.utcnow() - timedelta(hours=n)).isoformat() + "Z"))
    env = _install_fake_gh(tmp_path, listing, zips)
    env["GITHUB_REPOSITORY"] = "owner/repo"
    for k in ("WEBHOOK_STATUS", "WEBHOOK_EARNINGS", "EA_DB_RESTORE_ARTIFACT_ID"):
        env.pop(k, None)
    return env


def test_quiet_offseason_db_is_NOT_a_fault(tmp_path):
    """The 2026-09-11 -> 2026-09-14 outage, reproduced.

    Every unexpired artifact carried events=2996 actuals=1757 and the identical
    watermark 2026-09-11 15:00:28 across five distinct sha256 digests -- runs were
    writing, no EVENT had changed, and there was no rollback anywhere. The guard must
    let this through: it is the best database available, and refusing to write to it
    is what made a quiet week into a three-day outage.
    """
    work = tmp_path / "work"
    work.mkdir()
    _make_db(work / "earnings_events.db", events=2996, actuals=1757, watermark=_recent(hours=76.29))
    peer = tmp_path / "peer.db"
    _make_db(peer, events=2996, actuals=1757, watermark=_recent(hours=76.29))

    res = _run(GUARD, work, _guard_env_with_artifacts(tmp_path, [(901, peer)]))
    assert res.returncode == 0, res.stdout + res.stderr
    assert "regression check clean" in res.stdout
    assert "ALARM" not in res.stdout


def test_guard_cannot_fail_on_age_alone(tmp_path):
    """The latch, asserted directly.

    No artifact is richer, so however old the watermark is there is nothing to heal
    to and nothing to refuse. A guard that fails here can never be recovered from by
    a later run, because a later run reads an even older watermark.
    """
    work = tmp_path / "work"
    work.mkdir()
    _make_db(work / "earnings_events.db", events=2996, actuals=1757, watermark=_recent(hours=720))
    peer = tmp_path / "peer.db"
    _make_db(peer, events=2996, actuals=1757, watermark=_recent(hours=720))

    res = _run(GUARD, work, _guard_env_with_artifacts(tmp_path, [(902, peer)]))
    assert res.returncode == 0, res.stdout + res.stderr
    assert "advisory" in res.stdout


def test_guard_heals_a_rollback_the_overdue_metric_calls_healthy(tmp_path):
    """The 2026-09-08 case, with the second side the old fixture was missing.

    reconcile_calendar restored a 2026-09-05 snapshot over a 2026-09-08 one: events
    2987 -> 2980, actuals 1744 -> 1740. Off-season the past-due metric reads ~2 on
    both, so only the comparison against the richer artifact can see it. Four actuals
    is the smallest real incident on record, so the tolerance has to be tighter than
    that or the check is decorative.
    """
    work = tmp_path / "work"
    work.mkdir()
    _make_db(work / "earnings_events.db", events=2980, actuals=1740, watermark=_recent(hours=98))
    good = tmp_path / "good.db"
    _make_db(good, events=2987, actuals=1744, watermark=_recent(hours=2))

    res = _run(GUARD, work, _guard_env_with_artifacts(tmp_path, [(903, good)]))
    assert res.returncode == 0, res.stdout + res.stderr
    assert "ROLLBACK" in res.stdout
    # Healed in place, and the run continues on the good copy rather than aborting:
    # refusing to write is what turned the previous incident into an outage.
    con = sqlite3.connect(work / "earnings_events.db")
    assert con.execute("SELECT COUNT(*) FROM events WHERE eps_actual IS NOT NULL").fetchone()[0] == 1744
    con.close()


def test_guard_heals_the_28_day_rollback(tmp_path):
    """The 2026-08-24 case: events 2912 -> 1735, actuals 1680 -> 763."""
    work = tmp_path / "work"
    work.mkdir()
    _make_db(work / "earnings_events.db", events=1735, actuals=763, watermark=_recent(hours=1))
    good = tmp_path / "good.db"
    _make_db(good, events=2912, actuals=1680, watermark=_recent(hours=30))

    res = _run(GUARD, work, _guard_env_with_artifacts(tmp_path, [(904, good)]))
    assert res.returncode == 0, res.stdout + res.stderr
    assert "ROLLBACK" in res.stdout
    con = sqlite3.connect(work / "earnings_events.db")
    assert con.execute("SELECT COUNT(*) FROM events WHERE eps_actual IS NOT NULL").fetchone()[0] == 1680
    con.close()


def test_a_fresh_db_is_not_healed_backwards(tmp_path):
    """A newer database with MORE actuals must never be replaced by an older one.

    This is the direction that would destroy live state -- kv_store watermarks, date
    locks and ticktick pointers that no re-run reconstructs.
    """
    work = tmp_path / "work"
    work.mkdir()
    _make_db(work / "earnings_events.db", events=2996, actuals=1757, watermark=_recent(hours=1))
    older = tmp_path / "older.db"
    _make_db(older, events=2990, actuals=1750, watermark=_recent(hours=48))

    res = _run(GUARD, work, _guard_env_with_artifacts(tmp_path, [(905, older)]))
    assert res.returncode == 0, res.stdout + res.stderr
    assert "regression check clean" in res.stdout
    con = sqlite3.connect(work / "earnings_events.db")
    assert con.execute("SELECT COUNT(*) FROM events WHERE eps_actual IS NOT NULL").fetchone()[0] == 1757
    con.close()


def test_unlistable_artifacts_do_not_fail_the_run_but_are_announced(tmp_path):
    """A token without actions:read is not evidence the database is bad.

    But the skip has to be visible: an unannounced skip reads exactly like a check
    that passed, which is the class this repo keeps meeting.
    """
    work = tmp_path / "work"
    work.mkdir()
    _make_db(work / "earnings_events.db", events=2996, actuals=1757, watermark=_recent(hours=1))
    peer = tmp_path / "peer.db"
    _make_db(peer, events=2996, actuals=1757, watermark=_recent(hours=1))

    env = _guard_env_with_artifacts(tmp_path, [(906, peer)])
    env["GH_FAIL"] = "1"
    res = _run(GUARD, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "regression check SKIPPED" in res.stdout


def test_corrupt_db_still_fails(tmp_path):
    """The one thing that must still be fatal: nothing readable to write to."""
    work = tmp_path / "work"
    work.mkdir()
    (work / "earnings_events.db").write_bytes(b"not a sqlite file at all")

    res = _run(GUARD, work, _guard_env_with_artifacts(tmp_path, []))
    assert res.returncode == 1, res.stdout + res.stderr
    assert "ALARM" in res.stdout



def test_a_side_artifact_restores_with_its_own_verify_table(tmp_path):
    """Board #298: the consensus-snapshots artifact is a SQLite file with only a
    consensus_snapshot table. EA_DB_VERIFY_TABLE lets the same restore (newest
    by created_at, verify before overwrite) serve it; the default stays
    `events` so the earnings-db restore is unchanged."""
    snap = tmp_path / "consensus_snapshots.db"
    con = sqlite3.connect(snap)
    con.execute("CREATE TABLE consensus_snapshot (ticker TEXT, taken_at TEXT)")
    con.execute("INSERT INTO consensus_snapshot VALUES ('XYZ', '2026-10-12T15:00:00Z')")
    con.commit()
    con.close()
    with zipfile.ZipFile(tmp_path / "s.zip", "w") as z:
        z.write(snap, "consensus_snapshots.db")
    env = _install_fake_gh(tmp_path, [_artifact(333, "2026-10-12T16:00:00Z")],
                           {"333": tmp_path / "s.zip"})
    env["EA_DB_ARTIFACT_NAME"] = "consensus-snapshots"
    env["EA_DB_PATH"] = "consensus_snapshots.db"
    work = tmp_path / "work"
    work.mkdir()

    # Without the override it is (correctly) refused: no events table.
    res = _run(RESTORE, work, env)
    assert res.returncode == 1 and not (work / "consensus_snapshots.db").exists()

    env["EA_DB_VERIFY_TABLE"] = "consensus_snapshot"
    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    got = sqlite3.connect(work / "consensus_snapshots.db").execute(
        "SELECT COUNT(*) FROM consensus_snapshot").fetchone()[0]
    assert got == 1


def test_the_newest_artifact_is_found_when_page_one_holds_only_expired_rows(tmp_path):
    """H3 (Codex round 9, Fable-corrected). The listing is PAGED, and the
    unexpired/branch filter runs AFTER the fetch -- so a page consisting entirely
    of expired rows yields zero candidates while the artifact we want sits on the
    next page. One request would miss it and report "treating as bootstrap".

    The fixture is deliberately page-1-all-expired. A fixture with candidates on
    page 1 passes against the single-request script too and proves nothing, which
    is the shape this whole finding is about.
    """
    live = tmp_path / "live.db"
    _make_db(live, events=42, actuals=30, watermark=_recent(hours=1))
    _zip_of(live, tmp_path / "live.zip")

    # per_page is forced to 2 so the fixture stays small and the page boundary is
    # explicit rather than incidental.
    listing = [
        _artifact(101, "2026-09-01T00:00:00Z", expired=True),
        _artifact(102, "2026-09-02T00:00:00Z", expired=True),
        _artifact(303, "2026-09-03T00:00:00Z"),          # page 2, the only candidate
    ]
    env = _install_fake_gh(tmp_path, listing, {"303": tmp_path / "live.zip"})
    env["EA_DB_ARTIFACT_PAGE"] = "2"
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "selected artifact 303" in res.stdout, res.stdout
    con = sqlite3.connect(work / "earnings_events.db")
    assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 42
    # Page 1 is full (2 rows at per_page=2) so the walk must continue; page 2 is
    # short, so it must stop there. Exactly two requests, in order.
    assert _list_calls(tmp_path) == ["1", "2"], _list_calls(tmp_path)


def test_paging_stops_on_a_short_page_not_on_an_empty_candidate_set(tmp_path):
    """The stop condition is the subtlety. "Stop when a page yields no
    candidates" stops at page 1 in the test above -- reproducing the defect while
    looking like the fix. This pins the correct condition from the other side: a
    page SHORTER than per_page ends the walk, so a complete listing costs exactly
    one extra request and never loops to the cap.
    """
    live = tmp_path / "live.db"
    _make_db(live, events=7, actuals=3, watermark=_recent(hours=1))
    _zip_of(live, tmp_path / "live.zip")

    listing = [_artifact(900, "2026-09-10T00:00:00Z")]      # 1 row, per_page 2 -> short
    env = _install_fake_gh(tmp_path, listing, {"900": tmp_path / "live.zip"})
    env["EA_DB_ARTIFACT_PAGE"] = "2"
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "selected artifact 900" in res.stdout, res.stdout
    # THE POINT OF THIS TEST. One row against per_page=2 is a short page, so the
    # walk ends immediately. Without this assertion the test passes whether the
    # script stops here or grinds to EA_DB_ARTIFACT_MAX_PAGES on every single
    # run -- which costs needless API calls and turns a healthy page-1 listing
    # into a restore FAILURE if any later call happens to fail.
    assert _list_calls(tmp_path) == ["1"], _list_calls(tmp_path)


def test_a_mandatory_artifact_refuses_an_empty_listing(tmp_path):
    """H2. For a CUMULATIVE artifact, "nothing to restore" and "the history was
    lost" are the same observation, and exit 0 lets the caller export a
    non-cumulative file and upload it as the newest -- silently resetting the
    history. Every alert predicate in the workflow reads steps.*.outcome, which
    was `success`, so none could fire.
    """
    env = _install_fake_gh(tmp_path, [], {})
    env["EA_DB_REQUIRE_ARTIFACT"] = "true"
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 1, res.stdout + res.stderr
    assert "MANDATORY" in res.stderr, res.stderr
    assert not (work / "earnings_events.db").exists()


def test_an_empty_listing_is_still_bootstrap_when_the_artifact_is_not_mandatory(tmp_path):
    """The other half, so the flag cannot become the outage: the main earnings-db
    keeps its bootstrap path, which the abort-if-missing step owns via
    EA_DB_BOOTSTRAPPED. Without this, enabling the flag anywhere would look like
    it had to be enabled everywhere.
    """
    env = _install_fake_gh(tmp_path, [], {})
    env.pop("EA_DB_REQUIRE_ARTIFACT", None)
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "treating as bootstrap" in res.stdout, res.stdout
    assert not (work / "earnings_events.db").exists()


# --- H4: the uploading workflows must check out a BRANCH REF, not a sha -------

UPLOADING_WORKFLOWS = [
    "daily_earnings_check.yml",
    "reconcile_calendar.yml",
    "post_earnings_check.yml",
    "season_progress.yml",
]


def test_every_uploading_workflow_checks_out_the_branch_ref_not_the_run_sha():
    """H4 (Codex round 9, shaped by Fable). Artifacts are filtered by head_branch
    only; nothing records which CODE wrote one. `actions/checkout` defaults to
    `github.sha`, and a GitHub re-run of an old run reuses that run's sha -- so a
    re-run executes OLD code, writes the shared earnings-db, and uploads it with
    a newest created_at. The rollback guard passes it (accumulated actuals are
    unchanged), and the next run restores a database written by code that
    predates the current schema.

    Asserting the EXACT value, not merely that a ref is present:

    - `${{ github.ref }}` makes a re-run take the branch's tip at checkout time,
      which closes the hole.
    - A literal `main` would close it too, and is WRONG: these workflows are
      schedule + workflow_dispatch, so on every normal trigger the two are the
      same -- but the literal silently stops a branch dispatch from testing its
      own branch. Same cost, one path removed for free. This test exists to stop
      a future "simplification" to the literal.

    Only the SELF checkout is constrained; the Coverage-Manager checkout in each
    file pins someone else's repository and must not be touched -- giving it
    `ref: ${{ github.ref }}` would make a branch dispatch try to resolve THIS
    repo's branch name inside CM, which does not have it.

    ⛑ **This asserts the EFFECTIVE YAML VALUE, not the file's text** (Codex
    round 10, C3). The previous version string-matched inside a step's text
    block, so commenting the line out --

        # ref: ${{ github.ref }}

    -- left BOTH assertions true, because the comment still contains the
    substring, while `actions/checkout` silently reverted to the run sha. A text
    pin cannot tell a setting from a note ABOUT a setting; only the parser can.
    Same defect shape as the `[:900]` character window this test outgrew once
    already -- a structural property beats a text window, twice over.
    """
    import yaml  # declared in requirements-dev.txt; never importorskip

    wf_dir = REPO / ".github" / "workflows"
    for name in UPLOADING_WORKFLOWS:
        doc = yaml.safe_load((wf_dir / name).read_text(encoding="utf-8"))
        steps = [s for job in doc["jobs"].values() for s in job.get("steps", [])]
        checkouts = [s for s in steps
                     if str(s.get("uses", "")).startswith("actions/checkout@")]
        assert checkouts, "%s: no checkout step found at all" % name

        self_checkouts = [s for s in checkouts
                          if "repository" not in (s.get("with") or {})]
        assert len(self_checkouts) == 1, (
            "%s: expected exactly one self-checkout, found %d"
            % (name, len(self_checkouts)))

        with_block = self_checkouts[0].get("with") or {}
        assert "ref" in with_block, (
            "%s: the self-checkout has no EFFECTIVE `ref:`, so it defaults to "
            "github.sha and a re-run of an old run would execute old code "
            "against the shared artifact" % name)
        assert with_block["ref"] == "${{ github.ref }}", (
            "%s: the self-checkout must pin `ref: ${{ github.ref }}` exactly; "
            "got %r. A literal branch name closes the same hole but removes "
            "branch dispatch." % (name, with_block["ref"]))

        # ...and a FOREIGN checkout must not be handed this repo's ref.
        for s in checkouts:
            w = s.get("with") or {}
            if "repository" in w:
                assert w.get("ref") != "${{ github.ref }}", (
                    "%s: the checkout of %s must not take THIS repo's ref -- a "
                    "branch dispatch would try to resolve that branch there"
                    % (name, w["repository"]))


def test_the_consensus_restore_step_ACTUALLY_SETS_the_mandatory_flag():
    """C1 (Codex round 10). The refusal above is gated on
    EA_DB_REQUIRE_ARTIFACT, and NOTHING in production set it -- the test beside
    it injected the value by hand, so the guard existed only inside its own
    test. `a-check-that-silently-matches-nothing`, shipped inside the fix for
    that class.

    ⛑ It must NOT be a literal "true", and this asserts the exact expression
    rather than merely "something is set". Measured 2026-09-23: the
    consensus-snapshots artifact has total_count = 0 -- never uploaded. A
    literal would exit 1 on the first post-merge run, which sets the step's
    `outcome` to failure, which skips the upload step (gated on
    outcome == 'success'), so the artifact could never come into existence and
    the alert would fire twice daily forever. The guard would BE the outage.

    The bootstrap variable is unset until an artifact exists, renders as the
    empty string, fails the script's `= "true"` test, and takes the bootstrap
    path -- and the upload step announces the flip so the inert window cannot
    be silently forgotten (earnings-db's equivalent lagged ~85 days).
    """
    import yaml

    doc = yaml.safe_load(
        (REPO / ".github" / "workflows" / "daily_earnings_check.yml"
         ).read_text(encoding="utf-8"))
    steps = [s for job in doc["jobs"].values() for s in job.get("steps", [])]

    restore = [s for s in steps if s.get("id") == "restore_snapshots"]
    assert len(restore) == 1, "expected exactly one consensus restore step"
    flag = (restore[0].get("env") or {}).get("EA_DB_REQUIRE_ARTIFACT")
    assert flag == "${{ vars.EA_CONSENSUS_BOOTSTRAPPED }}", (
        "the consensus restore step must arm the mandatory-artifact guard via "
        "the bootstrap VARIABLE, not a literal (a literal deadlocks the lane: "
        "total_count is 0 and the step it would fail is the only thing that "
        "can create the artifact); got %r" % (flag,))

    # ...and the inert window must announce itself, or this is just the same
    # "never set in production" defect with an extra human step in front.
    announce = [s for s in steps
                if "EA_CONSENSUS_BOOTSTRAPPED == ''" in str(s.get("if", ""))]
    assert announce, (
        "no step announces that the bootstrap variable is still unset, so the "
        "guard can sit inert indefinitely with nothing saying so")


def test_the_walk_is_sized_by_total_count_not_by_a_short_page(tmp_path):
    """H5 (Codex round 10). The walk used to run to a fixed cap of 4 x 50 = 200
    rows. Measured 2026-09-23 the live earnings-db listing holds 825 artifacts
    and EXPIRED ROWS NEVER LEAVE IT, so it was truncated on every run -- and the
    review's remedy, failing whenever the final page is full at the cap, would
    have failed 100% of runs on all four restoring workflows.

    Sizing the walk by `total_count` removes both the truncation and the
    guesswork. This pins the saving: with an EXACT page multiple the short-page
    rule alone cannot know the listing ended, so it must spend one more request
    to discover an empty page. total_count knows in advance.
    """
    live = tmp_path / "live.db"
    _make_db(live, events=7, actuals=3, watermark=_recent(hours=1))
    _zip_of(live, tmp_path / "live.zip")

    # 4 artifacts, per_page 2 -> exactly 2 full pages, no short page at all.
    listing = [_artifact(700, "2026-09-01T00:00:00Z"),
               _artifact(800, "2026-09-05T00:00:00Z"),
               _artifact(850, "2026-09-08T00:00:00Z"),
               _artifact(900, "2026-09-10T00:00:00Z")]
    env = _install_fake_gh(tmp_path, listing, {"900": tmp_path / "live.zip"})
    env["EA_DB_ARTIFACT_PAGE"] = "2"
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "selected artifact 900" in res.stdout, res.stdout
    # ceil(4 / 2) = 2. A short-page-only walk would spend a third request to
    # find the empty page; this one does not.
    assert _list_calls(tmp_path) == ["1", "2"], _list_calls(tmp_path)


def test_exhausting_the_sanity_cap_FAILS_instead_of_selecting_from_a_prefix(
        tmp_path):
    """The other half. Reaching the cap with a FULL final page means the
    listing is longer than this script will walk, so "newest" cannot be
    established -- and with the cap now at ~2 years of growth that is a genuine
    anomaly rather than the routine state it used to be.

    It must FAIL rather than select the newest of a prefix. Selecting would be
    betting on the API's undocumented ordering, which this script's own header
    says it deliberately does not do.
    """
    live = tmp_path / "live.db"
    _make_db(live, events=7, actuals=3, watermark=_recent(hours=1))
    _zip_of(live, tmp_path / "live.zip")

    # ⛑ Page 1 holds only OLDER artifacts and the newest sits on page 2, and a
    # zip is provided for BOTH. That shape is deliberate: it makes selecting
    # from the truncated prefix SUCCEED, returning a stale artifact. Without
    # it the mutation "delete the refusal" still exits non-zero -- because the
    # prefix's pick has no downloadable zip -- and the test cannot tell the
    # refusal from an unrelated failure. Verified: this mutation survived the
    # first version of this test for exactly that reason.
    listing = [_artifact(700, "2026-09-01T00:00:00Z"),
               _artifact(800, "2026-09-05T00:00:00Z"),
               _artifact(850, "2026-09-08T00:00:00Z"),
               _artifact(900, "2026-09-10T00:00:00Z")]
    env = _install_fake_gh(tmp_path, listing,
                           {"800": tmp_path / "live.zip",
                            "900": tmp_path / "live.zip"})
    env["EA_DB_ARTIFACT_PAGE"] = "2"
    env["EA_DB_ARTIFACT_MAX_PAGES"] = "1"        # 2 rows walked, 4 available
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 1, res.stdout + res.stderr
    assert "exceeded 1 pages" in res.stderr, res.stderr
    assert "refusing to guess" in res.stderr, res.stderr
    # It must not have picked ANYTHING -- selecting 800 here would be the
    # stale-artifact restore this whole script exists to prevent.
    assert "selected artifact" not in res.stdout, res.stdout
    assert not (work / "earnings_events.db").exists()


def test_a_per_page_above_the_api_maximum_is_clamped_not_trusted(tmp_path):
    """The GitHub API clamps per_page at 100 silently. The walk's stop
    condition is `rows < PAGE`, so an unclamped PAGE=200 would read the API's
    100-row page as SHORT and declare a truncated listing COMPLETE after one
    request -- turning the sizing fix into a new way to select a stale
    artifact. Live-probed 2026-09-23: per_page=200 returns 100 rows.
    """
    live = tmp_path / "live.db"
    _make_db(live, events=7, actuals=3, watermark=_recent(hours=1))
    _zip_of(live, tmp_path / "live.zip")

    listing = [_artifact(900, "2026-09-10T00:00:00Z")]
    env = _install_fake_gh(tmp_path, listing, {"900": tmp_path / "live.zip"})
    env["EA_DB_ARTIFACT_PAGE"] = "200"
    work = tmp_path / "work"
    work.mkdir()

    res = _run(RESTORE, work, env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "clamping to 100" in res.stdout, res.stdout
    # THE EFFECT, not just the message. Asserting only the log line let a
    # mutation that deleted `PAGE=100` survive -- the announcement stayed and
    # the clamp did not happen, which is precisely the always-true-flag shape
    # in miniature. The walk log echoes the EFFECTIVE page size.
    assert "page(s) of 100" in res.stdout, res.stdout
    assert "of 200" not in res.stdout, res.stdout
