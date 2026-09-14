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
        "import json, shutil, sys\n"
        "mode = sys.argv[1]\n"
        "if mode == 'list':\n"
        "    for a in json.load(open(sys.argv[2]))['artifacts']:\n"
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
        f'    python "{helper}" list "{tmp_path / "artifacts.json"}"\n'
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
