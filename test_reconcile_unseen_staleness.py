"""The reconcile's per-row staleness: "the providers stopped listing it", not
"the row has not changed lately".

2026-10-21: the per-row check aged `updated_at`, which only moves when a row's
DATA changes (the sync skips an unchanged re-listed event without writing it).
A settled, company-confirmed date therefore aged into "stale" and was never
pushed: BSX moved to Oct 28 on Aug 28 and its task sat on Oct 20 (89 of 282
rows skipped that day). The check now reads `unseen_run_count`, the B2 counter.

Every test drives the real `reconcile_ticktick_tasks` under the DEFAULT
`max_db_staleness_days` with a fresh sync heartbeat, so a regression in the
global guard is not hidden by an overridden threshold. The last test is a seam
test: the real sync writes the counter, the real reconcile reads it.
"""
from datetime import date, timedelta

import ticktick
from config import UNSEEN_STALE_RUNS
from storage import init_db, upsert_event, date_to_quarter, mark_sync_completed
from test_ticktick_reconcile import _stub_api, _std_items

TODAY = date(2026, 10, 21)
OLD = "2026-08-28 03:38:42"          # BSX's real updated_at
P_CORE = {"id": "P_CORE",
          "name": "3Q26 Earnings - Core Watchlist - Positions/Researching"}


def _iso(d):
    return f"{d}T09:00:00.000+0000"


def _conn(*rows):
    """rows: (ticker, event_date, hour, updated_at, unseen, locked, reported)."""
    conn = init_db(":memory:")
    for tk, d, hour, upd, unseen, locked, reported in rows:
        upsert_event(conn, tk, d, hour, None, quarter=date_to_quarter(d),
                     reported=reported, tier=1, company_name=tk)
        conn.execute(
            "UPDATE events SET updated_at=?, unseen_run_count=?, date_locked=? "
            "WHERE ticker=? AND event_date=?", (upd, unseen, int(locked), tk, d))
    mark_sync_completed(conn, TODAY.isoformat())
    conn.commit()
    return conn


def _task(tid, title, d=None, **extra):
    t = {"id": tid, "title": title, "content": "body", "status": 0,
         "items": _std_items()}
    if d:
        t["startDate"] = t["dueDate"] = _iso(d)
    t.update(extra)
    return t


def _run(monkeypatch, conn, tasks):
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    posts = _stub_api(monkeypatch, {"P_CORE": tasks})
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [P_CORE])
    stats = ticktick.reconcile_ticktick_tasks(conn, TODAY)   # default staleness
    return stats, posts


def _posts_for(posts, tid):
    return [p["body"] for p in posts if p["url"].endswith(tid)]


def test_a_settled_confirmed_date_is_pushed_however_old_the_row(monkeypatch):
    """(a) The BSX case, verbatim. Fails on the updated_at version."""
    conn = _conn(("BSX", "2026-10-28", "bmo", OLD, 0, False, False))
    stats, posts = _run(monkeypatch, conn, [
        _task("T_BSX", "BSX Q3 2026 Earnings (Oct 20 BMO)", "2026-10-20")])

    assert stats["skipped_stale"] == 0
    assert stats["date_fixed"] == 1
    body, = _posts_for(posts, "T_BSX")
    assert body["title"] == "BSX Q3 2026 Earnings (Oct 28 BMO)"
    assert body["startDate"] == body["dueDate"] == _iso("2026-10-28")


def test_an_undated_task_gets_its_confirmed_date(monkeypatch):
    """(e) The HUM case: confirmed Nov 6, task undated, row untouched since Aug."""
    conn = _conn(("HUM", "2026-11-06", "bmo", "2026-08-08 11:41:35", 0, False, False))
    stats, posts = _run(monkeypatch, conn, [_task("T_HUM", "HUM Q3 2026 Earnings")])

    assert stats["date_fixed"] == 1
    body, = _posts_for(posts, "T_HUM")
    assert body["startDate"] == _iso("2026-11-06")


def test_one_unseen_sync_is_not_yet_stale(monkeypatch):
    """(c) Boundary: one missed sync is noise, not abandonment (kills `>= 1`)."""
    conn = _conn(("BSX", "2026-10-28", "bmo", OLD, UNSEEN_STALE_RUNS - 1, False, False))
    stats, posts = _run(monkeypatch, conn, [
        _task("T_BSX", "BSX Q3 2026 Earnings (Oct 20 BMO)", "2026-10-20")])

    assert stats["skipped_stale"] == 0
    assert stats["date_fixed"] == 1


def test_a_row_the_providers_stopped_listing_is_not_pushed(monkeypatch):
    """(b) ENSG shape, made discriminating: updated_at is TODAY, so the old
    check called it fresh and pushed it. The counter says the providers have
    dropped it — its date is untrusted and must not move the task."""
    conn = _conn(("ENSG", "2026-10-28", "amc", "2026-10-21 03:00:00",
                  UNSEEN_STALE_RUNS, False, False))
    stats, posts = _run(monkeypatch, conn, [
        _task("T_ENSG", "ENSG Q3 2026 Earnings (Oct 20 AMC)", "2026-10-20")])

    assert stats["skipped_stale"] == 1
    assert stats["date_fixed"] == 0
    assert _posts_for(posts, "T_ENSG") == []


def test_a_locked_row_is_trusted_whatever_the_counter_says(monkeypatch):
    """(d) The counter climbs on a lock (the sync never re-sees a pinned date),
    so the lock exemption must survive or every locked row is skipped forever."""
    conn = _conn(("BSX", "2026-10-28", "bmo", OLD, 5, True, False))
    stats, posts = _run(monkeypatch, conn, [
        _task("T_BSX", "BSX Q3 2026 Earnings (Oct 20 BMO)", "2026-10-20")])

    assert stats["skipped_stale"] == 0
    assert stats["date_fixed"] == 1


def test_a_reported_row_is_marked_whatever_the_counter_says(monkeypatch):
    """(d) Reported rows carry the real report date; staleness never applies."""
    conn = _conn(("BSX", "2026-10-20", "bmo", OLD, 5, False, True))
    conn.execute("UPDATE events SET eps_actual=0.9, eps_estimate=0.8")
    conn.commit()
    stats, posts = _run(monkeypatch, conn, [
        _task("T_BSX", "BSX Q3 2026 Earnings (Oct 20 BMO)", "2026-10-20")])

    assert stats["skipped_stale"] == 0
    assert stats["marked_reported"] == 1


def test_an_unconfirmed_unseen_row_is_still_stripped(monkeypatch):
    """(f) JP's no-date-until-confirmed rule is freshness-independent: removing
    a date pushes nothing untrusted, so the counter must not block it."""
    conn = _conn(("ABC", "2026-10-28", "", OLD, 5, False, False))
    stats, posts = _run(monkeypatch, conn, [
        _task("T_ABC", "ABC Q3 2026 Earnings (Oct 28)", "2026-10-28")])

    assert stats["date_cleared"] == 1
    assert stats["skipped_stale"] == 0
    body, = _posts_for(posts, "T_ABC")
    assert body["startDate"] is None and body["dueDate"] is None


# --- (g) Seam: the real sync writes the counter, the real reconcile reads it ---

def test_the_sync_and_the_reconcile_agree_on_the_counter(monkeypatch, tmp_path):
    """A hand-set column proves only that the reconcile reads it. This drives
    the writer too: two real syncs with the event absent from the fetch must
    make the real reconcile skip it, and a sync that sees it again must make
    the reconcile push it."""
    import sqlite3
    import test_run_integration as tri

    today = date.today()
    ev_date = (today + timedelta(days=10)).isoformat()
    wrong = (today + timedelta(days=3)).isoformat()
    db_path = str(tmp_path / "ea.db")
    tri._seed_upcoming(db_path, ["ARGX"], ev_date)          # confirmed ("amc")

    # No foreign-listing rescue: the vendor genuinely has nothing for ARGX.
    monkeypatch.setattr(
        "main.probe_foreign_listing_dates", lambda client, pairs: set())

    def sync(events):
        tri._run_env(monkeypatch, tmp_path, coverage=[tri._tkr("ARGX")],
                     events=events)

    def reconcile():
        conn = sqlite3.connect(db_path)
        try:
            rq = ticktick._reporting_quarter(ev_date)
            project = {"id": "P", "name": f"{rq} Earnings - Core Watchlist - "
                                          "Positions/Researching"}
            monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
            posts = _stub_api(monkeypatch, {"P": [
                _task("T_ARGX", "ARGX wrong-dated", wrong)]})
            monkeypatch.setattr(ticktick, "_list_all_projects", lambda t: [project])
            return ticktick.reconcile_ticktick_tasks(conn, today), posts
        finally:
            conn.close()

    for _ in range(UNSEEN_STALE_RUNS):
        sync([tri._ev("OTHER", ev_date)])                    # ARGX absent
    stats, posts = reconcile()
    assert stats["errors"] == 0, "the real sync must stamp a fresh heartbeat"
    assert stats["skipped_stale"] == 1
    assert _posts_for(posts, "T_ARGX") == []

    sync([tri._ev("ARGX", ev_date)])                         # seen again
    stats, posts = reconcile()
    assert stats["skipped_stale"] == 0
    body, = _posts_for(posts, "T_ARGX")
    assert body["startDate"] == _iso(ev_date)
