"""
Tests for the TickTick update-path rewrite + reconcile pass.

Covers the two hard facts verified live 2026-07-22:
  - the single-task GET is dead, so reads must go through /project/{id}/data
    (here: through list_tasks_in_project, which we stub);
  - a task's date needs BOTH startDate and dueDate set together, or the server
    snaps dueDate back to the stale startDate.
"""
from datetime import date

import ticktick
from storage import init_db, upsert_event, find_existing_event, date_to_quarter


class _Resp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload or {"id": "x"}
        self.text = ""

    def json(self):
        return self._payload


def _stub_api(monkeypatch, tasks_by_project):
    """Stub the two network primitives ticktick.py uses; capture POST bodies."""
    posts = []

    def fake_list_tasks(token, project_id):
        # Return copies so mutations by code-under-test don't leak between calls.
        return [dict(t) for t in tasks_by_project.get(project_id, [])]

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append({"url": url, "body": json})
        return _Resp(200, json or {"id": "x"})

    monkeypatch.setattr(ticktick, "list_tasks_in_project", fake_list_tasks)
    monkeypatch.setattr(ticktick, "_list_tasks_strict", fake_list_tasks)  # gather reader
    monkeypatch.setattr(ticktick.requests, "post", fake_post)
    return posts


def test_update_task_sets_both_dates_and_preserves_body(monkeypatch):
    tasks = {
        "L1": [{
            "id": "T1",
            "title": "UNH Q2 2026 Earnings (Jul 27 BMO)",
            "content": "user checklist\n- [x] Read transcript",
            "startDate": "2026-07-27T09:00:00.000+0000",
            "dueDate": "2026-07-27T09:00:00.000+0000",
            "status": 0,
        }]
    }
    posts = _stub_api(monkeypatch, tasks)

    ok = ticktick.update_task_content(
        "tok", "L1", "T1",
        new_title="UNH Q2 2026 Earnings (Jul 16 BMO)",
        new_date="2026-07-16",
    )
    assert ok is True
    assert len(posts) == 1
    body = posts[0]["body"]
    # BOTH date fields moved (dueDate-alone would revert server-side).
    assert body["startDate"] == "2026-07-16T09:00:00.000+0000"
    assert body["dueDate"] == "2026-07-16T09:00:00.000+0000"
    assert body["title"] == "UNH Q2 2026 Earnings (Jul 16 BMO)"
    # Body untouched (new_content omitted) -> checklist ticks preserved.
    assert body["content"] == "user checklist\n- [x] Read transcript"
    # Required identity fields present for the POST.
    assert body["id"] == "T1" and body["projectId"] == "L1"


def test_update_task_missing_task_returns_false(monkeypatch):
    posts = _stub_api(monkeypatch, {"L1": []})  # task not present in project
    ok = ticktick.update_task_content("tok", "L1", "GONE", new_date="2026-07-16")
    assert ok is False
    assert posts == []  # never POSTed


def test_reconcile_marks_reported_fixes_dates_and_skips_done(monkeypatch):
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")

    conn = init_db(":memory:")
    # Reported name whose task is still open + not [REPORTED] (the UNH class).
    upsert_event(conn, "UNH", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 eps_estimate=4.89, eps_actual=6.38, rev_estimate=111e9, rev_actual=112e9,
                 reported=True, tier=2, company_name="UnitedHealth")
    # Pre-report name whose task date drifted from the DB's current date.
    upsert_event(conn, "ABC", "2026-08-05", "bmo", None,
                 quarter=date_to_quarter("2026-08-05"),
                 eps_estimate=1.0, reported=False, tier=2, company_name="ABC Inc")
    # Reported name whose task the user already completed -> must NOT be touched.
    upsert_event(conn, "XYZ", "2026-07-10", "amc", None,
                 quarter=date_to_quarter("2026-07-10"),
                 eps_actual=2.0, reported=True, tier=2, company_name="XYZ Corp")

    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {
        "P_HC": [
            {"id": "T_UNH", "title": "UNH Q2 2026 Earnings (Jul 27 BMO)",
             "content": "body", "startDate": "2026-07-27T09:00:00.000+0000",
             "dueDate": "2026-07-27T09:00:00.000+0000", "status": 0},
            {"id": "T_ABC", "title": "ABC Q2 2026 Earnings (Aug 12 BMO)",
             "content": "body", "startDate": "2026-08-12T09:00:00.000+0000",
             "dueDate": "2026-08-12T09:00:00.000+0000", "status": 0},
            {"id": "T_XYZ", "title": "XYZ Q2 2026 Earnings (Jul 10 AMC)",
             "content": "body", "startDate": "2026-07-10T09:00:00.000+0000",
             "dueDate": "2026-07-10T09:00:00.000+0000", "status": 2},  # DONE
        ]
    }
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000
    )

    assert stats["marked_reported"] == 1     # UNH
    assert stats["date_fixed"] == 1          # ABC
    assert stats["skipped_done"] == 1        # XYZ untouched
    assert stats["errors"] == 0

    # Pointers backfilled onto the DB rows.
    assert find_existing_event(conn, "UNH", "2026-07-16")["ticktick_task_id"] == "T_UNH"
    assert find_existing_event(conn, "ABC", "2026-08-05")["ticktick_task_id"] == "T_ABC"

    # Exactly two writes (UNH mark-reported, ABC date-fix); XYZ never written.
    posted_ids = [p["url"].rsplit("/", 1)[-1] for p in posts]
    assert sorted(posted_ids) == ["T_ABC", "T_UNH"]
    unh_body = next(p["body"] for p in posts if p["url"].endswith("T_UNH"))
    assert unh_body["title"].startswith("[REPORTED]")
    assert unh_body["dueDate"] == "2026-07-16T09:00:00.000+0000"


def test_sector_tag_and_merge():
    assert ticktick.sector_tag("Healthcare Services") == "Healthcare Services"
    assert ticktick.sector_tag("MedTech") == "MedTech"
    assert ticktick.sector_tag("Technology") is None
    assert ticktick.sector_tag(None) is None
    # merge: add when missing (case-insensitive), None when already present
    assert ticktick._merge_tags(None, "MedTech") == ["MedTech"]
    assert ticktick._merge_tags(["mine"], "MedTech") == ["mine", "MedTech"]
    assert ticktick._merge_tags(["medtech"], "MedTech") is None
    assert ticktick._merge_tags(["MedTech", "x"], "MedTech") is None


def test_create_task_includes_tags_and_both_dates(monkeypatch):
    posts = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append(json)
        return _Resp(200, {"id": "NEW"})

    monkeypatch.setattr(ticktick.requests, "post", fake_post)
    tid = ticktick.create_task(
        "tok", "L1", "ABC Q2 2026 Earnings (Aug 05 BMO)", "body",
        "2026-08-05", tags=["MedTech"],
    )
    assert tid == "NEW"
    body = posts[0]
    assert body["tags"] == ["MedTech"]
    # startDate == dueDate so a later date correction (moves both) has a start.
    assert body["startDate"] == "2026-08-05T09:00:00.000+0000"
    assert body["dueDate"] == "2026-08-05T09:00:00.000+0000"


def test_reconcile_adds_missing_sector_tag_preserving_user_tags(monkeypatch):
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    # Pre-report, date is correct -> only the tag is missing.
    upsert_event(conn, "ABC", "2026-08-05", "bmo", None,
                 quarter=date_to_quarter("2026-08-05"),
                 reported=False, tier=2, company_name="ABC Inc")

    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_ABC", "title": "ABC Q2 2026 Earnings (Aug 05 BMO)",
        "content": "body", "dueDate": "2026-08-05T09:00:00.000+0000",
        "status": 0, "tags": ["my-own-tag"],
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), sector_by_ticker={"ABC": "MedTech"},
        max_db_staleness_days=10_000,
    )

    assert stats["tag_added"] == 1
    assert stats["date_fixed"] == 0
    assert len(posts) == 1
    # User's tag preserved, sector tag appended; date untouched.
    assert posts[0]["body"]["tags"] == ["my-own-tag", "MedTech"]


def test_reconcile_skips_tag_when_already_present(monkeypatch):
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-08-05", "bmo", None,
                 quarter=date_to_quarter("2026-08-05"),
                 reported=False, tier=2, company_name="ABC Inc")
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_ABC", "title": "ABC Q2 2026 Earnings (Aug 05 BMO)",
        "content": "body", "dueDate": "2026-08-05T09:00:00.000+0000",
        "status": 0, "tags": ["MedTech"],
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), sector_by_ticker={"ABC": "MedTech"},
        max_db_staleness_days=10_000,
    )
    assert stats["tag_added"] == 0
    assert posts == []  # nothing to write


def test_reconcile_fixes_stale_date_on_already_reported_task(monkeypatch):
    """Finding 1: an already-[REPORTED] task with a stale date must still be
    date-corrected (reconcile is a true date desired-state), keeping its
    [REPORTED] prefix and not rewriting the body."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "UNH", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 eps_actual=6.38, reported=True, tier=2, company_name="UnitedHealth")
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_UNH", "title": "[REPORTED] UNH Q2 2026 Earnings (Jul 27 BMO)",
        "content": "actuals body — do not clobber",
        "startDate": "2026-07-27T09:00:00.000+0000",
        "dueDate": "2026-07-27T09:00:00.000+0000", "status": 0,
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000)

    assert stats["date_fixed"] == 1
    assert stats["marked_reported"] == 0
    body = posts[0]["body"]
    assert body["startDate"] == "2026-07-16T09:00:00.000+0000"
    assert body["dueDate"] == "2026-07-16T09:00:00.000+0000"
    assert body["title"].startswith("[REPORTED]") and "Jul 16" in body["title"]
    assert body["content"] == "actuals body — do not clobber"  # body preserved


def test_reconcile_fixes_stale_startdate_when_duedate_matches(monkeypatch):
    """Finding 1: dueDate matching event_date is not enough — a stale startDate
    (which TickTick would snap dueDate back to) must also trigger a fix."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-08-05", "bmo", None,
                 quarter=date_to_quarter("2026-08-05"),
                 reported=False, tier=2, company_name="ABC Inc")
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_ABC", "title": "ABC Q2 2026 Earnings (Aug 05 BMO)", "content": "b",
        "startDate": "2026-08-01T09:00:00.000+0000",   # stale
        "dueDate": "2026-08-05T09:00:00.000+0000",     # matches
        "status": 0,
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000)

    assert stats["date_fixed"] == 1
    assert posts[0]["body"]["startDate"] == "2026-08-05T09:00:00.000+0000"


def test_reconcile_honors_db_pointer_over_arbitrary_sibling(monkeypatch):
    """Finding 2: when a ticker has tasks in two sibling lists of one quarter,
    an existing DB pointer must win — don't clobber it with an arbitrary pick."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 reported=False, tier=2, company_name="ABC Inc")
    conn.execute("UPDATE events SET ticktick_task_id='T_GOOD' WHERE ticker='ABC'")
    conn.commit()

    p_legacy = {"id": "P_OLD", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    p_hc = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    stale = "2026-07-27T09:00:00.000+0000"
    tasks = {
        "P_OLD": [{"id": "T_OLD", "title": "ABC Q2 2026 Earnings (Jul 27 BMO)",
                   "content": "b", "startDate": stale, "dueDate": stale, "status": 0}],
        "P_HC": [{"id": "T_GOOD", "title": "ABC Q2 2026 Earnings (Jul 27 BMO)",
                  "content": "b", "startDate": stale, "dueDate": stale, "status": 0}],
    }
    posts = _stub_api(monkeypatch, tasks)
    # Legacy list enumerated FIRST — "first wins" would pick T_OLD.
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [p_legacy, p_hc])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000)

    # The write went to the DB-pointed task, not the arbitrary first sibling.
    assert [p["url"].rsplit("/", 1)[-1] for p in posts] == ["T_GOOD"]
    assert stats["pointer_backfilled"] == 0  # good pointer left intact
    assert find_existing_event(conn, "ABC", "2026-07-16")["ticktick_task_id"] == "T_GOOD"


def test_reconcile_phantom_row_does_not_corrupt_reported_task(monkeypatch):
    """Finding 3: a same-quarter unreported phantom (ICLR class) must NOT move
    the reported task to the phantom date or strip [REPORTED]."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    # Real reported row + a no-actuals phantom forward date, same quarter (1Q26).
    upsert_event(conn, "ICLR", "2026-05-27", "amc", None,
                 quarter=date_to_quarter("2026-05-27"),
                 eps_actual=2.52, reported=True, tier=2, company_name="ICON")
    upsert_event(conn, "ICLR", "2026-06-02", "amc", None,
                 quarter=date_to_quarter("2026-06-02"),
                 reported=False, tier=2, company_name="ICON")
    project = {"id": "P_HC", "name": "1Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_ICLR", "title": "[REPORTED] ICLR Q1 2026 Earnings (May 27 AMC)",
        "content": "actuals", "startDate": "2026-05-27T09:00:00.000+0000",
        "dueDate": "2026-05-27T09:00:00.000+0000", "status": 0,
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 6, 2), max_db_staleness_days=10_000)

    # Canonical row is the reported one; task already converged -> no writes.
    assert posts == [], "phantom row moved/stripped the reported task"
    assert stats["date_fixed"] == 0 and stats["marked_reported"] == 0


def test_reconcile_prefers_open_candidate_when_pointer_missing(monkeypatch):
    """Finding 4: with no DB pointer and both a completed legacy task and an
    open current task, reconcile must act on the OPEN one, not skip a done one."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 reported=False, tier=2, company_name="ABC Inc")  # no pointer
    p_legacy = {"id": "P_OLD", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    p_hc = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    stale = "2026-07-27T09:00:00.000+0000"
    tasks = {
        "P_OLD": [{"id": "T_OLD", "title": "ABC Q2 2026 Earnings (Jul 27 BMO)",
                   "content": "b", "startDate": stale, "dueDate": stale, "status": 2}],  # DONE
        "P_HC": [{"id": "T_GOOD", "title": "ABC Q2 2026 Earnings (Jul 27 BMO)",
                  "content": "b", "startDate": stale, "dueDate": stale, "status": 0}],  # OPEN
    }
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [p_legacy, p_hc])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000)

    # Acted on the OPEN task; backfilled its id, not the completed legacy one.
    assert [p["url"].rsplit("/", 1)[-1] for p in posts] == ["T_GOOD"]
    assert find_existing_event(conn, "ABC", "2026-07-16")["ticktick_task_id"] == "T_GOOD"
    assert stats["skipped_done"] == 0


def test_reconcile_phantom_guard_covers_out_of_window_reported(monkeypatch):
    """Finding 5: the reported row is OUTSIDE the reconcile window but its quarter
    is reported — the in-window phantom must still be skipped (DB-wide guard)."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    # Reported May 1 (outside a -14d window from Jun 20), phantom Jun 20 (in window).
    upsert_event(conn, "ICLR", "2026-05-01", "amc", None,
                 quarter=date_to_quarter("2026-05-01"),
                 eps_actual=2.52, reported=True, tier=2, company_name="ICON")
    upsert_event(conn, "ICLR", "2026-06-20", "amc", None,
                 quarter=date_to_quarter("2026-06-20"),
                 reported=False, tier=2, company_name="ICON")
    project = {"id": "P_HC", "name": "1Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_ICLR", "title": "[REPORTED] ICLR Q1 2026 Earnings (May 01 AMC)",
        "content": "actuals", "startDate": "2026-05-01T09:00:00.000+0000",
        "dueDate": "2026-05-01T09:00:00.000+0000", "status": 0,
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 6, 20), max_db_staleness_days=10_000)

    assert posts == [], "out-of-window reported task was corrupted by phantom"
    assert stats["skipped_phantom"] == 1


def test_reconcile_staleness_guard_ignores_unrelated_fresh_row(monkeypatch):
    """Finding 6: a fresh UNRELATED row must not mask stale reconcile targets;
    the guard scopes to unreported Tier 1/2 in-window rows."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "UNH", "2026-07-27", "bmo", None,
                 quarter=date_to_quarter("2026-07-27"),
                 reported=False, tier=2, company_name="UnitedHealth")
    upsert_event(conn, "XYZ", "2026-07-20", "bmo", None,
                 quarter=date_to_quarter("2026-07-20"),
                 reported=False, tier=3, company_name="XYZ")  # unrelated, will be fresh
    # UNH stale, XYZ (Tier 3, out of scope) fresh.
    conn.execute("UPDATE events SET updated_at='2026-06-05 03:00:00' WHERE ticker='UNH'")
    conn.execute("UPDATE events SET updated_at='2026-07-22 03:00:00' WHERE ticker='XYZ'")
    conn.commit()
    called = {"projects": False}
    monkeypatch.setattr(
        ticktick, "_list_all_projects",
        lambda token: called.__setitem__("projects", True) or [],
    )

    stats = ticktick.reconcile_ticktick_tasks(conn, date(2026, 7, 22))  # default staleness

    assert stats["errors"] == 1, "guard should abort on stale Tier 1/2 target"
    assert called["projects"] is False


def test_reconcile_mark_reported_and_tag_is_single_write(monkeypatch):
    """Finding 7: marking reported + adding a missing sector tag must be ONE
    write (title+date+body+tag together), not two — a split tag write can
    re-read stale /project/data and clobber the [REPORTED] repair."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "UNH", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 eps_estimate=4.89, eps_actual=6.38, rev_estimate=111e9, rev_actual=112e9,
                 reported=True, tier=2, company_name="UnitedHealth")
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{
        "id": "T_UNH", "title": "UNH Q2 2026 Earnings (Jul 27 BMO)", "content": "body",
        "startDate": "2026-07-27T09:00:00.000+0000",
        "dueDate": "2026-07-27T09:00:00.000+0000", "status": 0,  # no tags
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22),
        sector_by_ticker={"UNH": "Healthcare Services"}, max_db_staleness_days=10_000)

    assert stats["marked_reported"] == 1 and stats["tag_added"] == 1
    assert len(posts) == 1, "must be a single combined write"
    body = posts[0]["body"]
    assert body["title"].startswith("[REPORTED]") and "Jul 16" in body["title"]
    assert body["dueDate"] == "2026-07-16T09:00:00.000+0000"
    assert body["tags"] == ["Healthcare Services"]


def test_reconcile_skips_row_with_stale_updated_at(monkeypatch):
    """Finding 9: partial staleness — a fresh sibling passes the global guard, so
    an individual stale unreported row must be skipped per-row, not pushed."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-07-27", "bmo", None,
                 quarter=date_to_quarter("2026-07-27"),
                 reported=False, tier=2, company_name="ABC Inc")   # will be stale
    upsert_event(conn, "XYZ", "2026-07-28", "bmo", None,
                 quarter=date_to_quarter("2026-07-28"),
                 reported=False, tier=2, company_name="XYZ Inc")   # fresh
    conn.execute("UPDATE events SET updated_at='2026-06-05 03:00:00' WHERE ticker='ABC'")
    conn.execute("UPDATE events SET updated_at='2026-07-22 03:00:00' WHERE ticker='XYZ'")
    conn.commit()
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    stale = "2026-08-01T09:00:00.000+0000"  # both tasks have a wrong date
    tasks = {"P_HC": [
        {"id": "T_ABC", "title": "ABC Q2 2026 Earnings (Aug 01 BMO)", "content": "b",
         "startDate": stale, "dueDate": stale, "status": 0},
        {"id": "T_XYZ", "title": "XYZ Q2 2026 Earnings (Aug 01 BMO)", "content": "b",
         "startDate": stale, "dueDate": stale, "status": 0},
    ]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    # Global guard passes (XYZ fresh -> MAX is fresh); per-row skips ABC.
    stats = ticktick.reconcile_ticktick_tasks(conn, date(2026, 7, 22))  # default staleness

    assert stats["skipped_stale"] == 1
    assert [p["url"].rsplit("/", 1)[-1] for p in posts] == ["T_XYZ"]  # only the fresh one


def test_reconcile_skips_when_db_pointer_matches_no_candidate(monkeypatch):
    """Finding 8: a set DB pointer that matches no live task this run (a transient
    list read failure looks like an empty list) must NOT be clobbered onto a
    wrong sibling — skip instead."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 reported=False, tier=2, company_name="ABC Inc")
    conn.execute("UPDATE events SET ticktick_task_id='T_GOOD' WHERE ticker='ABC'")
    conn.commit()
    # Only a WRONG sibling is visible this run (T_GOOD's list "failed to read").
    p_old = {"id": "P_OLD", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    stale = "2026-07-27T09:00:00.000+0000"
    tasks = {"P_OLD": [{"id": "T_OLD", "title": "ABC Q2 2026 Earnings (Jul 27 BMO)",
                        "content": "b", "startDate": stale, "dueDate": stale, "status": 0}]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [p_old])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000)

    assert stats["skipped_ambiguous"] == 1
    assert posts == []  # T_OLD not mutated
    # DB pointer left intact (not clobbered to T_OLD).
    assert find_existing_event(conn, "ABC", "2026-07-16")["ticktick_task_id"] == "T_GOOD"


def test_update_task_skips_completed_task(monkeypatch):
    """Finding 11: the completed-task guard lives at the write chokepoint, so
    BOTH reconcile and the notify_results/--check-results path honor it."""
    tasks = {"L1": [{"id": "T1", "title": "x", "content": "c", "status": 2,
                     "startDate": "2026-07-27T09:00:00.000+0000",
                     "dueDate": "2026-07-27T09:00:00.000+0000"}]}
    posts = _stub_api(monkeypatch, tasks)

    ok = ticktick.update_task_content("tok", "L1", "T1",
                                      new_title="[REPORTED] x", new_date="2026-07-16")
    assert ok is True      # no-op success, not an error
    assert posts == []     # completed task NOT rewritten

    # Escape hatch works when explicitly requested.
    ok2 = ticktick.update_task_content("tok", "L1", "T1", new_title="y",
                                       allow_completed=True)
    assert ok2 is True and len(posts) == 1


def test_mark_task_reported_skips_completed_task(monkeypatch):
    """Finding 11: the direct notify_results path (mark_task_reported) also must
    not rewrite a task the user completed."""
    tasks = {"L1": [{"id": "T1", "title": "UNH Q2 2026 Earnings (Jul 27 BMO)",
                     "content": "c", "status": 2,
                     "startDate": "2026-07-27T09:00:00.000+0000",
                     "dueDate": "2026-07-27T09:00:00.000+0000"}]}
    posts = _stub_api(monkeypatch, tasks)
    ok = ticktick.mark_task_reported(
        "tok", "T1", ticker="UNH", event_date="2026-07-16", hour="bmo", tier=2,
        company_name="UnitedHealth", eps_estimate=4.89, eps_actual=6.38,
        revenue_estimate=111e9, revenue_actual=112e9, list_id="L1")
    assert ok is True
    assert posts == []  # completed -> untouched


def test_reconcile_locked_stale_row_does_not_abort(monkeypatch):
    """Finding 10: a date_locked row is an operator override — legitimately old.
    It must not count toward staleness or the whole reconcile aborts."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-08-20", "bmo", None,
                 quarter=date_to_quarter("2026-08-20"),
                 reported=False, tier=2, company_name="ABC Inc")
    # Operator-pinned, and legitimately old.
    conn.execute("UPDATE events SET date_locked=1, updated_at='2026-06-05 03:00:00' "
                 "WHERE ticker='ABC'")
    conn.commit()
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{"id": "T_ABC", "title": "ABC Q2 2026 Earnings (Aug 20 BMO)",
                       "content": "b", "startDate": "2026-08-20T09:00:00.000+0000",
                       "dueDate": "2026-08-20T09:00:00.000+0000", "status": 0}]}
    _stub_api(monkeypatch, tasks)
    called = {"projects": False}
    monkeypatch.setattr(
        ticktick, "_list_all_projects",
        lambda token: called.__setitem__("projects", True) or [project])

    stats = ticktick.reconcile_ticktick_tasks(conn, date(2026, 7, 22))  # default staleness

    assert called["projects"] is True, "locked stale row wrongly aborted reconcile"
    assert stats["errors"] == 0
    assert stats["skipped_stale"] == 0  # locked row not skipped either


def test_reconcile_skips_null_pointer_adoption_on_read_failure(monkeypatch):
    """Finding 13: if a quarter's list read failed, a NULL-pointer ticker must
    NOT be adopted onto a stale sibling from a still-readable list."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "ABC", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 reported=False, tier=2, company_name="ABC Inc")  # NULL pointer
    p_old = {"id": "P_OLD", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    p_hc = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    stale = "2026-07-27T09:00:00.000+0000"
    # P_OLD returns a stale sibling; P_HC (where the real task lives) fails to read.
    old_tasks = [{"id": "T_OLD", "title": "ABC Q2 2026 Earnings (Jul 27 BMO)",
                  "content": "b", "startDate": stale, "dueDate": stale, "status": 0}]

    posts = []

    def fake_strict(token, project_id):
        if project_id == "P_HC":
            raise ticktick.TickTickError("simulated read timeout")
        return [dict(t) for t in old_tasks]

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append({"url": url, "body": json})
        return _Resp(200, json or {"id": "x"})

    monkeypatch.setattr(ticktick, "_list_tasks_strict", fake_strict)
    monkeypatch.setattr(ticktick, "list_tasks_in_project",
                        lambda token, pid: [] if pid == "P_HC" else [dict(t) for t in old_tasks])
    monkeypatch.setattr(ticktick.requests, "post", fake_post)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [p_old, p_hc])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), max_db_staleness_days=10_000)

    assert stats["skipped_ambiguous"] == 1
    assert posts == []  # did NOT adopt/mutate the stale sibling
    assert find_existing_event(conn, "ABC", "2026-07-16")["ticktick_task_id"] is None


def test_reconcile_refuses_stale_db(monkeypatch):
    """A stale DB must abort reconcile — pushing stale dates onto TickTick would
    revert correct dates / un-report names (the frozen-local-DB corruption)."""
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    # An UNREPORTED upcoming Tier 1/2 event is a reconcile date-target; if it's
    # stale the whole DB is frozen and its date can't be trusted.
    upsert_event(conn, "UNH", "2026-07-27", "bmo", None,
                 quarter=date_to_quarter("2026-07-27"),
                 reported=False, tier=2, company_name="UnitedHealth")
    # Force the newest updated_at far into the past.
    conn.execute("UPDATE events SET updated_at = '2026-06-05 03:00:00'")
    conn.commit()

    called = {"projects": False}
    monkeypatch.setattr(
        ticktick, "_list_all_projects",
        lambda token: called.__setitem__("projects", True) or [],
    )

    stats = ticktick.reconcile_ticktick_tasks(conn, date(2026, 7, 22))

    assert stats["errors"] == 1
    assert stats["marked_reported"] == 0
    assert called["projects"] is False, "should abort before touching the API"


def test_reconcile_dry_run_writes_nothing(monkeypatch):
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    conn = init_db(":memory:")
    upsert_event(conn, "UNH", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 eps_actual=6.38, reported=True, tier=2, company_name="UnitedHealth")
    project = {"id": "P_HC", "name": "2Q26 Earnings - HC Svcs & MedTech"}
    tasks = {"P_HC": [{"id": "T_UNH", "title": "UNH Q2 2026 Earnings (Jul 27 BMO)",
                       "content": "body", "dueDate": "2026-07-27T09:00:00.000+0000",
                       "status": 0}]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22), dry_run=True, max_db_staleness_days=10_000
    )

    assert stats["marked_reported"] == 1   # counted as "would do"
    assert posts == []                      # but nothing written
    # DB pointer also left alone in dry-run.
    assert find_existing_event(conn, "UNH", "2026-07-16")["ticktick_task_id"] is None
