"""JP 2026-10-21, three asks in one message:

1. "Add HC REITs group" on the results card.
2. "Add a REIT tag" on TickTick tasks.
3. "Why is ISRG not in the ... Positions/Researching list? ... I definitely want
   portfolio companies to show on the positions/research list." A company whose
   tier changes mid-quarter kept its task in the old list, because creation
   dedups across the quarter's lists and nothing ever moved a task. Six were
   misplaced (ISRG, ATEC, GRAL, KRC, VTR, NTRA). The reconcile now moves a task
   into the list its current tier maps to, via POST /open/v1/task/move.
"""
from datetime import date

import ticktick
from notifications import ResultRow, _results_subcategory, _SUBCATEGORY_ORDER
from storage import init_db, upsert_event, date_to_quarter, mark_sync_completed, \
    find_existing_event
from test_ticktick_reconcile import _stub_api, _std_items

TODAY = date(2026, 10, 21)
P_POS = {"id": "P_POS", "name": "3Q26 Earnings - Positions/Researching"}
P_HC = {"id": "P_HC", "name": "3Q26 Earnings - HC Svcs, MedTech & Biopharma"}
DUE = "2026-10-28T09:00:00.000+0000"


def _conn(ticker, tier, pointer=None):
    conn = init_db(":memory:")
    upsert_event(conn, ticker, "2026-10-28", "bmo", None,
                 quarter=date_to_quarter("2026-10-28"),
                 reported=False, tier=tier, company_name=ticker)
    if pointer:
        conn.execute("UPDATE events SET ticktick_task_id=?", (pointer,))
    mark_sync_completed(conn, TODAY.isoformat())
    conn.commit()
    return conn


def _task(tid, ticker, tags=None):
    return {"id": tid, "title": f"{ticker} Q3 2026 Earnings (Oct 28 BMO)",
            "content": "b", "status": 0, "startDate": DUE, "dueDate": DUE,
            "items": _std_items(), "tags": tags or []}


def _run(monkeypatch, conn, tasks, projects, **kw):
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    posts = _stub_api(monkeypatch, tasks)
    echo = ticktick.requests.post

    def post(url, headers=None, json=None, timeout=None):
        resp = echo(url, headers=headers, json=json, timeout=timeout)
        if url.endswith("/task/move"):
            # The live shape (captured 2026-10-21 moving ISRG): one object per
            # moved task, keyed `id` — NOT an echo of the request body.
            resp._payload = [{"id": m["taskId"], "etag": "e"} for m in json]
        return resp

    monkeypatch.setattr(ticktick.requests, "post", post)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: projects)
    return ticktick.reconcile_ticktick_tasks(conn, TODAY, **kw), posts


def _moves(posts):
    return [p["body"] for p in posts if p["url"].endswith("/task/move")]


# --- 3. list routing --------------------------------------------------------

def test_a_positions_company_in_the_hc_list_is_moved_to_positions(monkeypatch):
    """The ISRG case: Tier 1 now, task still in HC Svcs."""
    conn = _conn("ISRG", tier=1, pointer="T1")
    stats, posts = _run(monkeypatch, conn, {"P_HC": [_task("T1", "ISRG")]},
                        [P_POS, P_HC])

    assert stats["moved"] == 1 and stats["errors"] == 0
    assert _moves(posts) == [[{"fromProjectId": "P_HC", "toProjectId": "P_POS",
                               "taskId": "T1"}]]
    # A move keeps the id, so the DB pointer stays valid untouched.
    assert find_existing_event(conn, "ISRG", "2026-10-28")["ticktick_task_id"] == "T1"


def test_a_task_already_in_its_list_is_not_moved(monkeypatch):
    conn = _conn("ISRG", tier=1, pointer="T1")
    stats, posts = _run(monkeypatch, conn, {"P_POS": [_task("T1", "ISRG")]},
                        [P_POS, P_HC])
    assert stats["moved"] == 0 and _moves(posts) == []


def test_a_tier_2_company_in_the_positions_list_moves_back(monkeypatch):
    """The rule is the CURRENT tier, both directions: a name that left the
    Position lists belongs in HC Svcs."""
    conn = _conn("UNH", tier=2, pointer="T1")
    stats, posts = _run(monkeypatch, conn, {"P_POS": [_task("T1", "UNH")]},
                        [P_POS, P_HC])
    assert stats["moved"] == 1
    assert _moves(posts)[0][0]["toProjectId"] == "P_HC"


def test_a_missing_target_list_is_never_created(monkeypatch):
    """Moving must not mint a list: no target, no move, no project POST."""
    conn = _conn("ISRG", tier=1, pointer="T1")
    stats, posts = _run(monkeypatch, conn, {"P_HC": [_task("T1", "ISRG")]}, [P_HC])
    assert stats["moved"] == 0
    assert not [p for p in posts if p["url"].endswith("/project")]


def test_dry_run_reports_the_move_without_making_it(monkeypatch):
    conn = _conn("ISRG", tier=1, pointer="T1")
    stats, posts = _run(monkeypatch, conn, {"P_HC": [_task("T1", "ISRG")]},
                        [P_POS, P_HC], dry_run=True)
    assert stats["moved"] == 1 and posts == []


def test_later_writes_in_the_same_pass_target_the_new_list(monkeypatch):
    """After a move, a tag write in the same pass must not address the OLD
    list (the task is no longer there, so the write would fail)."""
    conn = _conn("ISRG", tier=1, pointer="T1")
    seen = {}
    real = ticktick.update_task_content

    def spy(token, list_id, task_id, **kw):
        seen["list_id"] = list_id
        return True

    monkeypatch.setattr(ticktick, "update_task_content", spy)
    stats, _ = _run(monkeypatch, conn, {"P_HC": [_task("T1", "ISRG")]},
                    [P_POS, P_HC], sector_by_ticker={"ISRG": "MedTech"})
    assert stats["moved"] == 1
    assert seen.get("list_id") == "P_POS"


# --- 2. REIT tag ------------------------------------------------------------

def test_subsector_tag_is_reit_only_for_healthcare_real_estate():
    assert ticktick.subsector_tag("Healthcare Real Estate") == "REIT"
    assert ticktick.subsector_tag("Retail REIT") is None
    assert ticktick.subsector_tag(None) is None


def test_the_reconcile_adds_the_reit_tag(monkeypatch):
    conn = _conn("ARE", tier=2, pointer="T1")
    stats, posts = _run(monkeypatch, conn, {"P_HC": [_task("T1", "ARE")]},
                        [P_POS, P_HC],
                        subsector_by_ticker={"ARE": "Healthcare Real Estate"})
    assert stats["tag_added"] == 1
    body = [p["body"] for p in posts if p["url"].endswith("/T1")][0]
    assert "REIT" in body["tags"]


# --- 1. results-card group --------------------------------------------------

def _row(**kw):
    base = dict(ticker="ARE", company_name="Alexandria", event_date="2026-10-26",
                event_hour="amc", eps_actual=None, eps_estimate=None,
                rev_actual=None, rev_estimate=None, tier=2,
                sector="Real Estate", subsector="Healthcare Real Estate")
    base.update(kw)
    return ResultRow(**base)


def test_a_healthcare_reit_gets_its_own_results_group():
    assert _results_subcategory(_row()) == "Healthcare REITs"
    assert "Healthcare REITs" in _SUBCATEGORY_ORDER


def test_a_position_still_outranks_the_reit_group():
    """Same priority as every sector group: Position first."""
    assert _results_subcategory(_row(position="Portfolio")) == "Portfolio"


def test_other_real_estate_is_not_a_healthcare_reit():
    assert _results_subcategory(_row(subsector="Retail REIT")) == "Other"
