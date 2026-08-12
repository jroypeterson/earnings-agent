"""Position tags, and reported-ness as a tag rather than a title prefix.

JP 2026-08-12, two asks in one message:

  *"going forward lets tag entries as portfolio or researching so I can sort by
  that"* — a Position tag on every managed task.

  *"Instead of having [Reported] at the front as a text lets just add it as a
  tag that the stock has reported"* — reported-ness stops being a string glued
  to the front of the title (which `_ticker_from_task_title` then had to parse
  back off) and becomes a facet TickTick can sort and filter natively.

The second is a convention change over ~700 live tasks, so the interesting
cases are all about the migration: a legacy task must converge exactly once,
must not lose its date or its actuals body doing so, and must never be
re-counted as newly reported.
"""
from datetime import date

import ticktick
from storage import init_db, upsert_event, date_to_quarter

from test_ticktick_reconcile import _stub_api, _std_items


# ── Tag vocabulary ──────────────────────────────────────────────────────

def test_position_tag_covers_every_position_not_just_the_two_named():
    """Ready to Buy / Ready to Short share the Core Watchlist list with
    Portfolio and Researching. Tagging only the two JP named would leave
    untagged rows in the middle of the sort he asked for."""
    assert ticktick.position_tag("Portfolio") == "Portfolio"
    assert ticktick.position_tag("Researching") == "Researching"
    assert ticktick.position_tag("Ready to Buy") == "Ready to Buy"
    assert ticktick.position_tag("Ready to Short") == "Ready to Short"
    assert ticktick.position_tag("Following for Interest") == "Following for Interest"


def test_position_tag_refuses_to_guess():
    """Mirrors sector_tag: an unknown value is left untagged, not invented."""
    assert ticktick.position_tag("") is None
    assert ticktick.position_tag(None) is None
    assert ticktick.position_tag("Watching Loosely") is None


def test_merge_tags_is_case_insensitive_because_ticktick_lowercases():
    """Probe-verified 2026-08-12: POSTing ['MixedCase','Portfolio'] reads back
    as ['portfolio','mixedcase']. A case-sensitive check would find 'Portfolio'
    missing from ['portfolio'] on EVERY pass and rewrite every task forever."""
    assert ticktick._merge_tags(["portfolio"], "Portfolio") is None
    assert ticktick._merge_tags(["BIOPHARMA"], "Biopharma") is None


def test_merge_tags_dedupes_within_its_own_arguments():
    """mark_task_reported appends REPORTED_TAG to a list the reconcile may have
    already put it in. Without this the task ends up tagged
    ['Reported', 'Healthcare Services', 'Reported']."""
    assert ticktick._merge_tags([], "Reported", "Healthcare Services", "Reported") == \
        ["Reported", "Healthcare Services"]


def test_merge_tags_preserves_tags_the_user_added():
    """Tags are merged, never assigned — JP's own tags must survive."""
    assert ticktick._merge_tags(["mine"], "Portfolio") == ["mine", "Portfolio"]


# ── Reported detection ──────────────────────────────────────────────────

def test_task_is_reported_reads_the_tag():
    assert ticktick.task_is_reported({"title": "UNH Q2 2026 Earnings", "tags": ["reported"]})


def test_task_is_reported_still_honours_the_legacy_prefix():
    """~700 tasks predate the change. If this returned False they would take
    the mark-reported branch, which rewrites the body and re-counts the task as
    newly reported on every pass until it converged."""
    assert ticktick.task_is_reported(
        {"title": "[REPORTED] UNH Q2 2026 Earnings (Jul 16 BMO)", "tags": []})


def test_task_is_reported_is_false_for_a_plain_open_task():
    assert not ticktick.task_is_reported(
        {"title": "UNH Q2 2026 Earnings (Jul 16 BMO)", "tags": ["Portfolio"]})


# ── The mark-reported write ─────────────────────────────────────────────

def test_mark_reported_tags_instead_of_prefixing_the_title(monkeypatch):
    tasks = {"L1": [{
        "id": "T1", "title": "UNH Q2 2026 Earnings (Jul 27 BMO)", "content": "old",
        "startDate": "2026-07-27T09:00:00.000+0000",
        "dueDate": "2026-07-27T09:00:00.000+0000", "status": 0,
    }]}
    posts = _stub_api(monkeypatch, tasks)
    ok = ticktick.mark_task_reported(
        "tok", "T1", ticker="UNH", event_date="2026-07-16", hour="bmo", tier=2,
        company_name="UnitedHealth", eps_estimate=4.89, eps_actual=6.38,
        revenue_estimate=111e9, revenue_actual=112e9, list_id="L1")
    assert ok is True
    body = posts[0]["body"]
    assert body["title"] == "UNH Q2 2026 Earnings (Jul 16 BMO)"
    assert "[REPORTED]" not in body["title"]
    assert "Reported" in body["tags"]


def test_mark_reported_does_not_strip_tags_when_the_caller_passes_none(monkeypatch):
    """main.py's notify_results calls mark_task_reported with no tags at all.
    Assigning rather than merging would strip the sector and Position tags off
    every task that path touches — which is most of them, every season."""
    tasks = {"L1": [{
        "id": "T1", "title": "UNH Q2 2026 Earnings (Jul 27 BMO)", "content": "old",
        "tags": ["Healthcare Services", "Portfolio"], "status": 0,
    }]}
    posts = _stub_api(monkeypatch, tasks)
    ticktick.mark_task_reported(
        "tok", "T1", ticker="UNH", event_date="2026-07-16", hour="bmo", tier=2,
        company_name="UnitedHealth", eps_estimate=None, eps_actual=6.38,
        revenue_estimate=None, revenue_actual=None, list_id="L1")
    tags = posts[0]["body"]["tags"]
    assert set(tags) == {"Healthcare Services", "Portfolio", "Reported"}


# ── Migration through the reconcile ─────────────────────────────────────

def _reported_conn():
    conn = init_db(":memory:")
    upsert_event(conn, "UNH", "2026-07-16", "bmo", None,
                 quarter=date_to_quarter("2026-07-16"),
                 eps_actual=6.38, reported=True, tier=1, company_name="UnitedHealth")
    return conn


def test_legacy_prefixed_task_migrates_to_the_tag_in_one_write(monkeypatch):
    """The whole migration path: strip the prefix, add the tag, same write —
    and count it as a title fix, NOT as newly reported."""
    conn = _reported_conn()
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    project = {"id": "P1", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    tasks = {"P1": [{
        "id": "T_UNH", "title": "[REPORTED] UNH Q2 2026 Earnings (Jul 16 BMO)",
        "content": "actuals body", "startDate": "2026-07-16T09:00:00.000+0000",
        "dueDate": "2026-07-16T09:00:00.000+0000", "status": 0, "items": _std_items(),
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    stats = ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22),
        sector_by_ticker={"UNH": "Healthcare Services"},
        position_by_ticker={"UNH": "Portfolio"},
        max_db_staleness_days=10_000)

    assert len(posts) == 1
    body = posts[0]["body"]
    assert body["title"] == "UNH Q2 2026 Earnings (Jul 16 BMO)"
    assert set(body["tags"]) == {"Healthcare Services", "Portfolio", "Reported"}
    assert body["content"] == "actuals body", "body must not be rewritten"
    assert stats["marked_reported"] == 0, "a migration is not a new report"
    assert stats["title_fixed"] == 1


def test_a_migrated_task_is_stable_on_the_next_pass(monkeypatch):
    """The reconcile runs 3x/day. If the converged shape still looked stale it
    would rewrite every managed task forever — the exact failure the
    case-insensitive tag compare exists to prevent, one level up."""
    conn = _reported_conn()
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    project = {"id": "P1", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    tasks = {"P1": [{
        "id": "T_UNH", "title": "UNH Q2 2026 Earnings (Jul 16 BMO)",
        "content": "actuals body", "startDate": "2026-07-16T09:00:00.000+0000",
        "dueDate": "2026-07-16T09:00:00.000+0000", "status": 0, "items": _std_items(),
        # lowercase, as TickTick actually stores them
        "tags": ["healthcare services", "portfolio", "reported"],
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 7, 22),
        sector_by_ticker={"UNH": "Healthcare Services"},
        position_by_ticker={"UNH": "Portfolio"},
        max_db_staleness_days=10_000)

    assert posts == [], "converged task was rewritten — the pass is not idempotent"


def test_position_tag_reaches_an_unreported_task_too(monkeypatch):
    """The sort has to work before a company reports, which is when JP is
    actually using the list to plan."""
    conn = init_db(":memory:")
    upsert_event(conn, "NVDA", "2026-08-26", "amc", None,
                 quarter=date_to_quarter("2026-08-26"),
                 # hour="amc" IS the confirmation signal — date_confirmed is
                 # derived from it, so this row projects as a DATED task.
                 reported=False, tier=1, company_name="Nvidia")
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    project = {"id": "P1", "name": "2Q26 Earnings - Core Watchlist - Positions/Researching"}
    tasks = {"P1": [{
        "id": "T_NV", "title": "NVDA Q2 2026 Earnings (Aug 26 AMC)", "content": "b",
        "startDate": "2026-08-26T09:00:00.000+0000",
        "dueDate": "2026-08-26T09:00:00.000+0000", "status": 0, "items": _std_items(),
    }]}
    posts = _stub_api(monkeypatch, tasks)
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    ticktick.reconcile_ticktick_tasks(
        conn, date(2026, 8, 12), position_by_ticker={"NVDA": "Portfolio"},
        max_db_staleness_days=10_000)

    assert len(posts) == 1
    assert posts[0]["body"]["tags"] == ["Portfolio"]
    assert "Reported" not in posts[0]["body"]["tags"], "not reported yet"


# ── A tracked name with no task must be NAMED, not just counted ─────────

def test_a_reported_row_with_no_task_is_named_in_a_warning(monkeypatch, caplog):
    """`no_task` counted this condition for two years and named nothing, so
    `no_task=20` scrolled past every run while LLY had no task for a whole
    season. The reported ones especially: main.py's creation query is
    forward-only, so nothing will ever mint their task automatically."""
    conn = init_db(":memory:")
    upsert_event(conn, "RVMD", "2026-08-05", "amc", None,
                 quarter=date_to_quarter("2026-08-05"),
                 eps_actual=1.1, reported=True, tier=2, company_name="Revolution")
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    project = {"id": "P1", "name": "2Q26 Earnings - HC Svcs, MedTech & Biopharma"}
    _stub_api(monkeypatch, {"P1": []})          # the list exists but is EMPTY
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    with caplog.at_level("WARNING"):
        stats = ticktick.reconcile_ticktick_tasks(
            conn, date(2026, 8, 12), max_db_staleness_days=10_000)

    assert stats["no_task"] == 1 and stats["no_task_open"] == 1
    warned = "\n".join(r.getMessage()
                       for r in caplog.records if r.levelname == "WARNING")
    assert "RVMD@2026-08-05" in warned
    assert "ALREADY REPORTED" in warned


def test_a_closed_row_with_no_task_is_not_warned_about(monkeypatch, caplog):
    """An acquired name that never reported SHOULD have no task — it was the
    majority (10 of 20) on the run that prompted this, and warning about it
    would bury the half that matters."""
    conn = init_db(":memory:")
    upsert_event(conn, "EXAS", "2026-08-04", "amc", None,
                 quarter=date_to_quarter("2026-08-04"),
                 reported=False, tier=2, company_name="Exact Sciences")
    conn.execute("UPDATE events SET closed_reason='delisted' WHERE ticker='EXAS'")
    conn.commit()
    monkeypatch.setenv("TICKTICK_ACCESS_TOKEN", "tok")
    project = {"id": "P1", "name": "2Q26 Earnings - HC Svcs, MedTech & Biopharma"}
    _stub_api(monkeypatch, {"P1": []})
    monkeypatch.setattr(ticktick, "_list_all_projects", lambda token: [project])

    with caplog.at_level("WARNING"):
        stats = ticktick.reconcile_ticktick_tasks(
            conn, date(2026, 8, 12), max_db_staleness_days=10_000)

    assert stats["no_task"] == 1, "still counted"
    assert stats["no_task_open"] == 0, "but not surfaced as a gap"
    # getMessage(), not .message: the latter is the unformatted template, so a
    # ticker that only appears via lazy %-args would slip past this assertion.
    assert "EXAS" not in "\n".join(r.getMessage() for r in caplog.records
                                   if r.levelname == "WARNING")
