"""Regression tests for the IR-signup TickTick push."""
from __future__ import annotations

import ir_ticktick
import ticktick


def _task(title, status=0, tid="x"):
    return {"title": title, "status": status, "id": tid}


def test_spaced_ticker_is_matched():
    """The live bug: `AFX DE` created a duplicate on every run.

    ticktick.find_existing_task_by_ticker splits the title on the first space, so for
    `AFX DE - Carl Zeiss Meditec AG` it returns `AFX`, never equal to the real ticker
    `AFX DE`. Dedup missed on all 11 exchange-suffixed names and the second run
    recreated each one.
    """
    tasks = [_task("AFX DE - Carl Zeiss Meditec AG")]
    assert ticktick.find_existing_task_by_ticker(tasks, "AFX DE") is None   # the old way
    assert ir_ticktick.find_existing(tasks, "AFX DE") is not None           # the fix


def test_plain_ticker_still_matches():
    assert ir_ticktick.find_existing([_task("LLY - Eli Lilly and Company")], "LLY")


def test_bare_title_matches():
    assert ir_ticktick.find_existing([_task("LLY")], "LLY")


def test_prefix_does_not_over_match():
    """`LL` must not match `LLY - ...`, or one company suppresses another's task."""
    assert ir_ticktick.find_existing([_task("LLY - Eli Lilly and Company")], "LL") is None


def test_unrelated_task_is_not_matched():
    assert ir_ticktick.find_existing([_task("ZTS - Zoetis Inc.")], "LLY") is None


def test_completed_task_still_counts_as_existing():
    """A ticked task means JP signed up; recreating it would erase that record."""
    assert ir_ticktick.find_existing([_task("LLY - Eli Lilly", status=2)], "LLY")


def test_title_uses_the_documented_separator():
    row = {"ticker": "AFX DE", "name": "Carl Zeiss Meditec AG"}
    assert ir_ticktick.task_title(row) == f"AFX DE{ir_ticktick._SEP}Carl Zeiss Meditec AG"
    assert ir_ticktick.find_existing([_task(ir_ticktick.task_title(row))], "AFX DE")


def test_title_collapses_when_name_equals_ticker():
    assert ir_ticktick.task_title({"ticker": "BIM", "name": "BIM"}) == "BIM"


def test_content_carries_link_alias_and_evidence():
    rep = {"window_days": 365, "generated": "2026-08-05"}
    row = {"ticker": "LLY", "name": "Eli Lilly", "url": "https://x.test/ir", "how": "probed"}
    c = ir_ticktick.task_content(row, rep)
    assert "https://x.test/ir" in c
    assert ir_coverage_alias() in c
    assert "365 days" in c and "2026-08-05" in c


def ir_coverage_alias():
    import ir_coverage
    return ir_coverage.IR_ALIAS


def test_unverified_link_is_labelled_not_silent():
    rep = {"window_days": 365, "generated": "2026-08-05"}
    row = {"ticker": "TSLA", "name": "Tesla", "url": "https://ir.tesla.com/", "how": "unverified"}
    assert "unverified" in ir_ticktick.task_content(row, rep)


def test_homepage_fallback_is_labelled_not_passed_off_as_ir():
    rep = {"window_days": 365, "generated": "2026-08-05"}
    row = {"ticker": "X", "name": "X Co", "url": "https://x.test/", "how": "homepage"}
    assert "homepage" in ir_ticktick.task_content(row, rep)


def test_missing_link_says_so():
    rep = {"window_days": 365, "generated": "2026-08-05"}
    row = {"ticker": "X", "name": "X Co", "url": None, "how": "none"}
    assert "not found" in ir_ticktick.task_content(row, rep)
