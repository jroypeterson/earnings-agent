"""The one-day move in the TickTick task header (board #297).

JP: "Append the stock's one-day post-earnings move to the end of the TickTick
entry header ... The size of the move is how JP decides which calls are worth
reading." It already went into the task BODY, which requires opening the task;
the point of a title is that it is visible in the list.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ticktick as tt


def test_the_move_is_appended_to_the_title():
    assert tt.build_task_title("ISRG", "2026-07-16", "amc", move_pct=5.23) == \
        "ISRG Q2 2026 Earnings (Jul 16 AMC) +5.2%"


def test_a_negative_move_keeps_its_sign():
    assert tt.build_task_title("ISRG", "2026-07-16", "amc", move_pct=-12.0).endswith(" -12.0%")


def test_a_rounded_away_move_is_still_a_measurement():
    """+0.0% and "no move recorded" are different facts and must look different."""
    assert tt.build_task_title("ISRG", "2026-07-16", "amc", move_pct=0.0).endswith(" +0.0%")
    assert not tt.build_task_title("ISRG", "2026-07-16", "amc").endswith("%")


def test_an_unconfirmed_row_never_carries_a_move():
    """No confirmed date means no reported move to speak of, and JP's standing
    rule is that an unconfirmed row shows nothing rather than something."""
    assert tt.build_task_title("ISRG", "2026-07-16", "amc",
                               confirmed=False, move_pct=5.2) == "ISRG Q2 2026 Earnings"


# ── the reconcile must TOLERATE and PRESERVE the suffix ────────────────────

def test_strip_and_extract_are_inverses_of_the_builder():
    bare = tt.build_task_title("ISRG", "2026-07-16", "amc")
    withm = tt.build_task_title("ISRG", "2026-07-16", "amc", move_pct=5.23)
    assert tt.strip_move_suffix(withm) == bare
    assert tt.move_suffix_of(withm) == " +5.2%"
    assert tt.strip_move_suffix(bare) == bare
    assert tt.move_suffix_of(bare) == ""


def test_the_stripper_does_not_eat_a_title_that_merely_ends_in_a_number():
    """A percentage is what is stripped, not any trailing digits."""
    assert tt.strip_move_suffix("ACME Q2 2026 Earnings (Jul 16)") == \
        "ACME Q2 2026 Earnings (Jul 16)"
    assert tt.move_suffix_of("100% owned Q2 2026 Earnings") == ""


@pytest.mark.parametrize("pct", [5.23, -12.0, 0.0, 143.7, -0.04])
def test_every_move_the_builder_can_emit_is_strippable(pct):
    """⛑ THE LOAD-BEARING INVARIANT. The move is computed live from yfinance at
    result time and is in NO database column, so `reconcile_ticktick_tasks` --
    which runs 3x a day off the DB and touches no price feed -- cannot rebuild a
    title carrying one. It therefore compares titles with the suffix stripped.

    If the builder can emit a suffix the stripper does not match, that task reads
    as title-stale on every reconcile pass and the move is written back OFF, for
    ever. A format the stripper cannot parse is silent, permanent churn.
    """
    title = tt.build_task_title("ISRG", "2026-07-16", "amc", move_pct=pct)
    assert tt.strip_move_suffix(title) == tt.build_task_title("ISRG", "2026-07-16", "amc")
