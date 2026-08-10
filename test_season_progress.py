"""Tests for the portfolio earnings-season progress lane.

Every test here is a defect the first build actually produced, or an invariant
whose violation would be silent. In particular: a name is never dropped from
the roster, a percentage never counts a name that has no date, and a blank cell
never means zero.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import date

import pandas as pd
import pytest

import season_render as R
from coverage import TickerInfo
from season_progress import (
    SeasonRow,
    attach_reactions,
    collect_season,
    is_announce_seeded,
    is_seeded,
    mark_announced,
    mark_settled,
    seed_watermark,
    select_unannounced,
    select_unsettled,
)
from storage import init_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _db() -> sqlite3.Connection:
    # init_db takes a PATH, not a connection (matches test_season_stats.py).
    d = tempfile.mkdtemp()
    p = os.path.join(d, "e.db")
    init_db(p)
    return sqlite3.connect(p)


def _cov(**names: str) -> dict[str, TickerInfo]:
    """{TICKER: position} -> coverage map."""
    return {
        t: TickerInfo(
            ticker=t, tier=1, company_name=f"{t} Inc", sector="Healthcare",
            subsector="", position=p,
        )
        for t, p in names.items()
    }


def _event(conn, ticker, event_date, quarter="2026Q2", **kw):
    cols = {
        "ticker": ticker, "quarter": quarter, "event_date": event_date,
        "event_hour": kw.get("hour", "bmo"), "reported": kw.get("reported", 0),
        "eps_estimate": kw.get("eps_estimate"), "eps_actual": kw.get("eps_actual"),
        "rev_estimate": None, "rev_actual": kw.get("rev_actual"),
        "tier": 1, "company_name": kw.get("company_name", f"{ticker} Inc"),
        "date_confirmed": kw.get("date_confirmed", 1), "date_locked": kw.get("date_locked", 0),
    }
    keys = ", ".join(cols)
    conn.execute(
        f"INSERT INTO events ({keys}) VALUES ({', '.join('?' * len(cols))})",
        tuple(cols.values()),
    )
    conn.commit()


AS_OF = date(2026, 8, 9)


# ---------------------------------------------------------------------------
# Roster: nothing is ever dropped
# ---------------------------------------------------------------------------


def test_every_in_scope_name_lands_in_exactly_one_bucket():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "BBB", "2026-08-20")
    _event(conn, "CCC", "2026-08-01")           # past-dated, no actuals
    # DDD has no event row at all.
    cov = _cov(AAA="Portfolio", BBB="Portfolio", CCC="Researching", DDD="Portfolio")

    p = collect_season(conn, cov, AS_OF)

    assert [r.ticker for r in p.reported] == ["AAA"]
    assert [r.ticker for r in p.upcoming] == ["BBB"]
    assert [r.ticker for r in p.overdue] == ["CCC"]
    assert [r.ticker for r in p.no_date] == ["DDD"]
    assert p.in_scope == 4


def test_out_of_scope_positions_are_excluded():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "FFF", "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio", FFF="Following for Interest")

    p = collect_season(conn, cov, AS_OF)
    assert [r.ticker for r in p.reported] == ["AAA"]
    assert p.in_scope == 1


def test_a_name_with_actuals_but_unflipped_reported_counts_as_reported():
    """`reported` is a POSTING flag held back for up to 3 days while the move is
    uncomputable. Keying the roster on it alone shows a company that reported
    this morning as 'still to report' — the one thing this table must get
    right."""
    conn = _db()
    _event(conn, "AAA", "2026-08-07", reported=0, eps_actual=1.23)
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)

    assert [r.ticker for r in p.reported] == ["AAA"]
    assert not p.upcoming and not p.overdue


def test_revenue_only_actuals_also_count_as_reported():
    conn = _db()
    _event(conn, "AAA", "2026-08-07", reported=0, rev_actual=9_000_000.0)
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)
    assert [r.ticker for r in p.reported] == ["AAA"]


def test_an_empty_scope_refuses_to_build_rather_than_reporting_a_finished_season():
    """An empty scope means the Coverage Manager read failed. Rendering
    '0 of 0 reported' would look like a completed season."""
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    p = collect_season(conn, _cov(AAA="Following for Interest"), AS_OF)

    assert p.in_scope == 0
    assert p.scheduled == 0
    assert p.pct_reported is None


def test_events_from_another_season_are_not_counted():
    conn = _db()
    _event(conn, "AAA", "2026-05-01", quarter="2026Q1", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)
    assert p.season == "2026Q2"
    assert [r.ticker for r in p.no_date] == ["AAA"]


# ---------------------------------------------------------------------------
# Denominator honesty
# ---------------------------------------------------------------------------


def test_names_with_no_date_are_excluded_from_the_percentage_not_counted_unreported():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "BBB", "2026-08-20")
    cov = _cov(AAA="Portfolio", BBB="Portfolio", CCC="Portfolio", DDD="Portfolio")

    p = collect_season(conn, cov, AS_OF)

    assert p.scheduled == 2          # not 4
    assert p.pct_reported == 50      # not 25
    assert len(p.no_date) == 2


def test_the_denominator_note_names_the_undated_tickers():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio", ZZZ="Portfolio"), AS_OF)

    note = R._denominator_note(p)
    assert "ZZZ" in note
    assert "EXCLUDED" in note


def test_an_unscheduled_season_says_so_rather_than_rendering_zero_percent():
    conn = _db()
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)
    line = R._funnel_line(p)
    assert "no scheduled reports" in line
    assert "0%" not in line


# ---------------------------------------------------------------------------
# Watermark / daily gate
# ---------------------------------------------------------------------------


def _settle(conn, p, as_of=AS_OF, resolved=True):
    """Post-and-settle, with every reported row's reaction treated as landed."""
    if resolved:
        for r in p.reported:
            r.move_pct = 1.0
    mark_settled(conn, p, as_of)


def test_a_season_starts_unseeded_then_seeds():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)

    assert not is_seeded(conn, p)
    seed_watermark(conn, p)
    assert is_seeded(conn, p)
    assert select_unsettled(conn, p) == set()


def test_an_unseeded_season_announces_nothing():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "BBB", "2026-08-02", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio", BBB="Portfolio"), AS_OF)
    assert select_unsettled(conn, p) == set()


def test_a_newly_reported_name_is_unsettled():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio", BBB="Portfolio")
    p = collect_season(conn, cov, AS_OF)
    seed_watermark(conn, p)

    _event(conn, "BBB", "2026-08-08", reported=1)
    p2 = collect_season(conn, cov, AS_OF)
    assert select_unsettled(conn, p2) == {"BBB"}


def test_a_name_whose_reaction_landed_is_never_shown_again():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio")
    p = collect_season(conn, cov, AS_OF)
    _settle(conn, p)

    p2 = collect_season(conn, cov, AS_OF)
    assert select_unsettled(conn, p2) == set()


def test_an_amc_print_with_a_pending_reaction_returns_on_the_next_card():
    """The bug this model exists to prevent: a name announced the night it
    reports, before its move exists, must come back once the move lands —
    otherwise its reaction is never posted on the daily surface at all."""
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio", BBB="Portfolio")
    seed_watermark(conn, collect_season(conn, cov, AS_OF))

    # Night 1: BBB reports AMC, reaction not computable yet.
    _event(conn, "BBB", "2026-08-08", reported=1, hour="amc")
    night1 = collect_season(conn, cov, date(2026, 8, 8))
    assert select_unsettled(conn, night1) == {"BBB"}
    mark_settled(conn, night1, date(2026, 8, 8))       # move_pct still None

    # Night 2: the move has landed. BBB must appear again.
    night2 = collect_season(conn, cov, date(2026, 8, 9))
    assert select_unsettled(conn, night2) == {"BBB"}

    for r in night2.reported:
        if r.ticker == "BBB":
            r.move_pct = -4.2
    mark_settled(conn, night2, date(2026, 8, 9))

    night3 = collect_season(conn, cov, date(2026, 8, 10))
    assert select_unsettled(conn, night3) == set()


def test_a_reaction_that_never_resolves_is_settled_rather_than_repeating_forever():
    """A delisted name, or a vendor gap, would otherwise reappear on the card
    every night for the rest of the season."""
    conn = _db()
    cov = _cov(AAA="Portfolio", BBB="Portfolio")

    # Seed on a season that contains only BBB, so AAA arrives genuinely new.
    _event(conn, "BBB", "2026-08-01", reported=1)
    seed_watermark(conn, collect_season(conn, cov, AS_OF))

    _event(conn, "AAA", "2026-08-01", reported=1)
    p = collect_season(conn, cov, AS_OF)
    assert select_unsettled(conn, p) == {"AAA"}

    # Its reaction never computes (move_pct stays None). One day on, keep it.
    mark_settled(conn, p, date(2026, 8, 2))
    assert select_unsettled(conn, collect_season(conn, cov, AS_OF)) == {"AAA"}

    # Five days on, settle it anyway rather than repeat forever.
    mark_settled(conn, p, date(2026, 8, 6))
    assert select_unsettled(conn, collect_season(conn, cov, AS_OF)) == set()


def test_a_name_leaving_coverage_does_not_resurface_everyone_as_new():
    """The watermark stores the SET, not a count. A count moves backwards when a
    name leaves mid-season and would re-announce the survivors."""
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "BBB", "2026-08-02", reported=1)
    cov = _cov(AAA="Portfolio", BBB="Portfolio")
    _settle(conn, collect_season(conn, cov, AS_OF))

    shrunk = _cov(AAA="Portfolio")                                # BBB departs
    assert select_unsettled(conn, collect_season(conn, shrunk, AS_OF)) == set()


def test_the_watermark_is_scoped_per_season():
    conn = _db()
    _event(conn, "AAA", "2026-05-01", quarter="2026Q1", reported=1)
    cov = _cov(AAA="Portfolio")
    _settle(conn, collect_season(conn, cov, date(2026, 5, 15)), date(2026, 5, 15))

    _event(conn, "AAA", "2026-08-01", quarter="2026Q2", reported=1)
    # A new season starts from its own seed, not from Q1's set.
    p = collect_season(conn, cov, AS_OF)
    assert not is_seeded(conn, p)
    assert select_unsettled(conn, p) == set()


# ---------------------------------------------------------------------------
# Reaction math
# ---------------------------------------------------------------------------


def _frame(**series: list[float]) -> pd.DataFrame:
    """Build a yfinance-shaped MultiIndex close frame over trading days."""
    idx = pd.to_datetime(pd.bdate_range("2026-07-01", periods=len(next(iter(series.values())))))
    cols, data = [], {}
    for ticker, closes in series.items():
        cols.append((ticker, "Close"))
        data[(ticker, "Close")] = closes
    return pd.DataFrame(data, index=idx, columns=pd.MultiIndex.from_tuples(cols))


def _flat(n: int, start: float = 100.0) -> list[float]:
    return [start] * n


def test_bmo_measures_the_report_day_against_the_prior_close():
    closes = _flat(40)
    closes[30] = 110.0                                   # +10% on the event day
    frame = _frame(AAA=closes, SPY=_flat(40))
    day = frame.index[30].date().isoformat()

    row = SeasonRow("AAA", "A", "Portfolio", day, "bmo", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)

    assert row.move_pct == pytest.approx(10.0)
    assert row.window_label.endswith("close")
    assert "->" in row.window_label


def test_amc_measures_the_next_session_against_the_report_day_close():
    closes = _flat(40)
    closes[31] = 110.0                                   # reaction lands next day
    frame = _frame(AAA=closes, SPY=_flat(40))
    day = frame.index[30].date().isoformat()

    row = SeasonRow("AAA", "A", "Portfolio", day, "amc", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)

    assert row.move_pct == pytest.approx(10.0)


def test_an_amc_print_with_no_next_close_yet_reads_as_pending_not_as_missing():
    frame = _frame(AAA=_flat(31), SPY=_flat(31))
    day = frame.index[30].date().isoformat()             # the last bar we have

    row = SeasonRow("AAA", "A", "Portfolio", day, "amc", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)

    assert row.move_pct is None
    assert "pending" in row.reaction_note


def test_the_benchmark_is_subtracted_over_the_identical_window():
    stock, bench = _flat(40), _flat(40)
    stock[30], bench[30] = 110.0, 105.0
    frame = _frame(AAA=stock, SPY=bench)
    day = frame.index[30].date().isoformat()

    row = SeasonRow("AAA", "A", "Portfolio", day, "bmo", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)

    assert row.move_pct == pytest.approx(10.0)
    assert row.rel_pct == pytest.approx(5.0)


def test_a_missing_benchmark_blanks_only_the_relative_column():
    closes = _flat(40)
    closes[30] = 110.0
    frame = _frame(AAA=closes)                            # no SPY
    day = frame.index[30].date().isoformat()

    row = SeasonRow("AAA", "A", "Portfolio", day, "bmo", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)

    assert row.move_pct == pytest.approx(10.0)
    assert row.rel_pct is None


def test_sigma_excludes_the_reaction_day():
    """Including it lets a large move inflate the very denominator meant to
    measure how large it is."""
    import random

    random.seed(7)
    closes = [100.0]
    for _ in range(39):
        closes.append(closes[-1] * (1 + random.uniform(-0.01, 0.01)))
    closes.append(closes[-1] * 1.30)                      # a 30% print
    frame = _frame(AAA=closes, SPY=_flat(len(closes)))
    day = frame.index[len(closes) - 1].date().isoformat()

    row = SeasonRow("AAA", "A", "Portfolio", day, "bmo", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)

    # With a ~0.6% daily sigma, a 30% move is many sigma. If the reaction day
    # were inside the window it would drag sigma up and collapse this number.
    assert row.sigma is not None and row.sigma > 10


def test_a_failed_download_blanks_the_metrics_and_says_why():
    row = SeasonRow("AAA", "A", "Portfolio", "2026-08-01", "bmo", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: None)

    assert row.move_pct is None and row.sigma is None
    assert row.reaction_note == "price download failed"


def test_unreported_rows_are_never_given_a_reaction():
    frame = _frame(AAA=_flat(40), SPY=_flat(40))
    row = SeasonRow("AAA", "A", "Portfolio", "2026-08-20", "bmo", False, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)
    assert row.move_pct is None and row.reaction_note is None


# ---------------------------------------------------------------------------
# EPS surprise
# ---------------------------------------------------------------------------


def _row(est, act):
    return SeasonRow("A", "A", "Portfolio", "2026-08-01", "bmo", True, True, False,
                     eps_estimate=est, eps_actual=act)


def test_a_normal_surprise_is_a_percentage():
    assert _row(1.00, 1.10).eps_surprise_pct == pytest.approx(10.0)


def test_a_near_zero_consensus_suppresses_the_percentage():
    """ARXS: $0.28 against $0.0292 is a true, useless +859% in a column of
    single digits."""
    assert _row(0.0292, 0.28).eps_surprise_pct is None


def test_a_zero_or_negative_consensus_suppresses_the_percentage():
    assert _row(0.0, 0.5).eps_surprise_pct is None
    assert _row(-0.40, 0.10).eps_surprise_pct is None


def test_a_missing_side_suppresses_the_percentage():
    assert _row(1.0, None).eps_surprise_pct is None
    assert _row(None, 1.0).eps_surprise_pct is None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def test_a_rounded_away_value_loses_its_sign():
    """BSX closed -0.004% and the table said `-0.0%`, which reads as a decline
    rather than as flat."""
    assert R._fmt_pct(-0.004) == "0.0%"
    assert R._fmt_pct(-0.4, 0) == "0%"
    assert R._fmt_sigma(-0.004) == "0.0"


def test_a_real_value_keeps_its_sign():
    assert R._fmt_pct(-1.5) == "-1.5%"
    assert R._fmt_pct(2.0) == "+2.0%"


def test_an_unmeasurable_value_is_blank_never_zero():
    assert R._fmt_pct(None) == ""
    assert R._fmt_sigma(None) == ""


def test_an_all_empty_column_is_dropped():
    headers = ["A", "B", "C"]
    rows = [["1", "", "3"], ["4", "", "6"]]
    out = R.render_table(headers, rows)
    assert "B" not in out[0]
    assert "A" in out[0] and "C" in out[0]


def test_a_column_with_one_value_is_kept_so_blanks_read_as_unmeasurable():
    out = R.render_table(["A", "B"], [["1", ""], ["2", "9"]])
    assert "B" in out[0]


def test_one_width_map_spans_every_chunk_of_a_split_table():
    """Per-chunk widths turn one table into several unrelated ones."""
    rows = [[f"T{i:03d}", "X" * 40, "+1.0%"] for i in range(200)]
    lines = R.render_table(["TICKER", "COMPANY", "MOVE"], rows)
    blocks = R._fenced_sections("Reported", lines)

    assert len(blocks) > 1, "expected this to split"
    headers = {
        b["text"]["text"].split("```")[1].strip().splitlines()[0]
        for b in blocks
    }
    assert len(headers) == 1, "each chunk must repeat the SAME header row"


def test_a_split_table_never_leaves_a_code_fence_open():
    rows = [[f"T{i:03d}", "X" * 40, "+1.0%"] for i in range(200)]
    blocks = R._fenced_sections("Reported", R.render_table(["A", "B", "C"], rows))
    for b in blocks:
        assert b["text"]["text"].count("```") == 2


def test_a_split_never_cuts_a_row_in_half():
    rows = [[f"T{i:03d}", "X" * 40, "+1.0%"] for i in range(200)]
    blocks = R._fenced_sections("Reported", R.render_table(["A", "B", "C"], rows))
    seen = []
    for b in blocks:
        body = b["text"]["text"].split("```")[1].strip().splitlines()[2:]
        seen.extend(body)
    assert len(seen) == 200
    assert all(line.startswith("T") for line in seen)


def test_continued_chunks_are_labelled():
    rows = [[f"T{i:03d}", "X" * 40, "+1.0%"] for i in range(200)]
    blocks = R._fenced_sections("Reported", R.render_table(["A", "B", "C"], rows))
    assert "cont." in blocks[1]["text"]["text"]
    assert "cont." not in blocks[0]["text"]["text"]


def test_no_section_exceeds_the_slack_limit():
    conn = _db()
    cov = {}
    for i in range(120):
        t = f"T{i:03d}"
        cov[t] = TickerInfo(t, 1, f"Company Number {i} Holdings", "HC", "", "Portfolio")
        _event(conn, t, "2026-08-01", reported=1, eps_estimate=1.0, eps_actual=1.1)
    p = collect_season(conn, cov, AS_OF)

    for block in R.build_progress_blocks(p) + R.build_forward_calendar_blocks(p):
        if block["type"] == "section":
            assert len(block["text"]["text"]) <= 3000


def test_the_column_key_appears_exactly_once():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)

    blocks = R.build_progress_blocks(p)
    keys = [
        b for b in blocks
        if b["type"] == "context"
        and "Reading the table" in b["elements"][0]["text"]
    ]
    assert len(keys) == 1


def test_the_marker_carries_direction_and_magnitude():
    up = SeasonRow("A", "A", "Portfolio", "2026-08-01", "bmo", True, True, False)
    up.move_pct, up.sigma = 5.0, 3.0
    assert R._marker(up) == "++"

    down = SeasonRow("A", "A", "Portfolio", "2026-08-01", "bmo", True, True, False)
    down.move_pct, down.sigma = -5.0, -3.0
    assert R._marker(down) == "--"

    small = SeasonRow("A", "A", "Portfolio", "2026-08-01", "bmo", True, True, False)
    small.move_pct, small.sigma = 0.5, 0.2
    assert R._marker(small) == "+"


def test_table_cells_are_ascii_only():
    """A wide or ambiguous-width glyph inside a monospace fence breaks the
    alignment the whole table depends on."""
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1, eps_estimate=1.0, eps_actual=1.1)
    _event(conn, "BBB", "2026-08-20")
    p = collect_season(conn, _cov(AAA="Portfolio", BBB="Researching"), AS_OF)

    cells = [
        c
        for row in R.reported_rows(p.reported) + R.upcoming_rows(p.upcoming, AS_OF)
        for c in row
    ]
    for c in cells:
        assert c.isascii(), f"non-ASCII table cell: {c!r}"


# ---------------------------------------------------------------------------
# Forward calendar
# ---------------------------------------------------------------------------


def test_the_week_ahead_splits_on_the_monday_after_a_sunday_run():
    conn = _db()
    _event(conn, "AAA", "2026-08-10")        # Monday, in the week ahead
    _event(conn, "BBB", "2026-08-24")        # later in the season
    cov = _cov(AAA="Portfolio", BBB="Portfolio")
    p = collect_season(conn, cov, date(2026, 8, 9))   # a Sunday

    text = " ".join(
        b["text"]["text"] for b in R.build_forward_calendar_blocks(p)
        if b["type"] == "section"
    )
    assert "1* of your names report this week" in text
    assert "Monday" in text
    assert "BBB" in text          # still listed, under rest-of-season


def test_a_quiet_week_says_so_rather_than_rendering_an_empty_card():
    conn = _db()
    _event(conn, "BBB", "2026-08-24")
    p = collect_season(conn, _cov(BBB="Portfolio"), date(2026, 8, 9))

    text = " ".join(
        b["text"]["text"] for b in R.build_forward_calendar_blocks(p)
        if b["type"] == "section"
    )
    assert "no Portfolio or Researching names report this week" in text
    assert "1 still outstanding" in text


def test_estimated_dates_are_marked_and_confirmed_ones_are_not():
    conn = _db()
    _event(conn, "AAA", "2026-08-10", date_confirmed=0)
    _event(conn, "BBB", "2026-08-11", date_confirmed=1)
    p = collect_season(conn, _cov(AAA="Portfolio", BBB="Portfolio"), date(2026, 8, 9))

    lines = [
        b["text"]["text"] for b in R.build_forward_calendar_blocks(p)
        if b["type"] == "section"
    ]
    text = "\n".join(lines)
    aaa = [l for l in text.splitlines() if "AAA" in l][0]
    bbb = [l for l in text.splitlines() if "BBB" in l][0]
    assert "(est.)" in aaa
    assert "(est.)" not in bbb


def test_undated_names_are_surfaced_on_the_calendar_not_silently_dropped():
    conn = _db()
    _event(conn, "AAA", "2026-08-10")
    p = collect_season(conn, _cov(AAA="Portfolio", ZZZ="Portfolio"), date(2026, 8, 9))

    text = " ".join(
        e["text"] for b in R.build_forward_calendar_blocks(p)
        if b["type"] == "context" for e in b["elements"]
    )
    assert "ZZZ" in text


def test_status_is_binary_on_trust_not_a_three_level_scale():
    """JP 2026-08-09: *"you have a confirmed and locked status... aren't those
    redundant?"* They are, for the question the column answers.

    `date_confirmed` (evidence: the company announced it) and `date_locked`
    (mechanism: a sync cannot move it) are INDEPENDENT axes. Rendering them as
    an ordered precedence made a row that was BOTH display only "Locked", which
    reads as an alternative kind of certainty rather than as the same one.
    """
    both = SeasonRow("A", "A", "Portfolio", "2026-08-20", "bmo",
                     reported=False, date_confirmed=True, date_locked=True)
    confirmed = SeasonRow("A", "A", "Portfolio", "2026-08-20", "bmo",
                          False, True, False)
    locked_only = SeasonRow("A", "A", "Portfolio", "2026-08-20", "bmo",
                            False, False, True)

    # All three are the same answer to "can I trust this date?"
    assert both.status == confirmed.status == locked_only.status == "Confirmed"

    # The lock survives as provenance, never as a status.
    assert both.pinned and locked_only.pinned
    assert not confirmed.pinned


def test_an_unevidenced_date_is_estimated():
    estimated = SeasonRow("A", "A", "Portfolio", "2026-08-20", "bmo",
                          False, False, False)
    assert estimated.status == "Estimated"
    assert not estimated.pinned


def test_reported_and_undated_still_have_their_own_status():
    reported = SeasonRow("A", "A", "Portfolio", "2026-08-20", "bmo",
                         True, False, True)
    assert reported.status == "Reported"

    undated = SeasonRow("A", "A", "Portfolio", None, None, False, False, False)
    assert undated.status == "No date"


# ---------------------------------------------------------------------------
# Revenue surprise
# ---------------------------------------------------------------------------


def _rev_row(est, act):
    return SeasonRow("A", "A", "Portfolio", "2026-08-01", "bmo", True, True, False,
                     rev_estimate=est, rev_actual=act)


def test_revenue_surprise_is_a_percentage():
    assert _rev_row(1_000_000.0, 1_100_000.0).rev_surprise_pct == pytest.approx(10.0)


def test_revenue_surprise_has_no_near_zero_floor():
    """Unlike EPS: a revenue consensus is an absolute dollar figure in the
    millions, so the unstable-denominator case that suppresses a 2.9-cent EPS
    estimate cannot arise. A small-but-real revenue estimate must still yield a
    number."""
    assert _rev_row(0.05, 0.06).rev_surprise_pct == pytest.approx(20.0)


def test_a_non_positive_revenue_estimate_is_suppressed():
    assert _rev_row(0.0, 500.0).rev_surprise_pct is None
    assert _rev_row(-100.0, 500.0).rev_surprise_pct is None


def test_a_missing_revenue_side_is_suppressed():
    assert _rev_row(1000.0, None).rev_surprise_pct is None
    assert _rev_row(None, 1000.0).rev_surprise_pct is None


# ---------------------------------------------------------------------------
# YTD
# ---------------------------------------------------------------------------


def test_ytd_is_anchored_on_the_prior_year_final_close():
    """Not the first close of January — the Jan 2 close is not the start of the
    year's return, and anchoring there silently drops the first session."""
    closes = _flat(40, start=100.0)
    closes[-1] = 125.0
    frame = _frame(AAA=closes, SPY=_flat(40))
    # bdate_range starts 2026-07-01, so every bar is inside 2026 and there is
    # no prior-year close to anchor on.
    row = SeasonRow("AAA", "A", "Portfolio", None, None, False, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame, with_ytd=True)
    assert row.ytd_pct is None          # unmeasurable, NOT zero


def test_ytd_is_computed_for_names_that_have_not_reported():
    import pandas as pd

    idx = pd.to_datetime(["2025-12-31", "2026-01-02", "2026-08-07"])
    frame = pd.DataFrame(
        {("AAA", "Close"): [100.0, 101.0, 150.0], ("SPY", "Close"): [10.0, 10.0, 10.0]},
        index=idx,
        columns=pd.MultiIndex.from_tuples([("AAA", "Close"), ("SPY", "Close")]),
    )
    upcoming = SeasonRow("AAA", "A", "Portfolio", "2026-08-20", "bmo",
                         reported=False, date_confirmed=True, date_locked=False)

    attach_reactions([upcoming], AS_OF, downloader=lambda *a: frame, with_ytd=True)

    assert upcoming.ytd_pct == pytest.approx(50.0)
    assert upcoming.move_pct is None     # still no reaction — it has not reported


def test_ytd_is_not_computed_unless_asked():
    frame = _frame(AAA=_flat(40), SPY=_flat(40))
    row = SeasonRow("AAA", "A", "Portfolio", "2026-08-01", "bmo", True, True, False)
    attach_reactions([row], AS_OF, downloader=lambda *a: frame)
    assert row.ytd_pct is None


def test_a_dropped_reaction_column_is_explained_rather_than_silent():
    """When the only reporter printed AMC this evening, MOVE/SIGMA/vs SPY are
    all blank and render_table DROPS them — the card would otherwise look like
    it never tried to measure anything."""
    conn = _db()
    _event(conn, "AAA", "2026-08-09", reported=1, hour="amc")
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)
    p.reported[0].reaction_note = "reaction pending (AMC — next close not in yet)"

    blocks = R.build_progress_blocks(p)
    text = " ".join(
        e["text"] for b in blocks if b["type"] == "context" for e in b["elements"]
    )
    assert "Reaction not yet measurable" in text
    assert "AAA" in text
    assert "pending" in text


def test_no_gap_note_when_every_reaction_landed():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio"), AS_OF)
    p.reported[0].move_pct = 3.0

    text = " ".join(
        e["text"] for b in R.build_progress_blocks(p)
        if b["type"] == "context" for e in b["elements"]
    )
    assert "Reaction not yet measurable" not in text


def test_the_weekday_card_notes_gaps_only_for_the_names_it_shows():
    conn = _db()
    _event(conn, "AAA", "2026-08-09", reported=1, hour="amc")
    _event(conn, "BBB", "2026-08-01", reported=1)
    p = collect_season(conn, _cov(AAA="Portfolio", BBB="Portfolio"), AS_OF)
    for r in p.reported:
        r.reaction_note = "reaction pending"
    p.fresh = {"AAA"}

    text = " ".join(
        e["text"] for b in R.build_progress_blocks(p, fresh_only=True)
        if b["type"] == "context" for e in b["elements"]
    )
    assert "AAA" in text
    assert "BBB" not in text


def test_every_card_links_the_full_season_page():
    """JP 2026-08-10 wanted to know when the table updates. The card fires
    exactly when something happened, so it carries the link rather than a
    separate post on every (daily, usually uneventful) page rebuild."""
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "BBB", "2026-08-20")
    p = collect_season(conn, _cov(AAA="Portfolio", BBB="Researching"), AS_OF)
    p.fresh = {"AAA"}

    for blocks in (R.build_progress_blocks(p),
                   R.build_progress_blocks(p, fresh_only=True)):
        text = " ".join(
            e["text"] for b in blocks if b["type"] == "context"
            for e in b["elements"]
        )
        assert R.SEASON_PAGE_URL in text


def test_the_page_link_appears_once_not_per_table():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    _event(conn, "BBB", "2026-08-20")
    p = collect_season(conn, _cov(AAA="Portfolio", BBB="Researching"), AS_OF)

    blocks = R.build_progress_blocks(p)
    n = sum(
        e["text"].count(R.SEASON_PAGE_URL)
        for b in blocks if b["type"] == "context" for e in b["elements"]
    )
    assert n == 1


# ---------------------------------------------------------------------------
# The terse #portfolio ping
# ---------------------------------------------------------------------------


def test_the_first_run_of_a_season_announces_nobody():
    """Otherwise day one pings fifty names at once."""
    conn = _db()
    for t in ("AAA", "BBB", "CCC"):
        _event(conn, t, "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio", BBB="Portfolio", CCC="Researching")
    p = collect_season(conn, cov, AS_OF)

    assert not is_announce_seeded(conn, p)
    mark_announced(conn, p)
    assert select_unannounced(conn, p) == []


def test_a_newly_reported_name_is_announced_once():
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio", RPD="Portfolio")
    mark_announced(conn, collect_season(conn, cov, AS_OF))      # seed

    _event(conn, "RPD", "2026-08-10", reported=1, hour="amc")
    p = collect_season(conn, cov, AS_OF)
    assert [r.ticker for r in select_unannounced(conn, p)] == ["RPD"]

    mark_announced(conn, p)
    assert select_unannounced(conn, collect_season(conn, cov, AS_OF)) == []


def test_an_amc_name_is_NOT_announced_again_when_its_reaction_lands():
    """The settled watermark deliberately re-surfaces an AMC name the evening
    its move resolves — right for the table, wrong for a bare 'RPD reported',
    which would be false the second time. The two watermarks must stay
    separate."""
    conn = _db()
    _event(conn, "AAA", "2026-08-01", reported=1)
    cov = _cov(AAA="Portfolio", RPD="Portfolio")
    seed_watermark(conn, collect_season(conn, cov, AS_OF))
    mark_announced(conn, collect_season(conn, cov, AS_OF))

    _event(conn, "RPD", "2026-08-10", reported=1, hour="amc")

    night1 = collect_season(conn, cov, date(2026, 8, 10))
    assert [r.ticker for r in select_unannounced(conn, night1)] == ["RPD"]
    mark_announced(conn, night1)
    mark_settled(conn, night1, date(2026, 8, 10))      # move still None

    night2 = collect_season(conn, cov, date(2026, 8, 11))
    # The TABLE still owes RPD a second appearance (its reaction landed)...
    assert select_unsettled(conn, night2) == {"RPD"}
    # ...but the PING must not fire again.
    assert select_unannounced(conn, night2) == []


def test_the_ping_is_terse_and_names_the_company():
    conn = _db()
    _event(conn, "RPD", "2026-08-10", reported=1, hour="amc",
           company_name="Rapid7, Inc.")
    p = collect_season(conn, _cov(RPD="Portfolio"), AS_OF)

    blocks = R.build_reported_ping(p.reported, p)
    text = " ".join(
        b["text"]["text"] for b in blocks if b["type"] == "section"
    )
    assert "RPD" in text and "Rapid7" in text and "reported" in text
    assert "after the close" in text          # AMC -> reaction not knowable yet
    assert "Portfolio" in text


def test_the_ping_carries_no_move_number():
    """A number here makes this a second, thinner results card competing with
    the real one — and it changes the moment an AMC name's next close lands."""
    conn = _db()
    _event(conn, "RPD", "2026-08-10", reported=1, hour="amc")
    p = collect_season(conn, _cov(RPD="Portfolio"), AS_OF)
    p.reported[0].move_pct = -4.2
    p.reported[0].sigma = -1.8

    text = " ".join(
        b["text"]["text"] for b in R.build_reported_ping(p.reported, p)
        if b["type"] == "section"
    )
    assert "-4.2" not in text and "4.2" not in text


def test_the_ping_flags_the_bookmark_and_the_page():
    conn = _db()
    _event(conn, "RPD", "2026-08-10", reported=1)
    p = collect_season(conn, _cov(RPD="Portfolio"), AS_OF)

    text = " ".join(
        e["text"] for b in R.build_reported_ping(p.reported, p)
        if b["type"] == "context" for e in b["elements"]
    )
    assert R.SEASON_PAGE_URL in text
    assert "Earnings in Season Progress" in text


def test_an_empty_ping_renders_nothing():
    conn = _db()
    p = collect_season(conn, _cov(RPD="Portfolio"), AS_OF)
    assert R.build_reported_ping([], p) == []
