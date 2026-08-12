"""A held name is Tier 1, and a stale tier cannot outlive coverage.

Two halves of one live failure. On 2026-08-12 JP asked whether LLY was in any
of his 2Q26 TickTick earnings lists. It was in none of them — no task had ever
been created for it, in any quarter.

**Half one: the rule.** Tier 1 required `Portfolio ∩ Core=Y`, and Coverage
Manager carried LLY at `Core=''` in `portfolio.json` until 2026-08-06. So a
name he owns was Tier 3 — below the `tier <= 2` gate that every TickTick,
calendar and digest-detail query uses — for the entire 2Q26 season. JP: *"I own
LLY so it should be in positions & researching TickTick regardless of how it's
categorized on coverage manager"*. `ir_ticktick.load_universe` had already
reached the same conclusion from the other direction on 2026-08-05 and named
the same 11 held-but-not-Core tickers.

**Half two: the staleness.** Fixing the rule is not enough, because
`events.tier` is only re-stamped when a provider happens to re-emit that
ticker+date. Measured 2026-08-11: 123 of ~850 in-window rows. So 37 open 3Q26
rows sat at tier 3 carrying a classification from before the biopharma Core
batch, invisible to every consumer, with nothing to correct them.
`restamp_tiers_from_coverage` re-derives tier from live coverage each run.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

UNIVERSE_COLS = ["Ticker", "Company Name", "Sector (JP)", "Subsector (JP)", "Core"]

# ticker, name,        sector,      subsector,      core
ROWS = [
    ("LLY",  "Eli Lilly",  "Biopharma", "Large Pharma", ""),   # HELD, not Core
    ("ADSK", "Autodesk",   "Tech",      "Software",     ""),   # HELD, not Core, non-HC
    ("UNH",  "UnitedHealth", "Healthcare Services", "Mgd Care", "Y"),
    ("UBER", "Uber",       "Tech",      "Platforms",    ""),   # RESEARCHING, not Core
    ("XOMA", "Xoma Shell", "Biopharma", "Biotech",      ""),   # in neither list
]


@pytest.fixture()
def exports(tmp_path, monkeypatch):
    """Coverage Manager exports with LLY + ADSK held at a BLANK Core flag."""
    root = tmp_path / "cm"
    ex = root / "exports"
    ex.mkdir(parents=True)

    with open(ex / "universe.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNIVERSE_COLS)
        w.writeheader()
        for t, name, sector, sub, core in ROWS:
            w.writerow({"Ticker": t, "Company Name": name, "Sector (JP)": sector,
                        "Subsector (JP)": sub, "Core": core})

    meta = {t: {"name": name, "sector": sector, "subsector": sub, "core": core}
            for t, name, sector, sub, core in ROWS}
    (ex / "universe_metadata.json").write_text(json.dumps(meta), encoding="utf-8")

    # The exact shape that produced the bug: held, with Core blank.
    (ex / "portfolio.json").write_text(json.dumps({
        "LLY": {"position": "Portfolio", "Core": "", "Sector (JP)": "Biopharma"},
        "ADSK": {"position": "Portfolio", "Core": "", "Sector (JP)": "Tech"},
    }), encoding="utf-8")
    (ex / "researching.json").write_text(json.dumps({
        "UBER": {"position": "Researching", "Core": "", "Sector (JP)": "Tech"},
    }), encoding="utf-8")
    for fname in ("following_for_interest.json", "ready_to_buy.json",
                  "ready_to_short.json"):
        (ex / fname).write_text("{}", encoding="utf-8")

    monkeypatch.setattr("config.COVERAGE_MANAGER_PATH", str(root), raising=False)
    import coverage as cov
    monkeypatch.setattr(cov, "COVERAGE_MANAGER_PATH", str(root), raising=False)
    return ex


def _tiers(exports):
    import coverage as cov
    return {t.ticker: t.tier for t in cov.load_coverage()}


# ── Half one: the rule ──────────────────────────────────────────────────

def test_a_held_name_is_tier_1_even_with_a_blank_core_flag(exports):
    """The LLY case verbatim. Core='' in portfolio.json must not demote it."""
    assert _tiers(exports)["LLY"] == 1


def test_a_held_name_outside_healthcare_is_also_tier_1(exports):
    """ADSK has no sector path to Tier 2 — without the rule it lands at 3,
    which is what made 8 of the 9 held-not-Core names wholly untracked."""
    assert _tiers(exports)["ADSK"] == 1


def test_researching_keeps_its_core_gate(exports):
    """Deliberately NOT changed. Being interested is weaker than owning, and
    dropping this gate too would pull in 11 more names nobody asked for."""
    assert _tiers(exports)["UBER"] != 1


def test_a_name_in_no_position_list_is_untouched(exports):
    """The gate drop must not leak into the universe at large."""
    assert _tiers(exports)["XOMA"] == 3


# ── Half two: the restamp ───────────────────────────────────────────────

def _db_with(rows):
    """rows = (ticker, event_date, tier, closed_reason)."""
    from storage import init_db
    conn = init_db(":memory:")
    for ticker, event_date, tier, closed in rows:
        conn.execute(
            "INSERT INTO events (ticker, quarter, event_date, tier, reported, "
            "closed_reason) VALUES (?, ?, ?, ?, 0, ?)",
            (ticker, "2026Q3", event_date, tier, closed),
        )
    conn.commit()
    return conn


def _tier_of(conn, ticker, event_date):
    return conn.execute(
        "SELECT tier FROM events WHERE ticker = ? AND event_date = ?",
        (ticker, event_date),
    ).fetchone()[0]


def test_restamp_promotes_a_future_row_left_behind_by_a_coverage_change():
    """The 37-row 3Q26 case: coverage says 1, the row still says 3."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("LLY", "2026-10-29", 3, None)])

    changed = restamp_tiers_from_coverage(conn, {"LLY": 1}, "2026-08-12")

    assert changed == [("LLY", "2026-10-29", 3, 1)]
    assert _tier_of(conn, "LLY", "2026-10-29") == 1


def test_restamp_leaves_past_rows_alone():
    """Scoping forward is what keeps `compute_season_stats` from silently
    restating finished seasons, and what stopped the 2026-08-12 snapshot from
    demoting CAT/CVNA/ACVA — the last of which has a live TickTick task the
    reconcile would then never see again."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("ACVA", "2026-05-06", 1, None)])

    assert restamp_tiers_from_coverage(conn, {"ACVA": 3}, "2026-08-12") == []
    assert _tier_of(conn, "ACVA", "2026-05-06") == 1


def test_restamp_includes_a_row_dated_exactly_today():
    """Today's event has not happened yet — a BMO name at the 7am run still
    needs its task. The boundary is `>=`, matching close_departed_events'
    mirror-image `<` for the same reason."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("LLY", "2026-08-12", 3, None)])

    restamp_tiers_from_coverage(conn, {"LLY": 1}, "2026-08-12")
    assert _tier_of(conn, "LLY", "2026-08-12") == 1


def test_restamp_skips_closed_rows():
    """A closed event is terminal; its tier decides nothing, and touching it
    would churn `updated_at` on rows the reconcile reads for staleness."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("APLS", "2026-10-29", 3, "delisted")])

    assert restamp_tiers_from_coverage(conn, {"APLS": 1}, "2026-08-12") == []
    assert _tier_of(conn, "APLS", "2026-10-29") == 3


def test_restamp_refuses_an_empty_coverage_map():
    """An empty universe is a failed exports read, not a real edit. Demoting
    every future row to 3 on that basis would switch off TickTick, the calendar
    and the digest universe-wide — the 2026-07-26 BOM incident's blast radius
    with a different mechanism."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("LLY", "2026-10-29", 1, None)])

    assert restamp_tiers_from_coverage(conn, {}, "2026-08-12") == []
    assert _tier_of(conn, "LLY", "2026-10-29") == 1


def test_restamp_ignores_a_ticker_absent_from_coverage():
    """Absence from the map is not evidence of Tier 3 — `close_departed_events`
    owns that case, and it closes rather than demotes. Silently re-tiering here
    would strip a name mid-quarter on one flaky read."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("SEMR", "2026-10-29", 2, None)])

    assert restamp_tiers_from_coverage(conn, {"LLY": 1}, "2026-08-12") == []
    assert _tier_of(conn, "SEMR", "2026-10-29") == 2


def test_restamp_is_idempotent():
    """It runs on every daily sync; a second pass must write nothing."""
    from storage import restamp_tiers_from_coverage
    conn = _db_with([("LLY", "2026-10-29", 3, None)])

    assert len(restamp_tiers_from_coverage(conn, {"LLY": 1}, "2026-08-12")) == 1
    assert restamp_tiers_from_coverage(conn, {"LLY": 1}, "2026-08-12") == []


def test_restamp_runs_before_the_ticktick_creation_query():
    """Ordering is the whole point: a row promoted by the restamp must get its
    task in the SAME run, not the next one. Pinned by source order because the
    alternative is discovering it a quarter later, which is how this bug was
    found in the first place."""
    src = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
    restamp_at = src.index("restamp_tiers_from_coverage(")
    creation_at = src.index("Gather all Tier 1+2 events that need TickTick tasks")
    assert restamp_at < creation_at


def test_restamp_runs_after_the_collapse_guard():
    """A broken exports read must abort before it can demote the universe.
    Same ordering rule, and same rationale, as close_departed_events."""
    src = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
    guard_at = src.index("_assert_coverage_not_collapsed(conn, coverage)")
    restamp_at = src.index("restamp_tiers_from_coverage(")
    assert guard_at < restamp_at
