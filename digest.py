"""
Weekly digest builder — queries upcoming earnings events and assembles
the structured data that notifications.py renders into Slack + email.

The digest covers two horizons:
  - 7 days  (next week: the week the user is about to enter)
  - 30 days (the broader earnings season view, for cadence awareness)

Events are joined in-memory against the Coverage Manager to attach sector
so we can detect sector clustering.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, timedelta

from coverage import TickerInfo
from market_data import fetch_ytd_performance

logger = logging.getLogger("earnings_agent")


@dataclass
class EventRow:
    ticker: str
    company_name: str
    event_date: str
    event_hour: str | None
    eps_estimate: float | None
    rev_estimate: float | None
    tier: int
    sector: str
    subsector: str
    ytd_pct: float | None = None


@dataclass
class DigestData:
    reference_date: date
    week_start: date
    week_end: date
    month_end: date
    tier1_week: list[EventRow] = field(default_factory=list)
    tier2_week: list[EventRow] = field(default_factory=list)
    tier3_week: list[EventRow] = field(default_factory=list)
    tier1_month: list[EventRow] = field(default_factory=list)
    tier2_month: list[EventRow] = field(default_factory=list)
    sector_clusters: list[tuple[str, int]] = field(default_factory=list)
    peak_week_start: date | None = None
    peak_week_count: int = 0

    @property
    def week_count(self) -> int:
        return len(self.tier1_week) + len(self.tier2_week) + len(self.tier3_week)

    @property
    def month_tracked_count(self) -> int:
        return len(self.tier1_month) + len(self.tier2_month)


def _fetch_events(
    conn: sqlite3.Connection, start: date, end: date
) -> list[tuple]:
    cur = conn.execute(
        """
        SELECT ticker, event_date, event_hour, eps_estimate, rev_estimate,
               tier, company_name
        FROM events
        WHERE event_date >= ? AND event_date <= ?
        ORDER BY event_date, ticker
        """,
        (start.isoformat(), end.isoformat()),
    )
    return cur.fetchall()


def _to_row(raw: tuple, coverage_map: dict[str, TickerInfo]) -> EventRow:
    ticker, ev_date, hour, eps, rev, tier, company_name = raw
    info = coverage_map.get(ticker)
    return EventRow(
        ticker=ticker,
        company_name=company_name or (info.company_name if info else ""),
        event_date=ev_date,
        event_hour=hour,
        eps_estimate=eps,
        rev_estimate=rev,
        tier=tier,
        sector=info.sector if info else "",
        subsector=info.subsector if info else "",
    )


def _detect_sector_clusters(
    rows: list[EventRow], min_cluster: int = 3
) -> list[tuple[str, int]]:
    """Return [(sector, count)] where count >= min_cluster, sorted by count desc."""
    counts: dict[str, int] = {}
    for r in rows:
        if not r.sector:
            continue
        counts[r.sector] = counts.get(r.sector, 0) + 1
    clusters = [(s, c) for s, c in counts.items() if c >= min_cluster]
    clusters.sort(key=lambda x: (-x[1], x[0]))
    return clusters


def _detect_peak_week(
    rows: list[EventRow], reference: date
) -> tuple[date | None, int]:
    """Slide a 7-day window across the next 30 days; return (start_date, count)."""
    if not rows:
        return None, 0
    by_date: dict[str, int] = {}
    for r in rows:
        by_date[r.event_date] = by_date.get(r.event_date, 0) + 1

    best_start: date | None = None
    best_count = 0
    for offset in range(0, 24):  # up to 30 - 7 + 1
        window_start = reference + timedelta(days=offset)
        window_end = window_start + timedelta(days=6)
        count = 0
        for iso_date, n in by_date.items():
            d = date.fromisoformat(iso_date)
            if window_start <= d <= window_end:
                count += n
        if count > best_count:
            best_count = count
            best_start = window_start
    return best_start, best_count


def build_weekly_digest(
    conn: sqlite3.Connection,
    coverage: list[TickerInfo],
    reference_date: date | None = None,
) -> DigestData:
    """Assemble the weekly digest. Reference date defaults to today."""
    today = reference_date or date.today()
    week_end = today + timedelta(days=7)
    month_end = today + timedelta(days=30)

    coverage_map = {t.ticker: t for t in coverage}

    month_raw = _fetch_events(conn, today, month_end)
    month_rows = [_to_row(r, coverage_map) for r in month_raw]
    week_rows = [r for r in month_rows if date.fromisoformat(r.event_date) <= week_end]

    digest = DigestData(
        reference_date=today,
        week_start=today,
        week_end=week_end,
        month_end=month_end,
        tier1_week=[r for r in week_rows if r.tier == 1],
        tier2_week=[r for r in week_rows if r.tier == 2],
        tier3_week=[r for r in week_rows if r.tier == 3],
        tier1_month=[r for r in month_rows if r.tier == 1],
        tier2_month=[r for r in month_rows if r.tier == 2],
    )

    digest.sector_clusters = _detect_sector_clusters(week_rows)
    tracked_month = [r for r in month_rows if r.tier <= 2]
    digest.peak_week_start, digest.peak_week_count = _detect_peak_week(
        tracked_month, today
    )

    # YTD performance — only for week rows, since that's what Slack renders.
    ytd_map = fetch_ytd_performance([r.ticker for r in week_rows])
    for r in week_rows:
        r.ytd_pct = ytd_map.get(r.ticker.upper())

    logger.info(
        f"Digest built: {digest.week_count} names in next 7d, "
        f"{digest.month_tracked_count} tracked in next 30d. "
        f"Peak tracked week starts {digest.peak_week_start} ({digest.peak_week_count})."
    )
    return digest
