"""
Coverage Manager integration — reads the canonical ticker universe and
resolves each ticker to a service tier.

Tier 1 (Top priority — held + actively researched + trigger-ready):
    - Portfolio + Researching ∩ Core (held names + active thesis work)
    - Ready to Buy + Ready to Short (trigger-ready; Core filter dropped
      because user explicitly committed by completing the thesis)
Tier 2 (HC Services + MedTech): universe tickers in those sectors, excluding Tier 1
Tier 3 (Other): everything else in the universe

The TickerInfo.position field carries the original Position state for any
ticker in one of the five Position lists (Portfolio / Researching /
Ready to Buy / Ready to Short / Following for Interest), regardless of
which tier the ticker landed in. Following for Interest names keep their
sector-derived tier (no automatic promotion to Tier 1) but still get a
non-empty .position so the digest can render them under their own
subgroup. notifications._subgroup uses .position before falling back to
sector-based bucketing.

Falls back to legacy core_watchlist.json (= Portfolio + Researching unioned)
during the Coverage Manager Phase B->C migration window.
"""

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass

from config import COVERAGE_MANAGER_PATH, TIER_2_SECTORS, TICKERS_FILE

logger = logging.getLogger("earnings_agent")


# Coverage Manager publishes weekly (Friday Windows Task Scheduler). One
# missed publish = 7d behind; alert at >7d so two consecutive misses are
# loud. CM's manifest.json carries `generated_at` (ISO Z); fall back to
# universe.csv mtime if the manifest is missing.
COVERAGE_STALENESS_DAYS = 7


@dataclass
class TickerInfo:
    ticker: str
    tier: int               # 1, 2, or 3
    company_name: str
    sector: str
    subsector: str
    # One of the five Position values from Coverage Manager, or "" for
    # tickers not in any Position list. Drives digest subgrouping in
    # notifications._subgroup. Expanded 2026-05-11 from {Portfolio,
    # Researching} to all five values.
    position: str = ""      # "Portfolio" | "Researching" | "Ready to Buy" | "Ready to Short" | "Following for Interest" | ""


@dataclass
class CoverageHealth:
    """Result of compute_coverage_freshness — pure data, no side effects."""
    stale: bool
    age_days: float | None
    source: str            # "manifest", "mtime", or "missing"
    message: str           # human-readable summary for logs/Slack


def compute_coverage_freshness() -> CoverageHealth:
    """Inspect Coverage Manager exports for staleness. Pure read — no DB,
    no Slack, no logger.warning side-effects (callers decide).

    Preference order:
      1. exports/manifest.json -> 'generated_at' field (canonical)
      2. exports/universe.csv mtime (fallback if manifest absent or malformed)
      3. neither -> stale=True, source='missing'
    """
    exports = Path(COVERAGE_MANAGER_PATH) / "exports"
    manifest = exports / "manifest.json"
    universe = exports / "universe.csv"
    now = datetime.now(timezone.utc)

    if manifest.exists():
        try:
            with open(manifest, encoding="utf-8") as f:
                data = json.load(f)
            ts = data.get("generated_at")
            if ts:
                # CM publishes "...Z" suffix; fromisoformat doesn't accept Z
                # before Python 3.11. Normalize defensively.
                ts_clean = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
                generated = datetime.fromisoformat(ts_clean)
                if generated.tzinfo is None:
                    generated = generated.replace(tzinfo=timezone.utc)
                age_days = (now - generated).total_seconds() / 86400
                stale = age_days > COVERAGE_STALENESS_DAYS
                msg = (
                    f"Coverage Manager exports generated_at={ts}; "
                    f"age={age_days:.1f}d (threshold {COVERAGE_STALENESS_DAYS}d)"
                )
                return CoverageHealth(stale=stale, age_days=age_days, source="manifest", message=msg)
        except (json.JSONDecodeError, ValueError, OSError) as exc:
            logger.debug(f"Manifest unparseable, falling back to mtime: {exc}")

    if universe.exists():
        age_days = (now.timestamp() - universe.stat().st_mtime) / 86400
        stale = age_days > COVERAGE_STALENESS_DAYS
        msg = (
            f"Coverage Manager universe.csv mtime age={age_days:.1f}d "
            f"(threshold {COVERAGE_STALENESS_DAYS}d) — manifest unavailable"
        )
        return CoverageHealth(stale=stale, age_days=age_days, source="mtime", message=msg)

    return CoverageHealth(
        stale=True, age_days=None, source="missing",
        message=f"Coverage Manager exports not found at {exports}",
    )


def _read_position_json(exports_path: Path, filename: str) -> dict[str, dict]:
    """Read portfolio.json or researching.json, return {TICKER: row dict}."""
    path = exports_path / filename
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not read {filename}: {e}")
        return {}
    if not isinstance(data, dict):
        return {}
    return {t.strip().upper(): row for t, row in data.items() if isinstance(row, dict)}


def _read_position_lists(exports_path: Path) -> dict[str, dict[str, dict]]:
    """Read all five Position files and return a {label: {ticker: row}} dict.

    Returns an empty inner dict for any file that's absent. Falls back to
    legacy watchlist.csv when none of the five new files are present
    (Coverage Manager Phase B->C migration window) — in that case the
    legacy union is returned under "Portfolio" only and the other four
    labels stay empty.

    Tier-1 promotion rules are applied by the caller, not here:
      - Portfolio + Researching require Core=Y for Tier 1 (legacy rule).
      - Ready to Buy + Ready to Short are Tier 1 unconditionally
        (trigger-ready ⇒ user committed).
      - Following for Interest is NOT auto-promoted; falls through to
        sector-based tiering.

    This function just surfaces the raw lists. Filter/promotion logic
    lives in load_coverage.
    """
    lists = {
        "Portfolio": _read_position_json(exports_path, "portfolio.json"),
        "Researching": _read_position_json(exports_path, "researching.json"),
        "Following for Interest": _read_position_json(exports_path, "following_for_interest.json"),
        "Ready to Buy": _read_position_json(exports_path, "ready_to_buy.json"),
        "Ready to Short": _read_position_json(exports_path, "ready_to_short.json"),
    }

    if not any(lists.values()):
        # Legacy fallback: read watchlist.csv (Portfolio + Researching unioned)
        # and treat everything as Portfolio for the migration window.
        watchlist_path = exports_path / "watchlist.csv"
        if not watchlist_path.exists():
            logger.warning(f"Neither position files nor legacy watchlist found at {exports_path}")
            return lists
        with open(watchlist_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("Core", "").strip().upper() == "Y":
                    ticker = row.get("Ticker", "").strip().upper()
                    if ticker:
                        lists["Portfolio"][ticker] = row

    return lists


def _read_universe_metadata(exports_path: Path) -> dict[str, dict]:
    """Read universe_metadata.json and return dict of ticker -> {name, sector, subsector}."""
    metadata_path = exports_path / "universe_metadata.json"
    if not metadata_path.exists():
        logger.warning(f"Universe metadata not found: {metadata_path}")
        return {}

    with open(metadata_path, encoding="utf-8") as f:
        raw = json.load(f)

    # Keys in the JSON may not have exchange suffixes — normalize to uppercase
    result = {}
    for ticker, info in raw.items():
        key = ticker.strip().upper()
        result[key] = info
    return result


def _read_universe_tickers(exports_path: Path) -> set[str]:
    """Read universe.csv and return set of all tickers."""
    universe_path = exports_path / "universe.csv"
    if not universe_path.exists():
        logger.warning(f"Universe not found: {universe_path}")
        return set()

    tickers = set()
    with open(universe_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ticker = row.get("Ticker", "").strip().upper()
            if ticker:
                tickers.add(ticker)
    return tickers


def _load_legacy_tickers() -> list[str]:
    """Fallback: load tickers from tickers.txt or TICKERS env var."""
    import os

    if TICKERS_FILE.exists():
        tickers = [
            line.strip().upper()
            for line in TICKERS_FILE.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if tickers:
            return tickers

    return [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]


def load_coverage() -> list[TickerInfo]:
    """
    Load tickers and tier assignments from Coverage Manager exports.

    Falls back to tickers.txt if Coverage Manager exports are not found,
    assigning all tickers to Tier 2 (default).

    Returns list of TickerInfo sorted by tier then ticker.
    """
    exports_path = Path(COVERAGE_MANAGER_PATH) / "exports"

    if not exports_path.exists():
        logger.warning(
            f"Coverage Manager exports not found at {exports_path}. "
            f"Falling back to legacy ticker sources."
        )
        legacy = _load_legacy_tickers()
        if not legacy:
            logger.error("No tickers found from any source.")
            return []
        logger.info(f"Loaded {len(legacy)} tickers from legacy source (all assigned Tier 2)")
        return [
            TickerInfo(ticker=t, tier=2, company_name="", sector="", subsector="")
            for t in sorted(legacy)
        ]

    # Load data from Coverage Manager
    position_lists = _read_position_lists(exports_path)
    metadata = _read_universe_metadata(exports_path)
    universe_tickers = _read_universe_tickers(exports_path)

    # Tier 1 promotion rule (expanded 2026-05-11):
    #   - Portfolio ∩ Core
    #   - Researching ∩ Core
    #   - Ready to Buy   (any; Core filter dropped — trigger-ready ⇒ committed)
    #   - Ready to Short (any; same reason)
    # Following for Interest is NOT auto-promoted — names land at their
    # sector-derived tier, but their `.position` field is set so the
    # digest renders them under their own subgroup.
    portfolio = position_lists["Portfolio"]
    researching = position_lists["Researching"]
    following = position_lists["Following for Interest"]
    ready_to_buy = position_lists["Ready to Buy"]
    ready_to_short = position_lists["Ready to Short"]

    portfolio_t1 = {t for t, row in portfolio.items() if (row.get("Core") or "").strip().upper() == "Y"}
    researching_t1 = {t for t, row in researching.items() if (row.get("Core") or "").strip().upper() == "Y"}
    ready_to_buy_t1 = set(ready_to_buy.keys())
    ready_to_short_t1 = set(ready_to_short.keys())
    tier1_tickers = portfolio_t1 | researching_t1 | ready_to_buy_t1 | ready_to_short_t1

    # All Position-list tickers carry richer row data we want to fall back
    # on when universe_metadata is incomplete. Position-priority resolution
    # for the .position field: Portfolio > Researching > Ready to Buy >
    # Ready to Short > Following for Interest. A ticker should only ever
    # appear in one Position list (Coverage Manager's positions.py
    # validate() rejects duplicates), so the priority order only matters
    # as a defensive tiebreaker.
    position_lookup: dict[str, tuple[str, dict]] = {}
    for label in ("Following for Interest", "Ready to Short", "Ready to Buy",
                  "Researching", "Portfolio"):
        for t, row in position_lists[label].items():
            position_lookup[t] = (label, row)  # later wins => Portfolio top priority

    all_tickers = universe_tickers | set(position_lookup.keys())

    result = []
    tier_counts = {1: 0, 2: 0, 3: 0}

    for ticker in sorted(all_tickers):
        meta = metadata.get(ticker, {})
        company_name = meta.get("name", "")
        sector = meta.get("sector", "")
        subsector = meta.get("subsector", "")

        # Position-list tickers may have richer data than universe metadata
        position_entry = position_lookup.get(ticker)
        if position_entry is not None:
            _, row = position_entry
            if not company_name:
                company_name = row.get("Company Name", "")
            if not sector:
                sector = row.get("Sector (JP)", "")
            if not subsector:
                subsector = row.get("Subsector (JP)", "")

        # Determine tier
        if ticker in tier1_tickers:
            tier = 1
        elif sector in TIER_2_SECTORS:
            tier = 2
        else:
            tier = 3

        # Determine position label (independent of tier — a Following ticker
        # at Tier 3 still gets position="Following for Interest" so the
        # digest can route it to its own subgroup).
        position = position_entry[0] if position_entry is not None else ""

        tier_counts[tier] += 1
        result.append(TickerInfo(
            ticker=ticker,
            tier=tier,
            company_name=company_name,
            sector=sector,
            subsector=subsector,
            position=position,
        ))

    logger.info(
        f"Loaded {len(result)} tickers from Coverage Manager: "
        f"Tier 1={tier_counts[1]}, Tier 2={tier_counts[2]}, Tier 3={tier_counts[3]} "
        f"(Position lists: P={len(portfolio)}, R={len(researching)}, "
        f"RtB={len(ready_to_buy)}, RtS={len(ready_to_short)}, FfI={len(following)})"
    )
    return result


def get_tickers_by_tier(coverage: list[TickerInfo], max_tier: int = 3) -> list[str]:
    """Return ticker symbols for tickers at or below the given tier level."""
    return [t.ticker for t in coverage if t.tier <= max_tier]


def get_ticker_info(coverage: list[TickerInfo], ticker: str) -> TickerInfo | None:
    """Look up a specific ticker's info from the coverage list."""
    for t in coverage:
        if t.ticker == ticker:
            return t
    return None
