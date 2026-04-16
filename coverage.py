"""
Coverage Manager integration — reads the canonical ticker universe and
resolves each ticker to a service tier.

Tier 1 (Core Watchlist): tickers in watchlist.csv with Core=Y
Tier 2 (HC Services + MedTech): universe tickers in those sectors, excluding Tier 1
Tier 3 (Other): everything else in the universe
"""

import csv
import json
import logging
from pathlib import Path
from dataclasses import dataclass

from config import COVERAGE_MANAGER_PATH, TIER_2_SECTORS, TICKERS_FILE

logger = logging.getLogger("earnings_agent")


@dataclass
class TickerInfo:
    ticker: str
    tier: int               # 1, 2, or 3
    company_name: str
    sector: str
    subsector: str


def _read_watchlist(exports_path: Path) -> dict[str, dict]:
    """Read watchlist.csv and return dict of ticker -> row data for Core=Y tickers."""
    watchlist_path = exports_path / "watchlist.csv"
    if not watchlist_path.exists():
        logger.warning(f"Watchlist not found: {watchlist_path}")
        return {}

    result = {}
    with open(watchlist_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("Core", "").strip().upper() == "Y":
                ticker = row.get("Ticker", "").strip().upper()
                if ticker:
                    result[ticker] = row
    return result


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
    watchlist = _read_watchlist(exports_path)
    metadata = _read_universe_metadata(exports_path)
    universe_tickers = _read_universe_tickers(exports_path)

    # Also include watchlist tickers in the universe set
    all_tickers = universe_tickers | set(watchlist.keys())

    result = []
    tier_counts = {1: 0, 2: 0, 3: 0}

    for ticker in sorted(all_tickers):
        meta = metadata.get(ticker, {})
        company_name = meta.get("name", "")
        sector = meta.get("sector", "")
        subsector = meta.get("subsector", "")

        # Watchlist tickers may have richer data
        if ticker in watchlist:
            row = watchlist[ticker]
            if not company_name:
                company_name = row.get("Company Name", "")
            if not sector:
                sector = row.get("Sector (JP)", "")
            if not subsector:
                subsector = row.get("Subsector (JP)", "")

        # Determine tier
        if ticker in watchlist:
            tier = 1
        elif sector in TIER_2_SECTORS:
            tier = 2
        else:
            tier = 3

        tier_counts[tier] += 1
        result.append(TickerInfo(
            ticker=ticker,
            tier=tier,
            company_name=company_name,
            sector=sector,
            subsector=subsector,
        ))

    logger.info(
        f"Loaded {len(result)} tickers from Coverage Manager: "
        f"Tier 1={tier_counts[1]}, Tier 2={tier_counts[2]}, Tier 3={tier_counts[3]}"
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
