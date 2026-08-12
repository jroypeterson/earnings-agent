"""
Coverage Manager integration — reads the canonical ticker universe and
resolves each ticker to a service tier.

Tier 1 (Top priority — held + actively researched + trigger-ready):
    - Portfolio (any; Core filter dropped 2026-08-12 — owning it is the
      commitment, see the note on `portfolio_t1` in load_coverage)
    - Researching ∩ Core (active thesis work)
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

from config import (COVERAGE_MANAGER_PATH, TIER_2_CORE_ONLY_SECTORS,
                    TIER_2_SECTORS, TICKERS_FILE)

logger = logging.getLogger("earnings_agent")


# Coverage Manager publishes weekly (Friday Windows Task Scheduler), so a
# healthy manifest age oscillates 0–7 days. The threshold must give that
# cadence slack: Friday's publish time drifts (06-05 landed ~11:00 UTC,
# 06-12 ~20:40 UTC) and the staleness check runs on the morning daily sync
# (11:13 UTC) + watchdog (13:37 UTC) — i.e. BEFORE Friday's evening publish.
# A flat 7d threshold therefore false-alarmed every Friday morning for the
# hours between crossing 7.0d and that day's publish (the 2026-06-12 alert).
# 10d cleanly separates a normal/slightly-late weekly publish (≤~7.5d) from a
# genuinely missed week (reaches 10d ~3 days after the skipped Friday — still
# a within-72h alert), which is the "a real miss is loud" intent. CM's
# manifest.json carries `generated_at` (ISO Z); fall back to universe.csv
# mtime if the manifest is missing.
COVERAGE_STALENESS_DAYS = 10


# --- Mass-removal (collapse) guard -----------------------------------------
# A tier does not lose a fifth of its names in one overnight run. When it
# does, the cause is a broken read of Coverage Manager's exports, not a
# universe edit -- 2026-07-26 is the reference incident: a UTF-8 BOM on CM's
# universe.csv (CM commit 4640968) made `csv.DictReader`'s first field
# a BOM-prefixed "Ticker", so `_read_universe_tickers` returned an EMPTY
# set. Tier 1 survived (derived from the position *JSON* files), but every Tier 2/3
# name not in a position list vanished: 197 Tier 2 + 817 Tier 3 gone, posted
# to #status-reports as a routine "Removed (197)" diff, and re-added two days
# later once CM republished BOM-free (3fc306b) -- a 197-name flip-flop during
# which the agent silently stopped tracking the entire Tier 2 slice.
#
# BOTH conditions must trip: a percentage alone would fire on a small tier
# losing a handful of names; a count alone would fire on a legitimately large
# universe pruning of Tier 3. Set the floor above any plausible manual edit.
COVERAGE_COLLAPSE_MIN_LOST = 25
COVERAGE_COLLAPSE_MAX_TIER_LOSS = 0.20

# Escape hatch for a genuinely large, intentional universe edit. Explicit
# operator action -- never set this in CI as a way to quiet the guard.
COVERAGE_COLLAPSE_OVERRIDE_ENV = "COVERAGE_ALLOW_MASS_REMOVAL"


@dataclass
class TierCollapse:
    """One tier that lost an implausible share of its names since the prior run."""
    tier: int
    prior_count: int
    lost_count: int
    removed: list[str]      # gone from coverage entirely
    demoted: list[str]      # still present but at a lower-priority (higher) tier

    @property
    def fraction(self) -> float:
        return self.lost_count / self.prior_count if self.prior_count else 0.0

    def describe(self) -> str:
        parts = [
            f"Tier {self.tier}: {self.lost_count} of {self.prior_count} names lost "
            f"({self.fraction * 100:.0f}%)"
        ]
        if self.removed:
            parts.append(f"removed={len(self.removed)}")
        if self.demoted:
            parts.append(f"demoted={len(self.demoted)}")
        return " | ".join(parts)


def detect_coverage_collapse(
    prior: dict[str, dict],
    current: dict[str, dict],
    *,
    min_lost: int = COVERAGE_COLLAPSE_MIN_LOST,
    max_tier_loss: float = COVERAGE_COLLAPSE_MAX_TIER_LOSS,
) -> list[TierCollapse]:
    """Pure comparison of two coverage snapshots -- no I/O, no Slack, no exit.

    `prior` and `current` are {ticker: {"tier": int, ...}} maps (the shape
    persisted in `kv_store.coverage_snapshot`).

    A ticker counts as LOST from tier N if it was tier N before and is now
    either absent entirely or at a numerically higher (lower-priority) tier.
    A tier-wide metadata failure demotes rather than removes -- e.g. a broken
    universe_metadata.json blanks every sector and collapses Tier 2 into
    Tier 3 -- so both shapes are the same defect and both are caught here.

    Returns one TierCollapse per tripping tier, worst first. Empty list = OK.
    """
    prior_by_tier: dict[int, list[str]] = {}
    for ticker, info in prior.items():
        try:
            tier = int(info.get("tier", 3))
        except (TypeError, ValueError):
            tier = 3
        prior_by_tier.setdefault(tier, []).append(ticker)

    collapses: list[TierCollapse] = []
    for tier, tickers in prior_by_tier.items():
        removed: list[str] = []
        demoted: list[str] = []
        for ticker in tickers:
            now = current.get(ticker)
            if now is None:
                removed.append(ticker)
                continue
            try:
                new_tier = int(now.get("tier", 3))
            except (TypeError, ValueError):
                new_tier = 3
            if new_tier > tier:
                demoted.append(ticker)
        lost = len(removed) + len(demoted)
        if lost < min_lost:
            continue
        if lost / len(tickers) <= max_tier_loss:
            continue
        collapses.append(TierCollapse(
            tier=tier,
            prior_count=len(tickers),
            lost_count=lost,
            removed=sorted(removed),
            demoted=sorted(demoted),
        ))

    collapses.sort(key=lambda c: (-c.fraction, c.tier))
    return collapses


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
      - Researching requires Core=Y for Tier 1; Portfolio does not
        (gate dropped 2026-08-12 — owning it is the commitment).
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
        with open(watchlist_path, newline="", encoding="utf-8-sig") as f:
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
    # utf-8-sig, not utf-8. A BOM on CM's export makes the first field
    # "﻿Ticker", so a plain read returns zero tickers and silently collapses
    # Tier 2/3 -- which is exactly what happened between 2026-07-25 and 07-27.
    with open(universe_path, newline="", encoding="utf-8-sig") as f:
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

    # Tier 1 promotion rule (expanded 2026-05-11, Portfolio gate dropped 2026-08-12):
    #   - Portfolio     (any; see below)
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

    # A HELD name is Tier 1 whether or not Coverage Manager flags it Core.
    # `Core` is an editorial marker on the coverage universe; ownership is a
    # fact about the book, and the two drift. JP 2026-08-12, on finding LLY in
    # none of the 2Q26 TickTick lists: "I own LLY so it should be in positions
    # & researching TickTick regardless of how it's categorized on coverage
    # manager". LLY sat at `Core=''` in portfolio.json until 2026-08-06 and was
    # therefore Tier 3 — no calendar event, no TickTick task, no digest detail —
    # for the whole 2Q26 season, on a name he owns.
    #
    # `ir_ticktick.load_universe` reached the same conclusion on 2026-08-05 from
    # the other direction ("Scope is Core=Y ∪ Portfolio, not Core alone... Owning
    # a company is the strongest possible reason to be on its list") and names
    # the same 11 held-but-not-Core tickers, LLY among them. This makes the two
    # lanes agree instead of disagreeing by one editorial flag.
    #
    # Researching KEEPS its Core gate — being interested in a name is a weaker
    # signal than owning it, and dropping that one too would add 11 more names
    # nobody asked for. Deliberately a separate decision.
    portfolio_t1 = set(portfolio.keys())
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
        is_core = (meta.get("core") or "").strip().upper() == "Y"
        if not is_core and position_entry is not None:
            is_core = (position_entry[1].get("Core") or "").strip().upper() == "Y"

        if ticker in tier1_tickers:
            tier = 1
        elif sector in TIER_2_SECTORS:
            tier = 2
        elif sector in TIER_2_CORE_ONLY_SECTORS and is_core:
            # Core-gated: see the TIER_2_CORE_ONLY_SECTORS note in config.py.
            # Biopharma is 687 rows to MedTech's 139, so sector alone would put
            # every clinical-stage shell into the TickTick sync.
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
