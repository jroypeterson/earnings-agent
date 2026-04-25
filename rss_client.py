"""
RSS / press-release feed client for earnings-date announcement detection.

Primary source: Seeking Alpha's per-ticker XML endpoint
  (`https://seekingalpha.com/api/sa/combined/{TICKER}.xml`)
which returns a clean ticker-scoped list of editorial + press-release items.

Optional override: per-company IR RSS URLs configured in `ir_feeds.json`
at the project root. When a ticker has an entry in that file, we use the
IR feed instead of Seeking Alpha (higher signal when available).

Announcement detection is conservative: the regex only matches press-
release phrasing ("to report Q1 earnings", "announces date", "financial
results conference call") and actively excludes Seeking Alpha's own
commentary ("earnings preview", "gears up to report", "what to expect").
"""

from __future__ import annotations

import re
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger("earnings_agent")

_USER_AGENT = (
    "Mozilla/5.0 (earnings-agent; https://github.com/jroypeterson/earnings-agent)"
)
_HTTP_TIMEOUT = 15

_IR_FEEDS_PATH = Path(__file__).parent / "ir_feeds.json"


@dataclass
class FeedItem:
    title: str
    link: str
    pub_date: date | None
    summary: str = ""


@dataclass
class AnnouncementMatch:
    feed_item: FeedItem
    announced_date: date | None
    source: str        # "IR" or "SeekingAlpha"


# ---------------------------------------------------------------------------
# IR feed overrides
# ---------------------------------------------------------------------------


def _load_ir_feeds() -> dict[str, str]:
    """
    Return {ticker: url} for tickers with an IR RSS URL configured.

    Reads from two sources, with kv_store taking precedence so feeds
    registered via Slack reply (`ir <url>`) win over the static JSON.
    JSON remains the bootstrapping mechanism for committed defaults.
    """
    out: dict[str, str] = {}

    # 1. JSON file (committed defaults)
    if _IR_FEEDS_PATH.exists():
        try:
            raw = json.loads(_IR_FEEDS_PATH.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(f"Could not read ir_feeds.json: {exc}")
            raw = {}
        for ticker, entry in raw.items():
            if ticker.startswith("__"):
                continue
            if isinstance(entry, str):
                out[ticker.upper()] = entry
            elif isinstance(entry, dict) and entry.get("url"):
                out[ticker.upper()] = entry["url"]

    # 2. kv_store overrides (mutable via Slack reply). Best-effort — the
    # DB may not exist yet on a fresh checkout, in which case we skip.
    try:
        from storage import init_db, kv_list_prefix
        conn = init_db()
        kv = kv_list_prefix(conn, "ir_feed:")
        conn.close()
        for key, url in kv.items():
            ticker = key.split(":", 1)[1].upper()
            if url:
                out[ticker] = url
    except Exception as exc:
        logger.debug(f"kv_store IR feed lookup skipped: {exc}")

    return out


# ---------------------------------------------------------------------------
# Feed fetching (RSS 2.0 + Atom)
# ---------------------------------------------------------------------------


def _parse_feed_date(raw: str) -> date | None:
    if not raw:
        return None
    raw = raw.strip()
    try:
        return parsedate_to_datetime(raw).date()
    except (TypeError, ValueError):
        pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        pass
    return None


def fetch_feed(url: str) -> list[FeedItem]:
    """Fetch + parse an RSS 2.0 or Atom feed. Returns [] on any error."""
    headers = {"User-Agent": _USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.debug(f"Feed fetch failed {url}: {exc}")
        return []

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as exc:
        logger.debug(f"Feed parse failed {url}: {exc}")
        return []

    items: list[FeedItem] = []

    # RSS 2.0: <channel><item>...
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_raw = item.findtext("pubDate") or item.findtext("{http://purl.org/dc/elements/1.1/}date") or ""
        items.append(FeedItem(
            title=title,
            link=link,
            pub_date=_parse_feed_date(pub_raw),
            summary=(item.findtext("description") or "").strip(),
        ))

    # Atom: <feed><entry>...
    atom_ns = "{http://www.w3.org/2005/Atom}"
    for entry in root.iter(f"{atom_ns}entry"):
        title = (entry.findtext(f"{atom_ns}title") or "").strip()
        link_el = entry.find(f"{atom_ns}link")
        link = link_el.get("href", "") if link_el is not None else ""
        pub_raw = (
            entry.findtext(f"{atom_ns}published")
            or entry.findtext(f"{atom_ns}updated")
            or ""
        )
        items.append(FeedItem(
            title=title,
            link=link,
            pub_date=_parse_feed_date(pub_raw),
            summary=(entry.findtext(f"{atom_ns}summary") or "").strip(),
        ))

    return items


def fetch_ticker_feed(ticker: str) -> tuple[list[FeedItem], str] | None:
    """
    Return (items, source) for a ticker that has an IR feed configured
    in ir_feeds.json, or None when there's no config.

    Aggregator feeds (Seeking Alpha, Nasdaq, Business Wire, PR Newswire)
    empirically do NOT carry company earnings-date press releases — they
    surface analyst commentary and post-release transcripts instead. So
    we only scan IR feeds that have been vetted and added to the config
    file; `--check-announcements` is a silent no-op for tickers without
    a config entry.
    """
    ir_map = _load_ir_feeds()
    ir_url = ir_map.get(ticker.upper())
    if not ir_url:
        return None
    return fetch_feed(ir_url), "IR"


# ---------------------------------------------------------------------------
# Announcement detection
# ---------------------------------------------------------------------------


# Matches IR press-release phrasing only. Requires:
#   - an announcement verb ("announces", "to report", "schedules", "sets", "will release", etc.)
#   - AND a quarter marker ("Q1/Q2/Q3/Q4" or "first/second/third/fourth quarter")
#     OR "financial results conference call" (a common pre-announcement phrase)
_ANNOUNCEMENT_RE = re.compile(
    r"("
    # verb + quarter
    r"(?:announces?|schedules?|sets?|will\s+(?:report|release|host|announce)|to\s+(?:report|host|release))"
    r"[^.!?\n]{0,60}?"
    r"(?:q[1-4]|first[\s-]quarter|second[\s-]quarter|third[\s-]quarter|fourth[\s-]quarter)"
    r"|"
    # fallback phrase that strongly implies a pre-announcement
    r"financial\s+results\s+conference\s+call"
    r"|"
    r"(?:earnings|results)\s+(?:release\s+)?date\s+(?:of|scheduled|set\s+for)"
    r")",
    re.IGNORECASE,
)

# Excludes Seeking Alpha / analyst commentary that mentions earnings but
# isn't a company announcement.
_NOISE_RE = re.compile(
    r"("
    r"earnings\s+preview|ahead\s+of\s+earnings|gears?\s+up\s+to\s+report|"
    r"what\s+to\s+expect|what\s+to\s+watch|on\s+deck|analysts?\s+(?:expect|estimate)|"
    r"price\s+target|upgrade|downgrade|buy\s+rating|sell\s+rating|"
    r"earnings\s+transcript|q\d\s+\d{4}\s+earnings\s+transcript|"
    r"earnings\s+call\s+(?:presentation|summary|transcript)|"
    r"earnings\s+beat|earnings\s+miss|beats?\s+.*earnings|"
    r"stock[- ]spiking|earnings\s+(?:report|results)\s+show|will\s+.*q\d\s+earnings"
    r")",
    re.IGNORECASE,
)

_TITLE_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2})(?:,?\s+(\d{4}))?",
    re.IGNORECASE,
)

_MONTH_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def _extract_date_from_title(title: str, hint_year: int) -> date | None:
    m = _TITLE_DATE_RE.search(title)
    if not m:
        return None
    try:
        month = _MONTH_NUM[m.group(1).lower()]
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else hint_year
        return date(year, month, day)
    except (ValueError, KeyError):
        return None


# Reporting-quarter mapping used to filter announcements to the RIGHT
# quarter — a Q1 release in April should not match a Feb press release
# about Q4 results.
_QUARTER_FROM_MONTH = {1: 4, 2: 4, 3: 4,  # Jan-Mar release => Q4 prior year
                       4: 1, 5: 1, 6: 1,  # Apr-Jun release => Q1
                       7: 2, 8: 2, 9: 2,  # Jul-Sep release => Q2
                       10: 3, 11: 3, 12: 3}  # Oct-Dec release => Q3

_QUARTER_WORD = {1: "first", 2: "second", 3: "third", 4: "fourth"}


def _title_matches_quarter(title: str, quarter_num: int) -> bool:
    """
    Return True if `title` mentions the given quarter, or mentions no
    quarter at all (in which case we don't filter on quarter).
    """
    t = title.lower()
    # Check for any quarter reference
    has_any = bool(re.search(r"\bq[1-4]\b|first[\s-]quarter|second[\s-]quarter|third[\s-]quarter|fourth[\s-]quarter", t))
    if not has_any:
        return True  # unspecific — allow
    want = f"q{quarter_num}"
    word = _QUARTER_WORD[quarter_num]
    return (
        want in t
        or f"{word} quarter" in t
        or f"{word}-quarter" in t
    )


def detect_announcement(
    items: list[FeedItem],
    for_date: date,
    *,
    lookback_days: int = 45,
    source: str = "IR",
) -> AnnouncementMatch | None:
    """
    Find the most recent item whose title matches the announcement
    regex, excludes noise, mentions the correct reporting quarter (or
    no quarter at all), and is published in [for_date - lookback_days,
    for_date + 5] (allowing a few days of post-release slop).
    """
    lower_bound = for_date - timedelta(days=lookback_days)
    upper_bound = for_date + timedelta(days=5)
    target_quarter = _QUARTER_FROM_MONTH[for_date.month]

    best: FeedItem | None = None
    for item in items:
        if not item.pub_date:
            continue
        if item.pub_date < lower_bound or item.pub_date > upper_bound:
            continue
        title = item.title
        if _NOISE_RE.search(title):
            continue
        if not _ANNOUNCEMENT_RE.search(title):
            continue
        if not _title_matches_quarter(title, target_quarter):
            continue
        if best is None or (best.pub_date is not None and item.pub_date > best.pub_date):
            best = item
    if not best:
        return None
    announced = _extract_date_from_title(best.title, for_date.year)
    return AnnouncementMatch(feed_item=best, announced_date=announced, source=source)
