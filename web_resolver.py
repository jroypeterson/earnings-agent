"""Web-search resolution for earnings-date source disagreements.

JP 2026-07-08: "use web search to resolve and make this a standard part of
resolving this issue in the project." When Finnhub and yfinance disagree on an
upcoming press-release date, the company has usually ANNOUNCED the date in an
IR press release — a web search finds it without waiting for a human reply in
the Slack thread.

Slots into the source-priority hierarchy between EDGAR (post-hoc, authoritative)
and the ask-the-operator Slack thread:

  - HIGH-confidence company announcement matching one candidate -> the caller
    auto-locks that date (same machinery as a Slack `lock` reply).
  - Anything weaker (medium/low, or a third date neither source has) -> the
    verdict is attached to the Slack question as a hint, never auto-locked —
    mirroring the uncorroborated-EDGAR philosophy.

Uses the Anthropic web_search server tool (headless-safe; no MCP). Never
raises into the cross-check: any failure returns None and the flow degrades
to the existing ask-the-operator behavior. Cost: bounded by the caller's
per-run cap and max_uses=4 searches per call (~cents/disagreement; new
disagreements are ~0-3/day).
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date

from config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

_MODEL = "claude-sonnet-4-6"
_MAX_SEARCHES = 4


@dataclass
class WebVerdict:
    announced_date: str | None   # YYYY-MM-DD the company itself announced, or None
    confidence: str              # "high" | "medium" | "low"
    source_url: str              # where the date was found ("" if none)
    note: str                    # one-line human-readable summary for the Slack thread

    def matches(self, candidate_iso: str) -> bool:
        return bool(self.announced_date) and self.announced_date == candidate_iso


def resolve_disagreement(
    ticker: str,
    company_name: str,
    finnhub_date: str,
    yf_dates: list,
    today: date,
) -> WebVerdict | None:
    """Search the web for the company-announced earnings date. Never raises."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, max_retries=2)
        yf_str = ", ".join(d.isoformat() for d in yf_dates) or "unknown"
        prompt = (
            f"Today is {today.isoformat()}. Two data providers disagree on the "
            f"upcoming quarterly earnings press-release date for "
            f"{company_name or ticker} (ticker {ticker}): Finnhub says "
            f"{finnhub_date}; yfinance says {yf_str}.\n\n"
            "Search the web for the COMPANY'S OWN announcement of when it will "
            "report its next quarterly results (its investor-relations page, or "
            "a press release via Business Wire / GlobeNewswire / PR Newswire). "
            "Ignore third-party earnings-calendar aggregators (Zacks, "
            "Nasdaq.com, TipRanks, MarketBeat...) — they are the same class of "
            "source that is disagreeing.\n\n"
            "Reply with ONLY a JSON object, no prose:\n"
            '{"announced_date": "YYYY-MM-DD" or null, '
            '"confidence": "high"|"medium"|"low", '
            '"source_url": "...", "note": "one sentence"}\n\n'
            'Rules: confidence "high" ONLY if you found the company\'s own '
            "announcement explicitly stating the date of its UPCOMING report "
            "(right quarter, right year). A date inferred from aggregators or "
            'historical cadence is at best "low". If you find nothing '
            "authoritative, announced_date is null."
        )
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=1500,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": _MAX_SEARCHES,
            }],
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(
            block.text for block in resp.content if getattr(block, "type", "") == "text"
        )
        verdict = _parse_verdict(text)
        # Trust gate (codex 2026-07-08): the model's self-reported confidence is
        # steerable by page content (prompt injection), and "high" is what
        # authorizes an auto-lock. Downgrade to medium — hint, never lock —
        # unless the claimed source (a) was actually retrieved by the search
        # (appears in the tool citations) and (b) is a company-IR or trusted
        # newswire domain. Worst case after the gate: a hint line the operator
        # sees, not a silently locked wrong date.
        if verdict and verdict.confidence == "high":
            cited = _cited_urls(resp.content)
            if not _url_was_cited(verdict.source_url, cited):
                verdict.confidence = "medium"
                verdict.note = ("[downgraded: claimed source not among search "
                                "citations] " + verdict.note)[:300]
            elif not _is_trusted_source(verdict.source_url):
                verdict.confidence = "medium"
                verdict.note = ("[downgraded: source not a company-IR/wire "
                                "domain] " + verdict.note)[:300]
        return verdict
    except Exception as exc:  # noqa: BLE001 — never break the cross-check
        logger.warning(f"web_resolver: {ticker} resolution failed: {exc}")
        return None


_TRUSTED_WIRE_DOMAINS = (
    "businesswire.com", "globenewswire.com", "prnewswire.com",
    "prnewswire.co.uk", "newswire.ca", "accesswire.com",
)


def _is_trusted_source(url: str) -> bool:
    """True for newswire domains and company investor-relations hosts/paths."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = (parsed.netloc or "").lower()
    if not host:
        return False
    if any(host == d or host.endswith("." + d) for d in _TRUSTED_WIRE_DOMAINS):
        return True
    path = (parsed.path or "").lower()
    return (
        host.startswith(("ir.", "investor.", "investors."))
        or "/investor" in path
        or "/ir/" in path
        or path.endswith("/ir")
    )


def _cited_urls(content_blocks) -> set[str]:
    """URLs the web_search tool actually retrieved this call."""
    urls: set[str] = set()
    for block in content_blocks:
        if getattr(block, "type", "") != "web_search_tool_result":
            continue
        results = getattr(block, "content", None) or []
        for r in results:
            u = getattr(r, "url", None)
            if isinstance(u, str) and u:
                urls.add(u)
    return urls


def _url_was_cited(source_url: str, cited: set[str]) -> bool:
    if not source_url:
        return False
    if source_url in cited:
        return True
    # Tolerate tracking-param / trailing-slash drift between the model's echo
    # and the raw citation, but require a real prefix relationship.
    base = source_url.rstrip("/")
    for c in cited:
        cb = c.rstrip("/")
        if cb.startswith(base) or base.startswith(cb):
            return True
    return False


def _parse_verdict(text: str) -> WebVerdict | None:
    """Extract the JSON verdict from the model's final text. Tolerant of
    surrounding prose/code fences; strict about field shapes."""
    m = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if not m:
        logger.warning("web_resolver: no JSON object in response")
        return None
    try:
        raw = json.loads(m.group(0))
    except ValueError:
        logger.warning("web_resolver: JSON parse failed")
        return None
    if not isinstance(raw, dict):
        return None
    announced = raw.get("announced_date")
    if announced is not None:
        announced = str(announced)
        try:
            date.fromisoformat(announced)
        except ValueError:
            announced = None
    confidence = str(raw.get("confidence") or "low").lower()
    if confidence not in ("high", "medium", "low"):
        confidence = "low"
    return WebVerdict(
        announced_date=announced,
        confidence=confidence,
        source_url=str(raw.get("source_url") or "")[:500],
        note=str(raw.get("note") or "")[:300],
    )
