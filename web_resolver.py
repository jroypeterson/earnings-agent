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
# Board #409: `web_search_20260209` adds dynamic filtering (the model filters
# results in server-side code before they reach its context) and IS supported
# on Sonnet 4.6 - but the production default stays on 20250305 BY
# MEASUREMENT, not by inertia. The 2026-10-01 replay (40 EDGAR-confirmed
# 2Q26 dates, both tools, diagnostics/web_search_tool_replay_2026-10-01.md):
# raw exact-date accuracy tied 38/40 v 38/40, but once answers resting on
# evidence published AFTER the simulated day are excluded (the replay
# searches today's web), legacy led ~34 v ~30; the new tool also cost +44%
# per call ($0.110 v $0.076) and produced fewer auto-lockable verdicts (28 v
# 32). The parser handles both shapes, so flipping is this one constant.
# If a non-legacy type is ever rejected for the model (400), the call is
# retried once on the legacy type rather than degrading the cross-check.
_LEGACY_WEB_SEARCH_TOOL = "web_search_20250305"
_DYNAMIC_WEB_SEARCH_TOOL = "web_search_20260209"
_WEB_SEARCH_TOOL = _LEGACY_WEB_SEARCH_TOOL
# Dynamic filtering runs a server-side sampling loop; at its iteration limit
# the turn stops with stop_reason "pause_turn" and NO final text. Resume it
# (re-send the turn so far) at most this many times.
_MAX_CONTINUATIONS = 2
# Code the model writes for dynamic filtering is billed as OUTPUT tokens: the
# 2026-10-01 replay peaked at 1,611 output tokens on 20260209 (798 on the
# legacy type), past the old 1,500 cap. A ceiling, not a spend.
_MAX_TOKENS = 4000


@dataclass
class WebVerdict:
    announced_date: str | None   # YYYY-MM-DD the company itself announced, or None
    confidence: str              # "high" | "medium" | "low"
    source_url: str              # where the date was found ("" if none)
    note: str                    # one-line human-readable summary for the Slack thread
    # True only when source_url was actually retrieved by the search AND is a
    # company-IR / trusted-wire host - the same two checks the high-confidence
    # trust gate applies. A verdict that fails them is a HINT: it may name a
    # date, but nothing may advise locking it (#515, codex r2).
    source_verified: bool = False

    def matches(self, candidate_iso: str) -> bool:
        return bool(self.announced_date) and self.announced_date == candidate_iso


def _build_prompt(ticker: str, company_name: str, finnhub_date: str,
                  yf_dates: list, today: date) -> str:
    yf_str = ", ".join(d.isoformat() for d in yf_dates) or "unknown"
    return (
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


def _field(obj, name, default=None):
    """Read `name` from an SDK model OR a plain dict. A block type the
    installed SDK does not know may surface as either."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _run_search(client, prompt: str, tool_type: str,
                meta: dict | None = None) -> tuple[list, dict]:
    """One resolution turn, resuming `pause_turn` up to _MAX_CONTINUATIONS.

    Returns (every content block across the turn, meta). Blocks accumulate:
    a paused turn's search results are part of what the final answer rests
    on, so the citation check must see them too. meta carries usage totals
    and the final stop_reason for callers that measure (the #409 replay).

    `meta` is filled IN PLACE when the caller passes one, so usage already
    billed survives an exception on a later continuation (codex r4) - the
    replay's spend cap reads it even when the chain fails."""
    messages = [{"role": "user", "content": prompt}]
    blocks: list = []
    meta = {} if meta is None else meta
    meta.update({"tool_type": tool_type, "input_tokens": 0, "output_tokens": 0,
                 "web_search_requests": 0, "continuations": 0,
                 "stop_reason": None, "block_types": [], "requests": 0})
    for attempt in range(_MAX_CONTINUATIONS + 1):
        meta["requests"] = attempt
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            tools=[{
                "type": tool_type,
                "name": "web_search",
                # max_uses is PER REQUEST, so a resumed turn would otherwise
                # get a fresh 4 (codex r2): give it only what is left, with a
                # floor of 1 so a turn that spent its searches can still finish.
                "max_uses": max(1, _MAX_SEARCHES - meta["web_search_requests"]),
            }],
            messages=messages,
        )
        blocks.extend(list(_field(resp, "content", None) or []))
        usage = _field(resp, "usage", None)
        if usage is not None:
            meta["input_tokens"] += int(_field(usage, "input_tokens", 0) or 0)
            meta["output_tokens"] += int(_field(usage, "output_tokens", 0) or 0)
            stu = _field(usage, "server_tool_use", None)
            if stu is not None:
                meta["web_search_requests"] += int(
                    _field(stu, "web_search_requests", 0) or 0)
        meta["stop_reason"] = _field(resp, "stop_reason", None)
        if meta["stop_reason"] != "pause_turn" or attempt == _MAX_CONTINUATIONS:
            break
        # Resume: re-send the turn so far as ONE assistant message; the API
        # sees the trailing server_tool_use and continues where it stopped.
        meta["continuations"] += 1
        messages = [{"role": "user", "content": prompt},
                    {"role": "assistant", "content": list(blocks)}]
    meta["block_types"] = [str(_field(b, "type", "") or "") for b in blocks]
    return blocks, meta


def _answer_candidates(blocks) -> list[tuple[str, bool]]:
    """(text, is_final_answer) candidates, most authoritative first.

    With dynamic filtering the model may narrate between tool calls, so a
    join of EVERY text block can put a stray brace-pair ahead of the verdict.
    The final answer is the text after the LAST non-text block. The full join
    stays as a fallback, flagged NOT final: text written before a later tool
    call is provisional. Provenance travels with the text (codex r5) - an
    empty trailing text block must not promote the fallback to "final"."""
    last_tool = -1
    for i, b in enumerate(blocks):
        if _field(b, "type", "") != "text":
            last_tool = i

    def _join(seq):
        return "".join(str(_field(b, "text", "") or "") for b in seq
                       if _field(b, "type", "") == "text")

    tail, full = _join(blocks[last_tool + 1:]), _join(blocks)
    out: list[tuple[str, bool]] = []
    if tail.strip():
        out.append((tail, True))
    if full.strip() and full != tail:
        out.append((full, False))
    return out


def resolve_with_meta(
    ticker: str,
    company_name: str,
    finnhub_date: str,
    yf_dates: list,
    today: date,
    tool_type: str | None = None,
) -> tuple[WebVerdict | None, dict]:
    """resolve_disagreement plus a measurement dict. Never raises."""
    meta: dict = {}
    if not ANTHROPIC_API_KEY:
        return None, meta
    tool_type = tool_type or _WEB_SEARCH_TOOL
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, max_retries=2)
        prompt = _build_prompt(ticker, company_name, finnhub_date, yf_dates, today)
        try:
            blocks, meta = _run_search(client, prompt, tool_type, meta)
        except anthropic.BadRequestError as exc:
            # Only a rejection of the FIRST request means "this tool type is
            # not accepted". A 400 on a continuation is about that turn, and
            # restarting a whole legacy chain would discard paid work
            # (codex r3).
            if tool_type == _LEGACY_WEB_SEARCH_TOOL or meta.get("requests"):
                raise
            logger.warning(
                f"web_resolver: {ticker} {tool_type} rejected ({exc}); "
                f"retrying on {_LEGACY_WEB_SEARCH_TOOL}"
            )
            meta = {"fell_back": True}
            blocks, meta = _run_search(client, prompt, _LEGACY_WEB_SEARCH_TOOL,
                                       meta)
        stop = meta.get("stop_reason")
        if stop == "refusal":
            logger.warning(f"web_resolver: {ticker} request refused")
            return None, meta
        verdict = None
        from_final_answer = False
        for text, is_final in _answer_candidates(blocks):
            verdict = _parse_verdict(text, quiet=True)
            if verdict:
                from_final_answer = is_final
                break
        if verdict is None:
            logger.warning(f"web_resolver: {ticker} no JSON verdict in response "
                           f"(stop_reason={stop})")
            return None, meta
        cited = _cited_urls(blocks)
        verdict.source_verified = (
            _url_was_cited(verdict.source_url, cited)
            and _is_trusted_source(verdict.source_url)
        )
        # Trust gate (codex 2026-07-08): the model's self-reported confidence is
        # steerable by page content (prompt injection), and "high" is what
        # authorizes an auto-lock. Downgrade to medium — hint, never lock —
        # unless the claimed source (a) was actually retrieved by the search
        # (appears in the tool citations) and (b) is a company-IR or trusted
        # newswire domain. Worst case after the gate: a hint line the operator
        # sees, not a silently locked wrong date.
        if verdict.confidence == "high":
            if not _url_was_cited(verdict.source_url, cited):
                verdict.confidence = "medium"
                verdict.note = ("[downgraded: claimed source not among search "
                                "citations] " + verdict.note)[:300]
            elif not _is_trusted_source(verdict.source_url):
                # Log the URL: ABT 2026-09-30 was downgraded here while a
                # re-run cited www.prnewswire.com (trusted), and with no URL
                # in the log the cause could not be reconstructed (#515).
                logger.warning(
                    f"web_resolver: {ticker} high verdict downgraded, untrusted "
                    f"source {verdict.source_url!r}"
                )
                verdict.confidence = "medium"
                verdict.note = ("[downgraded: source not a company-IR/wire "
                                "domain] " + verdict.note)[:300]
        # A verdict from a turn that never finished (still paused, or cut at
        # max_tokens) may rest on a search it did not get to read. It can
        # still be a hint; it must never be the thing that authorizes a lock.
        if stop in ("pause_turn", "max_tokens") and verdict.confidence == "high":
            verdict.confidence = "medium"
            verdict.note = (f"[downgraded: turn incomplete ({stop})] "
                            + verdict.note)[:300]
        # Same for a verdict recovered from text written BEFORE a later tool
        # call (codex r1): it is provisional - the model searched again after
        # writing it and never restated it - so it may hint, never lock.
        if not from_final_answer and verdict.confidence == "high":
            verdict.confidence = "medium"
            verdict.note = ("[downgraded: verdict not in the final answer] "
                            + verdict.note)[:300]
        return verdict, meta
    except Exception as exc:  # noqa: BLE001 — never break the cross-check
        logger.warning(f"web_resolver: {ticker} resolution failed: {exc}")
        return None, meta


def resolve_disagreement(
    ticker: str,
    company_name: str,
    finnhub_date: str,
    yf_dates: list,
    today: date,
) -> WebVerdict | None:
    """Search the web for the company-announced earnings date. Never raises."""
    return resolve_with_meta(ticker, company_name, finnhub_date, yf_dates,
                             today)[0]


_TRUSTED_WIRE_DOMAINS = (
    "businesswire.com", "globenewswire.com", "prnewswire.com",
    "prnewswire.co.uk", "newswire.ca", "accesswire.com",
)


def _normalize_url(url: str) -> str:
    """A scheme-less echo ("www.prnewswire.com/news-releases/...") parses
    with an EMPTY netloc, which read as untrusted AND uncited (#515). Both
    halves of the trust gate must see the same normalized URL."""
    url = (url or "").strip()
    if url and "://" not in url:
        url = "https://" + url
    return url


def _safe_host(url: str) -> str:
    """Destination hostname of an http(s) URL, or "" when it cannot be
    trusted. `netloc` includes userinfo, so "https://prnewswire.com:443@evil.
    example/x" has a prnewswire-looking netloc but goes to evil.example
    (codex r3) - read `hostname`, and refuse any URL carrying credentials."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(_normalize_url(url))
        host = parsed.hostname or ""
    except ValueError:
        return ""
    if parsed.scheme not in ("http", "https") or "@" in (parsed.netloc or ""):
        return ""
    return host.lower()


def _is_trusted_source(url: str) -> bool:
    """True for newswire domains and company investor-relations hosts/paths."""
    from urllib.parse import urlparse
    url = _normalize_url(url)
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = _safe_host(url)
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
    """URLs the web_search tool actually retrieved this call.

    A successful result's `content` is a LIST of web_search_result; an error
    result's is a single `web_search_tool_result_error` object, which
    retrieved nothing - iterating it would walk a pydantic model's fields, so
    only a list/tuple is read. Under dynamic filtering (#409, verified live
    2026-10-01) the search is called from server-side code, yet the API still
    returns one `web_search_tool_result` block per search (carrying a
    `caller`), so those remain the citations; `code_execution_tool_result`
    blocks hold only encrypted stdout and retrieve nothing themselves."""
    urls: set[str] = set()
    for block in content_blocks or []:
        if _field(block, "type", "") != "web_search_tool_result":
            continue
        results = _field(block, "content", None)
        if not isinstance(results, (list, tuple)):
            continue
        for r in results:
            u = _field(r, "url", None)
            if isinstance(u, str) and u:
                urls.add(u)
    return urls


def _url_was_cited(source_url: str, cited: set[str]) -> bool:
    source_url = _normalize_url(source_url)
    if not source_url:
        return False
    if source_url in cited:
        return True
    # (A raw string-prefix branch used to sit here. It compared URLs as text,
    # so "www.prnewswire.com" was a prefix of a cited
    # "https://www.prnewswire.com.evil.example/..." (codex r4). Tracking-param
    # / slash drift is fully covered by the parsed-host comparison below.)
    # Same-host fallback: the model often reconstructs a canonical URL rather
    # than echoing the retrieved one verbatim (live 2026-07-09: Mastercard's
    # real Business Wire release was downgraded on an exact-URL miss). If the
    # search actually retrieved content from the SAME host, the claim "this
    # domain says X" is verified at domain granularity — still blocks the
    # injected-aggregator case, which fails the trusted-domain check anyway.
    from urllib.parse import urlparse
    try:
        host = _safe_host(source_url)
    except ValueError:
        return False
    if not host:
        return False
    for c in cited:
        try:
            if _safe_host(c) == host:
                return True
        except ValueError:
            continue
    return False


_VERDICT_KEYS = ("announced_date", "confidence", "source_url", "note")


def _parse_verdict(text: str, quiet: bool = False) -> WebVerdict | None:
    """Extract the JSON verdict from the model's final text. Tolerant of
    surrounding prose/code fences; strict about field shapes.

    Scans every flat {...} in the text, LAST first, and takes the first that
    parses to a dict carrying a verdict key - so a brace-pair in narration
    ahead of the answer cannot shadow it (#409)."""
    raw = None
    for m in reversed(list(re.finditer(r"\{[^{}]*\}", text or "", re.DOTALL))):
        try:
            cand = json.loads(m.group(0))
        except ValueError:
            continue
        if isinstance(cand, dict) and any(k in cand for k in _VERDICT_KEYS):
            raw = cand
            break
    if raw is None:
        if not quiet:
            logger.warning("web_resolver: no JSON verdict in response")
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
