"""Tests for the cross-check web-search resolver (web_resolver.py)."""
from datetime import date

from web_resolver import WebVerdict, _parse_verdict


def test_parse_verdict_clean_json():
    v = _parse_verdict(
        '{"announced_date": "2026-07-21", "confidence": "high", '
        '"source_url": "https://investor.iconplc.com/x", "note": "ICON IR page"}'
    )
    assert v.announced_date == "2026-07-21"
    assert v.confidence == "high"
    assert v.source_url.startswith("https://investor.iconplc.com")


def test_parse_verdict_tolerates_surrounding_prose_and_fences():
    v = _parse_verdict(
        'Here is my answer:\n```json\n{"announced_date": null, '
        '"confidence": "low", "source_url": "", "note": "nothing found"}\n```'
    )
    assert v.announced_date is None
    assert v.confidence == "low"


def test_parse_verdict_rejects_bad_date_and_confidence():
    v = _parse_verdict(
        '{"announced_date": "July 21st", "confidence": "certain", '
        '"source_url": "", "note": ""}'
    )
    assert v.announced_date is None       # non-ISO date rejected
    assert v.confidence == "low"          # unknown confidence coerced down


def test_parse_verdict_no_json_returns_none():
    assert _parse_verdict("I could not find anything.") is None


def test_matches_exact_iso_only():
    v = WebVerdict("2026-07-21", "high", "", "")
    assert v.matches("2026-07-21")
    assert not v.matches("2026-07-23")
    assert not WebVerdict(None, "high", "", "").matches("2026-07-21")


def test_resolver_returns_none_without_key(monkeypatch):
    import web_resolver
    monkeypatch.setattr(web_resolver, "ANTHROPIC_API_KEY", None)
    assert web_resolver.resolve_disagreement(
        "ICLR", "ICON PLC", "2026-07-21", [date(2026, 7, 23)], date(2026, 7, 8)
    ) is None


def test_trusted_source_domains():
    from web_resolver import _is_trusted_source
    assert _is_trusted_source("https://investor.iconplc.com/news/x")
    assert _is_trusted_source("https://ir.example.com/press")
    assert _is_trusted_source("https://www.businesswire.com/news/home/2026/x")
    assert _is_trusted_source("https://example.com/investor-relations/news")
    assert not _is_trusted_source("https://www.zacks.com/stock/ICLR")
    assert not _is_trusted_source("https://evil-businesswire.com.attacker.io/x")
    assert not _is_trusted_source("")


def test_url_was_cited_tolerates_param_drift():
    from web_resolver import _url_was_cited
    cited = {"https://investor.iconplc.com/news/release-1?utm_source=search"}
    assert _url_was_cited("https://investor.iconplc.com/news/release-1", cited)
    assert not _url_was_cited("https://investor.other.com/x", cited)
    assert not _url_was_cited("", cited)


def test_high_confidence_downgraded_without_verified_citation(monkeypatch):
    """codex 2026-07-08: model-claimed 'high' must not authorize a lock unless
    the source was actually retrieved AND is a company-IR/wire domain."""
    import web_resolver
    from datetime import date as _date

    class _Text:
        type = "text"
        text = ('{"announced_date": "2026-07-21", "confidence": "high", '
                '"source_url": "https://www.zacks.com/x", "note": "aggregator"}')

    class _Resp:
        content = [_Text()]

    class _Msgs:
        def create(self, **kw):
            return _Resp()

    class _Client:
        def __init__(self, **kw):
            self.messages = _Msgs()

    monkeypatch.setattr(web_resolver, "ANTHROPIC_API_KEY", "k")
    import anthropic
    monkeypatch.setattr(anthropic, "Anthropic", _Client)
    v = web_resolver.resolve_disagreement(
        "ICLR", "ICON PLC", "2026-07-21", [_date(2026, 7, 23)], _date(2026, 7, 8))
    assert v.confidence == "medium"          # downgraded -> caller won't lock
    assert "downgraded" in v.note
