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
