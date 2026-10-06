"""Board #409: web_resolver against the web_search_20260209 (dynamic
filtering) response shape, plus the resilience paths that shape makes
reachable (pause_turn, error results, narrated preambles, a rejected tool).

The production-shape fixture is a REAL response (ABT, captured 2026-10-01,
encrypted payloads blanked) parsed through the installed SDK's own Message
model, so a test here sees exactly the block classes production sees.
"""
import json
from datetime import date
from pathlib import Path

import anthropic
import pytest

import web_resolver

_FIXTURE = Path(__file__).parent / "fixtures" / "web_search_20260209_response.json"
_TRUSTED = "https://www.prnewswire.com/news-releases/abbott-q2-date.html"
_VERDICT = ('{"announced_date": "2026-07-16", "confidence": "high", '
            '"source_url": "%s", "note": "n"}' % _TRUSTED)


def _construct(d):
    """SDK `construct` builds the typed block classes WITHOUT strict
    validation, so the tests survive SDK releases that add required fields
    (CI installs an unpinned `anthropic`)."""
    return anthropic.types.Message.construct(**d)


def _real_message():
    return _construct(json.loads(_FIXTURE.read_text()))


class _Recorder:
    """Stub client returning scripted responses and recording each request."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        outer = self

        class _Msgs:
            def create(self, **kw):
                outer.calls.append(kw)
                r = outer.responses.pop(0)
                if isinstance(r, Exception):
                    raise r
                return r

        self.messages = _Msgs()


def _install(monkeypatch, responses):
    rec = _Recorder(responses)
    monkeypatch.setattr(web_resolver, "ANTHROPIC_API_KEY", "k")
    monkeypatch.setattr(anthropic, "Anthropic", lambda **kw: rec)
    return rec


def _resolve(tool_type=web_resolver._DYNAMIC_WEB_SEARCH_TOOL):
    return web_resolver.resolve_with_meta(
        "ABT", "Abbott Laboratories", "2026-07-16", [date(2026, 7, 23)],
        date(2026, 7, 8), tool_type=tool_type)


def _msg(content, stop_reason="end_turn"):
    return _construct({
        "id": "msg_x", "type": "message", "role": "assistant",
        "model": "claude-sonnet-4-6", "content": content,
        "stop_reason": stop_reason, "stop_sequence": None,
        "usage": {"input_tokens": 10, "output_tokens": 5,
                  "server_tool_use": {"web_search_requests": 1,
                                      "web_fetch_requests": 0}},
    })


def _search_block(urls, caller=True):
    b = {"type": "web_search_tool_result", "tool_use_id": "srvtoolu_1",
         "content": [{"type": "web_search_result", "url": u, "title": "t",
                      "encrypted_content": "x", "page_age": None} for u in urls]}
    if caller:
        b["caller"] = {"type": "code_execution_20260120", "tool_id": "srvtoolu_0"}
    return b


def _text(t):
    return {"type": "text", "text": t, "citations": None}


# --- the production shape ---------------------------------------------------

def test_real_dynamic_filtering_response_yields_a_verdict(monkeypatch):
    rec = _install(monkeypatch, [_real_message()])
    v, meta = _resolve()
    assert v is not None and v.announced_date == "2026-07-16"
    # The real reply's source (abbott.mediaroom.com) is neither an ir./investor
    # host nor a wire, so the trust gate must still hold it below "high".
    assert v.confidence == "medium" and v.source_verified is False
    assert "code_execution_tool_result" in meta["block_types"]
    assert rec.calls[0]["tools"][0]["type"] == "web_search_20260209"


def test_production_default_is_the_measured_winner(monkeypatch):
    """#409 replay 2026-10-01: legacy kept (leak-free ~34 v ~30 of 40, -30%
    cost). Flipping this must be a deliberate, re-measured change."""
    assert web_resolver._WEB_SEARCH_TOOL == "web_search_20250305"
    rec = _install(monkeypatch, [_msg([_search_block([_TRUSTED], caller=False),
                                       _text(_VERDICT)])])
    web_resolver.resolve_disagreement(
        "ABT", "Abbott", "2026-07-16", [date(2026, 7, 23)], date(2026, 7, 8))
    assert rec.calls[0]["tools"][0]["type"] == "web_search_20250305"
    assert rec.calls[0]["max_tokens"] >= 1611   # replay's peak output


def test_provisional_verdict_before_a_later_search_cannot_lock(monkeypatch):
    """codex r1: [text(high JSON), search, result] with NO trailing text - the
    only verdict was written before the model searched again."""
    _install(monkeypatch, [_msg([
        _text(_VERDICT),
        {"type": "server_tool_use", "id": "srvtoolu_0", "name": "web_search",
         "input": {"query": "q"}},
        _search_block([_TRUSTED], caller=False),
    ])])
    v, _ = _resolve()
    assert v.confidence == "medium" and "final answer" in v.note


def test_verdict_only_in_an_earlier_text_segment_cannot_lock(monkeypatch):
    _install(monkeypatch, [_msg([
        _text(_VERDICT),
        _search_block([_TRUSTED], caller=False),
        _text("Searching complete; nothing further."),
    ])])
    v, _ = _resolve()
    assert v.confidence == "medium"


def test_citations_are_read_from_code_called_search_results():
    urls = web_resolver._cited_urls(_real_message().content)
    assert any("abt-2026q2xexhibitx991" in u for u in urls)


def test_code_execution_blocks_carry_no_citations():
    """A code_execution_tool_result is an OBJECT, not a list of results."""
    blocks = [b for b in _real_message().content
              if b.type == "code_execution_tool_result"]
    assert blocks and web_resolver._cited_urls(blocks) == set()


def test_cited_wire_source_under_dynamic_filtering_is_verified(monkeypatch):
    _install(monkeypatch, [_msg([
        {"type": "server_tool_use", "id": "srvtoolu_0", "name": "code_execution",
         "input": {"code": "r = await web_search({'query': 'q'})"}},
        _search_block([_TRUSTED]),
        _text(_VERDICT),
    ])])
    v, _ = _resolve()
    assert v.confidence == "high" and v.source_verified is True


# --- resilience -------------------------------------------------------------

def test_search_error_object_is_not_iterated_as_results():
    err = _construct({
        "id": "m", "type": "message", "role": "assistant", "model": "x",
        "stop_reason": "end_turn", "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
        "content": [{"type": "web_search_tool_result", "tool_use_id": "s",
                     "content": {"type": "web_search_tool_result_error",
                                 "error_code": "max_uses_exceeded"}}],
    })
    assert web_resolver._cited_urls(err.content) == set()


def test_dict_shaped_blocks_are_read():
    blocks = [_search_block([_TRUSTED]), _text(_VERDICT)]
    assert web_resolver._cited_urls(blocks) == {_TRUSTED}
    assert web_resolver._answer_candidates(blocks)[0] == (_VERDICT, True)


def test_narrated_preamble_braces_do_not_shadow_the_verdict(monkeypatch):
    _install(monkeypatch, [_msg([
        _text('Let me filter results {"ticker": "ABT", "q": 2} first.'),
        {"type": "server_tool_use", "id": "srvtoolu_0", "name": "code_execution",
         "input": {"code": "x"}},
        _search_block([_TRUSTED]),
        _text(_VERDICT),
    ])])
    v, _ = _resolve()
    assert v is not None and v.announced_date == "2026-07-16"


def test_parse_verdict_prefers_the_last_verdict_object():
    v = web_resolver._parse_verdict(
        '{"announced_date": "2026-07-01", "confidence": "low"} then '
        '{"x": 1} and finally ' + _VERDICT)
    assert v.announced_date == "2026-07-16"


def test_pause_turn_is_resumed_and_its_citations_kept(monkeypatch):
    first = _msg([{"type": "server_tool_use", "id": "srvtoolu_0",
                   "name": "web_search", "input": {"query": "q"}},
                  _search_block([_TRUSTED], caller=False)],
                 stop_reason="pause_turn")
    second = _msg([_text(_VERDICT)])
    rec = _install(monkeypatch, [first, second])
    v, meta = _resolve()
    assert meta["continuations"] == 1 and len(rec.calls) == 2
    # max_uses is per request: the resumed turn gets only what is left (r2).
    assert [c["tools"][0]["max_uses"] for c in rec.calls] == [
        web_resolver._MAX_SEARCHES, web_resolver._MAX_SEARCHES - 1]
    resumed = rec.calls[1]["messages"]
    assert resumed[-1]["role"] == "assistant" and resumed[-1]["content"]
    # The citation came from the PAUSED response; losing it would downgrade.
    assert v.confidence == "high" and v.source_verified is True


def test_turn_still_paused_after_the_cap_cannot_authorize_a_lock(monkeypatch):
    paused = [_msg([_search_block([_TRUSTED]), _text(_VERDICT)],
                   stop_reason="pause_turn")
              for _ in range(web_resolver._MAX_CONTINUATIONS + 1)]
    rec = _install(monkeypatch, paused)
    v, meta = _resolve()
    assert len(rec.calls) == web_resolver._MAX_CONTINUATIONS + 1
    assert v.confidence == "medium" and "incomplete" in v.note


def test_max_tokens_cut_cannot_authorize_a_lock(monkeypatch):
    _install(monkeypatch, [_msg([_search_block([_TRUSTED]), _text(_VERDICT)],
                                stop_reason="max_tokens")])
    v, _ = _resolve()
    assert v.confidence == "medium"


def test_refusal_returns_no_verdict(monkeypatch):
    _install(monkeypatch, [_msg([_text(_VERDICT)], stop_reason="refusal")])
    v, _ = _resolve()
    assert v is None


def test_no_text_at_all_returns_none(monkeypatch):
    _install(monkeypatch, [_msg([_search_block([_TRUSTED])])])
    v, _ = _resolve()
    assert v is None


def _transport():
    """The HTTP library the INSTALLED SDK builds its errors from. anthropic 1.x moved
    from httpx to httpx2, and CI (`anthropic>=0.40`, unpinned) installs 1.x while the
    laptop has 0.86 -- so a bare `import httpx` failed only in CI (since 2026-10-02)."""
    try:
        import httpx2 as transport
    except ImportError:
        import httpx as transport
    return transport


def test_rejected_new_tool_falls_back_to_the_legacy_type(monkeypatch):
    httpx = _transport()
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    bad = anthropic.BadRequestError(
        "tool type not supported", response=httpx.Response(400, request=req),
        body=None)
    rec = _install(monkeypatch, [bad, _msg([_search_block([_TRUSTED]),
                                            _text(_VERDICT)])])
    v, meta = web_resolver.resolve_with_meta(
        "ABT", "Abbott", "2026-07-16", [date(2026, 7, 23)], date(2026, 7, 8),
        tool_type="web_search_20260209")
    assert [c["tools"][0]["type"] for c in rec.calls] == [
        "web_search_20260209", web_resolver._LEGACY_WEB_SEARCH_TOOL]
    assert meta.get("fell_back") is True and v.announced_date == "2026-07-16"


def test_rejected_legacy_tool_does_not_loop(monkeypatch):
    httpx = _transport()
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    bad = anthropic.BadRequestError(
        "bad", response=httpx.Response(400, request=req), body=None)
    rec = _install(monkeypatch, [bad])
    v, _ = web_resolver.resolve_with_meta(
        "ABT", "Abbott", "2026-07-16", [date(2026, 7, 23)], date(2026, 7, 8),
        tool_type=web_resolver._LEGACY_WEB_SEARCH_TOOL)
    assert v is None and len(rec.calls) == 1


def test_api_error_never_raises(monkeypatch):
    _install(monkeypatch, [RuntimeError("boom")])
    assert web_resolver.resolve_disagreement(
        "ABT", "Abbott", "2026-07-16", [date(2026, 7, 23)],
        date(2026, 7, 8)) is None


@pytest.mark.parametrize("tool", ["web_search_20250305", "web_search_20260209"])
def test_both_shapes_produce_the_same_verdict(monkeypatch, tool):
    legacy = [_search_block([_TRUSTED], caller=False), _text(_VERDICT)]
    dynamic = [{"type": "server_tool_use", "id": "srvtoolu_0",
                "name": "code_execution", "input": {"code": "x"}},
               _search_block([_TRUSTED]),
               {"type": "code_execution_tool_result", "tool_use_id": "srvtoolu_0",
                "content": {"type": "encrypted_code_execution_result",
                            "encrypted_stdout": "x", "stderr": "",
                            "return_code": 0, "content": []}},
               _text(_VERDICT)]
    _install(monkeypatch, [_msg(legacy if tool.endswith("0305") else dynamic)])
    v, _ = web_resolver.resolve_with_meta(
        "ABT", "Abbott", "2026-07-16", [date(2026, 7, 23)], date(2026, 7, 8),
        tool_type=tool)
    assert (v.announced_date, v.confidence, v.source_verified) == (
        "2026-07-16", "high", True)


def test_replay_budget_reserves_a_full_continuation_chain():
    """codex r2: the replay's per-call reservation must cover every request
    a resolution can legally make, not one typical request."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "replay", Path(__file__).parent / "scripts" / "replay_web_search_tool.py")
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)
    n_req = 1 + web_resolver._MAX_CONTINUATIONS
    # 60,295 = the replay's observed peak input tokens for ONE resolution.
    floor = (n_req * (60_295 * 3 / 1e6 + web_resolver._MAX_TOKENS * 15 / 1e6)
             + (web_resolver._MAX_SEARCHES + web_resolver._MAX_CONTINUATIONS)
             * 0.01)
    assert replay._WORST_CALL > floor
    assert replay.call_cost({"input_tokens": 0, "output_tokens": 0,
                             "web_search_requests": 3}) == pytest.approx(0.03)


def test_a_400_on_a_continuation_does_not_restart_on_legacy(monkeypatch):
    """codex r3: only a rejection of the FIRST request means the tool type is
    unsupported; a mid-chain 400 must not discard paid work and start over."""
    httpx = _transport()
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    bad = anthropic.BadRequestError(
        "bad", response=httpx.Response(400, request=req), body=None)
    paused = _msg([_search_block([_TRUSTED])], stop_reason="pause_turn")
    rec = _install(monkeypatch, [paused, bad])
    v, _ = _resolve()
    assert v is None and len(rec.calls) == 2


def test_scorer_excludes_rows_that_fell_back(tmp_path, capsys):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "score", Path(__file__).parent / "scripts" / "score_web_search_replay.py")
    score = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(score)
    base = {"ticker": "ABT", "truth": "2026-07-16", "today": "2026-07-08",
            "finnhub": "2026-07-16", "yfinance": "2026-07-23",
            "announced_date": "2026-07-16", "confidence": "high",
            "source_url": _TRUSTED, "cost": 0.1}
    rows = [dict(base, tool="web_search_20250305",
                 meta={"tool_type": "web_search_20250305"}),
            dict(base, tool="web_search_20260209",
                 meta={"tool_type": "web_search_20250305", "fell_back": True})]
    f = tmp_path / "r.jsonl"
    f.write_text("\n".join(json.dumps(r) for r in rows))
    score.main(str(f))
    out = capsys.readouterr().out
    assert "EXCLUDED 1" in out and "complete pairs=0" in out


def test_billed_usage_survives_a_failed_continuation(monkeypatch):
    """codex r4: a chain that dies on its continuation must still report the
    usage of the request that DID complete, or the replay's cap undercounts."""
    paused = _msg([_search_block([_TRUSTED])], stop_reason="pause_turn")
    _install(monkeypatch, [paused, RuntimeError("connection reset")])
    v, meta = _resolve()
    assert v is None
    assert meta["input_tokens"] == 10 and meta["web_search_requests"] == 1


def test_empty_trailing_text_does_not_make_a_provisional_verdict_final(monkeypatch):
    """codex r5: [text(high JSON), search, result, text("")] - the trailing
    block is text but carries no answer, so the verdict is still provisional."""
    _install(monkeypatch, [_msg([
        _text(_VERDICT),
        {"type": "server_tool_use", "id": "srvtoolu_0", "name": "web_search",
         "input": {"query": "q"}},
        _search_block([_TRUSTED], caller=False),
        _text(""),
    ])])
    v, _ = _resolve()
    assert v.confidence == "medium" and "final answer" in v.note
