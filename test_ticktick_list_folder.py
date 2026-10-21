"""Which TickTick folder a newly created list lands in.

JP 2026-10-21: earnings lists go to the "Earnings" folder; the IR-signup lists
stay in "Work - Investing". Both are created through find_or_create_list, so
the folder has to be decided per caller, not by one shared default.
"""

import ir_ticktick
import ticktick


class _Resp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


def _stub_requests(monkeypatch, existing=()):
    posts = []
    monkeypatch.setattr(ticktick.requests, "get",
                        lambda *a, **k: _Resp(200, list(existing)))

    def _post(url, headers=None, json=None, timeout=None):
        posts.append(json)
        return _Resp(200, {"id": "new-id", **json})

    monkeypatch.setattr(ticktick.requests, "post", _post)
    return posts


def test_new_earnings_list_is_created_in_the_earnings_folder(monkeypatch):
    monkeypatch.delenv("TICKTICK_EARNINGS_GROUP_ID", raising=False)
    posts = _stub_requests(monkeypatch)
    name = ticktick._quarter_list_name("2027-01-20", tier=1)
    assert ticktick.find_or_create_list("tok", name) == "new-id"
    assert posts == [{"name": name, "groupId": ticktick.EARNINGS_GROUP_ID}]


def test_existing_list_is_returned_and_never_recreated(monkeypatch):
    posts = _stub_requests(
        monkeypatch, existing=[{"id": "old", "name": "4Q26 Earnings - X",
                                "groupId": ticktick.WORK_INVESTING_GROUP_ID}])
    assert ticktick.find_or_create_list("tok", "4Q26 Earnings - X") == "old"
    assert posts == []


def test_ir_signup_lists_stay_in_work_investing(monkeypatch):
    posts = _stub_requests(monkeypatch)
    monkeypatch.setattr(ticktick, "list_tasks_in_project", lambda *a: [])
    ir_ticktick.push([], ir_ticktick.PORTFOLIO_LIST, {"covered": []}, token="tok")
    assert posts == [{"name": ir_ticktick.PORTFOLIO_LIST,
                      "groupId": ticktick.WORK_INVESTING_GROUP_ID}]
