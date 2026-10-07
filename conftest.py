"""Suite-wide isolation: no test may reach the live network.

⛑ Found 2026-10-07 by `scripts/pytest_fleet_guard.py --measure-all` (board #368),
not by any assertion here: two tests of `main.run_edgar_results_fallback` --
`test_dedup.py::test_edgar_fallback_blind_sweep_flags_unlisted_tier1` and
`test_run_integration.py::test_edgar_fallback_raises_on_degraded_edgar` -- opened
a real HTTPS connection to SEC EDGAR on every run. Both stub
`find_earnings_release_filing`, but the Tier-1 blind sweep then falls through to
`find_results_6k` (the foreign-filer path, added later), which was not stubbed --
so the suite queried EDGAR with a fabricated CIK. The sweep wraps each probe in
`except Exception: continue`, so the call was invisible twice over: the test
passed with or without the wire, and a slow or blocked SEC only showed up as
wall clock.

So this file does three things:

1. **Refuses** a non-loopback connect BEFORE it is made.
2. **From `pytest_configure`, not from a fixture.** A function fixture starts only
   after every test module has been IMPORTED, so a module-level network call at
   collection would sail past it (Codex review 2026-10-07, P1). The root
   conftest is loaded before collection, so installing here covers import time.
3. **Fails loudly even when the refusal is swallowed** -- the code under test
   catches exceptions by design, so a refusal alone would be silent too. A refusal
   inside a test fails that test at teardown; one during collection aborts the
   run. A new collaborator added to a code path is therefore caught the first
   time anything drives it, without anyone maintaining a stub list.

Opt out with `@pytest.mark.allow_network` for a test that genuinely needs the
wire. Nothing in this suite does.

⚠ **SCOPE: THE PYTEST PROCESS ONLY.** These patches do not reach a CHILD process
(Codex r4). The suite's subprocess tests (`test_ci_db_restore.py` runs the CI
shell scripts with a fake `gh` first on PATH and `cwd=tmp_path`) are isolated by
their own stubs, not by this file. The cross-process backstop is the fleet guard
(`scripts/pytest_fleet_guard.py`), whose sitecustomize hook loads in every Python
child and logs every subprocess launch -- re-measure there after adding one.
"""
from __future__ import annotations

import ipaddress
import socket

import pytest

_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain"})
_REAL = {}
# Every refusal, as [phase, address, claimed]. A refusal is CLAIMED only by the
# test that was running when it happened (that test then fails at teardown).
# Anything still unclaimed at session end -- collection, a session/module
# fixture, a teardown between tests -- fails the run. Nothing is filtered by
# phase name, so no phase can be the one a filter forgot (Codex r2, P1).
_REFUSED: list = []
_STATE = {"phase": "<collection>", "allow": False}


def _is_loopback(address) -> bool:
    """EXACT names, or an IP literal that parses as loopback -- never a prefix.
    `startswith("localhost")` admitted `localhost.attacker.example` and
    `127.example.com` (Codex r3, P1), which then resolved and connected for real."""
    host = address[0] if isinstance(address, tuple) and address else address
    if not isinstance(host, str):
        return False
    if host.lower().rstrip(".") in _LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _check(address):
    if _STATE["allow"] or _is_loopback(address):
        return
    _REFUSED.append([_STATE["phase"], address, False])
    raise ConnectionRefusedError(
        f"live network refused in tests: {address!r}. Stub the collaborator, "
        f"or mark the test `@pytest.mark.allow_network` if it truly needs it.")


def _guarded_connect(self, address, *a, **kw):
    _check(address)
    return _REAL["connect"](self, address, *a, **kw)


def _guarded_connect_ex(self, address, *a, **kw):
    _check(address)
    return _REAL["connect_ex"](self, address, *a, **kw)


def _guarded_getaddrinfo(host, *a, **kw):
    # ⛑ DNS is refused too, BEFORE the lookup (Codex r2, P1): requests/urllib3
    # resolve the hostname before any connect, so a connect-only guard let a
    # live DNS query out -- and a DNS failure swallowed by the code under test
    # left nothing recorded. `None` / loopback names are the local cases.
    if host is not None:
        h = host.decode() if isinstance(host, bytes) else host
        _check((h,))
    return _REAL["getaddrinfo"](host, *a, **kw)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "allow_network: this test may open a non-loopback socket")
    if not _REAL:
        _REAL["connect"] = socket.socket.connect
        _REAL["connect_ex"] = socket.socket.connect_ex
        _REAL["getaddrinfo"] = socket.getaddrinfo
        socket.socket.connect = _guarded_connect
        socket.socket.connect_ex = _guarded_connect_ex
        socket.getaddrinfo = _guarded_getaddrinfo


def pytest_unconfigure(config):
    if _REAL:
        socket.socket.connect = _REAL.pop("connect")
        socket.socket.connect_ex = _REAL.pop("connect_ex")
        socket.getaddrinfo = _REAL.pop("getaddrinfo")


def pytest_collection_modifyitems(session, config, items):
    _STATE["phase"] = "<outside a test>"
    during = [r[1] for r in _REFUSED if not r[2]]
    if during:
        raise pytest.UsageError(
            f"live network attempted {len(during)} time(s) while IMPORTING tests, "
            f"first {during[0]!r} -- a module-level call reached for the wire.")


def pytest_sessionfinish(session, exitstatus):
    """A refusal outside any test (a session/module fixture, a teardown) has no
    test to fail, so it fails the run instead -- never silence."""
    stray = [r[1] for r in _REFUSED if not r[2]]
    if stray:
        print(f"\n!! live network attempted {len(stray)} time(s) outside any test, "
              f"first {stray[0]!r}")
        session.exitstatus = 1


@pytest.fixture(autouse=True)
def _no_live_network(request):
    start = len(_REFUSED)
    _STATE["phase"] = request.node.nodeid
    _STATE["allow"] = request.node.get_closest_marker("allow_network") is not None
    try:
        yield
    finally:
        _STATE["phase"] = "<outside a test>"
        _STATE["allow"] = False
    mine = []
    for r in _REFUSED[start:]:
        if r[0] == request.node.nodeid:
            r[2] = True
            mine.append(r[1])
    assert not mine, (
        f"this test tried to reach the live network {len(mine)} time(s), first "
        f"{mine[0]!r} -- the code under test swallowed the refusal, so only this "
        f"teardown check can see it. Stub the collaborator that made the call.")
