# Codex brief — IR coverage audit takes ownership from the brokers, not Coverage Manager (board #549)

Date: 2026-10-20. Requester: Claude (overnight). Implementer: Codex, write access to this repo's
working tree. Do NOT commit or push. Do not edit README/CHANGELOG.

## Goal

`ir_coverage.py` decides which companies the IR-mailing-list audit covers: Core names UNION the
names JP owns. It learns "what JP owns" from `_portfolio_tickers()` (ir_coverage.py ~line 127), which
reads Coverage Manager's `exports/portfolio.json`. JP ruled that the brokers own "what I own"
(board #549): the authoritative file is `portfolio_daily/exports/held.json`, read through the fleet's
canonical reader `held_feed.py`, whose `join_ownership()` is THE ownership rule. CM's copy lags (ZTS,
sold 2026-09-24, stayed "held" in CM for 14 days). 19 other readers already moved; this is one of the
last.

`_portfolio_tickers()` today also fails SILENTLY: any read error returns an empty set, which makes
every held non-Core name invisible to the audit with no message. That must stop.

## What to build

1. **Vendor the canonical reader**: copy `../portfolio_daily/scripts/held_feed.py` byte-for-byte to
   `held_feed.py` at this repo's root (next to `ir_coverage.py`). Do not edit the copy — a fleet check
   compares it to the canonical bytes.
2. **Rewrite `_portfolio_tickers()`** to follow the exact pattern of
   `../exec_interviews/ownership.py` (`portfolio_set`, lines ~44-58):
   - `cm_portfolio` = keys of CM's `exports/portfolio.json` (as today);
   - `known` = every `Ticker` in CM's `exports/universe.csv` (the file `UNIVERSE` already points at);
   - `j = held_feed.join_ownership(cm_portfolio, known, aliases_path=<CM exports>/ticker_aliases.json,
     repo_root=ROOT / "earnings_agent")` — check `held_feed.default_path(repo_root)` to confirm what
     repo_root it expects so it resolves `../portfolio_daily/exports/held.json`;
   - print every `j.notes` and `j.warnings` line to stderr, prefixed `note:` / `warning:`;
   - if `not j.applied`: return CM's portfolio set (the documented fallback);
   - else return `set(j.promote) | (cm_portfolio - j.demote)`.
   - A failure to read CM's `portfolio.json` is no longer silent: print a `warning:` to stderr naming the
     file, and continue with an empty CM portfolio (held.json can still promote names).
3. Keep `_portfolio_tickers()`'s signature and the `_scope` logic in `load_universe()` unchanged.

## Out of scope

No change to any other module, to CM, to portfolio_daily, or to `scripts/sync_held_feed.py` (the
requester registers the new copy there). No network calls. No scheduled-task changes.

## Tests you may run (safe: no network, no live Gmail)

`python -m pytest -q test_ir_coverage.py`

Add small tests to `test_ir_coverage.py` only if cheap, using tmp files and monkeypatching
`ir_coverage.ROOT` (or the paths you introduce): (a) a held name absent from CM's portfolio.json is
promoted into scope; (b) a CM portfolio name the brokers no longer hold leaves scope; (c) an unreadable
held.json falls back to CM's set and prints a warning; (d) an unreadable portfolio.json prints a warning
instead of failing silently. The requester will add any you skip.

## Done when

`python -c "import ir_coverage; print(sorted(ir_coverage._portfolio_tickers()))"` run from this repo
prints the brokers' holdings (it should NOT contain ZTS) and `test_ir_coverage.py` passes.
