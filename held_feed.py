"""Read OWNERSHIP from the brokers' published book (`portfolio_daily/exports/held.json`),
and THE OWNERSHIP RULE that joins it to Coverage Manager's names (`join_ownership`).

CANONICAL COPY: `portfolio_daily/scripts/held_feed.py` -- the publisher owns the reader
contract, and its own test round-trips `export_held.write_held()` through this loader.
VENDORED byte-identical into every consumer repo (board #549, 2026-10-06).
DO NOT EDIT A VENDORED COPY: edit the canonical, then run
`python scripts/sync_held_feed.py` from the fleet root, which copies it out;
`--check` (run by portfolio_daily's tests and `shiplog.py finish-check`) fails on any drift.
Plan: `portfolio_daily/docs/held_feed_canonical_plan.md`.

JP, 2026-10-06: *"lets not have coverage manager be a source of truth of what I own or
even consume it"*. Ownership comes from the brokers via this file; intent (Researching /
Following for Interest / Ready to Buy / Ready to Short) stays in Coverage Manager.

Contract (`held.json` schema 1, written by `portfolio_daily/scripts/export_held.py`):

    {"schema": 1, "generated_at": "...",
     "brokers": [{"broker": "IBKR", "as_of": "2026-10-05", ...}, ...],
     "held": [{"ticker": "AAPL", "shares": 13.0, "avg_cost": 67.5, "brokers": ["Fidelity"]}]}

Tickers are BROKER symbols (Fiserv is `FISV`; Coverage Manager keys it `FI`). A consumer
joining to CM applies CM's published alias map itself.

FAILURE SEMANTICS - the one rule: this module never returns an empty book.
  * missing / unreadable / not JSON / unknown schema / no brokers / a broker with no
    parseable as_of / zero holdings / a blank ticker  -> raises HeldFeedError.
    The caller alarms. "I could not read the book" must never read as "I own nothing".
  * a holding attributed to a broker that has no `brokers[]` row (so no as_of, so no
    age) -> raises too: it would otherwise read as fresh.
  * a broker whose `age_source` is not "as_of" (the publisher fell back to the file's
    mtime because the CSV declared no as_of) has NO as_of here: its `brokers` value is
    None and `unverified_brokers()` names it. A file-touch date must never become the
    "updated" date a consumer shows (Codex, #549). Its age is unknown, so it is stale.
  * STALENESS IS DATA, NOT AN ERROR - two different clocks, reported separately:
      - `publisher_age_days()`: days since `generated_at`. portfolio_daily rewrites the
        file Mon-Fri 16:30 and on failure LEAVES THE PREVIOUS FILE, so a dead publisher
        shows up as a file that quietly ages. Past PUBLISHER_STALE_DAYS that is the
        process not running, and the caller must say THAT, not "a broker is stale".
      - `stale_brokers()`: a broker whose as_of is old. Refusing the whole book would
        also freeze the fresh broker's sales (the two refresh on different interactive
        jobs), so it is named, not refused. portfolio_daily's own heartbeat already
        alarms on it; consumers show it, they need not re-alarm.

THE RULE (`join_ownership`, 2026-10-15) was hand-copied into 13 lanes and had drifted;
it lives here now so there is one copy. Its alias reader REFUSES a doubtful map, unlike
portfolio_daily's `symbols.py`, which passes through: that one translates a price lookup
(a miss costs one price), this one decides a SALE (a miss demotes a held name).
Deliberately opposite; do not "fix" one to match the other.

Stdlib only, no network. I/O: held.json, plus CM's ticker_aliases.json and
universe_metadata.json at the path the caller passes to `join_ownership`.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

CONTRACT = "held_feed/1"
ACCEPTED_SCHEMAS = frozenset({1})
#: Same number as Coverage Manager `universe/held.py: HELD_STALE_MAX_DAYS`.
DEFAULT_STALE_DAYS = 10
#: The export runs Mon-Fri; 5 calendar days covers a long weekend and nothing more.
PUBLISHER_STALE_DAYS = 5
ENV_PATH = "PORTFOLIO_HELD_JSON"


def today_et() -> date:
    """Today in America/New_York - the fleet's clock - not the host's local date, so a
    UTC host near midnight does not shift every age by a day. Falls back to the local
    date only if the tz database is unavailable."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).date()
    except Exception:                                       # noqa: BLE001
        return date.today()


class HeldFeedError(Exception):
    """held.json could not be trusted. Never interpret as an empty book."""


def default_path(repo_root) -> Path:
    """`$PORTFOLIO_HELD_JSON`, else `<workspace>/portfolio_daily/exports/held.json`,
    where <workspace> is the parent of the CONSUMER repo root the caller passes. The
    caller passes its own root because this file sits at different depths per repo."""
    env = os.environ.get(ENV_PATH)
    if env:
        return Path(env)
    return Path(repo_root).resolve().parent / "portfolio_daily" / "exports" / "held.json"


@dataclass(frozen=True)
class HeldBook:
    path: Path
    generated_at: str
    brokers: dict            # broker name -> as_of (date), or None if not declared
    tickers: dict            # TICKER -> tuple of broker names
    today: date = field(default_factory=today_et)

    def age_days(self, broker: str) -> int:
        """Days since the broker's as_of, measured against `today` - never the
        age_days frozen into the file at publish time. None when the broker declared
        no as_of (see `unverified_brokers`) or declared one in the FUTURE - a negative
        age would read fresher-than-fresh and silence the staleness check."""
        d = self.brokers[broker]
        if d is None:
            return None
        age = (self.today - d).days
        return None if age < 0 else age

    def unverified_brokers(self) -> list:
        return sorted(b for b in self.brokers if self.brokers[b] is None)

    def stale_brokers(self, max_days: int = DEFAULT_STALE_DAYS) -> list:
        """Brokers older than `max_days`, plus any whose age is unknown."""
        return sorted(b for b in self.brokers
                      if self.age_days(b) is None or self.age_days(b) > max_days)

    def publisher_age_days(self) -> int | None:
        """Days since `generated_at`. None if absent, unparseable or in the FUTURE
        (a future stamp would otherwise read as fresh forever) - `publisher_stale()`
        treats None as stale."""
        try:
            gen = date.fromisoformat(self.generated_at.strip()[:10])
        except (AttributeError, ValueError):
            return None
        age = (self.today - gen).days
        return None if age < 0 else age

    def publisher_stale(self, max_days: int = PUBLISHER_STALE_DAYS) -> bool:
        age = self.publisher_age_days()
        return age is None or age > max_days

    def owns(self, ticker: str) -> bool:
        return str(ticker or "").strip().upper() in self.tickers


def _parse_date(value, broker: str) -> date:
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except (TypeError, ValueError) as exc:
        raise HeldFeedError(f"broker {broker!r} has no parseable as_of ({value!r})") from exc


def load_held(path=None, *, repo_root=None, today: date | None = None) -> HeldBook:
    """Load and validate held.json. Raises HeldFeedError; never returns an empty book.

    Pass `path`, or `repo_root` (the consumer repo's root) to use `default_path`."""
    if path is None:
        if repo_root is None:
            raise HeldFeedError("load_held needs a path or the consumer's repo_root")
        path = default_path(repo_root)
    path = Path(path)
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except (OSError, ValueError) as exc:             # ValueError: UnicodeDecodeError
        raise HeldFeedError(f"cannot read {path}: {exc}") from exc
    try:
        doc = json.loads(raw)
    except (ValueError, RecursionError) as exc:
        raise HeldFeedError(f"{path.name} is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise HeldFeedError(f"{path.name} is a {type(doc).__name__}, not an object")
    schema = doc.get("schema")
    if type(schema) is not int or schema not in ACCEPTED_SCHEMAS:   # [] is unhashable
        raise HeldFeedError(f"{path.name} schema {doc.get('schema')!r} not in "
                            f"{sorted(ACCEPTED_SCHEMAS)}")

    brokers_raw = doc.get("brokers")
    if not isinstance(brokers_raw, list) or not brokers_raw:
        raise HeldFeedError(f"{path.name} lists no brokers")
    brokers = {}
    for b in brokers_raw:
        if not isinstance(b, dict) or not str(b.get("broker") or "").strip():
            raise HeldFeedError(f"{path.name} has a malformed broker entry: {b!r}")
        name = str(b["broker"]).strip()
        as_of = _parse_date(b.get("as_of"), name)
        # Absent age_source = a pre-2026-09-07 publisher, which wrote file MTIME into
        # `as_of`. Only an explicit "as_of" source is a declared broker date.
        brokers[name] = as_of if b.get("age_source") == "as_of" else None

    held_raw = doc.get("held")
    if not isinstance(held_raw, list) or not held_raw:
        raise HeldFeedError(f"{path.name} carries zero holdings - refusing to read "
                            f"that as 'owns nothing'")
    tickers = {}
    for h in held_raw:
        t = str((h or {}).get("ticker") or "").strip().upper() if isinstance(h, dict) else ""
        if not t:
            raise HeldFeedError(f"{path.name} has a holding with no ticker: {h!r}")
        shares = h.get("shares")
        if (isinstance(shares, bool) or not isinstance(shares, (int, float))
                or not math.isfinite(shares) or shares <= 0):
            # The publisher drops non-positive positions; one here is corruption,
            # and counting it would label a name owned.
            raise HeldFeedError(f"{path.name}: holding {t} has no positive share "
                                f"count ({shares!r})")
        names = h.get("brokers") or []
        if not isinstance(names, list):
            names = [names]
        names = {str(n).strip() for n in names if str(n).strip()}
        unknown = names - set(brokers)
        if not names or unknown:
            raise HeldFeedError(f"{path.name}: holding {t} is attributed to "
                                f"{sorted(unknown) or 'no broker'}, which has no as_of row")
        merged = set(tickers.get(t, ())) | names
        tickers[t] = tuple(sorted(merged))

    today = today or today_et()
    # A FUTURE declared date is not a usable as_of: drop it here, at the source, so no
    # renderer (e.g. a page title) can publish it (Codex #549 notion r2).
    brokers = {b: (d if d is None or d <= today else None) for b, d in brokers.items()}
    return HeldBook(path=path, generated_at=str(doc.get("generated_at") or ""),
                    brokers=brokers, tickers=tickers, today=today)


# =============================================================================================
# THE OWNERSHIP RULE - join the book to Coverage Manager's spellings (board #549, 2026-10-15).
# One copy for every lane. Before this, 13 lanes each hand-copied it and the copies had
# drifted (see portfolio_daily/docs/held_feed_canonical_plan.md). A lane passes in what it
# knows (CM's portfolio names, every CM name, where CM's alias map is) and maps the result
# onto its own structure; it never re-implements the join or the guards.
# =============================================================================================

#: Same bound as CM `universe/held.py: MAX_DEMOTIONS_PER_RUN`. A real day sells one or two
#: names; more at once means the feed is present, valid and WRONG.
MAX_SOLD_PER_RUN = 5


class AliasMapError(ValueError):
    """CM's ticker_aliases.json cannot be trusted for the join."""


def _ascii(text) -> str:
    return str(text).encode("ascii", "replace").decode("ascii")


def _no_duplicate_keys(pairs):
    """json object_pairs_hook: a key given twice is refused - `json.loads` would otherwise
    keep the last one silently and re-point the join (Codex #549 focus_today r2)."""
    out: dict = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate key {k!r}")
        out[k] = v
    return out


def load_alias_map(path, known) -> dict:
    """CM's `alias_to_canonical` (broker symbol -> CM ticker), upper-cased. Raises
    AliasMapError on anything that could re-point the join:
      * unreadable / not JSON / a duplicate key / no `alias_to_canonical` object;
      * a blank or non-string entry;
      * two keys that collide after normalising (" fisv " and "FISV");
      * an alias that is itself a CM ticker (`known`), or that chains (A->B, B->C): with
        both covered it would merge two companies."""
    path = Path(path)
    try:
        doc = json.loads(path.read_text(encoding="utf-8-sig"),
                         object_pairs_hook=_no_duplicate_keys)
    except (OSError, ValueError, RecursionError) as exc:
        raise AliasMapError(str(exc)) from exc
    amap = doc.get("alias_to_canonical") if isinstance(doc, dict) else None
    if not isinstance(amap, dict):
        raise AliasMapError("alias_to_canonical is not an object")
    norm: dict = {}
    for k, v in amap.items():
        if not (isinstance(v, str) and str(k).strip() and v.strip()):
            raise AliasMapError(f"malformed entry for {k!r}")
        key, val = str(k).strip().upper(), v.strip().upper()
        if norm.setdefault(key, val) != val:
            raise AliasMapError(f"{key} maps to both {norm[key]} and {val}")
    up = {str(t).strip().upper() for t in known}
    merges = sorted(k for k in norm if k in up or k in norm.values())
    if merges:
        raise AliasMapError(f"aliases that are themselves CM tickers or chain: "
                            f"{merges[:5]}")
    return norm


def cm_identity(exports_dir) -> set:
    """Every ticker Coverage Manager covers - the keys of `universe_metadata.json` (flat, or
    under "tickers"), upper-cased; empty if unreadable. `join_ownership` always adds this to
    the caller's `known`, so every lane judges "is this held symbol a CM name" against the
    SAME identity set. Two lanes used their state files only, so a buy of a covered name
    filed in no state list read as a rename and froze every sale (Fable #549 canonical H1)."""
    try:
        d = json.loads((Path(exports_dir) / "universe_metadata.json")
                       .read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, RecursionError):
        return set()
    if isinstance(d, dict) and isinstance(d.get("tickers"), dict):
        d = d["tickers"]
    return {str(t).strip().upper() for t in d if str(t).strip()} if isinstance(d, dict) else set()


def open_book(path=None, *, repo_root=None, today: date | None = None):
    """(book or None, warnings, notes). Never raises on bad data.
    * book None + one warning: held.json cannot be trusted; the caller keeps CM's copy.
    * a warning when the PUBLISHER is stale (the process is not running);
    * a NOTE per stale broker - print it, do not alarm (JP 2026-10-07, "just note it":
      portfolio_daily's own heartbeat already alarms on it)."""
    try:
        book = load_held(path, repo_root=repo_root, today=today)
    except HeldFeedError as exc:
        return None, [f"ownership feed unavailable ({_ascii(exc)}) - Coverage Manager's "
                      f"copy stands, which lags the brokers"], []
    warnings, notes = [], []
    if book.publisher_stale():
        age = book.publisher_age_days()
        warnings.append(f"ownership: held.json was last regenerated "
                        f"{'at an unknown time' if age is None else f'{age}d ago'} "
                        f"({book.generated_at or 'no generated_at'}) - portfolio_daily's "
                        f"daily export is not running, so ownership may be out of date")
    for b in book.stale_brokers():
        age = book.age_days(b)
        notes.append(f"held.json {b} positions are "
                     f"{'of unknown age' if age is None else f'{age}d old'} - a sale or "
                     f"purchase there since then is not reflected here or in CM's copy")
    return book, warnings, notes


@dataclass(frozen=True)
class OwnershipJoin:
    """What the brokers' book says about Coverage Manager's names. A lane applies it as:
        if j.source == "cm" or j.rejected:  change nothing (CM's copy stands)
        else:  every name in `promote` is owned;  every name in `demote` was sold.
    `promote`/`demote` hold ONLY names the caller passed, in the caller's spelling, so a lane
    keyed by its own rows can index with them. A held CM name the caller did not pass
    (known only from universe_metadata) is in `promote_uncovered`, upper-cased; a lane
    that can add rows opts in to it explicitly (Fable #549 canonical r2). `owned` is EVERY spelling of
    every held name, CM-covered or not: lanes that route on any held symbol (activist_13d,
    filing_blackline, transcript_highlights, screens_equity badges) read it - but only when
    `applied`; on `rejected` it is the feed judged wrong."""
    source: str                      # "held.json" | "cm" (feed unusable)
    owned: frozenset                 # UPPER: every spelling of every held name
    promote: frozenset               # caller spellings of CM names the brokers hold
    demote: frozenset                # caller spellings of CM portfolio names judged sold
    complete: bool                   # absence from held.json is trusted as "not owned"
    rejected: bool                   # >MAX_SOLD_PER_RUN leaving: feed judged wrong
    unjoined: tuple                  # held symbols matching no CM name
    warnings: tuple                  # why the result is not fully trusted (-> `partial`)
    notes: tuple                     # stale brokers: print only
    book: "HeldBook | None" = None
    aliases: "dict | None" = None    # the validated map, or None when unusable
    promote_uncovered: frozenset = frozenset()   # UPPER: held CM names not passed in

    @property
    def applied(self) -> bool:
        """True when the brokers' book was applied at all (promotions happen)."""
        return self.source == "held.json" and not self.rejected


def join_ownership(cm_portfolio, known, *, aliases_path, held_path=None, repo_root=None,
                   today: date | None = None, book=None, max_sold: int = MAX_SOLD_PER_RUN
                   ) -> OwnershipJoin:
    """THE rule. `cm_portfolio` = CM's own portfolio names; `known` = every CM name, both
    in the caller's spelling (matched case-insensitively). `known` is widened with
    `cm_identity(aliases_path.parent)` (CM's exports dir). Pass `book` to reuse one the
    caller already opened with `open_book`, whose warnings/notes the caller already has:
    then this emits none about the book. Otherwise it is opened here (`held_path`, else
    `default_path(repo_root)`). Guards, in this order - the first that fires decides:
      * held.json unusable -> source "cm", nothing to promote or demote, a warning;
      * more than `max_sold` CM portfolio names leaving at once -> the feed is judged
        WRONG: `rejected`, nothing to promote or demote, a warning - promotions included,
        because a feed wrong in one direction is no evidence in the other (Codex #549
        focus_today r2);
      * no usable alias map, or a held symbol matching no CM name -> promotions apply,
        no demotion (`complete` False), a warning: a renamed issuer's old CM row would
        otherwise read as sold, and without the join FISV reads as the sale of CM's FI.
    """
    warnings: list = []
    notes: list = []
    cm_portfolio = [str(t) for t in cm_portfolio]
    if book is None:
        book, w, notes = open_book(held_path, repo_root=repo_root, today=today)
        warnings.extend(w)
    if book is None:
        return OwnershipJoin(source="cm", owned=frozenset(), promote=frozenset(),
                             demote=frozenset(), complete=False, rejected=False,
                             unjoined=(), warnings=tuple(warnings), notes=tuple(notes))
    spelling: dict = {}
    for t in sorted(str(k) for k in known):
        spelling.setdefault(t.strip().upper(), t)
    for t in sorted(cm_portfolio):                   # the portfolio's own spelling wins
        spelling[t.strip().upper()] = t
    mine = set(spelling)                             # what the caller passed
    identity = cm_identity(Path(aliases_path).parent)
    if not identity:
        notes.append(f"{Path(aliases_path).parent / 'universe_metadata.json'} unreadable "
                     f"or empty - CM's identity set is this lane's rows only, so a held "
                     f"name CM covers elsewhere reads as unjoined")
    for t in sorted(identity):
        spelling.setdefault(t, t)
    up = set(spelling)
    try:
        aliases = load_alias_map(aliases_path, up)
    except AliasMapError as exc:
        aliases = None
        warnings.append(f"ownership: {Path(aliases_path).name} unusable ({_ascii(exc)}) - "
                        f"broker symbols cannot be joined to CM's, so no CM portfolio name "
                        f"is treated as sold this run")
    reverse: dict = {}
    for alias, canon in (aliases or {}).items():
        reverse.setdefault(canon, set()).add(alias)
    owned: set = set()
    unjoined: list = []
    for raw in book.tickers:
        t = raw.strip().upper()
        canon = (aliases or {}).get(t, t)
        spellings = {t, canon} | reverse.get(canon, set())
        owned |= spellings
        if not spellings & up:
            unjoined.append(t)
    sold = sorted(t for t in set(cm_portfolio) if t.strip().upper() not in owned)
    if len(sold) > max_sold:
        warnings.append(f"ownership: held.json would retire {len(sold)} CM portfolio names "
                        f"at once ({', '.join(sold)}), more than {max_sold} - the feed is "
                        f"probably wrong, so Coverage Manager's copy stands this run")
        return OwnershipJoin(source="held.json", owned=frozenset(owned),
                             promote=frozenset(), demote=frozenset(), complete=False,
                             rejected=True, unjoined=tuple(sorted(unjoined)),
                             warnings=tuple(warnings), notes=tuple(notes), book=book,
                             aliases=aliases)
    infer = aliases is not None
    if infer and unjoined:
        infer = False
        warnings.append(f"ownership: held.json name(s) {', '.join(sorted(unjoined))} match "
                        f"no Coverage Manager row (a rename or a buy CM does not cover) - "
                        f"no CM portfolio name is treated as sold this run")
    return OwnershipJoin(source="held.json", owned=frozenset(owned),
                         promote=frozenset(spelling[t] for t in owned & mine),
                         promote_uncovered=frozenset((owned & up) - mine),
                         demote=frozenset(sold) if infer else frozenset(),
                         complete=infer, rejected=False, unjoined=tuple(sorted(unjoined)),
                         warnings=tuple(warnings), notes=tuple(notes), book=book,
                         aliases=aliases)
