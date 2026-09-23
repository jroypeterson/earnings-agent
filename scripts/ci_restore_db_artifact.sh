#!/usr/bin/env bash
# Deterministic restore of the `earnings-db` SQLite artifact.
#
# WHY THIS REPLACED dawidd6/action-download-artifact
# --------------------------------------------------
# The downloader selected an artifact by ENUMERATING RUNS (search_artifacts +
# workflow_search + workflow_conclusion=success) and taking the first run whose
# artifacts included `earnings-db`. That enumeration is a heuristic over a paged,
# cross-workflow run list, and it does not reliably return the newest artifact.
#
# MEASURED 2026-09-08 over the twelve most recent runs of the four workflows that
# restore this database: TWO restored a stale artifact.
#
#   reconcile_calendar run 34259752925  started 2026-09-08T17:53:07Z
#     picked    artifact 9959117413   created 2026-09-05T00:22:41Z
#     newest    artifact 10062609223  created 2026-09-08T15:06:39Z   <- skipped
#
# That run then re-uploaded what it had restored as artifact 10069391408, so a
# database 3.6 days old became "the newest earnings-db" and the next daily run
# restored it in good faith. Measured content loss against the artifact that was
# skipped: events 2987 -> 2980, eps_actual 1744 -> 1740, reported 1752 -> 1748,
# kv_store 70 -> 69. This is the same rollback mechanism that cost 28 days in
# August 2026 (see scripts/ci_db_rollback_guard.sh), still live, just smaller.
#
# It stayed invisible because the rollback guard's trigger -- tracked events past
# due with no actuals -- is SEASON DEPENDENT. On 2026-09-08, between reporting
# seasons, the restored rollback measured 2 past-due events against a threshold of
# 40, so the guard correctly reported "healthy" on a database that had just lost
# four days. The guard is not wrong; it cannot see this off-season. The staleness
# assertion in ci_db_rollback_guard.sh was added alongside this script to cover it.
#
# WHAT THIS DOES INSTEAD
# ----------------------
# Ask the artifacts API for the artifacts named `earnings-db` and take the newest
# unexpired one, by created_at, explicitly sorted here rather than trusting the
# API's (undocumented) ordering. One list call, one download by id. There is no
# run enumeration and no heuristic, so there is nothing to select wrongly.
#
# Selection is by UPLOAD TIME, deliberately -- CONTENT is the rollback guard's job,
# and it ranks candidates by accumulated actuals precisely because upload time can
# lie. Two layers, two different signals: this script guarantees "newest", the
# guard adjudicates "good". Collapsing them into one content-ranked restore would
# make the normal path pay a multi-artifact download every run.
#
# Branch: every workflow gates its upload on `github.ref == refs/heads/main`, so a
# feature branch cannot publish this artifact at all. The head_branch filter below
# is belt-and-braces against that gate being removed, not the primary defence.
#
# Run conclusion is deliberately NOT filtered. season_progress.yml uploads under
# `if: always()`, so a run that failed a later step still published a valid, fully
# written database; excluding it would discard good state to honour a status field
# that says nothing about the file.
#
# EA_DB_RESTORE_ARTIFACT_ID (the recovery lever) is intentionally not handled here.
# ci_db_rollback_guard.sh already honours it unconditionally and runs immediately
# after this script, so the pin still wins. One owner for that behaviour.
#
# Contract with the workflow:
#   - exit 0 having written ./earnings_events.db            -> restored
#   - exit 0 having written NOTHING, when no artifact exists -> bootstrap; the
#     "Abort if DB artifact missing post-bootstrap" step owns that case, exactly as
#     it did under `if_no_artifact_found: warn`.
#   - exit 1 -> the artifact exists but could not be retrieved intact. Loud, never
#     silent: a restore that half-worked must not become a fresh-DB run.
#
# Requires: gh with actions:read, python with sqlite3 + zipfile.

set -uo pipefail

REPO="${GITHUB_REPOSITORY:-jroypeterson/earnings-agent}"
NAME="${EA_DB_ARTIFACT_NAME:-earnings-db}"
DB="${EA_DB_PATH:-earnings_events.db}"
BRANCH="${EA_DB_ARTIFACT_BRANCH:-main}"
PAGE="${EA_DB_ARTIFACT_PAGE:-100}"
# The GitHub artifacts API CLAMPS per_page at 100 and says nothing about
# it. That matters here because the walk stops on `rows < PAGE`: with
# PAGE=200 the API returns 100 rows, 100 < 200 reads as a SHORT page, and
# a truncated listing is declared COMPLETE after one request. Live-probed
# 2026-09-23 (per_page=200 -> 100 rows). Clamped rather than refused:
# clamping restores the comparison's meaning, while exiting here would
# add a new way for one mistyped variable to stop four workflows.
if [ "$PAGE" -gt 100 ]; then
  # `log` is defined further down, so this one echoes directly rather than
  # silently vanishing as a "command not found" on stderr.
  echo "[db-restore] per_page ${PAGE} exceeds the API maximum; clamping to 100"
  PAGE=100
fi
TRIES="${EA_DB_RESTORE_TRIES:-3}"
# The table whose presence proves the file is what it claims to be. Default
# `events` (the earnings DB, which must also be non-empty). The board #298
# `consensus-snapshots` side artifact passes `consensus_snapshot`: existence
# only, since a first-season export can legitimately hold zero rows.
VERIFY_TABLE="${EA_DB_VERIFY_TABLE:-events}"
WORK=".db_restore"

log() { echo "[db-restore] $*"; }

PY_BIN=""
for cand in python3 python py; do
  # Execute the candidate rather than trusting `command -v`: on Windows the
  # Microsoft Store python3 stub is on PATH, is not an interpreter, and exits 49.
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import sqlite3,zipfile" >/dev/null 2>&1; then
    PY_BIN="$cand"; break
  fi
done
if [ -z "$PY_BIN" ]; then
  echo "[db-restore] no working python interpreter (sqlite3 + zipfile) on PATH" >&2
  exit 1
fi

# ---- select ------------------------------------------------------------------
# jq does a FLAT PROJECTION only; the filtering and the sort happen below in the
# shell. Two reasons, both learned here:
#
#   1. Testability. The sort is the whole point of this script, and a jq program
#      inside `gh api` can only be exercised with a real `gh` and a real `jq`. The
#      first version of the test suite skipped its five selection tests silently
#      because jq is not on PATH on this machine -- a check that matches nothing,
#      reporting green. With the logic in the shell, a stubbed `gh` that prints
#      canned lines exercises the real code path.
#   2. `gh api` has no --arg/--argjson, so any value interpolated into a jq program
#      is string-spliced. --raw-field looks like the answer and is not: it turns the
#      call into a POST with a body field, and returned `{"message":"Not Found"}` for
#      every request when tried against the live API on 2026-09-08.
#
# Field order is fixed here and consumed positionally below: id created expired branch run.
PROJECTION='.artifacts[] | "\(.id) \(.created_at) \(.expired) \(.workflow_run.head_branch) \(.workflow_run.id)"'

# PAGINATION. The repository artifacts endpoint is paged, and the filter below
# (unexpired AND on BRANCH) runs AFTER the fetch -- so a page consisting entirely
# of expired or other-branch rows yields zero candidates while the artifact we
# want sits on the next page. One request could therefore miss the newest
# artifact entirely, which is precisely what the comment above claims cannot
# happen ("deterministic ... nothing to select wrongly"). That claim was true of
# the selection LOGIC and false of its INPUT.
#
# ⛑ The stop condition is the whole subtlety: it must be "the RAW page came back
# shorter than per_page" (i.e. the API has no more rows), NOT "this page yielded
# no candidates". The second one stops at page 1 in exactly the scenario this
# fixes -- an all-expired first page -- so it would reproduce the defect while
# looking like the fix. Filtering is done once, after paging, for the same reason.
# SIZE THE WALK BY total_count; do not guess a prefix (round 10, H5).
#
# The old cap was 4 pages x 50 = 200 rows. Measured 2026-09-23 the
# earnings-db listing holds 825 artifacts -- EXPIRED ROWS NEVER LEAVE IT,
# so it only grows (~7/day). The walk was therefore truncated on every
# run, and the review's remedy ("fail when the final page is full at the
# cap") would have failed 100% of runs across all four restoring
# workflows, none of which has continue-on-error on its restore step. A
# guard that becomes the outage.
#
# Sizing by total_count drops the guesswork AND the ordering assumption
# for a cost already being spent: 825/100 = 9 requests, +1 per ~14 days,
# against a 1,000/hour GITHUB_TOKEN budget at 28 runs/day. MAX_PAGES is
# now only a sanity bound (~2 years of growth), so reaching it is a real
# anomaly and can fail loudly. Selecting from a truncated prefix was
# considered and rejected: it would contradict this script's own header,
# which sorts "rather than trusting the API's (undocumented) ordering".
MAX_PAGES="${EA_DB_ARTIFACT_MAX_PAGES:-50}"
RAW=""
LIST_OK=0
for attempt in $(seq 1 "$TRIES"); do
  # Refetched EVERY attempt (Codex round 12). Read once, a retry would
  # size and validate its walk against a snapshot of the world taken
  # before the failure it is retrying.
  # A SEPARATE call, not an extra line spliced into the row projection:
  # that stream is consumed positionally (NF==5) and its non-empty lines
  # are counted to detect a short page, so a count line would corrupt both.
  TOTAL_COUNT="$(gh api \
    "repos/${REPO}/actions/artifacts?name=${NAME}&per_page=1" \
    --jq '.total_count' 2>/dev/null || true)"
  case "$TOTAL_COUNT" in
    ''|*[!0-9]*) TOTAL_COUNT='' ;;
  esac
  if [ -n "$TOTAL_COUNT" ]; then
    NEED_PAGES=$(( (TOTAL_COUNT + PAGE - 1) / PAGE ))
    [ "$NEED_PAGES" -lt 1 ] && NEED_PAGES=1
    log "artifact listing: ${TOTAL_COUNT} total, walking ${NEED_PAGES} page(s) of ${PAGE}"
  else
    NEED_PAGES="$MAX_PAGES"
    log "artifact listing: total_count unavailable; walking up to ${MAX_PAGES} page(s)"
  fi
  WALK_PAGES="$NEED_PAGES"
  [ "$WALK_PAGES" -gt "$MAX_PAGES" ] && WALK_PAGES="$MAX_PAGES"
  RAW=""
  ATTEMPT_OK=1
  page=1
  while [ "$page" -le "$WALK_PAGES" ]; do
    if ! PAGE_RAW="$(gh api \
        "repos/${REPO}/actions/artifacts?name=${NAME}&per_page=${PAGE}&page=${page}" \
        --jq "$PROJECTION" 2>/dev/null)"; then
      ATTEMPT_OK=0
      break
    fi
    [ -n "$PAGE_RAW" ] && RAW="${RAW}${RAW:+$'\n'}${PAGE_RAW}"
    # A short (or empty) RAW page means the API has nothing further. Count the
    # raw rows, never the surviving candidates.
    rows="$(printf '%s\n' "$PAGE_RAW" | awk 'NF' | wc -l | tr -d ' ')"
    [ "$rows" -lt "$PAGE" ] && break
    page=$((page + 1))
  done
  if [ "$ATTEMPT_OK" -eq 1 ]; then
    # OFFSET PAGINATION IS MUTABLE, and the listing is written by four
    # workflows. An artifact inserted at the front between page N and
    # page N+1 shifts every later row down: a row already seen comes back
    # (harmless on its own) and a row at the tail is NEVER FETCHED -- and
    # the tail is where the NEWEST artifact sits when ordering is not
    # newest-first. Reproduced round 12: pages [100,200] then [200,300]
    # against total_count=4 selected 300 while 900 (2026-09-10, the
    # newest) was never seen. The duplicate even keeps the candidate
    # count looking right, which is what hid it.
    #
    # Dedupe by id, then compare against total_count: fewer UNIQUE rows
    # than the API says exist means the listing moved under the walk. That
    # is a failed attempt, not a result -- retry with a fresh count.
    RAW="$(printf '%s\n' "$RAW" | awk 'NF && !seen[$1]++')"
    uniq_rows="$(printf '%s\n' "$RAW" | awk 'NF' | wc -l | tr -d ' ')"
    # Only when the walk was NOT deliberately truncated. A capped walk
    # legitimately holds fewer rows than total_count, and calling that a
    # shift would retry three times and then report the wrong cause --
    # the truncation check below owns that case.
    if [ -n "$TOTAL_COUNT" ] && [ "$NEED_PAGES" -le "$MAX_PAGES" ] \
       && [ "$uniq_rows" -lt "$TOTAL_COUNT" ]; then
      log "listing shifted mid-walk (${uniq_rows} unique of ${TOTAL_COUNT}); re-walking"
      ATTEMPT_OK=0
    fi
  fi
  if [ "$ATTEMPT_OK" -eq 1 ]; then
    # TRUNCATED is not the same as "ended on a full page" (Codex r11).
    # The first version of this check asked whether the LAST PAGE WAS
    # FULL, which is true of ANY listing whose size is an exact multiple
    # of PAGE -- so a COMPLETE walk was refused whenever it finished
    # exactly at the cap. Reproduced: total_count=4, per_page=2,
    # MAX_PAGES=2 reads both pages, consumes the entire listing, and then
    # errored. Projected to reach production around 2028-05-12 at 5,000
    # artifacts: the guard written to prevent an outage carrying its own.
    #
    # When total_count is KNOWN, completeness is known -- the walk was cut
    # short only if more pages were NEEDED than the cap allows. The
    # full-final-page heuristic is only meaningful in the fallback where
    # no count was available.
    if { [ -n "$TOTAL_COUNT" ] && [ "$NEED_PAGES" -gt "$MAX_PAGES" ]; } \
       || { [ -z "$TOTAL_COUNT" ] && [ "$page" -gt "$WALK_PAGES" ] \
            && [ "${rows:-0}" -ge "$PAGE" ]; }; then
      echo "ERROR: artifact listing exceeded ${MAX_PAGES} pages of ${PAGE}." >&2
      echo "  total_count=${TOTAL_COUNT:-unknown}. Cannot establish the newest" >&2
      echo "  artifact from a truncated listing; refusing to guess." >&2
      exit 1
    fi
    LIST_OK=1
    break
  fi
  log "artifact listing failed (attempt ${attempt}/${TRIES}); retrying in $((attempt * 5))s"
  sleep "$((attempt * 5))"
done

# Filter, then sort by created_at descending. `sort -r` on an ISO-8601 Z timestamp
# is a correct reverse chronological sort because the format is fixed-width and
# lexicographically ordered; the field is emitted by the GitHub API, not by us.
# Sorting on the timestamp COLUMN, never on the id: artifact ids happen to increase
# with time today, and relying on that would be position-as-key.
LISTING="$(printf '%s\n' "$RAW" \
  | awk -v br="$BRANCH" 'NF==5 && $3=="false" && $4==br {print $2, $1, $5}' \
  | sort -r \
  | awk '{print $2, $1, $3}')"

# Distinguish "the API never answered" from "the API answered, and there are no
# artifacts". Conflating them is what turns a transient outage into a fresh-DB run
# that silently erases date locks, kv_store watermarks and Slack thread state.
if [ "$LIST_OK" -ne 1 ]; then
  echo "[db-restore] the artifacts API could not be reached after ${TRIES} attempts." >&2
  echo "  Refusing to continue: an unreachable API is NOT an absent artifact." >&2
  exit 1
fi

if [ -z "$LISTING" ]; then
  # "The listing was empty" and "the restore succeeded" must not be the same exit
  # code for a caller whose artifact is CUMULATIVE. For the main earnings-db the
  # abort-if-missing step owns this case (EA_DB_BOOTSTRAPPED), so exit 0 is right.
  # The consensus-snapshots side artifact had no such owner: an empty successful
  # listing produced no file, no merge, an export of only the current DB's table,
  # and an upload that became the newest side artifact -- silently resetting
  # cumulative history. Every alert predicate in the workflow reads
  # `steps.*.outcome`, which was `success`, so none of them could fire.
  #
  # One mechanism, not two: a non-zero exit here is sufficient, because the
  # persist step already gates on `steps.restore_snapshots.outcome == 'success'`
  # and both alert steps key off the same outcome. Do NOT add a second refusal in
  # the yml to match.
  if [ "${EA_DB_REQUIRE_ARTIFACT:-}" = "true" ]; then
    echo "[db-restore] no unexpired '${NAME}' artifact on ${BRANCH}, and" >&2
    echo "  EA_DB_REQUIRE_ARTIFACT=true declares this artifact MANDATORY." >&2
    echo "  Refusing to continue: for a cumulative artifact, 'nothing to restore'" >&2
    echo "  is indistinguishable from 'the history was lost' and must not pass as" >&2
    echo "  a successful restore." >&2
    exit 1
  fi
  log "no unexpired '${NAME}' artifact on ${BRANCH} - treating as bootstrap."
  log "the abort-if-missing step owns this case; not writing ${DB}."
  exit 0
fi

log "candidates (newest first):"
printf '%s\n' "$LISTING" | head -3 | while read -r i c r; do
  log "  artifact $i created $c (run $r)"
done

read -r AID CREATED RUN_ID <<<"$(printf '%s\n' "$LISTING" | head -1)"
TOTAL=$(printf '%s\n' "$LISTING" | wc -l)
log "selected artifact ${AID}, created ${CREATED}, from run ${RUN_ID} (${TOTAL} candidates)."

# ---- download ----------------------------------------------------------------
fetch() {
  rm -rf "$WORK" && mkdir -p "$WORK" || return 1
  gh api "repos/${REPO}/actions/artifacts/${AID}/zip" > "${WORK}/a.zip" 2>/dev/null || return 1
  "$PY_BIN" -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" \
    "${WORK}/a.zip" "$WORK" 2>/dev/null || return 1
  [ -f "${WORK}/${DB}" ] || return 1
  # Verify BEFORE overwriting the working copy. A truncated download that still
  # unzips would otherwise be promoted to the live database and then re-uploaded
  # as newest -- the failure this whole file exists to stop.
  "$PY_BIN" - "${WORK}/${DB}" "$VERIFY_TABLE" <<'PY' || return 1
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
table = sys.argv[2]
if con.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
    raise SystemExit("quick_check failed")
if not con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
    raise SystemExit(f"no {table} table")
if table == "events" and con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0:
    raise SystemExit("events table is empty")
PY
}

OK=0
for attempt in $(seq 1 "$TRIES"); do
  if fetch; then OK=1; break; fi
  log "download/verify of artifact ${AID} failed (attempt ${attempt}/${TRIES}); retrying in $((attempt * 5))s"
  sleep "$((attempt * 5))"
done

if [ "$OK" -ne 1 ]; then
  echo "[db-restore] artifact ${AID} (created ${CREATED}) exists but could not be" >&2
  echo "  downloaded and verified after ${TRIES} attempts. Refusing to proceed with" >&2
  echo "  no database rather than starting a fresh-DB run." >&2
  rm -rf "$WORK"
  exit 1
fi

cp "${WORK}/${DB}" "$DB" || exit 1
rm -rf "$WORK"

STATS="$("$PY_BIN" - "$DB" "$VERIFY_TABLE" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
q = lambda s: con.execute(s).fetchone()[0]
if sys.argv[2] != "events":
    print(f"{sys.argv[2]}={q(f'SELECT COUNT(*) FROM {sys.argv[2]}')}")
    raise SystemExit(0)
tabs = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
kv = q("SELECT COUNT(*) FROM kv_store") if "kv_store" in tabs else 0
print(f"events={q('SELECT COUNT(*) FROM events')} "
      f"actuals={q('SELECT COUNT(*) FROM events WHERE eps_actual IS NOT NULL')} "
      f"kv_store={kv} watermark={q('SELECT MAX(updated_at) FROM events')}")
PY
)"
log "restored ${DB} from artifact ${AID}: ${STATS}"

# Emit the selection so a future rollback is reconstructable from the run log
# alone. The August incident cost three days largely because nothing in the log
# said WHICH artifact had been restored.
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "artifact_id=${AID}"
    echo "artifact_created=${CREATED}"
    echo "source_run_id=${RUN_ID}"
  } >> "$GITHUB_OUTPUT"
fi
exit 0
