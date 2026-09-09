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
PAGE="${EA_DB_ARTIFACT_PAGE:-50}"
TRIES="${EA_DB_RESTORE_TRIES:-3}"
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

RAW=""
LIST_OK=0
for attempt in $(seq 1 "$TRIES"); do
  if RAW="$(gh api "repos/${REPO}/actions/artifacts?name=${NAME}&per_page=${PAGE}" \
      --jq "$PROJECTION" 2>/dev/null)"; then
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
  "$PY_BIN" - "${WORK}/${DB}" <<'PY' || return 1
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
if con.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
    raise SystemExit("quick_check failed")
if not con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='events'").fetchone():
    raise SystemExit("no events table")
if con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0:
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

STATS="$("$PY_BIN" - "$DB" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
q = lambda s: con.execute(s).fetchone()[0]
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
