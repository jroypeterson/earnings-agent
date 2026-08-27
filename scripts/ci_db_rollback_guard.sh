#!/usr/bin/env bash
# Rollback guard + self-heal for the earnings-db artifact restore.
#
# WHY THIS EXISTS
# ---------------
# On 2026-08-24 the artifact restore began silently returning a snapshot from
# 2026-07-27. Every run then wrote to that snapshot and re-uploaded it as the newest
# `earnings-db`, so the database was rolled back ~28 days, repeatedly. 2026Q2 went
# from 958 events carrying eps_actual to 256 and from 962 `reported` to 41 - the
# season page showed 203 companies as "past due, no results" that had reported weeks
# earlier, and the Slack funnel said "41 of 1020 reported (4%)".
#
# Nothing caught it because the only guard was
#   if: hashFiles('earnings_events.db') == ''
# which tests PRESENCE and never CONTENT. A 28-day-old restore passes that, gets
# written to, and is promoted to newest. CI stayed green throughout: this is data
# destruction BETWEEN runs, not a job failure.
#
# WHAT IT MEASURES, AND WHY NOT THE OBVIOUS THING
# -----------------------------------------------
# The obvious metric is `MAX(events.updated_at)` - and it does NOT work. Measured on
# the live artifacts: the corrupt one reports a watermark of 2026-08-27, because each
# run restores the July data and then writes today's rows on top, advancing
# updated_at while the CONTENT stays 28 days poorer. A recency metric cannot see a
# rollback that is still being written to. This was caught by testing the guard
# against the real artifact rather than trusting the design.
#
# So the trigger is a SELF-CONTAINED symptom instead - the same thing JP noticed by
# eye: tracked events whose date has passed and which still carry no actuals.
# Measured on the two real databases:
#
#     clean (2026-08-24 artifact)   events=2912  actuals=1680  past-due-no-actuals=  3
#     rolled back (live)            events=1735  actuals= 763  past-due-no-actuals=214
#
# Three versus two hundred and fourteen. It needs no history, no repo variable, and
# no stored high-water mark, and it is the symptom a human would report.
#
# Self-heal then ranks candidate artifacts by ACCUMULATED ACTUALS, never by upload
# time - upload time is exactly what lied.
#
# EA_DB_RESTORE_ARTIFACT_ID pins the restore to one specific artifact: the recovery
# lever. Set it once via workflow_dispatch to climb back to a known-good snapshot,
# then leave it unset.
#
# Threshold shape is borrowed from this repo's own `detect_coverage_collapse`: a
# proportional test AND an absolute one must both trip, so a small database cannot
# fire on a percentage and a large one cannot fire on a raw count.
#
# Requires: gh, a python with sqlite3, GH_TOKEN with actions:read.

set -uo pipefail

DB="${EA_DB_PATH:-earnings_events.db}"
MAX_OVERDUE="${EA_DB_MAX_OVERDUE:-40}"        # healthy measured 3; broken measured 214
OVERDUE_GRACE_DAYS="${EA_DB_OVERDUE_GRACE_DAYS:-7}"
MIN_ACTUALS_GAIN="${EA_DB_MIN_ACTUALS_GAIN:-50}"
SCAN_LIMIT="${EA_DB_SCAN_LIMIT:-12}"
REPO="${GITHUB_REPOSITORY:-jroypeterson/earnings-agent}"
PINNED="${EA_DB_RESTORE_ARTIFACT_ID:-}"

log() { echo "[db-rollback-guard] $*"; }

# Each candidate is EXECUTED, not merely located. On Windows `command -v python3`
# succeeds against the Microsoft Store stub, which is on PATH, is not an interpreter,
# and exits 49 telling you to install Python. Testing for presence and inferring
# validity is the exact mistake this script exists to correct.
PY_BIN=""
for cand in python3 python py; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import sqlite3" >/dev/null 2>&1; then
    PY_BIN="$cand"; break
  fi
done
if [ -z "$PY_BIN" ]; then
  echo "[db-rollback-guard] no working python interpreter (with sqlite3) on PATH" >&2
  exit 1
fi

# "<total_events> <actuals> <past_due_without_actuals>", or empty if unreadable.
db_stats() {
  "$PY_BIN" - "$1" "$OVERDUE_GRACE_DAYS" <<'PY' 2>/dev/null
import sqlite3, sys, datetime
try:
    con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    cut = (datetime.date.today() - datetime.timedelta(days=int(sys.argv[2]))).isoformat()
    q = lambda s, *a: con.execute(s, a).fetchone()[0]
    total = q("SELECT COUNT(*) FROM events")
    actuals = q("SELECT COUNT(*) FROM events WHERE eps_actual IS NOT NULL")

    # The predicate is built from the columns this database ACTUALLY has. The whole
    # point of the guard is to inspect OLD snapshots, and old snapshots have old
    # schemas: closed_reason arrived in v13 (2026-08-07), so the 2026-07-27 artifact
    # this guard exists to catch does not have it. Referencing it unconditionally
    # made the query raise, which the guard reported as "unreadable or corrupt" - a
    # true-sounding alarm for the wrong reason, on the exact input it was written for.
    cols = {r[1] for r in con.execute("PRAGMA table_info(events)")}
    where = ["event_date < ?", "COALESCE(reported, 0) = 0", "eps_actual IS NULL"]
    if "tier" in cols:
        where.append("tier <= 2")
    if "closed_reason" in cols:
        where.append("closed_reason IS NULL")
    overdue = q(f"SELECT COUNT(*) FROM events WHERE {' AND '.join(where)}", cut)

    if total == 0:
        sys.exit(1)
    print(total, actuals, overdue)
except Exception as exc:
    print(f"db_stats failed: {type(exc).__name__}: {exc}", file=sys.stderr)
    sys.exit(1)
PY
}

alarm() {
  local msg="$1"
  local hook="${WEBHOOK_STATUS:-${WEBHOOK_EARNINGS:-}}"
  local run_url="${GITHUB_SERVER_URL:-https://github.com}/${REPO}/actions/runs/${GITHUB_RUN_ID:-0}"
  log "ALARM: $msg"
  if [ -n "$hook" ]; then
    "$PY_BIN" - "$hook" "$msg" "$run_url" <<'PY' || true
import json, sys, urllib.request
hook, msg, run_url = sys.argv[1], sys.argv[2], sys.argv[3]
text = (":rotating_light: *earnings-db rollback guard tripped* - refusing to write to a "
        f"database that looks rolled back.\n{msg}\n<{run_url}|run details>")
req = urllib.request.Request(hook, data=json.dumps({"text": text}).encode(),
                             headers={"Content-Type": "application/json"})
try:
    urllib.request.urlopen(req, timeout=15).read()
except Exception:
    pass
PY
  fi
}

fetch_artifact() {
  local aid="$1" dest="$2"
  rm -rf "$dest" && mkdir -p "$dest" || return 1
  gh api "repos/${REPO}/actions/artifacts/${aid}/zip" > "${dest}/a.zip" 2>/dev/null || return 1
  "$PY_BIN" -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" \
    "${dest}/a.zip" "$dest" 2>/dev/null || return 1
  [ -f "${dest}/earnings_events.db" ]
}

if [ ! -f "$DB" ]; then
  log "no $DB on disk - bootstrap or a genuinely missing artifact; the existing"
  log "abort-if-missing step owns that case. Nothing to check."
  exit 0
fi

read -r cur_total cur_actuals cur_overdue <<<"$(db_stats "$DB")"
if [ -z "${cur_total:-}" ]; then
  alarm "The restored \`earnings_events.db\` is unreadable or has no events - it may be truncated or corrupt."
  exit 1
fi
log "restored DB: events=$cur_total actuals=$cur_actuals past-due-without-actuals=$cur_overdue"

# ---- recovery lever ---------------------------------------------------------------
if [ -n "$PINNED" ]; then
  log "EA_DB_RESTORE_ARTIFACT_ID=$PINNED - pinning the restore to that artifact."
  if ! fetch_artifact "$PINNED" ".db_pinned"; then
    alarm "Pinned artifact \`$PINNED\` could not be downloaded."; exit 1
  fi
  read -r p_total p_actuals p_overdue <<<"$(db_stats ".db_pinned/earnings_events.db")"
  if [ -z "${p_total:-}" ]; then
    alarm "Pinned artifact \`$PINNED\` is unreadable."; exit 1
  fi
  log "pinned artifact: events=$p_total actuals=$p_actuals past-due-without-actuals=$p_overdue"
  cp ".db_pinned/earnings_events.db" "$DB" || exit 1
  log "restored from pinned artifact $PINNED."
  exit 0
fi

# ---- the trigger ------------------------------------------------------------------
if [ "$cur_overdue" -le "$MAX_OVERDUE" ]; then
  log "healthy (past-due-without-actuals $cur_overdue <= $MAX_OVERDUE) - no artifact scan."
  exit 0
fi

log "SUSPECT: $cur_overdue tracked events are past due with no actuals (threshold $MAX_OVERDUE)."
log "Scanning up to $SCAN_LIMIT artifacts, ranking by ACCUMULATED ACTUALS not upload time."

mapfile -t ids < <(gh api "repos/${REPO}/actions/artifacts?name=earnings-db&per_page=${SCAN_LIMIT}" \
  --jq '.artifacts[] | select(.expired==false) | .id' 2>/dev/null)

if [ "${#ids[@]}" -eq 0 ]; then
  alarm "Database looks rolled back ($cur_overdue past-due events with no actuals) and no unexpired \`earnings-db\` artifacts could be listed."
  exit 1
fi

best_actuals="$cur_actuals"; best_id=""; best_overdue="$cur_overdue"
for aid in "${ids[@]}"; do
  fetch_artifact "$aid" ".db_cand" || { log "  artifact $aid: download failed"; continue; }
  read -r a_total a_actuals a_overdue <<<"$(db_stats ".db_cand/earnings_events.db")"
  if [ -z "${a_total:-}" ]; then log "  artifact $aid: unreadable"; continue; fi
  log "  artifact $aid: events=$a_total actuals=$a_actuals past-due-without-actuals=$a_overdue"
  if [ "$a_actuals" -gt "$(( best_actuals + MIN_ACTUALS_GAIN ))" ]; then
    best_actuals="$a_actuals"; best_id="$aid"; best_overdue="$a_overdue"
    cp ".db_cand/earnings_events.db" ".db_best" || true
  fi
done

if [ -n "$best_id" ] && [ -f ".db_best" ]; then
  cp ".db_best" "$DB" || exit 1
  log "SELF-HEALED from artifact $best_id: actuals $cur_actuals -> $best_actuals, past-due $cur_overdue -> $best_overdue."
fi

if [ "$best_overdue" -gt "$MAX_OVERDUE" ]; then
  alarm "Every available \`earnings-db\` artifact looks rolled back - best has $best_actuals actuals and still $best_overdue tracked events past due with no actuals (threshold $MAX_OVERDUE). Refusing to write. Recover by dispatching with \`EA_DB_RESTORE_ARTIFACT_ID\` set to a known-good artifact id."
  exit 1
fi

exit 0
