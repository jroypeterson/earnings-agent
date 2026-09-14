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
# A SECOND TRIGGER ON THE SAME QUANTITY runs on every invocation: if any recent
# artifact holds MORE accumulated actuals than the database we are about to write to,
# the restore went backwards. That covers the off-season case the overdue metric is
# blind to, and it replaces a wall-clock staleness check that failed the build and
# latched the pipeline down for three days in September 2026. The long note above that
# section is the record; do not reintroduce a time-based fail here.
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

# "<total_events> <actuals> <past_due_without_actuals> <kv_rows>", or empty if the
# file fails an integrity check, lacks an events table, or is otherwise unreadable.
db_stats() {
  "$PY_BIN" - "$1" "$OVERDUE_GRACE_DAYS" <<'PY' 2>/dev/null
import sqlite3, sys, datetime
try:
    con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    cut = (datetime.date.today() - datetime.timedelta(days=int(sys.argv[2]))).isoformat()
    q = lambda s, *a: con.execute(s, a).fetchone()[0]

    # Integrity BEFORE counting. Three successful queries against `events` say nothing
    # about the rest of the file, and this script's output decides whether a candidate
    # gets to overwrite the working copy. A database with an intact events B-tree and a
    # corrupt kv_store page would rank fine here and then take the watermarks down with
    # it. quick_check is a page-level scan and costs ~0.1s on a 10MB file.
    if q("PRAGMA quick_check(1)") != "ok":
        raise RuntimeError("PRAGMA quick_check failed")

    tables = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if "events" not in tables:
        raise RuntimeError("no events table")

    total = q("SELECT COUNT(*) FROM events")
    actuals = q("SELECT COUNT(*) FROM events WHERE eps_actual IS NOT NULL")

    # kv_store carries the settle watermarks, announced-set and dedup keys - durable
    # state that no re-run reconstructs. It is reported so the caller can refuse to
    # trade it away, and probed rather than assumed present: it arrived in schema v9.
    kv = q("SELECT COUNT(*) FROM kv_store") if "kv_store" in tables else 0

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
    print(total, actuals, overdue, kv)
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

read -r cur_total cur_actuals cur_overdue cur_kv <<<"$(db_stats "$DB")"
if [ -z "${cur_total:-}" ]; then
  alarm "The restored \`earnings_events.db\` failed its integrity check, has no events table, or is unreadable - it may be truncated or corrupt."
  exit 1
fi
log "restored DB: events=$cur_total actuals=$cur_actuals past-due-without-actuals=$cur_overdue kv_store=$cur_kv"

# ---- recovery lever ---------------------------------------------------------------
if [ -n "$PINNED" ]; then
  log "EA_DB_RESTORE_ARTIFACT_ID=$PINNED - pinning the restore to that artifact."
  if ! fetch_artifact "$PINNED" ".db_pinned"; then
    alarm "Pinned artifact \`$PINNED\` could not be downloaded."; exit 1
  fi
  read -r p_total p_actuals p_overdue p_kv <<<"$(db_stats ".db_pinned/earnings_events.db")"
  if [ -z "${p_total:-}" ]; then
    alarm "Pinned artifact \`$PINNED\` is unreadable."; exit 1
  fi
  log "pinned artifact: events=$p_total actuals=$p_actuals past-due-without-actuals=$p_overdue kv_store=$p_kv"
  cp ".db_pinned/earnings_events.db" "$DB" || exit 1
  log "restored from pinned artifact $PINNED."
  exit 0
fi

# ---- content age: REPORTED, never fatal -------------------------------------------
#
# ⛑ THIS CHECK USED TO FAIL THE BUILD, AND THAT IS WHAT TOOK THE PIPELINE DOWN FOR
# THREE DAYS (2026-09-11 15:00 -> 2026-09-14). Read this before restoring it.
#
# The original trigger was `now - MAX(events.updated_at) > 24h => exit 1`, on the
# reasoning that "a gap above 24h with no rollback means no run has written in over a
# day, which is itself an outage worth failing on". Two things were wrong with it, and
# the second is the serious one.
#
# 1. IT MEASURES THE WRONG QUANTITY. `events.updated_at` advances when an EVENT
#    changes, not when a RUN writes. Between reporting seasons nothing changes, so the
#    watermark legitimately goes quiet while every run succeeds. Measured on the eight
#    most recent unexpired artifacts on 2026-09-14: all eight carried events=2996,
#    actuals=1757, past-due-without-actuals=2, kv_store=72 and the identical watermark
#    2026-09-11 15:00:28, across five DISTINCT sha256 digests. Different bytes, same
#    content -- runs were writing kv_store and page layout the whole time. There was no
#    rollback and no data loss anywhere in that window.
#
# 2. IT LATCHED. The guard runs BEFORE the agent writes, so once it tripped, nothing
#    could advance the watermark, so every later run read an OLDER one. The alarm ages
#    tracked wall-clock exactly -- 47.91h, 52.43h, 73.94h, 76.29h -- which is the
#    signature of a check that has become the condition it reports. The documented
#    recovery lever does not help either: every candidate artifact carried the same
#    watermark, so pinning to any of them re-trips the same check.
#
# Compared against the artifact's own `created_at` instead of `now` it would still
# latch, just more slowly -- off-season the gap between a re-uploaded artifact and its
# unchanged content grows without bound too. The fix is not a better clock. It is to
# stop asking a time question at all: see the regression check below, which asks the
# question the incidents were actually about.
#
# The age is still worth SEEING, so it is logged. It is not alarmed, because off-season
# it is true every day and this fleet has already learned what a permanently-true flag
# does to the reader. "No workflow has succeeded recently" is a real question with a
# real owner -- the Workflow Watchdog, which measures run history rather than row
# timestamps, and which correctly reported this outage while it was happening.
MAX_CONTENT_AGE_H="${EA_DB_MAX_CONTENT_AGE_HOURS:-24}"
content_age_h="$("$PY_BIN" - "$DB" <<'PY' 2>/dev/null
import sqlite3, sys, datetime
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
w = con.execute("SELECT MAX(updated_at) FROM events").fetchone()[0]
if not w:
    raise SystemExit(1)
# Stored naive UTC, matching how the agent writes it.
wd = datetime.datetime.strptime(str(w)[:19], "%Y-%m-%d %H:%M:%S")
print(f"{(datetime.datetime.utcnow() - wd).total_seconds() / 3600:.2f}")
PY
)"
if [ -z "${content_age_h:-}" ]; then
  # No watermark at all is not "fresh". Say so rather than skipping the check --
  # a check that silently matches nothing is the class this repo keeps meeting.
  alarm "The restored database has no readable \`MAX(events.updated_at)\` watermark, so its content age could not be established."
  exit 1
fi
log "content watermark age: ${content_age_h}h (advisory; limit ${MAX_CONTENT_AGE_H}h)"
if awk "BEGIN{exit !($content_age_h > $MAX_CONTENT_AGE_H)}"; then
  log "NOTE: no event row has changed in ${content_age_h}h. Off-season that is normal and"
  log "      is NOT treated as a fault. Whether the WORKFLOWS are running is a different"
  log "      question, owned by the Workflow Watchdog."
fi

# ---- regression: the check that actually catches a rollback ------------------------
#
# A rollback is not "old", it is POORER. Both real incidents were losses of accumulated
# rows, and both are visible without consulting a clock:
#
#     2026-08-24   events 2912 -> 1735, actuals 1680 -> 763   (a 28-day snapshot)
#     2026-09-08   events 2987 -> 2980, actuals 1744 -> 1740  (a 3.6-day snapshot)
#
# ⚠ ACTUALS IS THE AXIS, NOT EVENTS. Within a season `eps_actual` only accumulates --
# `upsert_event` writes it as `COALESCE(?, eps_actual)` so it is never nulled, and the
# DELETE on the legacy-schema path is guarded on `reported = 0`, which a row carrying an
# actual is not. Event COUNT, by contrast, legitimately falls when `--cleanup` removes a
# duplicate, so a raw event-count drop is not on its own evidence of anything.
#
# The 09-08 loss was FOUR actuals. `MIN_ACTUALS_GAIN` (50) governs the overdue-driven
# self-heal below, where a big margin is right because that path trades away live state;
# here the tolerance has to be tighter than the smallest real incident or the check is
# decorative. Default 0: any older artifact holding MORE accumulated actuals than the
# copy we are about to write to means the copy we are about to write to went backwards.
#
# ⛑ The asymmetry is deliberate. A false positive costs at most one `--cleanup` and
# arrives with an alarm a human can read. A false negative is silent multi-day data
# destruction, which this repository has now suffered twice. So this heals and CONTINUES
# rather than failing -- refusing to write is what turned the last incident into an
# outage, and the point is to get a good database in front of the agent, not to stop.
REGRESSION_TOLERANCE="${EA_DB_ACTUALS_REGRESSION_TOLERANCE:-0}"
REGRESSION_SCAN="${EA_DB_REGRESSION_SCAN_LIMIT:-6}"

scan_ids() {
  # The real `gh api --jq '… | .id'` prints one id per line. Take the first
  # whitespace-delimited field rather than the whole line, so a richer projection --
  # or a stub that emits one -- cannot silently turn every id into a malformed URL.
  gh api "repos/${REPO}/actions/artifacts?name=earnings-db&per_page=$1" \
    --jq '.artifacts[] | select(.expired==false) | .id' 2>/dev/null \
    | awk 'NF {print $1}'
}

mapfile -t reg_ids < <(scan_ids "$REGRESSION_SCAN")
if [ "${#reg_ids[@]}" -eq 0 ]; then
  # Not fatal: a fresh repo, an API blip or a token without actions:read all land here,
  # and none of them is evidence that the database on disk is bad. Say it out loud
  # though -- an unannounced skip is indistinguishable from a check that passed.
  log "regression check SKIPPED: no unexpired earnings-db artifacts could be listed."
else
  reg_best_actuals="$cur_actuals"; reg_best_id=""; reg_best_total=""; reg_best_kv=""
  for aid in "${reg_ids[@]}"; do
    fetch_artifact "$aid" ".db_reg" || { log "  [regression] artifact $aid: download failed"; continue; }
    read -r r_total r_actuals r_overdue r_kv <<<"$(db_stats ".db_reg/earnings_events.db")"
    if [ -z "${r_total:-}" ]; then log "  [regression] artifact $aid: unreadable"; continue; fi
    log "  [regression] artifact $aid: events=$r_total actuals=$r_actuals kv_store=$r_kv"
    if [ "$r_actuals" -gt "$(( reg_best_actuals + REGRESSION_TOLERANCE ))" ]; then
      reg_best_actuals="$r_actuals"; reg_best_id="$aid"
      reg_best_total="$r_total"; reg_best_kv="$r_kv"
      cp ".db_reg/earnings_events.db" ".db_reg_best" || true
    fi
  done

  if [ -n "$reg_best_id" ] && [ -f ".db_reg_best" ]; then
    cp ".db_reg_best" "$DB" || exit 1
    alarm "ROLLBACK: the restored database carries ${cur_actuals} accumulated actuals, but artifact \`${reg_best_id}\` carries ${reg_best_actuals}. Accumulated actuals do not decrease, so the restore returned a snapshot that had gone backwards. Healed in place from \`${reg_best_id}\` (events ${cur_total} -> ${reg_best_total}, actuals ${cur_actuals} -> ${reg_best_actuals}, kv_store ${cur_kv} -> ${reg_best_kv}) and continuing, so this run writes to the good copy instead of re-uploading the poor one as the newest \`earnings-db\`. Check the \`[db-restore] selected artifact\` line above; if the selector keeps choosing the poor snapshot, pin the good one with \`restore_artifact_id\`."
    # Re-read: everything downstream must describe the database we actually kept.
    read -r cur_total cur_actuals cur_overdue cur_kv <<<"$(db_stats "$DB")"
    if [ -z "${cur_total:-}" ]; then
      alarm "The healed database failed its integrity check."; exit 1
    fi
    log "after heal: events=$cur_total actuals=$cur_actuals past-due-without-actuals=$cur_overdue kv_store=$cur_kv"
  else
    log "regression check clean: no artifact carries more accumulated actuals than ${cur_actuals}."
  fi
fi

# ---- the trigger ------------------------------------------------------------------
if [ "$cur_overdue" -le "$MAX_OVERDUE" ]; then
  log "healthy (past-due-without-actuals $cur_overdue <= $MAX_OVERDUE) - no artifact scan."
  exit 0
fi

log "SUSPECT: $cur_overdue tracked events are past due with no actuals (threshold $MAX_OVERDUE)."
log "Scanning up to $SCAN_LIMIT artifacts, ranking by ACCUMULATED ACTUALS not upload time."

mapfile -t ids < <(scan_ids "$SCAN_LIMIT")

if [ "${#ids[@]}" -eq 0 ]; then
  alarm "Database looks rolled back ($cur_overdue past-due events with no actuals) and no unexpired \`earnings-db\` artifacts could be listed."
  exit 1
fi

best_actuals="$cur_actuals"; best_id=""; best_overdue="$cur_overdue"
for aid in "${ids[@]}"; do
  fetch_artifact "$aid" ".db_cand" || { log "  artifact $aid: download failed"; continue; }
  read -r a_total a_actuals a_overdue a_kv <<<"$(db_stats ".db_cand/earnings_events.db")"
  if [ -z "${a_total:-}" ]; then log "  artifact $aid: unreadable"; continue; fi
  log "  artifact $aid: events=$a_total actuals=$a_actuals past-due-without-actuals=$a_overdue kv_store=$a_kv"
  # A candidate must be strictly richer on BOTH axes before it is allowed to overwrite
  # what is on disk. Actuals alone is not a safe ranking: overwriting the working copy
  # discards date locks, kv_store watermarks, Slack thread state and ticktick_task_id
  # pointers, and this repo documents that state as unrecoverable. The dangerous case is
  # a PROVIDER OUTAGE - past-due-without-actuals climbs above the threshold with no
  # rollback at all, and healing to an older artifact would then destroy real state to
  # "fix" a problem that was never a rollback. In an outage the live DB still has MORE
  # events than any older artifact, so requiring a higher event count too refuses the
  # trade. In a genuine rollback, as measured today, the good artifact wins on both
  # (2912 events / 1680 actuals against 1735 / 763).
  if [ "$a_actuals" -gt "$(( best_actuals + MIN_ACTUALS_GAIN ))" ] \
     && [ "$a_total" -gt "$cur_total" ]; then
    best_actuals="$a_actuals"; best_id="$aid"; best_overdue="$a_overdue"
    cp ".db_cand/earnings_events.db" ".db_best" || true
  fi
done

if [ -n "$best_id" ] && [ -f ".db_best" ]; then
  cp ".db_best" "$DB" || exit 1
  log "SELF-HEALED from artifact $best_id: actuals $cur_actuals -> $best_actuals, past-due $cur_overdue -> $best_overdue."
fi

if [ "$best_overdue" -gt "$MAX_OVERDUE" ]; then
  # State the OBSERVATION, not a cause. Two very different situations produce this exact
  # reading - an artifact rollback, and a provider outage where actuals simply never
  # arrived - and they are indistinguishable from here. Naming one of them in the alarm
  # would send the reader to the wrong fix half the time.
  healed_note="the working copy was left untouched"
  if [ -n "$best_id" ]; then
    healed_note="the working copy was replaced from artifact $best_id, which was better but still short"
  fi
  alarm "$best_overdue tracked events are past due with no actuals (threshold $MAX_OVERDUE) and no available artifact clears it; best carries $best_actuals actuals. Refusing to write, and $healed_note. Two causes look identical here: an artifact ROLLBACK (check whether the restore step logged an old run date) or a PROVIDER OUTAGE where actuals never arrived (check Finnhub/FMP). If it is a rollback, recover by dispatching \`daily_earnings_check\` with \`restore_artifact_id\` set to a known-good artifact id."
  exit 1
fi

exit 0
