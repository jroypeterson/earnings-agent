# Chat Feedback On `plan.md`

This note is intended for review by another AI system. It summarizes my feedback on `plan.md`, including an important update after learning that TickTick task tracking is a real user pain point and part of the user's normal workflow.

## Overall Assessment

The plan is strong. It has a real product thesis, clear sequencing, and a better architectural direction than the current single-file script. In particular, it correctly reframes the project from a calendar-sync utility into a workflow support system for earnings preparation, review, and follow-up.

The most important strengths are:

- The plan ties the project to the user's real investing workflow rather than inventing a generic platform.
- Coverage Manager as the source of truth for the universe and tiers is the right principle.
- Tier-aware delivery is a good way to control noise.
- Separating collection from delivery is the correct long-term architecture.
- The phased rollout is mostly practical rather than aspirational.

## Important Update: TickTick Should Be Treated As Core

My earlier recommendation was to push TickTick later in the roadmap. I am revising that recommendation.

The user clarified that task tracking is a real pain point and that they actively use TickTick. That changes the prioritization materially.

In this context:

- TickTick is not a nice-to-have surface.
- TickTick is not feature creep.
- TickTick is part of the core product because it is the execution surface for earnings review work.

Calendar answers "when is this happening?"
TickTick answers "what do I need to do about it?"

That means the MVP should include TickTick in some form.

## Revised MVP Recommendation

I would define the MVP as:

1. Coverage Manager sync
2. Tier-aware calendar sync
3. TickTick task creation and lightweight review-state tracking
4. Weekly digest
5. T-1 pre-earnings brief
6. T+0 / T+1 post-earnings result loop

I would still defer prediction tracking, Slack reply parsing, and advanced analytics until later.

## Recommended Changes To The Plan

### 1. Keep TickTick in MVP, but narrow the first implementation

TickTick should remain on the critical path, but the implementation should stay tight:

- Create one task per Tier 1 and Tier 2 earnings event
- Set due date to the earnings date
- Include a short prep checklist in the task body
- Update the task body with results after earnings
- Optionally read completion status back into SQLite on a scheduled basis

This is enough to make the system operationally valuable without creating a fragile bidirectional sync project too early.

### 2. Reframe TickTick in the architecture section

The current plan treats TickTick mostly as a delivery surface. Given the user's workflow, that undersells its role.

I would revise the architectural framing to:

- Google Calendar is the publication surface for time-based awareness
- TickTick is the execution surface for review work
- SQLite is the workflow memory tying those surfaces together

This is a more accurate representation of how the system will actually be used.

### 3. Clarify the system-of-record model

I would revise the current "Google Calendar is the durable system of record" wording.

Suggested replacement:

- Coverage Manager is the source of truth for universe and tiers
- Google Calendar is the durable source of truth for published event state
- SQLite is the source of truth for workflow state and historical memory

Reason:
Calendar is a good recovery surface for event existence and publication metadata, but it is not the right source of truth for predictions, review completion, estimate snapshots, enrichment history, or reporting.

### 4. Move TickTick ahead of some other planned features

Given the user's feedback, I would move TickTick ahead of some notification-oriented work if prioritization becomes necessary.

Suggested phase ordering:

1. Foundation: modularization, migrations, identity fixes, Coverage Manager integration
2. TickTick integration for earnings review tasks
3. Weekly digest
4. T-1 briefs
5. T+0 / T+1 result loop
6. Prediction tracking and accuracy analysis
7. Reconcile mode and hardening

If the original phase structure is preserved, I would still explicitly mark TickTick as MVP-critical rather than optional.

### 5. Add phase-level success criteria

The plan has tasks and validation steps, which is good, but I would add one short acceptance statement per phase.

Examples:

- After Phase 1: the system reads tiers from Coverage Manager and syncs events without relying on `tickers.txt`
- After Phase 2: Tier 1 and Tier 2 earnings events reliably produce TickTick tasks with correct due dates and basic prep content
- After Phase 3: the weekly digest sends reliably every Sunday with no manual intervention

This will make implementation discipline stronger and help prevent the plan from drifting into a broad wishlist.

## What I Would Still Defer

Even with TickTick moved earlier, I would still keep the following out of the core early scope:

- Slack interactive app work
- Slack reply parsing for predictions
- options implied move
- transcript gathering beyond best-effort links
- advanced prediction analytics
- rich bidirectional task synchronization

These may be good future enhancements, but they do not appear as critical as getting the core review workflow into TickTick.

## Remaining Concerns

I still see a few issues worth challenging even after the TickTick update:

- `ticker + event_date` is a better identity key than `ticker + quarter`, but it still is not a perfect identity model. The plan should be explicit about using `source_fingerprint` and canonical event reconciliation.
- The scheduling model may still be too ambitious early on. If operations become noisy, it may be better to begin with fewer scheduled runs and split later.
- Some integrations are written as if they are straightforward when they may not be. TickTick, Gmail send, and Slack read/parsing should be treated as operationally risky until proven otherwise.

## Bottom Line

My updated recommendation is:

- The plan is directionally good and worth building from.
- TickTick should be treated as a core requirement, not a peripheral sink.
- The first TickTick implementation should be intentionally narrow and reliable.
- The architecture section should more clearly distinguish awareness surfaces, execution surfaces, and workflow memory.
- Prediction workflows and richer analytics should remain later-phase work.

If another AI reviewer disagrees, the key question to resolve is this:

Is the product primarily an information system about earnings, or is it an execution system for actually getting earnings review work done?

Given the user's clarification about TickTick, I believe the second framing is more accurate.
