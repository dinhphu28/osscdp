# 19 — Journey Orchestration

Status: **Phases 1–2 implemented.** Later phases are designed but not built.

A **journey** is a versioned, ordered flow a customer profile *enters* (via segment
membership) and *advances* through with per-profile state. Phase 1 ships a linear
`wait → send` flow: enter a segment, wait N, deliver to a destination — idempotent,
crash-safe, GDPR-erasable, identity-merge-safe.

Journey orchestration was previously an explicit non-goal (see `00-index.md`,
`01-architecture-overview.md`, `07`, `10`, `11`). This document supersedes those
deferrals for the linear case; complex branching remains staged (Phase 3+).

## Design principle: reuse the segment substrate

The journey engine is a second instance of the proven stateful-segmentation machinery,
not new infrastructure:

- **Entry** rides the existing `segment_membership_outbox → memRelay →
  TopicSegmentMembershipChanged` path. Journey adds only a new consumer group
  (`<group>-journey`); there is **no new outbox, relay, or Kafka topic** in Phase 1.
- **Progression** is a clone of `segment.Runner` + the `ClaimDuePending` fair-claim CTE +
  `FailPending` exponential-backoff/park dead-letter.
- **Sends** reuse the activation task/sender/circuit-breaker/consent stack directly via an
  extracted `activation.Service.EnqueueSend`.

## Data model

| Table | Migration | Purpose |
|---|---|---|
| `journey` | `00019` | Head: identity, `status`, `entry_segment_id`, `current_version`. Partial-unique active name (mirrors `00016`). |
| `journey_version` | `00019` | Immutable `definition_json` (ordered step array). An edit mints version N+1. |
| `journey_enrollment` | `00020` | **Single per-enrollment state row** — see below. |
| `destination_subscription` (partial-unique index) | `00021` | Makes the per-destination `trigger_type='journey'` subscription get-or-create race-safe. |

### The single-table enrollment (the decisive choice)

`journey_enrollment` deliberately **folds** the position (`segment_membership`:
`status`, `current_step_index`, `step_seq`) and the deadline work-queue
(`segment_pending_eval`: `due_at`, `claimed_at`, `attempts`, `parked_at`) into **one
row** keyed `(tenant_id, journey_id, customer_profile_id, enrollment_seq)`.

Because position, claim-fence, and advance-seq all live on one row:

- **Advance is a single-table claim-fenced `UPDATE`** — `SET current_step_index=…,
  step_seq=step_seq+1, claimed_at=NULL WHERE … AND claimed_at=$fence AND
  step_seq=$expected`. A reclaimed slow runner writes zero rows: no rewind, no
  double-advance.
- **Identity merge moves or drops the enrollment atomically** (one row, one
  `UPDATE … WHERE NOT EXISTS`), avoiding the two-table reparent desync that an
  enrollment-row + pending-row split would create.
- `enrollment_seq` (0 in Phase 1) namespaces the activation idempotency key so a future
  re-entry after completion never dedups against a prior run.

## Runtime flow

**Entry** — `journey.Service.EnrollOnMembership` consumes `segment_membership_changed`;
on `entered`, for each active journey whose `entry_segment_id` matches, it inserts an
enrollment `ON CONFLICT (…) WHERE status='active' DO NOTHING` (idempotent under
at-least-once redelivery; one live enrollment per journey/profile).

**Progression** — `journey.Runner` (a `segment.Runner` clone) fair-claims due
enrollments per tenant and calls `journey.Service.Advance` on each:

- **wait** → advance to the next step with `due_at = now + ParseWindow(duration)`. The
  sweeper won't re-claim until that future deadline (timed waits are free).
- **send** → `activation.EnqueueJourneySend` (get-or-create the destination's journey
  subscription, apply the consent gate, insert an `activation_task` with a
  step-scoped idempotency key) **before** the fenced advance. A crash between the two
  re-runs the send (deduped by `ON CONFLICT`) then advances — effective exactly-once with
  no new outbox. Reaching the last step completes the enrollment.

Poison steps back off exponentially and park after N attempts; operators inspect via
`ListParked` and re-arm via `UnparkEnrollment` (admin routes under `…/journeys/{id}/
enrollments/parked` and `…/enrollments/{profileID}/retry`).

## Versioning

An in-flight enrollment PINS the `journey_version` captured at enroll; a re-authored flow
(new version, bumped `journey.current_version`) never disturbs a customer mid-wait.

## Exit-on-segment-leave (Phase 2)

A journey created with `exit_on_segment_leave=true` terminates a profile's **active**
enrollment (`status='exited'`) when the profile *leaves* the entry segment
(`segment_membership_changed` with `change='exited'`) — so a customer who no longer
qualifies stops receiving the journey's sends. The `-journey` consumer's
`OnMembershipChanged` dispatches `entered → enroll`, `exited → exit`. Default `false`
preserves run-to-completion.

Two correctness properties enforce exit safety:

- **Exit wins in-flight** — `Repo.Advance` gained `AND status='active'`, so an exit that
  fires while the runner holds a claim causes the concurrent fenced advance to no-op (at
  most one already-enqueued send goes out).
- **Once-only re-entry** — `Enroll`'s `ON CONFLICT` targets the enrollment PK
  (`…, enrollment_seq`), so re-entering the segment after a terminal (completed/exited)
  enrollment is a clean no-op rather than a primary-key error. Re-entry as a new run is
  Phase 5 (`enrollment_seq` allocation).

## Lifecycle integration

- **Erasure** — `governance.Service.Delete` deletes `journey_enrollment` for the profile
  before the `customer_profile` row (counted in `DeleteCounts.JourneyEnrollments`). Phase 1
  has no journey outbox, so there is no JSON-match erasure point.
- **Identity merge** — `profile.reparentProfileChildren` unions the loser's enrollments
  onto the survivor (dedup on `journey_id`; survivor wins on conflict, documented
  progress-loss tradeoff, same class as `segment_membership`).
- **Retention** — `journey_enrollment` is small and non-partitioned (not on the
  `behavioral_event` DROP-PARTITION path). A terminal-row sweep is Phase 5.

## Phase roadmap

1. **(done)** Linear segment-entry `wait → send`; erasure + merge hooks + parked
   admin + runner metrics.
2. **(done)** Exit-on-segment-leave (`exited` change) + once-only re-entry guard +
   exit-wins-in-flight advance guard. (A conditional "goal step" — exit early on
   conversion — is folded into Phase 3, where condition evaluation lands; a stop step in
   a purely linear flow is just the last step.)
3. Branching: `condition` (embeds `segment.Rule`, evaluated via `segment.Evaluate` +
   `behavior.Store`) + weighted `split`. **Same phase** widens `behavior.Retention`'s
   horizon to `UNION` `journey_version.max_window_seconds`, so a wait-then-condition
   journey never reads pruned behavioral data.
4. Event entry (`TopicProfileUpdated` on an entry event) + `journey_seed_job` backfill
   (a `segment_seed_job`/`SeedRunner` clone) to enroll the already-qualified population.
5. Lifecycle polish: re-entry (`enrollment_seq` allocation) + per-journey caps + terminal
   retention sweep + richer merge fidelity.

## Key files

`internal/journey/` (`journey.go`, `dsl.go`, `repo.go`, `service.go`, `runner.go`,
`handler.go`); `activation.Service.EnqueueSend` / `EnqueueJourneySend` /
`EnsureJourneySubscription`; `governance.go` (erasure); `profile/repo.go` (reparent);
`rbac/roles.go` (`journey:read` / `journey:write`); worker + api wiring in `cmd/`.
Integration coverage in `internal/foundationtest/journey_test.go`.
