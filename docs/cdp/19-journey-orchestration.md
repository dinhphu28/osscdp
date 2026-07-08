# 19 — Journey Orchestration

Status: **Phases 1–5 implemented** — the core journey engine is feature-complete.

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

## Branching (Phase 3)

The definition becomes a forward DAG over the (still index-addressed) step array:

- **condition** — embeds a `segment.Rule`, evaluated by `segment.Evaluate` +
  `behavior.Store` against the profile with no triggering event (`at=now`), routing to
  `if_true` / `if_false` target indices.
- **split** — a weighted branch; `splitTarget` hashes a **stable** key
  (`tenant|journey|profile|enrollment_seq|step_index`, sha256) so at-least-once
  redelivery / reclaim always routes to the same branch.
- **`next`** — wait/send steps take an optional explicit forward target (default
  `index+1`), so two branch arms stay disjoint (each arm jumps past the other).

Branch targets are **forward-only** (strictly greater than the branching step's index,
`≤ len(steps)`; a target `== len` completes). Forward-only makes every definition an
acyclic DAG that terminates in `≤ len(steps)` advances — no cycle guard needed. Each
step commits exactly one claim-fenced `Advance`; condition/split are side-effect-free
routing that commit their decision atomically **before** any downstream send, so a
pre-commit re-route on current state can never double-send.

**Version metadata + retention (same phase).** `journey_version` gains
`referenced_event_names` + `max_window_seconds`, derived at create/update from every
condition rule via the new exported `segment.AnalyzeRule` (mirrors `segment_version`,
migration `00023`). `behavior.Retention.EffectiveHorizon` now `UNION`s the journey
windows of any `journey_version` pinned by a live enrollment or the current version of
an active journey — so a long-wait-then-behavioral-condition journey never evaluates
over DROP-partitioned `behavioral_event` data.

## Entry modes + backfill (Phase 4)

A journey has exactly **one entry mode** (DB `CHECK` XOR):

- **segment** (`entry_segment_id`) — enter when the profile joins the entry segment
  (Phases 1–3), via the `segment_membership_changed` consumer.
- **event** (`entry_event_name`) — enter when the profile emits that event, via a new
  `-journey-event` consumer group on the existing `TopicProfileUpdated` stream
  (`EnrollOnEvent` matches `pu.Event.EventName`). Idempotent by the once-only `Enroll`.

**Population backfill.** Creating a *segment*-entry journey enqueues a durable
`journey_seed_job` in the same tx (a clone of `segment_seed_job`): a `journey.SeedRunner`
pages the entry segment's **current** active members (keyset cursor on
`customer_profile_id`, claim-fenced, resumable) and enrolls each — so a journey reaches
the already-qualified population, not just future joiners. The job snapshots
`entry_segment_id` + `journey_version`; the page INSERT is gated on the journey still
being active (archive mid-drain stops admitting) and is `ON CONFLICT DO NOTHING`
(idempotent with the live membership-entry path). Event-entry journeys have no existing
population and do not seed.

## Re-entry + retention (Phase 5)

- **Re-entry / caps** — `journey.max_enrollments` (default 1 = once-only) caps how many
  times a profile may enter a journey. `Repo.Enroll` allocates a fresh
  `enrollment_seq = max+1` per run and is a no-op while an active enrollment exists
  (`WHERE NOT EXISTS active`) or the total reaches `max_enrollments`
  (`count < max_enrollments`). Concurrency-safe: a **bare** `ON CONFLICT DO NOTHING`
  absorbs both a PK collision (two racing enrolls compute the same seq) and the
  partial-unique-active index (never two active runs) — exactly one survives a race.
  `max_enrollments=1` reproduces once-only exactly (`count<1` admits only a
  zero-prior-enrollment profile). Backfill (`SeedJobPage`) stays seq=0 first-entry.
- **Terminal-row retention** — a `RetentionSweeper` prunes aged terminal
  (completed/exited) `journey_enrollment` rows in bounded batches
  (`PruneTerminalEnrollments`), keeping the non-partitioned table bounded. Active rows
  are never touched.

**Deferred (documented tradeoff):** richer identity-merge fidelity. On merge the survivor
wins and the loser's mid-journey progress is dropped — the same class of behavior as
`segment_membership`, so it is acceptable and left as-is.

## Phase roadmap

1. **(done)** Linear segment-entry `wait → send`; erasure + merge hooks + parked
   admin + runner metrics.
2. **(done)** Exit-on-segment-leave (`exited` change) + once-only re-entry guard +
   exit-wins-in-flight advance guard.
3. **(done)** Branching: `condition` (embeds `segment.Rule`) + weighted `split` +
   forward-DAG `next` targets; `journey_version` metadata + behavioral retention-horizon
   widening. (The conditional "goal step" — exit early on conversion — is expressible as
   a condition whose matching arm targets `len(steps)`.)
4. **(done)** Event entry (`TopicProfileUpdated` on an entry event, XOR with segment
   entry) + `journey_seed_job` backfill (a `segment_seed_job`/`SeedRunner` clone) to
   enroll the already-qualified population.
5. **(done)** Lifecycle polish: re-entry (`enrollment_seq` allocation) + per-journey caps
   (`max_enrollments`) + terminal-row retention sweep. (Richer merge fidelity is a
   documented, deferred tradeoff.)

## Key files

`internal/journey/` (`journey.go`, `dsl.go`, `repo.go`, `service.go`, `runner.go`,
`handler.go`); `activation.Service.EnqueueSend` / `EnqueueJourneySend` /
`EnsureJourneySubscription`; `governance.go` (erasure); `profile/repo.go` (reparent);
`rbac/roles.go` (`journey:read` / `journey:write`); worker + api wiring in `cmd/`.
Integration coverage in `internal/foundationtest/journey_test.go`.
