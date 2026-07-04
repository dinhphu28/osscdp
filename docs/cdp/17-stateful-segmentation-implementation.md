# 17 — Level 3 Stateful Segmentation: Implementation Plan

This is the **step-by-step build plan** for the design of record in
[16 — Level 3 Stateful Behavioral Segmentation](16-stateful-segmentation.md). Doc 16 holds the
*rationale* (why buckets + log + deadline queue, why not Redis/Kafka-Streams, and how every reviewed
gap is resolved); this doc holds the *how* — the exact migrations, structs, repo methods, handlers,
worker wiring, and tests to write, in the order to write them. It is written in the style of
[15 — Future Enhancements](15-future-enhancements.md): each phase says **what exists today** (with
`file:line` anchors into the real code), **what to add**, the **exact shapes**, and the **tests**.

Grounded against branch `enhance/identity`. Nothing here is wired yet — treat it as the backlog that
implements doc 16.

## Conventions used throughout

- **Anchors** point at the code as it is today; e.g. `internal/segment/eval.go:21` is `Evaluate`.
- Each phase **compiles, passes tests, and is shippable on its own**: a rule with no `behavior` node
  is byte-for-byte unchanged, so partial rollout never breaks stateless L1/L2 segmentation.
- **Clock injection is mandatory** (doc 16 finding #26): every windowed read / `due_at` / boundary
  takes an explicit instant passed as a SQL bind (`$now`), never SQL-side `now()`. This mirrors the
  existing `internal/activation/breaker.go:16` `now func() time.Time`.
- New tables live in a new package `internal/behavior`; `profile` depends on it only through a
  nil-safe interface (no hard `profile → behavior`/`segment` import cycle).

## Phase map

| # | Phase | New migration | Primary packages touched | Ship criterion |
|---|-------|---------------|--------------------------|----------------|
| 1 | DSL + validation | — | `internal/segment` | Admins author/validate windowed rules; eval ignores them |
| 2 | Durable log + write hook + merge fold | `00011` | `internal/behavior`, `internal/profile` | `behavioral_event` populates exactly-once; merges commit |
| 3 | Exact-mode evaluator + clock injection | — | `internal/segment`, `internal/behavior`, `cmd/cdp-worker` | Event-driven positive transitions work end-to-end |
| 4 | Atomic membership + outbox emit + `transition_seq` | `00013` (partial) | `internal/segment`, `internal/relay`, `internal/activation`, `cmd/cdp-worker` | No double-emit, no lost emit, no idempotency collision |
| 5 | Deadline queue + sweeper + seed + safety sweep | `00012` | `internal/segment`, `internal/config`, `cmd/cdp-worker`, `internal/platform/database` | Absence/expiry fire with no inbound event, incl. dormant profiles |
| 6 | Bucket rollup (hot path) + segment metadata + prefilter | `00013` (rest) | `internal/behavior`, `internal/segment` | `O(1)`-ish writes, bounded reads, cached fan-out |
| 7 | Governance erasure + segment lifecycle | — | `internal/governance`, `internal/segment` | Erasure + retire/drain correct |
| 8 | Retention + schema-version + observability + docs | — | `internal/behavior`, `internal/platform/metrics`, `docs` | Operationally complete |

The ordering deliberately front-loads two safety items that a naive plan defers (doc 16 §Implementation
phases): the **identity-merge fold ships in Phase 2** with the FK-bearing table it protects (finding
#23), and the **population seed + safety sweep ship in Phase 5** with the sweeper (findings #7, #32).

---

## Phase 1 — DSL + validation (no behaviour yet)

Rationale: doc 16 §DSL extension. Ship a parseable/validated `behavior` leaf that evaluation still
ignores, so the schema and admin surface land before any storage.

### What exists today

- `Rule` is a recursive logical/comparison node (`internal/segment/dsl.go:39-45`): `Operator`,
  `Conditions`, `Field`, `Op`, `Value`. `isLogical()` at `:47`.
- `Validate(r Rule)` (`internal/segment/dsl.go:57`) walks the tree; leaf must have `Field` +
  a known `comparisonOps` `Op` (`dsl.go:31`).
- Both admin write paths already validate: `Handler.Create` (`internal/segment/handler.go:44`) and
  `Handler.Update` (`internal/segment/handler.go:81`) call `Validate(req.Rule)` and return
  `400 invalid rule` on error. So anything `Validate` rejects is rejected at author time for free.

### What to add — `internal/segment/dsl.go`

Add one `omitempty` field to `Rule` and a `BehaviorSpec` type (exact shape from doc 16):

```go
type Rule struct {
	Operator   string        `json:"operator,omitempty"`
	Conditions []Rule        `json:"conditions,omitempty"`
	Field      string        `json:"field,omitempty"`
	Op         string        `json:"op,omitempty"`
	Value      any           `json:"value,omitempty"`
	Behavior   *BehaviorSpec `json:"behavior,omitempty"` // NEW; nil => today's node, byte-for-byte
}

type BehaviorSpec struct {
	Kind      string         `json:"kind"`                 // count | frequency | recency | absence | sequence
	EventName string         `json:"event_name,omitempty"` // required except sequence
	Window    string         `json:"window,omitempty"`     // "7d","24h","30m" -> time.Duration
	Op        string         `json:"op,omitempty"`         // count/frequency: gte|gt|lte|lt|eq
	Value     *float64       `json:"value,omitempty"`      // pointer: absent is distinct from 0; required for count/frequency
	ValueProp string         `json:"value_prop,omitempty"` // frequency-of-value numeric property
	Where     *Rule          `json:"where,omitempty"`      // OPTIONAL props filter (comparison-leaf grammar)
	Steps     []BehaviorSpec `json:"steps,omitempty"`      // sequence: ordered A,B,...
	Within    string         `json:"within,omitempty"`     // sequence: max gap between steps
	Anchor    *BehaviorSpec  `json:"anchor,omitempty"`     // correlated absence: "no E within Window AFTER anchor"
	Exact     bool           `json:"exact,omitempty"`      // force behavioral_event re-query path
}

const (
	BehaviorCount     = "count"
	BehaviorFrequency = "frequency"
	BehaviorRecency   = "recency"
	BehaviorAbsence   = "absence"
	BehaviorSequence  = "sequence"
)
```

Add a duration parser (accept `d`/`h`/`m`/`s`; Go's `time.ParseDuration` lacks `d`, so handle a
trailing-`d` prefix then fall back):

```go
func ParseWindow(s string) (time.Duration, error) // "7d" -> 168h; "24h","30m" via time.ParseDuration
```

Extend `Validate` (`dsl.go:57`) with a mutually-exclusive branch **before** the logical/leaf split:

- If `r.Behavior != nil`: `Operator`, `Field`, `Op`, `Value`, `Conditions` **must be empty**
  (a behaviour leaf is neither logical nor comparison).
- `Kind` must be one of the five constants.
- `Window` (and `Within` for sequence) must `ParseWindow` cleanly.
- `count`/`frequency`: require non-empty `EventName`, a comparison `Op` in `{gte,gt,lte,lt,eq}`, and a
  `Value`.
- `recency`/`absence`: require non-empty `EventName` and `Window`.
- `sequence`: require ≥2 `Steps`, each with non-empty `EventName`; require `Within`.
- **Force-`exact` (or reject `exact:false`)** for any leaf buckets cannot serve honestly (doc 16
  findings #16, #17, #4): `Kind==sequence`, `Where != nil`, `ValueProp != "" && Where != nil`, and
  `Anchor != nil`. Set `r.Behavior.Exact = true` in place, or return a `ValidationError` telling the
  author it will run exact.
- **Reject `exact`/`sequence` on high-frequency event names** (doc 16 finding #13). Ship the
  event-rate list as config for Phase 1 stub it as an empty set and wire the real gate in Phase 6.
- Recurse into `Where` with the existing `Validate` (it is a comparison-leaf subtree) and into `Steps`
  / `Anchor` as `BehaviorSpec`s.

`Validate` still returns `*ValidationError` (`dsl.go:52`), so `handler.go:45/82`'s `400` path is
unchanged.

### Tests (`internal/segment/dsl_test.go`, unit only)

- Back-compat: a table of today's rules (logical/comparison, incl. the stored
  `{"field":"profile.computed_attributes.total_orders","op":"gte","value":1}`) round-trips through
  `json.Marshal`→`Unmarshal`→`Validate` **unchanged** and `Validate` returns nil.
- Each valid `BehaviorSpec` example from doc 16 §JSON examples validates.
- Rejections: behaviour leaf with a stray `Field`; unknown `Kind`; bad `Window` (`"7x"`); `count`
  without `Op`; `sequence` with 1 step or missing `Within`; `where` present with `exact:false` →
  auto-set or rejected; empty `EventName` for `count`.
- `ParseWindow` table: `7d→168h`, `24h`, `30m`, `90s`, error on `""` and `"7"`.

**Ship:** admins can author/validate windowed rules through the existing `POST/PUT` segment routes;
evaluation continues to ignore `behavior` leaves (the Phase-3 evaluator adds the branch).

---

## Phase 2 — Durable log + write hook + merge fold

Rationale: doc 16 §Data model (`00011`), §Architecture item 1–2, §Tenant isolation (merge fold),
findings #23, #31, #20. The FK-bearing log and its identity-merge fold **must ship together** or the
first cluster merge FK-crashes the profile worker.

### What exists today

- `updateTx` (`internal/profile/service.go:106`) opens `tx`, loads/creates the profile, checks
  `alreadyApplied` (`service.go:148` → `repo.go:224`), merges traits, `update`s, then `markApplied`
  (`service.go:199` → `repo.go:210`, `ON CONFLICT DO NOTHING` on `(tenant,profile,event_id)`), then
  commits. `Update` (`service.go:77`) skips the emit when `!res.Applied` (`service.go:82`).
- `profile.Service` already has nil-safe hooks `OnUpdated`, `Audit`, `Logger`
  (`internal/profile/service.go:61-66`) — the pattern to mirror.
- `reparentProfileChildren` (`internal/profile/repo.go:141`) already folds
  `customer_profile_history`, `customer_consent`, `segment_membership`, and activation rows loser →
  survivor **in the same tx**, then `DELETE FROM customer_profile` (`repo.go:202`). The
  re-key-what-survivor-lacks pattern is at `repo.go:145-153`.
- Envelope timestamps: client-supplied `Timestamp` (`internal/events/envelope.go:48`) vs server
  `ReceivedAt` (`:49`); `EventName` is empty for identify/alias (`:45`, `omitempty`).
- Migration style: goose up/down, FK `REFERENCES tenant(id)`, `UNIQUE (tenant_id, ...)` dedup
  (`migrations/00006_customer_profile.sql:35`). Embedded via `migrations/embed.go`.

### What to add

**Migration `migrations/00011_behavioral_event.sql`** — durable profile-keyed log, **range-partitioned
by `occurred_at`** so retention is `DROP PARTITION` (doc 16 finding #9). Exact DDL is in doc 16
§`00011`; the load-bearing points:

- PK `(tenant_id, customer_profile_id, event_id, occurred_at)` — idempotent append; `occurred_at` is
  the *clamped* deterministic value so redelivery collides and `ON CONFLICT DO NOTHING` drops it.
- `event_name TEXT NOT NULL CHECK (event_name <> '')`, `props_json JSONB`, `schema_version INT DEFAULT 1`,
  `inserted_at TIMESTAMPTZ DEFAULT now()` (retention keys off `inserted_at`, not client `occurred_at`).
- `CREATE INDEX idx_behavioral_event_window ON behavioral_event (tenant_id, customer_profile_id, event_name, occurred_at DESC)`.
- `PARTITION BY RANGE (occurred_at)` + create an initial `_default` partition and the current/next
  weekly partitions inline (the Phase-8 retention job creates future ones).
- **No FK on `customer_profile_id`** (a partitioned table cannot cheaply FK a huge parent, and erasure
  handles the delete explicitly in Phase 7). Keep the `tenant_id` FK.

**New package `internal/behavior`** — `recorder.go`:

```go
package behavior

type Recorder struct {
	OnAppended func() // nil-safe metric hook (BehaviorEventsAppended)
}

func NewRecorder() *Recorder { return &Recorder{} }

// Append writes one behavioral_event row inside the caller's tx. Gated to track
// events with a non-empty event_name (finding #20). occurred_at is clamped to
// LEAST(env.Timestamp, env.ReceivedAt) (finding #31). No-op + nil for other types.
func (r *Recorder) Append(ctx context.Context, tx pgx.Tx, profileID uuid.UUID, env events.Envelope) error
```

- Gate: `if env.Type != events.TypeTrack || env.EventName == "" { return nil }`.
- Clamp: `occurredAt := env.Timestamp; if env.ReceivedAt.Before(occurredAt) { occurredAt = env.ReceivedAt }`.
- `INSERT INTO behavioral_event (...) VALUES (...) ON CONFLICT DO NOTHING` with `props_json = env.Properties`.
- Take the `tx` as `pgx.Tx` so it commits atomically with the profile change.

**Wire the hook into `profile.Service`** (`internal/profile/service.go`):

```go
// BehaviorRecorder is the nil-safe hook the profile worker calls inside updateTx.
type BehaviorRecorder interface {
	Append(ctx context.Context, tx pgx.Tx, profileID uuid.UUID, env events.Envelope) error
}
// add field to Service (mirrors OnUpdated/Audit/Logger, service.go:61-66):
Behavior BehaviorRecorder
```

In `updateTx`, **after** the `alreadyApplied` short-circuit (`service.go:152-157`, so redelivery
never double-counts) and **before/with** `markApplied` (`service.go:199`), inside the same `tx`:

```go
if s.Behavior != nil {
	if err := s.Behavior.Append(ctx, tx, prof.ID, env); err != nil {
		return Result{}, err
	}
}
```

This is where **exactly-once counters come for free** (doc 16 §Chosen architecture): the append lives
behind the same idempotency ledger as the profile write.

**Extend `reparentProfileChildren`** (`internal/profile/repo.go:141`) — add, in-tx, before the loser
`customer_profile` DELETE (`repo.go:202`): re-key `behavioral_event` rows whose `event_id` the
survivor lacks, mirroring the history re-key at `repo.go:145-153`:

```sql
UPDATE behavioral_event b SET customer_profile_id=$3
WHERE b.tenant_id=$1 AND b.customer_profile_id=$2
  AND NOT EXISTS (SELECT 1 FROM behavioral_event s
                  WHERE s.tenant_id=$1 AND s.customer_profile_id=$3 AND s.event_id=b.event_id);
DELETE FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2; -- leftovers survivor already had
```

(Bucket + `segment_pending_eval` folds are added in Phases 6/5 when those tables exist; the
`behavioral_event` fold ships **now** because `00011` creates the row that the loser DELETE would
otherwise orphan.)

**Worker wiring** (`cmd/cdp-worker/main.go:114`): after constructing `profileSvc`, set
`profileSvc.Behavior = behavior.NewRecorder()` and its `OnAppended` metric hook (Phase 8 metric).

### Tests

- **Unit** (`internal/behavior/recorder_test.go` is thin; most value is integration).
- **Testcontainers** `internal/foundationtest/behavior_test.go` (mirror
  `internal/activation/repo_integration_test.go:1-58` for the pool/migrate harness):
  - `TestRecorder_AppendsOnceUnderRedelivery`: drive `profileSvc.Update` twice with the same
    `event_id`; assert exactly one `behavioral_event` row (append is behind `alreadyApplied`).
  - `TestRecorder_SkipsIdentifyAndAlias`: an identify event (empty `EventName`) writes **no** row.
  - `TestRecorder_ClampsFutureTimestamp`: a `Timestamp` in the future stores `occurred_at =
    received_at` (finding #31).
  - `TestProfile_MergeFoldsBehavioralEvents` (mirror
    `internal/foundationtest/profile_test.go` `TestProfile_ReparentOnMerge`): loser + survivor each
    have behaviour rows incl. one shared `event_id`; after a merge the survivor owns the union with
    **no double-count**, and the merge **commits** (proves no FK crash, finding #23).

**Ship:** `behavioral_event` populates exactly-once; cluster merges with behaviour rows commit.

---

## Phase 3 — Exact-mode evaluator + clock injection

Rationale: doc 16 §Evaluation semantics, findings #1, #3, #16, #17, #26. This is the first phase that
makes windowed rules **do** something — event-driven positive transitions (`viewed ≥3 in 7d` enters
on the qualifying view). Absence/expiry (no-inbound-event) waits for the Phase-5 sweeper.

### What exists today

- `Evaluate(r Rule, ec EvalContext) bool` (`internal/segment/eval.go:21`) recurses logicals, then at
  `:42` calls `resolveField` + `applyOp`. `resolveField` (`:46`) understands `profile.*` and
  `event.*` paths; `event.properties.*` decodes `ec.Event.Properties` (`:68-69`).
- `Service.Evaluate` (`internal/segment/service.go:66`) loads the profile, `ActiveSegmentVersions`
  (`repo.go:167`), builds `EvalContext{Profile, Event}` (`service.go:79`), and calls `Evaluate` per
  segment (`service.go:85`).
- `makeSegmentHandler` (`cmd/cdp-worker/main.go:265`) unmarshals `profile.ProfileUpdated` and calls
  `svc.Evaluate`. `ProfileUpdated.Event` (`internal/profile/service.go:35`) carries the reason
  envelope, so `event.occurred_at` is available on the edge path.

### What to add

**`BehaviorStore` in `internal/behavior/store.go`** — windowed reads over `behavioral_event`, every
query taking an explicit `at time.Time` bind (`$now`), all `tenant_id`-scoped:

```go
type Store struct {
	pool *pgxpool.Pool
	OnStatefulEvaluated func()
	OnStatefulMatched   func()
}

// Count: COUNT(*) WHERE event_name=$ AND occurred_at >= $at - window [AND props match where].
func (s *Store) Count(ctx, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (int64, error)
// Recency: COALESCE(MAX(occurred_at),'-infinity') >= $at - window  (finding #1: never-emitted -> false)
func (s *Store) Recent(ctx, ..., at) (bool, error)
// Absence: COALESCE(MAX(occurred_at),'-infinity') < $at - window   (finding #1: never-emitted -> true)
func (s *Store) Absent(ctx, ..., at) (bool, error)
// CorrelatedAbsent: find anchor occurrence t_a, test no E in [t_a, t_a+window]  (finding #4)
func (s *Store) CorrelatedAbsent(ctx, ..., at) (bool, error)
// Sequence: ordered LAG/LATERAL self-join enforcing occurred_at(B) in (A, A+Within]  (finding #17)
func (s *Store) Sequence(ctx, ..., at) (bool, error)
// SumValue: SUM((props_json->>value_prop)::numeric) for frequency-of-value.
func (s *Store) SumValue(ctx, ..., at) (float64, error)
```

- `Spec` is a small package-local struct the segment package fills from `BehaviorSpec` (avoids a
  `behavior → segment` import cycle; the segment package owns the DSL, `behavior` owns storage).
- **`where` handling** (finding #16): the exact path selects each in-window row's `props_json` and
  decodes it into a synthetic `events.Envelope{Properties: props_json}` so the existing
  `resolveField`/`applyOp` (`eval.go:46/107`) evaluate `event.properties.*` — SQL cannot run the
  comparison-leaf grammar. Cap the scanned rows (finding #13); fall back / error above the cap.
- **`-infinity` coalesce is load-bearing** for absence/recency (finding #1) — write it explicitly in
  SQL, not in Go, so a never-emitted event is `absent=true` / `recent=false`.

**Change `Evaluate`'s signature** (`internal/segment/eval.go:21`) to thread ctx + store + instant:

```go
func Evaluate(ctx context.Context, r Rule, ec EvalContext, store BehaviorStore, at time.Time) bool
```

- Add a branch at the top of the leaf path (before `resolveField`, `eval.go:42`): if
  `r.Behavior != nil`, translate to a `behavior.Spec` and dispatch on `Kind` to the store, comparing
  count/frequency results via the existing numeric ops. Route to the exact path when `Behavior.Exact`
  (always true for sequence/where/anchor after Phase-1 validation).
- Define a `BehaviorStore` interface in `internal/segment` (so tests inject a **no-op store** that
  keeps all stateless tests green — doc 16 §Risks, "no-op store shim"):

```go
type BehaviorStore interface {
	Count(...); Recent(...); Absent(...); CorrelatedAbsent(...); Sequence(...); SumValue(...)
}
```

- Update all call sites: `Service.Evaluate` (`service.go:85`) passes `ctx`, the store, and
  `at = pu.Event.Timestamp` **clamped** (edge path uses the event's own clamped `occurred_at`, not
  `now()` — finding #3, so redelivery is deterministic). All in-tree `Evaluate(...)` callers in
  `*_test.go` get the no-op store + a fixed `at`.

**Service + worker wiring:**

- `segment.NewService` (`internal/segment/service.go:57`) gains a `store BehaviorStore` param (nil →
  no-op). Add nil-safe `OnStatefulEvaluated`/`OnStatefulMatched` hooks alongside
  `OnEvaluated`/`OnMatched` (`service.go:52-53`).
- `cmd/cdp-worker/main.go:126`: construct `behavior.NewStore(pool)` and pass into
  `segment.NewService(...)`.

### Tests

- **Unit** `internal/segment/eval_behavior_test.go` with a **fake store** (returns canned
  counts/booleans): the flagship `AND(count(product_viewed)>=3, absence(order_completed,24h))`
  matches/doesn't per store answers; mixed stateless+behaviour (`country=US AND count>=1`) short-circuits
  correctly; a no-op store leaves pure-stateless rules matching exactly as today.
- **Testcontainers** `internal/behavior/store_test.go`: seed `behavioral_event` rows at controlled
  `occurred_at`, drive `Store` with an explicit `at`:
  - count-in-window boundary (event exactly at `at-W` in/out), `exact` `COUNT(*)`.
  - `recency`/`absence` over a **never-emitted** event (zero rows) → `recent=false` / `absent=true`
    (finding #1 regression lock).
  - correlated absence: order 20h vs 26h after the anchor view (finding #4).
  - sequence `within:1h`: A then B at +5m matches; A then B at +5d does **not** (finding #17).
  - `where`-filtered count only counts rows whose `props_json.price >= 100` (finding #16).

**Ship:** event-driven positive transitions work end-to-end through the existing membership switch
(`service.go:93`). Membership writes are still the current non-atomic `Enter`/`Exit` — Phase 4 hardens
them.

---

## Phase 4 — Atomic membership + outbox emit + `transition_seq`

Rationale: doc 16 §Edge-triggered membership, findings #2, #27, #28. Independently valuable — it closes
a latent double-emit/lost-emit race in **today's** stateless engine, and is a prerequisite for the
sweep (Phase 5) being a safe second writer.

### What exists today

- `Service.Evaluate` reads `MembershipStatus` (`internal/segment/service.go:89` → `repo.go:194`) then
  separately `Enter`/`Exit`/`TouchEvaluated` (`service.go:95/106/102`) — a **non-atomic
  read-then-write**, so two writers can race the same row and double-emit.
- `Enter` (`repo.go:209`) is an unconditional upsert; `Exit` (`repo.go:224`) an unconditional update —
  neither guards on the current status, so a redundant transition still "succeeds".
- `emit` (`service.go:117`) publishes `MembershipChanged` (`service.go:33`) **directly** via
  `s.pub.Publish` (`service.go:132`) to key `tenant|canonical`, with `ReasonEventID = pu.EventID`
  (`service.go:125`). This is **outside** any DB tx.
- Activation dedups by `IdempotencyKey(tenant, dest, sub, profile, sourceEventID, change)`
  (`internal/activation/idempotency.go:15`), fed `mc.ReasonEventID` (`internal/activation/service.go:75`).
- The outbox pattern exists: `event_outbox` (`migrations/00002_event_outbox.sql`) drained by
  `relay.Relay` (`internal/relay/relay.go:37`), but the relay's table name is **hardcoded**
  `event_outbox` (`relay.go:73`) and it publishes to one fixed topic.

### What to add

**Migration `migrations/00013_...` (partial — the `transition_seq` column now; segment columns in
Phase 6):**

```sql
ALTER TABLE segment_membership ADD COLUMN transition_seq BIGINT NOT NULL DEFAULT 0;
```

Keep it separate from the existing `segment_membership.version` (`internal/segment/repo.go:60`,
`migrations/00007_segment.sql:35`) which stores the **rule** version — do not overload it (doc 16
§Data model note).

**A segment membership outbox.** Two viable shapes; pick one and be consistent:

- **Recommended:** parameterize `relay.New` with a table name (default `event_outbox`), fixing the
  hardcode at `relay.go:73`, and add a second outbox table `segment_membership_outbox` (columns:
  `id UUID PK, tenant_id, partition_key TEXT, payload_json JSONB, status DEFAULT 'pending',
  created_at, published_at`, index on `(status, created_at)`). Then wire a **second relay** in
  `cmd/cdp-worker/main.go` bound to `bus.TopicSegmentMembershipChanged` draining that table.
- Alternative: add a `topic TEXT` column to `event_outbox` and make the relay topic-aware.

**Harden `Enter`/`Exit`** (`internal/segment/repo.go:209/224`) into **conditional flips that also do
the outbox insert in one tx** and bump `transition_seq`:

```sql
UPDATE segment_membership
SET status=$target, transition_seq = transition_seq + 1,
    entered_at = CASE WHEN $target='active' THEN now() ELSE entered_at END,
    exited_at  = CASE WHEN $target='exited' THEN now() ELSE exited_at END,
    last_evaluated_at = now(), version=$ruleVersion
WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3
  AND status IS DISTINCT FROM $target
RETURNING transition_seq;
```

(Enter must first `INSERT ... ON CONFLICT DO NOTHING` a fresh row so the first `entered` also flips
from "no row".) The method returns the new `transition_seq` and whether a row flipped
(`RowsAffected()==1` / a returned seq); **emit only when it flipped.**

**Move the emit into the tx.** Replace `s.pub.Publish` (`service.go:132`) with an insert into the
membership outbox in the **same tx** as the flip, so flip + emit commit atomically and the relay drains
at-least-once (finding #28). Rewrite `Service.Evaluate`'s per-segment block (`service.go:93-112`) to
open one tx per `(segment, profile)` transition: conditional flip + outbox insert + commit; drop the
separate `MembershipStatus` read (the conditional UPDATE subsumes it).

**Encode `transition_seq` into the emit + idempotency key** (findings #2, #27):

- `MembershipChanged.ReasonEventID` becomes a **unique per-flip token**: `"<event_id>:<seq>"` on the
  edge, `"sweep:<seq>"` on the sweep (Phase 5). Add a `TransitionSeq int64` field to
  `MembershipChanged` (`service.go:33`).
- Activation applies **last-writer-wins per `(tenant, segment, profile)` by `transition_seq`**: in
  `OnMembershipChanged` (`internal/activation/service.go:44`), before `CreateTask`, discard a change
  whose `TransitionSeq` is ≤ the last one applied for that membership (persist the high-water mark, or
  fold `transition_seq` into the idempotency key so a stale re-order is a distinct-but-ignored key).
  At minimum, the `transition_seq` is already in `ReasonEventID`, so
  `IdempotencyKey(...)` (`idempotency.go:15`) yields a distinct key per real flip — no enter/exit
  re-entry collision (finding #27).

### Tests

- **Testcontainers** `internal/segment/membership_test.go`:
  - conditional flip: two concurrent `Enter` calls on the same row produce **one** `transition_seq`
    bump and **one** outbox row (finding #2).
  - redundant transition (already `active`, evaluate matched again) writes **no** outbox row.
  - enter → exit → enter cycle produces three distinct `ReasonEventID`s / idempotency keys
    (finding #27); assert `activation` `CreateTask` returns `created=true` all three times.
  - crash-between-commit-and-publish is simulated by asserting the outbox row exists post-commit even
    if `Publish` is never called (relay will drain it) — finding #28.
- **Unit** activation: `IdempotencyKey` with two different `transition_seq` tokens differs; LWW
  discards a lower-seq change.

**Ship:** no double-emit, no lost emit, no idempotency collision. Today's engine is strictly more
correct even without any stateful rule.

---

## Phase 5 — Deadline queue + sweeper + population seed + safety sweep

Rationale: doc 16 §`due_at` computation and the sweep, §Tenant isolation, §Backfill, findings #6, #7,
#8, #11, #29, #32. This is the phase that fires **absence/expiry transitions with no inbound event** —
the flagship "did NOT purchase within 24h" finally works, including for dormant profiles.

### What exists today

- `activation.Runner` (`internal/activation/runner.go:14`) is the template: `Run` tickers
  (`runner.go:39`), `RunOnce` claims a batch (`runner.go:55`), `ClaimDueTasks` uses
  `FOR UPDATE SKIP LOCKED` (`internal/activation/repo.go:200`) — but with a **global** `ORDER BY
  ... LIMIT` and **no tenant predicate** (the fairness defect, finding #8).
- Config knobs `ActivationPollInterval`/`ActivationBatchSize` (`internal/config/config.go:28-29`,
  defaults `:59-60`) and their env parsing (`getEnvDuration`/`getEnvInt`).
- The pool is `pgxpool.New(ctx, url)` with **no `MaxConns`** (`internal/platform/database/db.go:14`) —
  pgx default `max(4, numCPU)` shared across all 9 goroutines + HTTP (finding #11).
- Worker goroutines: `wg.Add(8)` (`cmd/cdp-worker/main.go:164`), `activationRunner.Run` launched at
  `:166`.

### What to add

**Migration `migrations/00012_profile_behavior_bucket.sql`** — creates `segment_pending_eval` (the
bucket table also lives here but is exercised in Phase 6). Exact DDL in doc 16 §`00012`; load-bearing:

```sql
CREATE TABLE segment_pending_eval (
	tenant_id UUID NOT NULL REFERENCES tenant(id),
	segment_id UUID NOT NULL REFERENCES segment(id),
	customer_profile_id UUID NOT NULL,
	due_at TIMESTAMPTZ NOT NULL,
	reason TEXT NOT NULL,           -- absence_deadline|window_expiry|version_change|seed|safety_sweep
	claimed_at TIMESTAMPTZ,         -- NULL=claimable; time-boxed for crash recovery (finding #29)
	PRIMARY KEY (tenant_id, segment_id, customer_profile_id)
);
CREATE INDEX idx_segment_pending_due   ON segment_pending_eval (tenant_id, due_at) WHERE claimed_at IS NULL;
CREATE INDEX idx_segment_pending_claim ON segment_pending_eval (claimed_at)        WHERE claimed_at IS NOT NULL;
```

**Edge-path `due_at` UPSERT.** After evaluating each stateful segment in `Service.Evaluate`, compute
the earliest by-elapse flip instant across **all** behaviour leaves and UPSERT `segment_pending_eval`
(or delete if none). Formulas (doc 16 §`due_at`): `absence(E,W)`→`last_at(E)+W`; correlated
`absence`→`t_anchor+W`; `recency(E,W)`→`last_at(E)+W`; `count(E,K,W)`→ scan buckets/log newest→oldest
summing to `K`, `bucket_start+W`. **Coalesce** the UPSERT — skip when the new `due_at` is within one
bucket-granularity of the stored one (finding #12), and only recompute the count boundary at/near `K`
(finding #10).

**New `segment.Runner`** (`internal/segment/runner.go`, cloned from `internal/activation/runner.go:14`):

- `Run(ctx)` tickers on `SEGMENT_SWEEP_INTERVAL`; `RunOnce` claims a batch and re-evaluates each
  `(segment, profile)` at `at = now()` through the **same Phase-4 atomic flip + outbox emit**, with
  `ReasonEventID = "sweep:<transition_seq>"`.
- **Per-tenant-fair claim** (finding #8) — do **not** inherit activation's global claim. Use the
  `ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY due_at)` CTE from doc 16 §Tenant isolation with
  a `$per_tenant_cap`, and a **time-boxed reclaim** predicate
  `claimed_at IS NULL OR claimed_at < $now - $reclaim` (finding #29). On success DELETE the row or
  re-arm it (`claimed_at=NULL`, new `due_at`).
- **Unconditional re-arm** (finding #6): on every fire recompute the full next-flip instant across all
  leaves and UPSERT `due_at`; DELETE only when provably no future elapse transition. A no-op wake at
  T1 must not discard the later T2 deadline of a composite rule.
- Nil-safe hooks: `OnClaimed`, `OnTransition`, `OnSweepLag` (Phase 8 metrics).

**Population seed + safety sweep ship here** (findings #7, #32), not Phase 8:

- **Seed enumeration:** a one-shot job (called on stateful-segment activation and from `UpdateSegment`)
  enumerates candidate profiles and inserts `segment_pending_eval` (`reason='seed'`/`'version_change'`)
  so dormant "did-not-do" profiles and existing members are evaluated without an inbound event. For a
  pure-absence segment the candidate set is essentially all active tenant profiles — page over
  `customer_profile` by `(tenant_id, id)` ranges.
- **Safety-net full sweep:** a **rate-limited rolling cursor** (bounded rows/sec, spread over the
  retention window by tenant + profile-id range — **never** enqueue-all, finding #11) re-enqueues
  active stateful memberships so a mis-computed `due_at` self-heals.

**Pool + config + worker wiring:**

- Set an explicit `MaxConns` in `database.Connect` (`db.go:14`) via `pgxpool.ParseConfig` and reserve a
  small sweeper budget (finding #11).
- Add to `internal/config/config.go`: `SegmentSweepInterval` (default `5s`), `SegmentSweepBatchSize`
  (default `100`), `SegmentSweepPerTenantCap` (default `50`), `SegmentSweepReclaimTimeout`
  (default `1m`) — mirroring the activation knobs at `config.go:59-60`.
- `cmd/cdp-worker/main.go`: construct the `segment.Runner`, bump `wg.Add(8)`→`wg.Add(9)`
  (`main.go:164`), and launch `go func(){ defer wg.Done(); segmentRunner.Run(ctx) }()` beside
  `activationRunner.Run` (`main.go:166`).

### Tests

- **Unit** (fake clock, mirrors `internal/activation/breaker_test.go`): `due_at` formulas for
  absence/recency/count; composite `AND(count aging-down at T1, absence maturing at T2)` re-arms to T2
  after a no-op wake at T1 (finding #6); coalesce skips a near-identical `due_at`.
- **Testcontainers** `internal/segment/runner_test.go` (advance the injected instant):
  - absence: seed one `order_completed` at t0; assert **no** membership until `at ≥ t0+24h`, then the
    sweep flips to `entered` and writes the outbox row — with **no inbound event** (the headline case).
  - dormant seed: a profile that never emits the referenced event is seeded and evaluated (finding #32).
  - fairness: one tenant with many overdue rows does **not** starve a second tenant's due row in the
    same tick (finding #8).
  - crash recovery: a row with a stale `claimed_at` is reclaimed after `$reclaim` (finding #29).

**Ship:** absence/expiry transitions fire without an inbound event, including for dormant profiles;
the flagship rule works end-to-end.

---

## Phase 6 — Bucket rollup (hot path) + segment metadata + prefilter

Rationale: doc 16 §Performance, §`00012` bucket, §`00013` segment columns, findings #5, #10, #14, #15,
#16, #21, #30. Turns per-event evaluation from exact-log scans into `O(1)` upserts + bounded PK range
reads, and caches/pre-filters the segment fan-out — **without** the correctness regression of gating a
whole segment on `referenced_event_names`.

### What exists today

- Phase 2's `Recorder.Append` writes only `behavioral_event`. Phase 3's `Store` reads only the log.
- `ActiveSegmentVersions` (`internal/segment/repo.go:167`) fetches + `json.Unmarshal`s every active
  segment's rule per event (`service.go:75`) — uncached (finding #14).
- `CreateSegment`/`UpdateSegment` (`internal/segment/repo.go:72/108`) write `segment_version.rule_json`
  but no derived metadata.

### What to add

**Migration `migrations/00012` bucket table** (partitioned, doc 16 §`00012`):
`profile_behavior_bucket (tenant_id, customer_profile_id, event_name, bucket_start, count, first_at,
last_at, sum_value)` PK `(tenant_id, customer_profile_id, event_name, bucket_start)`,
`PARTITION BY RANGE (bucket_start)`, `CHECK (event_name <> '')`.

**Migration `migrations/00013` (rest)** — segment metadata, populated from the parsed rule:

```sql
ALTER TABLE segment_version ADD COLUMN is_stateful           BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE segment_version ADD COLUMN has_stateless_leaves  BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE segment_version ADD COLUMN referenced_event_names TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE segment_version ADD COLUMN max_window_seconds    BIGINT NOT NULL DEFAULT 0;
```

**Populate metadata** in `CreateSegment`/`UpdateSegment` (`repo.go:72/108`): add a
`analyzeRule(Rule) (isStateful bool, hasStateless bool, events []string, maxWindow time.Duration)`
walk in `internal/segment/dsl.go` and pass the four columns into the `INSERT INTO segment_version`
(`repo.go:93/128`).

**Bucket upsert** in `Recorder.Append` (in the same tx, only for track + non-empty event_name), the
exact SQL from doc 16 §Bucket upsert (`count+1`, `LEAST(first_at)`, `GREATEST(last_at)`,
`date_trunc('hour', $clamped_occurred_at)`). Add `OnBucketsUpserted` hook.

**Route non-exact leaves to buckets** in `Store`: add bucket-backed `Count`/`Recent`/`Absent`
implementations that `SUM(count)` / read `MAX(last_at)` landmarks over the bucket PK range; keep the
`behavioral_event` path for `Exact` leaves, sequences, `where`, `anchor`, and `value_prop+where`
(findings #16, #17). **Default equality/threshold counts to exact** (or subtract the boundary bucket's
out-of-window portion via `first_at`) so leading-edge over-inclusion cannot create false membership
(finding #5). `value_prop` frequency stays on the exact path unless a per-`event_name` canonical
value-property is configured (buckets are rule-agnostic and cannot know which property to sum) —
document this rather than silently summing the wrong thing.

**Leaf-level prefilter + per-tenant cache** (findings #14, #15, #30):

- Cache parsed `ActiveSegmentVersions` (rules + `referenced_event_names` + `has_stateless_leaves`) per
  tenant, invalidated on Create/Update/Activate (`repo.go:72/108`).
- **Never gate a whole segment** on `referenced_event_names`. Always evaluate every segment that has
  any stateless leaf (as today), because a trait-changing event can newly match it. Use
  `referenced_event_names` **only** to decide whether a behaviour *leaf* needs a store lookup, and
  `is_stateful` only to scope the sweeper. SQL pre-filter may narrow the fetch with
  `WHERE has_stateless_leaves OR $eventName = ANY(referenced_event_names)` — this keeps mixed/stateless
  segments correct (finding #15/#30).

**Extend the merge fold** (`internal/profile/repo.go:141`) to **rebuild** the survivor's affected
`(event_name, bucket_start)` buckets by re-aggregating from the now-deduped `behavioral_event` log —
**not** a blind `SUM` across profiles (finding #21) — and to fold/re-enqueue `segment_pending_eval`.

### Tests

- **Testcontainers** `internal/behavior/bucket_test.go`: upsert accumulates `count`/`first_at`/`last_at`
  correctly; bucket `Count` matches exact `Count` within ±1 bucket; equality/threshold defaults to
  exact and does not over-include at the leading edge (finding #5).
- Merge: bucket rebuild-from-log after a merge equals a fresh aggregation and does **not** double-count
  the shared `event_id` (finding #21).
- Prefilter: a mixed `country=US AND count>=3` segment with `referenced_event_names={product_viewed}`
  **still enters** on an identify event that flips `country` to US (finding #15/#30 regression lock).
- Cache invalidation: `UpdateSegment` makes the next `Evaluate` see the new rule.

**Ship:** `O(1)`-ish writes, bounded windowed reads, cached and correctly-prefiltered fan-out.

---

## Phase 7 — Governance erasure + segment lifecycle

Rationale: doc 16 §Governance, §Segment lifecycle, findings #22, #24, #25. The new tables carry PII
(`behavioral_event.props_json` is a second verbatim copy of raw event data) and reference
`customer_profile`/`segment`, so erasure and retirement must be extended or every forget/retire path
breaks.

### What exists today

- `governance.Service.Delete` (`internal/governance/governance.go:186`) is an **ordered manual
  cascade** (no `ON DELETE CASCADE` anywhere in the repo): `activation_delivery` → `activation_task` →
  `customer_profile_history` → `segment_membership` → `customer_consent` → `customer_profile`
  (`:244`) → identity nodes/clusters, returning `DeleteCounts` (`:173`). All in one tx.
- `UpdateSegment` (`internal/segment/repo.go:108`) only inserts a new version + repoints current — it
  enqueues **no** re-evaluation (finding #24).
- Segments have a `status` (`SegmentActive`, `internal/segment/repo.go:18`); `ActiveSegmentVersions`
  filters `status=active` (`repo.go:172`). There is no deactivate/delete path today (finding #25).

### What to add

**Erasure** — in `governance.Delete`, **before** the `customer_profile` delete (`governance.go:244`),
in the same tx + tenant/profile scope (doc 16 §Governance):

```sql
DELETE FROM segment_pending_eval    WHERE tenant_id=$1 AND customer_profile_id=$2;
DELETE FROM behavioral_event        WHERE tenant_id=$1 AND customer_profile_id=$2;
DELETE FROM profile_behavior_bucket WHERE tenant_id=$1 AND customer_profile_id=$2;
```

Add their counts to `DeleteCounts` (`governance.go:173`). Gate `props_json` capture under consent
purpose so behavioural capture cannot resurrect opted-out PII.

**Version-change recompute** (finding #24) — `UpdateSegment` (`repo.go:108`) enqueues
`segment_pending_eval` (`reason='version_change'`) for every current active member so the sweep
re-evaluates them against the new rule; loosened rules additionally run the Phase-5 candidate
enumeration so newly-qualifying profiles enter.

**Retire path** (finding #25) — add `DeactivateSegment`/`DeleteSegment` (or extend an existing status
setter): on retire, **purge** `segment_pending_eval` for that `segment_id` (orphan due-rows FK
`segment(id)` but the sweep can't resolve an inactive segment's rule), apply a membership-drain policy
(emit `exited` for active members, or freeze), and **keep** buckets (rule-agnostic, shared). Add a
sweep guard that deletes claimed due-rows whose segment is no longer active.

### Tests

- **Testcontainers** `internal/foundationtest/governance_test.go`: a profile with behaviour rows +
  buckets + a pending row is fully erased — `Delete` **commits** (no FK crash, finding #22) and
  `DeleteCounts` reports the new tables.
- `UpdateSegment` enqueues `version_change` rows for active members; the sweep exits a member who no
  longer matches the tightened rule (finding #24).
- Retire: deactivating a segment purges its `segment_pending_eval` and (per policy) drains members;
  the sweep does not mis-fire on a stranded due-row (finding #25).

**Ship:** right-to-be-forgotten and segment retirement are correct across the new tables.

---

## Phase 8 — Retention + schema-version + observability + docs

Rationale: doc 16 §Retention, §Observability, findings #9, #33. Operational completeness.

### What exists today

- Metrics live on `metrics.Metrics` over a private registry (`internal/platform/metrics/metrics.go:12`)
  and are wired as nil-safe hooks in the worker (`cmd/cdp-worker/main.go:127-128` for
  `SegmentEvaluated`/`SegmentMatched`).
- `behavioral_event`/`profile_behavior_bucket` are partitioned by time (Phases 2/6) — retention is
  `DROP PARTITION`, not a `DELETE`.
- doc `06-segmentation-engine.md:70-88` still defers Level 3.

### What to add

- **Retention job** (a low-frequency goroutine or the safety-sweep tick): create next week's partitions
  ahead of time and `DROP` partitions older than `max(max_window_seconds ever referenced) + margin`,
  keyed off **server `inserted_at`/partition range**, never client `occurred_at`, and never inside a
  live window (finding #9). Config `BEHAVIOR_RETENTION` (default e.g. `40d`). `UpdateSegment` warns /
  extends retention when a new version's `max_window_seconds` exceeds the retained horizon (finding #19).
- **Schema-version** (finding #33): `behavioral_event.schema_version` is already stamped (Phase 2);
  emit a `BehaviorSchemaDrift` metric when a live-window rule references a property whose
  `schema_version` changed, and require a new `segment_version` on a property-shape change.
- **Metrics** on `metrics.Metrics` (`metrics.go:12`) + nil-safe hooks (doc 16 §Observability):
  `SegmentStatefulEvaluated`, `SegmentStatefulMatched`, `BehaviorEventsAppended`,
  `BehaviorBucketsUpserted`, `SegmentSweepClaimed`, `SegmentSweepTransitions`, `SegmentSweepLagSeconds`
  (histogram of `now()-due_at` at claim), `SegmentPendingBacklog` (gauge), `SegmentPendingReclaimed`,
  `BehaviorReparentFolds`, `BehaviorRetentionPruned`, `BehaviorSchemaDrift`. Wire each to the
  corresponding hook already added in Phases 2/3/5/6.
- **Docs:** update `docs/cdp/06-segmentation-engine.md:70-88` to mark Level 3 shipped with the windowing
  support matrix (which kinds are bucket-served vs exact-only) and the sequence/correlated-absence
  limitations; link back to docs 16/17.

### Tests

- **Testcontainers**: retention drops an old partition and leaves in-window data untouched; a windowed
  read after a drop still returns the correct in-horizon count.
- **Unit**: metric hooks fire on their events (assert counters increment via the private registry).
- `BehaviorSchemaDrift` fires when a rule's referenced property changes `schema_version` mid-window.

**Ship:** operationally complete; doc 06 reflects reality.

---

## Cross-cutting testing gate

Doc 16 makes testing a **phase gate, not an afterthought**. Because every window read, `due_at`, and
boundary uses an injected instant (`$now`/`at`), the suite is table-driven over
boundary / late-event / redelivery / never-emitted cases with a fake clock (mirroring
`internal/activation/breaker_test.go`), plus testcontainers integration tests that **advance the
clock** to assert absence/expiry sweep firing (Phase 5) and merge tests asserting behaviour-row folds
commit (Phases 2, 6). No phase merges without its clock-driven tests.

## Anchor index (for the implementer)

`internal/segment/{dsl.go:39,dsl.go:57,eval.go:21,eval.go:42,eval.go:46,eval.go:68,service.go:33,service.go:57,service.go:66,service.go:79,service.go:85,service.go:89,service.go:93,service.go:117,service.go:132,repo.go:60,repo.go:72,repo.go:108,repo.go:167,repo.go:194,repo.go:209,repo.go:224,handler.go:44,handler.go:81}`,
`internal/profile/{service.go:35,service.go:61,service.go:106,service.go:148,service.go:199,repo.go:141,repo.go:145,repo.go:202,repo.go:210,repo.go:224}`,
`internal/activation/{runner.go:14,runner.go:39,runner.go:55,repo.go:183,repo.go:200,idempotency.go:15,breaker.go:16,service.go:44,service.go:75}`,
`internal/relay/relay.go:{37,73,101}`, `internal/bus/consumer.go:{43,45,74,78}`,
`internal/governance/governance.go:{173,186,244}`, `internal/events/envelope.go:{40,45,48,49}`,
`internal/config/config.go:{28,59}`, `internal/platform/database/db.go:14`,
`internal/platform/metrics/metrics.go:12`,
`cmd/cdp-worker/main.go:{85,114,126,127,164,166,265}`, `cmd/cdp-api/main.go:{99,141}`,
`migrations/{00002_event_outbox.sql,00006_customer_profile.sql:35,00007_segment.sql:{27,35},00008_activation.sql}`,
new `migrations/{00011_behavioral_event.sql,00012_profile_behavior_bucket.sql,00013_segment_stateful.sql}`,
new `internal/behavior/{recorder.go,store.go}`, new `internal/segment/runner.go`.
