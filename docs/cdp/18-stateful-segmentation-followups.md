# 18 — Level 3 Stateful Segmentation: Follow-Up Implementation Plans

This is the **detailed backlog** for the three follow-ups that
[17 — Implementation Plan](17-stateful-segmentation-implementation.md) shipped as *deferred*: the
8-phase engine is complete on `main` and the durable seed job is merged (that follow-up is now done),
leaving schema-version drift detection, poison-row dead-lettering, and bucket-accelerated
recency/absence. Each section is written in the doc-17 style — **what exists today** (with `file:line`
anchors into the real merged code), **what to add** (exact migrations, structs, SQL, wiring), and the
**tests** — so a later implementation turn is executable directly from here. Grounded against `main`
after PR #16 (migrations through `00017_segment_seed_job.sql`).

| Item | Doc-16 origin | Recommendation | Effort |
|---|---|---|---|
| **A — Schema-version drift detection** | finding #33 | **Implement** (stamp + observe; admin-in-the-loop) | Small–medium; no migration |
| **B — Poison-row dead-letter** | Phase-5 follow-up | **Implement** (attempts + exp-backoff + park-after-N) | Medium; one migration + admin surface |
| **C — Bucket recency/absence** | Phase-6 follow-up | **Decline** — evaluated net-negative; exact recipe recorded | None (2 regression-lock tests optional) |

---

## A — Schema-version drift detection (doc 16 finding #33)

**Rationale:** doc 16 finding #33 and doc 17 Phase 8 shipped `schema_version` on `behavioral_event` as
**DEFERRED** — the column exists but is never written or read, and doc 06 §Level 3 records it as a known
limitation. A `where` / `value_prop` rule references an event property; if that property's JSON
shape/type changes mid-window (e.g. `amount` was a `number`, becomes a `string`; or `price` was a
`number`, becomes an `{currency,value}` object), the windowed evaluation silently mixes incompatible
values and mis-computes membership. This follow-up closes the *silent* part of that gap: it stamps a
shape fingerprint at write time and makes in-window type drift **loud** (a metric + WARN log) on the
exact scan paths, without pretending to fully repair an already-mixed window.

### What exists today (verified)

- `behavioral_event.schema_version INT NOT NULL DEFAULT 1` is declared in
  `migrations/00011_behavioral_event.sql:13` (comment "detect event-shape drift across a live window").
  It is **never populated**: `Recorder.Append`'s INSERT lists only
  `(tenant_id, customer_profile_id, event_id, event_name, occurred_at, props_json)`
  (`internal/behavior/recorder.go:78-85`) — so every row defaults to `1`.
- The consent gate can null `props` before the insert (`recorder.go:69-77`); a dropped-props row
  therefore has `props_json IS NULL`.
- `Store` is a bare `struct{ pool *pgxpool.Pool }` with **no hook fields** (`internal/behavior/store.go:24-29`).
- The two paths that read `props_json` for a rule-referenced property never look at shape:
  - `Count` where-scan selects `props_json` and runs `spec.WhereMatch` in Go, capped by `maxWhereScan`
    (`store.go:51-75`).
  - `SumValue` value_prop path is a single SQL aggregate with a **lenient numeric guard**:
    `SUM(CASE WHEN jsonb_typeof(props_json->$3)='number' THEN (props_json->>$3)::numeric ELSE 0 END)`
    (`store.go:173-176`) — value_prop type drift already degrades to an under-count (non-numeric rows
    contribute 0) rather than crashing; it is just invisible. The value_prop-with-where variant scans
    rows and sums in Go (`store.go:179-209`).
- `Spec` is the DSL-agnostic storage view; it already carries a `WhereMatch func(json.RawMessage) bool`
  closure so the store never imports DSL types (`internal/behavior/spec.go:15-30`).
- `toSpec` builds the `Spec` and sets `spec.Exact` for `where`/`anchor`/`value_prop`
  (`internal/segment/eval.go:131-170`, the `WhereMatch` closure at `:155-161`, value_prop→Exact at `:135`).
- Metrics live on `metrics.Metrics` over a private registry; there is **no** `SchemaDrift` field
  (`internal/platform/metrics/metrics.go:12-48`, constructed + `MustRegister`ed in `New`). Nil-safe hooks
  are attached in the worker, e.g. `segmentRunner.OnSweepLag = m.SweepLagSeconds.Observe`
  (`cmd/cdp-worker/main.go:156`).
- The store is constructed **inline** and passed straight into the service:
  `segment.NewService(pool, profile.NewRepo(pool), behavior.NewStore(pool))` (`cmd/cdp-worker/main.go:137`)
  — not held in a variable, so today there is nowhere to attach a hook.

### Recommended design (pragmatic minimum that closes the *silent* gap)

**(a) Stamp — shape fingerprint, not a registry.** Reject a per-`(tenant,event_name,property)` shape
registry: it needs a read-modify-write per event inside the hot profile-update tx, adds row contention,
and buys nothing detection needs. Use a **pure in-process fingerprint of the stored props' top-level
shape** (property name → JSON type map), computed in `Recorder.Append` with no extra DB round-trip and
stamped into the existing `schema_version` column. Deterministic (redelivery recomputes the same value),
idempotent, **no migration**. The column name becomes a slight misnomer (it holds a shape fingerprint,
not a monotonic version); acceptable because nothing else reads it — noted in a comment.

**(b) Detect — property-level, recomputed at read time, not from the stamped int.** The live decision
compares the JSON type of the **rule-referenced property** across in-window rows via `jsonb_typeof`,
**not** `COUNT(DISTINCT schema_version)`. This is deliberate: comparing the stamped int would fire a
false positive for the entire `maxWindow` after first deploy (old rows are all `1`, new rows carry a
fingerprint). Recomputing the referenced property's type from `props_json` is precise, DSL-agnostic, and
immune to that deploy-boundary artifact. The stamped column is retained for audit / a future cheap SQL
detector, but the shipping detector does not depend on its history.

**(c) Metric** `BehaviorSchemaDrift` on `metrics.Metrics` + a nil-safe `OnSchemaDrift` hook on `Store`,
mirroring `SweepLagSeconds`/`OnSweepLag`.

**(d) Enforcement — admin-in-the-loop, NOT automatic.** Reject "automatically require a new
segment_version on shape change": a new version does not un-mix a window that already spans both shapes,
and silent auto-re-versioning is surprising. Reject "hard-fail the read under drift": the evaluator
threads store errors out to fail the handler for at-least-once retry (`internal/segment/service.go:142-147`),
so a *persistent* drift would poison-retry forever, and treating drift as non-match silently suppresses
real matches. Recommend **observe, don't block**: emit the metric + a WARN log so an operator republishes
the segment_version (which re-baselines going forward via the existing edge + sweep re-evaluation) or
fixes the producer. Matches the system's stated philosophy for windowing slack — "communicated to
authors and surfaced at admin time" (doc 16 §Risks).

Net: **no migration**, a ~6-line recorder stamp, a DSL-agnostic prop-name list on `Spec`, one nil-safe
hook + one cheap SQL probe per referenced prop on the exact scan paths, one metric, and a doc note.

### What to add

**No migration.** Reuse `behavioral_event.schema_version` (`migrations/00011_behavioral_event.sql:13`).

**`internal/behavior/recorder.go` — stamp the shape fingerprint.** Add a pure helper and include it in
the INSERT. Compute over the **stored** props (post-consent-gate), so a dropped-props row stamps the
empty fingerprint and never manufactures drift:

```go
// propsShapeVersion is a stable 31-bit fingerprint of the TOP-LEVEL shape of a
// props object: the sorted set of (key -> JSON type) pairs. Same shape -> same
// value; a key changing JSON type (number->string, number->object, ...) -> a
// different value. Empty/absent/non-object props map to 1, matching the column
// DEFAULT so pre-stamp rows and genuinely-empty rows agree. Key order independent.
// It does NOT distinguish two numbers of different magnitude/unit (both "number") —
// unit drift is undetectable by shape and is a documented limitation.
func propsShapeVersion(raw []byte) int32 {
    if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
        return 1
    }
    var m map[string]json.RawMessage
    if json.Unmarshal(raw, &m) != nil || len(m) == 0 {
        return 1 // non-object (array/scalar) or unparseable: shape-neutral
    }
    keys := make([]string, 0, len(m))
    for k := range m { keys = append(keys, k) }
    sort.Strings(keys)
    h := fnv.New32a()
    for _, k := range keys {
        fmt.Fprintf(h, "%s:%s;", k, jsonKind(m[k])) // jsonKind -> object|array|string|number|bool|null
    }
    v := int32(h.Sum32() & 0x7fffffff) // fit a positive signed INT
    if v == 0 { v = 1 }
    return v
}
```

Extend the existing INSERT (`recorder.go:78-85`) to write `schema_version = propsShapeVersion(propsBytes)`
where `propsBytes` is the post-gate stored bytes (`nil` when the gate dropped props → fingerprint 1).
`jsonKind(json.RawMessage) string` is a trivial first-non-space-byte switch (`{`→object, `[`→array,
`"`→string, `t`/`f`→bool, `n`→null, else number). The compute is pure Go, so it stays inside the
profile-update tx with zero added round-trips or contention.

**`internal/behavior/spec.go` — carry the referenced prop names (DSL-agnostic).** Add to `Spec`:

```go
// DriftProps names the TOP-LEVEL props this leaf references (value_prop + the
// event.properties.* fields of a where filter). When non-empty, the exact scan
// paths probe whether any of them shows >1 distinct JSON type in-window and fire
// OnSchemaDrift. Empty => no drift probe (recency/absence/plain count pay nothing).
DriftProps []string
```

**`internal/behavior/store.go` — hook + detector.** Add the hook field to `Store` and a bounded probe,
called from `Count` (when `spec.WhereMatch != nil`) and both `SumValue` branches:

```go
type Store struct {
    pool *pgxpool.Pool
    // OnSchemaDrift (nil-safe) fires once per windowed evaluation when a referenced
    // property changed JSON type across the window (finding #33).
    OnSchemaDrift func()
}

// checkDrift fires OnSchemaDrift if any of props shows >1 distinct jsonb_typeof over
// [from, at] for this event. One bounded, indexed range query per referenced prop;
// deterministic in `at` (never now()). Rows with props_json NULL or lacking the key
// are excluded, so consent-dropped rows never manufacture drift.
func (s *Store) checkDrift(ctx context.Context, tenantID, profileID uuid.UUID, eventName string, props []string, from, at time.Time) {
    if s.OnSchemaDrift == nil || len(props) == 0 {
        return
    }
    for _, p := range props {
        var kinds int
        if err := s.pool.QueryRow(ctx, `
            SELECT count(*) FROM (
                SELECT DISTINCT jsonb_typeof(props_json->$3) AS t
                FROM behavioral_event
                WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$4
                  AND occurred_at >= $5 AND occurred_at <= $6
                  AND props_json ? $3
            ) x WHERE t IS NOT NULL`,
            tenantID, profileID, p, eventName, from, at).Scan(&kinds); err != nil {
            return // drift probe is best-effort; never fail the real evaluation
        }
        if kinds > 1 {
            s.OnSchemaDrift()
            return // one signal per evaluation is enough
        }
    }
}
```

Call `s.checkDrift(ctx, tenantID, profileID, spec.EventName, spec.DriftProps, from, at)` before the
`return` in `Count` (`store.go:51-75`) and in **both** `SumValue` branches (`store.go:167-210`) — the
latter covers the exact case the lenient guard at `store.go:173-176` silently absorbs. It runs only when
`DriftProps` is populated, so plain count / recency / absence pay nothing.

**`internal/segment/eval.go` — populate `DriftProps` in `toSpec`** (`eval.go:131-170`):

```go
if b.ValueProp != "" {
    spec.DriftProps = append(spec.DriftProps, b.ValueProp)
}
if b.Where != nil {
    spec.DriftProps = append(spec.DriftProps, referencedPropNames(b.Where)...)
}
```

with `referencedPropNames(r *Rule) []string` walking the where sub-rule, taking the top-level segment of
any `event.properties.*` field (`event.properties.price` → `price`; `event.properties.a.b` → `a`,
dedup). Nested paths collapse to their top-level container — a documented limitation.

**`internal/platform/metrics/metrics.go` — the metric.** Add a `SchemaDrift prometheus.Counter` field,
construct it in `New`, and add it to `MustRegister`:

```go
SchemaDrift: prometheus.NewCounter(prometheus.CounterOpts{
    Name: "behavior_schema_drift_total",
    Help: "Windowed evaluations where a rule-referenced event property changed JSON type across the window.",
}),
```

**`cmd/cdp-worker/main.go` — wiring.** Hoist the store into a variable so the hook attaches (today inline
at `main.go:137`), mirroring `behaviorRec`:

```go
behaviorStore := behavior.NewStore(pool)
behaviorStore.OnSchemaDrift = m.SchemaDrift.Inc
segmentSvc := segment.NewService(pool, profile.NewRepo(pool), behaviorStore)
```

If a WARN log line is wanted, widen the hook to `OnSchemaDrift func(tenantID uuid.UUID, event, prop string)`
and have the worker both `Inc` and `logger.Warn`; the pragmatic minimum keeps the bare hook + an alert.

### Tests

**Unit (pure, no container):**
- `recorder_shape_test.go` — table over `propsShapeVersion`: identical shape / key reorder → equal;
  `{"a":1}` vs `{"a":"x"}` → different; `{"a":1}` vs `{"a":{"v":1}}` → different; `""`, `"null"`, `[]`,
  `"scalar"`, `{}` → `1`; result fits a positive int32.
- `eval_drift_test.go` — `referencedPropNames` extracts `price` from `event.properties.price`, `a` from
  `event.properties.a.b`, dedups, ignores `profile.*`; `toSpec` sets `DriftProps` for value_prop + where.
- `metrics_test.go` — `m.SchemaDrift.Inc()` then scrape the private registry → `behavior_schema_drift_total 1`.

**Testcontainers (fake clock via explicit `at`, using `newPool`/`seedTenant`/`seedEvent` from
`internal/behavior/store_test.go:27-61`):**
- `TestStore_SchemaDrift_ValueProp`: seed `purchase` rows in one 7d window with `{"amount":10}` (early)
  and `{"amount":"10"}` (late); `SumValue` with `DriftProps:["amount"]` → hook fires **once** and the sum
  equals the numeric-only rows (locks the graceful-degradation contract of `store.go:173-176`).
- `TestStore_SchemaDrift_Homogeneous`: all rows `{"amount":<number>}` → hook **not** fired.
- `TestStore_SchemaDrift_Where`: a where on `event.properties.price` numeric-then-object → hook fires
  once; `Count` still returns without crashing.
- `TestStore_SchemaDrift_OutOfWindowIgnored`: shape change strictly before `at-Window` → **no** drift
  (regression lock on the injected instant).
- `TestStore_SchemaDrift_ConsentDroppedRows`: `props_json NULL` rows mixed with numeric → not counted as
  a distinct type (`props_json ? $prop` excludes them).
- `internal/foundationtest/behavior_test.go` (extend): two track events of differing shape → the two
  `behavioral_event` rows carry **distinct** `schema_version`; empty props stamps `1`.

### Risks / rejected alternatives

- **Deploy-boundary false positives — avoided by design** (detection recomputes `jsonb_typeof`, not
  `COUNT(DISTINCT schema_version)`).
- **Unit/semantic drift is undetectable by shape** (cents→dollars, both `number`). Out of scope;
  documented residual limitation.
- **Nested where paths** collapse to their top-level container; deep-path probing is a later refinement.
- **Cost:** one `SELECT DISTINCT jsonb_typeof(...)` range query per referenced prop, on the already-exact
  path, skipped when `DriftProps` is empty; `checkDrift` swallows its own errors (best-effort).
- **Determinism:** the probe uses the same `from`/`at` binds as the evaluation (no `now()`), consistent
  with clock injection (finding #26).
- **Rejected:** automatic re-versioning (can't un-mix an in-flight window); hard-fail/non-match under
  drift (poison-retry or silent suppression); per-property registry table (hot-path RMW contention).

**Ship:** `schema_version` is stamped; `where`/`value_prop` windows that span an event-property type
change raise `behavior_schema_drift_total` (+ a WARN) instead of silently mis-evaluating; value_prop
drift keeps its existing under-count degradation. Update doc 06 §Level 3 and doc 16 finding #33 from
"DEFERRED" to "detection shipped; window-repair and unit-drift remain known limitations."

---

## B — Poison-row dead-letter for the deadline sweeper (doc 17 Phase 5)

**Rationale:** doc 17 Phase 5 "Known trade-offs / follow-ups", the **Poison rows** bullet
(`17-…:592-594`): a `(segment, profile)` `segment_pending_eval` row whose `SweepEvaluate` persistently
errors is deferred with a *fixed* backoff (`DeferPending(now + reclaim)`) so it neither tight-loops nor
starves its tenant's fair-claim slots, "but there is no attempt counter / dead-letter yet — it retries
indefinitely." This designs that: an `attempts` counter, an **exponential** backoff, and a **park**
(dead-letter) marker that excludes a chronically-failing row from the fair claim, surfaces it
(metric + `last_error`), and lets an operator manually retry it.

### What exists today

- The sweeper's error branch is a **fixed** defer. `Runner.RunOnce` (`internal/segment/runner.go:83-96`)
  calls `s.svc.SweepEvaluate`; on error it fires the nil-safe `OnError` hook, logs, then
  `r.svc.Repo().DeferPending(ctx, …, now.Add(r.reclaim))` (`runner.go:92`) and `continue`s. The deferred
  `due_at` is always `now + reclaim` (one minute default) with **no attempt tracking and no ceiling** — a
  permanently poisoned row re-claims every minute forever.
- `Repo.DeferPending` (`repo.go:350-358`) is a bare `UPDATE … SET due_at=$4, claimed_at=NULL WHERE
  (tenant,segment,profile)`. It clears `claimed_at`, so a deferred row is scheduled purely by `due_at`,
  never by the time-boxed reclaim (the two timers do not overlap).
- The claim is `Repo.ClaimDuePending` (`repo.go:390-426`): a per-tenant-fair CTE (`ranked` →
  `ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY due_at)`, `picked` capped at `$per_tenant_cap`,
  `locked` re-checks under `FOR UPDATE SKIP LOCKED`), whose claimability predicate is
  `due_at <= $now AND (claimed_at IS NULL OR claimed_at < $now-$reclaim)` in **both** the `ranked`
  (`repo.go:396`) and `locked` (`repo.go:406`) CTEs.
- `Repo.PendingBacklog` (`repo.go:321-330`) is the sweeper-lag SLI gauge — same claimability predicate;
  `Runner.RunOnce` sets it each tick via `OnBacklog` (`runner.go:65-71`).
- Other row writers: `UpsertPendingTx` (`repo.go:334-345`, edge re-arm, `ON CONFLICT DO UPDATE SET
  due_at, reason, claimed_at=NULL`), `SeedPendingForSegment`/`SeedJobPage` (`repo.go:437/520`), the
  `version_change` enqueue (`repo.go:162-170`), and `SafetyReEnqueue` (`repo.go:584-601`,
  `NOT EXISTS … ON CONFLICT DO NOTHING`). `DeactivateSegment` **deletes** all pending rows for a segment
  (`repo.go:276-279`).
- Migration `migrations/00013_segment_pending_eval.sql` — columns `tenant_id, segment_id,
  customer_profile_id, due_at, reason, claimed_at`, PK `(tenant_id, segment_id, customer_profile_id)`,
  partial index `idx_segment_pending_due ON (tenant_id, due_at) WHERE claimed_at IS NULL` (line 17) and
  `idx_segment_pending_claim ON (claimed_at) WHERE claimed_at IS NOT NULL` (line 19). Latest on disk:
  `00017_segment_seed_job.sql`.
- Metric wiring to mirror: `SweepError` counter `segment_sweep_error_total`
  (`metrics.go:32,108-110`), `PendingBacklog` gauge `segment_pending_backlog` (`metrics.go:34,117-119`),
  both in the `reg.MustRegister(...)` call. Worker binds them in `cmd/cdp-worker/main.go:151-157`.
- Config knobs mirror `SegmentSweep*` (`internal/config/config.go:31-35` struct, `:75-79` defaults),
  parsed by `getEnvInt`/`getEnvDuration`.
- Admin surface: `segment.Handler` (`internal/segment/handler.go:15-21`, `repo *Repo`), routes in
  `cmd/cdp-api/main.go:142-146` under `auth.Require(rbac.PermSegment{Write,Read})`.
- Test harness: `internal/foundationtest/sweep_test.go` — `insertPending` (`:21-27`), fake-clock runner
  `segment.NewRunner(...).WithClock(...)` (`:63-64`), fairness/reclaim assertions (`:216-273`).
  `BehaviorStore` is an interface (`internal/segment/eval.go:27-37`) so a failing fake store can be
  injected via `segment.NewService(pool, profiles, store)`.

### What to add

**(a) Migration `migrations/00018_segment_pending_park.sql`.**

```sql
-- +goose Up
-- Phase 5 follow-up (doc 17 "Poison rows"): dead-letter a segment_pending_eval row whose
-- SweepEvaluate persistently errors. attempts drives exponential backoff; parked_at marks a
-- row past the retry ceiling so the fair claim excludes it (it stops churning and stops
-- sorting ahead of healthy rows) until an operator retries it.
ALTER TABLE segment_pending_eval ADD COLUMN attempts   INT  NOT NULL DEFAULT 0;
ALTER TABLE segment_pending_eval ADD COLUMN last_error TEXT;                 -- NULL until first failure
ALTER TABLE segment_pending_eval ADD COLUMN parked_at  TIMESTAMPTZ;          -- NULL = live; non-NULL = dead-lettered

DROP INDEX idx_segment_pending_due;
CREATE INDEX idx_segment_pending_due
    ON segment_pending_eval (tenant_id, due_at)
    WHERE claimed_at IS NULL AND parked_at IS NULL;

CREATE INDEX idx_segment_pending_parked
    ON segment_pending_eval (tenant_id, segment_id)
    WHERE parked_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_segment_pending_parked;
DROP INDEX IF EXISTS idx_segment_pending_due;
CREATE INDEX idx_segment_pending_due ON segment_pending_eval (tenant_id, due_at) WHERE claimed_at IS NULL;
ALTER TABLE segment_pending_eval DROP COLUMN parked_at;
ALTER TABLE segment_pending_eval DROP COLUMN last_error;
ALTER TABLE segment_pending_eval DROP COLUMN attempts;
```

`DEFAULT 0` / `DEFAULT NULL` make this a metadata-only, instant `ALTER` — no table rewrite; existing rows
and current writers keep working.

**(b) Repo `FailPending`** (replaces the fixed defer in the runner error branch). One atomic statement
bumps `attempts`, records the truncated error, and branches on the post-increment count: park (set
`parked_at`, leave `due_at`, `claimed_at=NULL`) at the ceiling, else schedule an exponential backoff.

```go
// FailPending records a failed sweep of (segment, profile): it bumps attempts, stores the
// (truncated) error, and either backs the row off exponentially or — once attempts reach
// maxAttempts — PARKS it (parked_at set) so ClaimDuePending stops re-claiming it. It always
// clears claimed_at so the row is scheduled purely by due_at (never simultaneously by the
// time-boxed reclaim). Returns the new attempt count and whether the row is now parked.
func (r *Repo) FailPending(ctx context.Context, tenantID, segmentID, profileID uuid.UUID,
    now time.Time, errMsg string, base, cap time.Duration, maxAttempts int) (attempts int, parked bool, err error) {

    const maxErrLen = 500
    if len(errMsg) > maxErrLen { errMsg = errMsg[:maxErrLen] }
    err = r.pool.QueryRow(ctx, `
        UPDATE segment_pending_eval SET
            attempts   = attempts + 1,
            last_error = $5,
            claimed_at = NULL,
            parked_at  = CASE WHEN attempts + 1 >= $6 THEN $4 ELSE NULL END,
            -- back off exponentially on the PRE-increment attempts: base*2^0, base*2^1, … capped.
            due_at     = CASE WHEN attempts + 1 >= $6 THEN due_at
                              ELSE $4 + LEAST($8, ($7 * power(2, attempts))::bigint) * interval '1 second'
                         END
        WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3
        RETURNING attempts, parked_at IS NOT NULL`,
        tenantID, segmentID, profileID, now, errMsg, maxAttempts,
        int64(base.Seconds()), int64(cap.Seconds())).Scan(&attempts, &parked)
    if err != nil { return 0, false, fmt.Errorf("fail pending: %w", err) }
    return attempts, parked, nil
}
```

The backoff uses the **pre-increment** `attempts` (first failure `now+base`, second `now+2·base`, …
capped at `cap`); parking keeps the last `due_at` (useful in the admin view) — exclusion is by
`parked_at`, not `due_at`. Backoff is computed in SQL (not Go) because the exponent lives in the row —
a Go computation is a read-modify-write race under concurrent sweepers.

**Exclude parked** from the fair claim + backlog SLI: add `AND parked_at IS NULL` to **both** the
`ranked` (`repo.go:396`) and `locked` (`repo.go:406`) WHERE of `ClaimDuePending`, and to
`PendingBacklog` (`repo.go:324`). Parked rows leave the fair-claim ordering entirely (no longer sorting
ahead of healthy rows by their old `due_at`) and drop out of the lag SLI.

**Surfacing + manual retry** repo methods:

```go
func (r *Repo) ParkedCount(ctx context.Context) (int64, error) // dead-letter depth, for the gauge

type ParkedRow struct {
    SegmentID, CustomerProfileID uuid.UUID
    Reason, LastError            string
    Attempts                     int
    DueAt, ParkedAt              time.Time
}
// ListParked: SELECT … WHERE tenant_id=$1 AND parked_at IS NOT NULL ORDER BY parked_at DESC LIMIT $2
func (r *Repo) ListParked(ctx context.Context, tenantID uuid.UUID, limit int) ([]ParkedRow, error)

// UnparkPending: SET parked_at=NULL, attempts=0, last_error=NULL, due_at=$now, claimed_at=NULL
//   WHERE tenant_id … AND parked_at IS NOT NULL RETURNING true
func (r *Repo) UnparkPending(ctx context.Context, tenantID, segmentID, profileID uuid.UUID, now time.Time) (bool, error)
```

**(c) Runner: exponential backoff + park in the error branch, park metric hooks.** Add fields + hooks to
`Runner` (`runner.go:14-40`):

```go
type Runner struct {
    // …existing…
    backoffBase time.Duration // first-failure backoff (default 30s)
    backoffCap  time.Duration // backoff ceiling (default 1h)
    maxAttempts int           // park after this many failures (default 10)

    OnParked        func()          // a row crossed into the dead-letter (mirrors OnError)
    OnParkedBacklog func(depth int) // current parked-row depth, per tick (mirrors OnBacklog)
}
```

Thread the policy via a `WithParkPolicy(base, cap time.Duration, max int) *Runner` builder (mirrors
`WithClock`, `runner.go:43`) — **preferred** over widening `NewRunner`'s signature, which would churn all
8 call-sites. Rewrite the error branch (`runner.go:83-96`):

```go
if err := r.svc.SweepEvaluate(ctx, row, now); err != nil {
    if r.OnError != nil { r.OnError() }
    r.logger.Error("sweep eval failed", "tenant", row.TenantID.String(),
        "segment", row.SegmentID.String(), "error", err.Error())
    attempts, parked, ferr := r.svc.Repo().FailPending(ctx,
        row.TenantID, row.SegmentID, row.CustomerProfileID, now,
        err.Error(), r.backoffBase, r.backoffCap, r.maxAttempts)
    if ferr != nil && ctx.Err() == nil {
        r.logger.Error("fail pending failed", "error", ferr.Error())
    } else if parked {
        if r.OnParked != nil { r.OnParked() }
        r.logger.Warn("sweep deadline parked (dead-letter)",
            "tenant", row.TenantID.String(), "segment", row.SegmentID.String(),
            "profile", row.CustomerProfileID.String(), "attempts", attempts, "error", err.Error())
    }
    continue
}
```

Set the parked-depth gauge each tick beside the backlog gauge (`runner.go:65-71`), guarded by
`OnParkedBacklog != nil`, calling `Repo.ParkedCount`.

**Metrics** (mirror `SweepError`/`PendingBacklog`): counter `segment_sweep_parked_total` (field
`SweepParked`) and gauge `segment_pending_parked` (field `PendingParked`), declared + registered in
`metrics.go`. `last_error` is **not** a metric label (unbounded → cardinality); it is surfaced via the
admin endpoint + the parked-warn log.

Worker wiring after the existing hooks (`cmd/cdp-worker/main.go:151-157`):

```go
segmentRunner = segmentRunner.WithParkPolicy(cfg.SegmentSweepBackoffBase,
    cfg.SegmentSweepBackoffCap, cfg.SegmentSweepMaxAttempts)
segmentRunner.OnParked = m.SweepParked.Inc
segmentRunner.OnParkedBacklog = func(n int) { m.PendingParked.Set(float64(n)) }
```

Config (`config.go:31-35` struct, `:75-79` defaults), mirroring `SegmentSweep*`:

```go
SegmentSweepBackoffBase time.Duration // getEnvDuration("SEGMENT_SWEEP_BACKOFF_BASE", 30*time.Second)
SegmentSweepBackoffCap  time.Duration // getEnvDuration("SEGMENT_SWEEP_BACKOFF_CAP", time.Hour)
SegmentSweepMaxAttempts int           // getEnvInt("SEGMENT_SWEEP_MAX_ATTEMPTS", 10)
```

Base 30s doubling to a 1h cap over ~10 attempts sums to **many hours** of retry before parking, so a
transient dependency blip self-heals via backoff and only a genuinely persistent poison reaches the
dead-letter.

**(c′) Surfacing + manual retry (admin).** Add two handlers to `internal/segment/handler.go` and register
them in `cmd/cdp-api/main.go:142-146`:

- `GET /admin/v1/tenants/{tenantID}/segments/{segmentID}/pending/parked` (`PermSegmentRead`) →
  `repo.ListParked(tenant, limit)` JSON (`{segment_id, customer_profile_id, reason, attempts,
  last_error, due_at, parked_at}`).
- `POST /admin/v1/tenants/{tenantID}/segments/{segmentID}/pending/{profileID}/retry`
  (`PermSegmentWrite`) → `repo.UnparkPending(...)`; 404 if not parked, 204 on success. The next tick
  re-claims it with `attempts=0`.

### Interactions / correctness subtleties

- **No double-counted lag vs. the time-boxed reclaim.** `FailPending` always sets `claimed_at=NULL`, so
  a backing-off row is scheduled *only* by `due_at`, never *also* by the reclaim path — the two timers
  never overlap. The eventual successful claim's `OnSweepLag = now - due_at` measures the last backoff
  interval, not accumulated poison-retry time (that lives in `attempts`); the histogram is not inflated.
- **A genuine new signal must not be silently swallowed by a park.** `SafetyReEnqueue` uses
  `ON CONFLICT DO NOTHING` (`repo.go:595`) → leaves a parked row parked (correct; the safety net must not
  auto-resurrect churn). But the *active* re-arm writers (edge `UpsertPendingTx`, `version_change`, seed
  pages) represent a real new evaluation → extend their `ON CONFLICT DO UPDATE SET …` to clear the
  dead-letter: `parked_at=NULL, attempts=0, last_error=NULL` (at `repo.go:339`, `:167`, `:450/530`).
  **Coalesce hazard:** the edge path may *skip* the upsert when the new `due_at` is within
  `coalesceGranularity` of the stored one (`service.go:248-252`, `planDeadline`), leaving a parked row
  parked despite a fresh edge event. Fix by having `CurrentDueAt` (`repo.go:373`) also return `parked
  bool` and `planDeadline` force `arm=true` when parked (never coalesce a parked row). Small but
  load-bearing.
- **Retire path already safe.** `DeactivateSegment` deletes all pending rows for the segment
  (`repo.go:276-279`), parked included.
- **Idempotent/at-least-once sweeper.** `FailPending` is a single PK-keyed UPDATE; a re-claimed row
  (crash between error and `FailPending`) bumps `attempts` once more — over-counting by at most one per
  crash, only slightly shortening the road to park. Acceptable.

### Rejected alternatives

- **Park via a far-future `due_at` (no `parked_at`).** Corrupts the two SLIs that read `due_at`
  (`OnSweepLag` goes massively negative; a far-future row still sorts in `ranked`) and is not cleanly
  queryable. An explicit `parked_at IS NOT NULL` is unambiguous, indexable, orthogonal to `due_at`.
- **A separate `segment_pending_dead_letter` table.** Doubles the write path (move-row on park/retry) for
  a rare row; an in-place marker + partial index is cheaper and keeps the claim a single-table scan.
- **Computing backoff in Go.** The attempt count lives in the row → a Go computation is a RMW race under
  concurrent sweepers.
- **A `status` enum (`live|parked`).** Strictly less information than `parked_at` (which doubles as
  *when*, usable for age alerting).

### Tests

**Unit** (`internal/segment/`, fake clock): a `backoffFor(attempts, base, cap)` helper mirroring the SQL
`CASE`, asserted to double per attempt and clamp at `cap` (documented as the source-of-truth mirror).

**Testcontainers** `internal/foundationtest/sweep_park_test.go` (mirror `sweep_test.go`). Inject a
**failing** `BehaviorStore` (a fake `errStore` implementing `eval.go:27-37`, every method returning a
sentinel error) via `segment.NewService(f.pool, profile.NewRepo(f.pool), errStore)` so `SweepEvaluate` of
an `absenceRule()` segment errors deterministically:

1. **Parks after N failures.** Insert a due row; `WithParkPolicy(1*time.Second, 8*time.Second, 3)` + an
   advancing clock. Loop `RunOnce`, each time past the row's backoff `due_at`; assert not parked for the
   first `maxAttempts-1` runs (`attempts` incrementing via a direct SELECT), and after the N-th failure
   `parked_at IS NOT NULL`, `attempts=3`, `last_error`==sentinel, `OnParked` fired exactly once.
2. **Backoff grows.** Between successive failed runs the stored `due_at` delta from `now` roughly doubles
   (1s → 2s → 4s).
3. **Parked row is not claimed.** After parking, `ClaimDuePending(now+1h, …)` returns **zero** rows for
   that profile though `due_at <= now`; `PendingBacklog` excludes it; `ParkedCount() == 1`.
4. **Healthy row unaffected.** A second, non-poison profile in the same tenant is claimed and transitions
   in the same tick a sibling parks — one poison `(segment,profile)` never blocks/parks neighbors and,
   once parked, frees the tenant's fair-claim slot.
5. **Manual retry.** `UnparkPending` returns found; the row now has `parked_at IS NULL, attempts=0,
   last_error IS NULL, due_at≈now`; a subsequent `ClaimDuePending` re-claims it (`ListParked` empty).
6. **Reclaim isolation.** A backing-off row (future `due_at`, `claimed_at=NULL`) is *not* picked up by
   the time-boxed reclaim before its `due_at` — a claim in the backoff gap returns zero.

**Ship:** a persistently-failing `(segment, profile)` deadline backs off exponentially and, past the
ceiling, dead-letters itself — stops churning, stops distorting the lag/backlog SLIs, stops crowding the
tenant's fair-claim slots — while remaining visible (`segment_sweep_parked_total` +
`segment_pending_parked` + the admin `pending/parked` list with `last_error`) and manually recoverable
(`POST …/retry`). Healthy rows are unaffected.

---

## C — Bucket-accelerated recency/absence (doc 17 Phase 6) — evaluated & declined

**Decision: DO NOT implement. Keep recency/absence on the single-seek `MAX(occurred_at)` index lookup.**
The exact bucket recipe is specified and proven correct below, but it is *net-negative* on cost — it
trades the current `O(1)` index seek for an `O(window/1h)` bucket range scan plus a log probe. This
section records the analysis and the exact recipe so a future *buckets-only cold-read tier* (the only
regime where it could pay off) can adopt it without re-deriving the boundary math.

**Rationale:** doc 16 §Performance / finding #5 (leading-edge exactness) and the doc-17 Phase 6 follow-up
bullet (`17-…:694`). Phase 6 deliberately left recency/absence on the exact log path and only
bucket-accelerated `Count`; this honestly re-assesses whether closing that gap is worth it.

### What exists today

- **Recency** is one aggregate over `idx_behavioral_event_window`: `store.go:79-87` runs
  `SELECT COALESCE(MAX(occurred_at),'-infinity') >= $at-window FROM behavioral_event WHERE
  tenant/profile/event_name AND occurred_at <= $at`. The lower bound lives only in the `>=` comparison,
  so Postgres satisfies `MAX(occurred_at)` by the classic *MAX-via-index* rewrite (index
  `(tenant_id, customer_profile_id, event_name, occurred_at DESC)`, `00011:29-30`) — a single btree
  descent to the first tuple `≤ at`. **One tuple read.**
- **Absence** is the exact complement, same single-seek shape: `store.go:91-99` (`… < $at-window`). The
  `COALESCE(…,'-infinity')` makes a never-emitted event correctly `absent=true` / `recent=false`.
- **Count** is the one bucket-accelerated predicate: `store.go:35-50` routes non-exact, non-`where`
  counts to `bucketCount` (`store.go:256-278`), which SUMs whole in-window hourly buckets and counts the
  two partial boundary hours exactly from the log. `spec.Exact` (`store.go:41-42`) is the routing seam.
- **Buckets carry exact landmarks.** `profile_behavior_bucket` has `first_at`/`last_at` = exact min/max
  `occurred_at` in the hour (`00014:18-19`), maintained by `Recorder.Append` with `last_at =
  GREATEST(last_at, $occurred_at)` only when the log insert was new (`recorder.go:91-101`, guarded by
  `RowsAffected()==0`). The migration comment (`00014:6-8`) explicitly reserves them for "future
  recency/absence acceleration" — i.e. this follow-up.
- **Routing.** `spec.Exact` (`spec.go:24`) is set by `toSpec` for anything buckets can't serve —
  `b.Exact || b.Where != nil || b.Anchor != nil || b.ValueProp != ""` (`eval.go:135`). Recency/absence
  dispatch at `eval.go:113-119`; correlated-absence goes to `CorrelatedAbsent` (`store.go:103-133`),
  exact regardless. So the *only* leaves a bucket path would serve are plain, `where`-less
  recency/absence.

### The exact bucket recipe (what it would look like)

**Key equivalence.** For a fixed instant `at`, `Recent ≡ (∃ event in [at−window, at]) ≡ Count(window) ≥
1`, and `Absent ≡ Count(window) == 0`. So recency/absence are pure **existence** checks over the same
window `Count` handles exactly. Two candidate implementations:

1. **Reduce to the existing exact count:** `n,_ := bucketCount(...); return n ≥ 1`. Correct, but runs
   *three* subqueries. Reject.
2. **Landmark `MAX(last_at)` for the bulk + one exact log probe on the `at`-hour** (`$4 = at−window`,
   `$5 = at`):

```sql
WITH w AS (SELECT $4::timestamptz AS from_ts, $5::timestamptz AS at_ts)
SELECT
  COALESCE(
    (SELECT MAX(b.last_at) FROM profile_behavior_bucket b, w
       WHERE b.tenant_id=$1 AND b.customer_profile_id=$2 AND b.event_name=$3
         AND b.bucket_start >= date_trunc('hour', w.from_ts)   -- lower boundary hour included
         AND b.bucket_start <  date_trunc('hour', w.at_ts))    -- at-hour EXCLUDED (strict)
     >= w.from_ts,
    false)
  OR EXISTS (
    SELECT 1 FROM behavioral_event e, w
      WHERE e.tenant_id=$1 AND e.customer_profile_id=$2 AND e.event_name=$3
        AND e.occurred_at >= GREATEST(w.from_ts, date_trunc('hour', w.at_ts))
        AND e.occurred_at <= w.at_ts);          -- upper bound respected here, not in buckets
```

`Absent` is `NOT` of that expression. **Why exact at both edges (and yes, a bucket `last_at` can exceed
`at` — the `at`-hour bucket):**

- **Upper edge (the reason for the log probe).** The `date_trunc('hour', at)` bucket accumulates
  `last_at` over *every* event in that clock hour, including events with `occurred_at > at` (under
  redelivery/backfill/sweep re-eval, `at` is historical, so such events genuinely exist). Including the
  `at`-hour in the bulk `MAX(last_at)` could make recency spuriously true. The strict
  `bucket_start < date_trunc(at)` bound excludes it; the `EXISTS` probe re-answers the `at`-hour on the
  exact log with `occurred_at ≤ at`. `GREATEST(from_ts, date_trunc(at))` makes the probe the *whole*
  window when `window < 1h`.
- **Lower edge (landmark alone, no probe).** The `from`-hour bucket's `last_at` is the exact per-hour
  max, and (for `window ≥ ~1h`) all its events are `< date_trunc(at) ≤ at`, so `MAX(last_at) ≥ from_ts`
  is a sufficient, exact witness for "∃ in-window event in the lower boundary hour."

**Routing** would be identical to `Count`: gate on `!spec.Exact && spec.WhereMatch == nil` inside
`store.Recent`/`store.Absent`; no `toSpec` change (recency/absence never carry `value_prop`;
correlated-absence never reaches `Recent`/`Absent`).

### Is the win real? — No; it's negative

| | Exact (current) | Bucket landmark (option 2) | Bucket-via-count (option 1) |
|---|---|---|---|
| Tuples read | **1** (MAX-via-index seek) | `⌈window/1h⌉` bucket rows (range scan) **+ 1** log seek | `⌈window/1h⌉` + 2 partial-hour counts |
| 30-day window | 1 | ~720 + 1 | ~720 + 2 |
| Extra write cost | none | none, or a new `(…, last_at DESC)` index (churns every append) | none |

The asymmetry with `Count` is the whole story. `Count` on the log is `O(events-in-window)`; buckets cut
it to `O(hours-in-window)`. **Recency/absence on the log are already `O(1)`** — a single DESC-index
tuple. `MAX(last_at)` cannot be answered from an index (`bucket_start` bounds the scan, not `last_at`),
so the bulk is a range scan over up to `window/1h` buckets. An index on `(…, last_at DESC)` doesn't
rescue it (the `bucket_start < date_trunc(at)` filter isn't a boundary on it) and would add write
amplification on the hot `Append` path to save *nothing*.

**Rejected explicitly:** option 1 (3 subqueries, strictly worse); a `last_at DESC` index (write churn for
zero read benefit); buckets-only-drop-the-probe (inexact at the upper edge — violates the exactness
invariant the engine is built on, finding #5).

**The one regime where the recipe would pay off** (out of scope now): a *buckets-only cold-read tier* —
if retention ever dropped `behavioral_event` partitions *earlier* than `profile_behavior_bucket`, or
reads were served from a bucket-only replica. Today both tables share the same partition retention
(`00014:6-9` mirrors `00011:19-25`) and retention must exceed `max_window_seconds` for `Count` anyway, so
the log is always present. The recipe is recorded so that tier, if built, adopts a *proven-exact*
boundary rule — though note it still needs the log for the `at`-hour, so a *truly* log-free tier can only
be hour-exact.

### Correctness subtleties on record

- **UTC session dependency** carries over: `date_trunc('hour', …)` on write (`recorder.go:97`) and this
  read must run under the same UTC session (`00014:9-11`).
- **`at`-hour bucket `last_at > at` is expected, not a bug** — any future implementer who folds the
  `at`-hour into the bulk `MAX(last_at)` reintroduces upper-edge over-inclusion under redelivery.
- **`where`/`anchor` must never reach the bucket path** (buckets hold no props): the
  `spec.Exact`/`WhereMatch` gate is load-bearing; correlated absence stays on `CorrelatedAbsent`.

### Tests

Because we are **not** implementing the bucket path, no bucket recency/absence tests are added. Two cheap
regression locks are worth landing to protect the decision (both in `internal/behavior/bucket_test.go`,
reusing `appendEvent`/`seedTenant`, fake-clock `at`):

1. **`RecencyEquivalentToCountAcrossBoundaries`** — over the same boundary-placement table as
   `BucketCountEqualsExactAcrossPlacements` (`bucket_test.go:93-121`), assert `Recent(at,window) ==
   (Count(non-exact) ≥ 1)` and `Absent == (Count == 0)` at every placement — including a split
   lower-boundary hour and a same-hour-after-`at` event (append one at `at.Add(+time.Minute)` in the
   `at`-hour and assert it does **not** flip `Recent`). Locks the upper-edge invariant without adding the
   bucket read path.
2. **`RecencyNeverEmitted`** — keep/extend `store_test.go:84-101` as the `-infinity` coalesce guard.

If the buckets-only tier is ever built, add the exact-parity suite (following
`BucketCountEqualsExactAcrossPlacements`): a table over `{window ∈ (sub-hour, 1h, multi-hour, multi-day)}
× {at at :00, mid-hour, :59}` asserting the option-2 SQL equals the exact `MAX(occurred_at)` seek, plus a
case seeding a same-hour event with `occurred_at > at` to prove the `at`-hour `last_at > at` does not
leak.

**Ship: nothing.** The gap Phase 6 left open is, on inspection, correctly left open — recency/absence are
already `O(1)` and bucketing them is a pessimization. This section converts the follow-up from "TODO" to
"evaluated, declined, with an exact recipe on file for the only future architecture that could justify it."
