package segment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status values.
const (
	SegmentActive    = "active"
	SegmentInactive  = "inactive"
	VersionActive    = "active"
	MembershipActive = "active"
	MembershipExited = "exited"
)

// Errors.
var (
	ErrNotFound      = errors.New("segment not found")
	ErrDuplicateName = errors.New("segment name already exists for tenant")
)

// Segment is a saved audience definition.
type Segment struct {
	ID               uuid.UUID  `json:"id"`
	TenantID         uuid.UUID  `json:"tenant_id"`
	Name             string     `json:"name"`
	Description      string     `json:"description,omitempty"`
	Status           string     `json:"status"`
	CurrentVersionID *uuid.UUID `json:"current_version_id,omitempty"`
	CurrentVersion   int        `json:"current_version"`
	Rule             Rule       `json:"rule"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// ActiveVersion is an active segment's current rule version plus the Phase-6
// metadata the worker uses to prefilter the per-event fan-out.
type ActiveVersion struct {
	SegmentID       uuid.UUID
	VersionID       uuid.UUID
	Version         int
	Rule            Rule
	IsStateful      bool
	HasStateless    bool
	ReferencedNames []string
}

// Membership is a customer's membership in a segment.
type Membership struct {
	SegmentID         uuid.UUID  `json:"segment_id"`
	CustomerProfileID uuid.UUID  `json:"customer_profile_id"`
	Status            string     `json:"status"`
	EnteredAt         *time.Time `json:"entered_at,omitempty"`
	ExitedAt          *time.Time `json:"exited_at,omitempty"`
	LastEvaluatedAt   time.Time  `json:"last_evaluated_at"`
	Version           int64      `json:"version"`
}

// Repo persists segments, versions, and membership.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// CreateSegment creates a segment with version 1 and points current at it.
func (r *Repo) CreateSegment(ctx context.Context, tenantID uuid.UUID, name, description string, rule Rule) (Segment, error) {
	ruleJSON, _ := json.Marshal(rule)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Segment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	segID := uuid.New()
	_, err = tx.Exec(ctx,
		`INSERT INTO segment (id, tenant_id, name, description, status) VALUES ($1,$2,$3,$4,$5)`,
		segID, tenantID, name, nullString(description), SegmentActive)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Segment{}, ErrDuplicateName
		}
		return Segment{}, fmt.Errorf("insert segment: %w", err)
	}
	verID := uuid.New()
	isStateful, hasStateless, events, maxWindow := analyzeRule(rule)
	if _, err := tx.Exec(ctx,
		`INSERT INTO segment_version (id, tenant_id, segment_id, version, rule_json, status, is_stateful, has_stateless_leaves, referenced_event_names, max_window_seconds)
		 VALUES ($1,$2,$3,1,$4,$5,$6,$7,$8,$9)`,
		verID, tenantID, segID, ruleJSON, VersionActive, isStateful, hasStateless, events, int64(maxWindow.Seconds())); err != nil {
		return Segment{}, fmt.Errorf("insert version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE segment SET current_version_id=$1, updated_at=now() WHERE id=$2`, verID, segID); err != nil {
		return Segment{}, fmt.Errorf("set current version: %w", err)
	}
	// Durably seed the existing population for a sweep-safe rule (dormant profiles
	// enter without an inbound event); the seed runner drains it, resumable on crash.
	if !referencesEvent(rule) {
		if err := r.EnqueueSeedJobTx(ctx, tx, tenantID, segID, "seed", time.Now().UTC()); err != nil {
			return Segment{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Segment{}, err
	}
	return r.GetSegment(ctx, tenantID, segID)
}

// UpdateSegment creates a new version and repoints current.
func (r *Repo) UpdateSegment(ctx context.Context, tenantID, segmentID uuid.UUID, description string, rule Rule) (Segment, error) {
	ruleJSON, _ := json.Marshal(rule)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Segment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var maxVer int
	err = tx.QueryRow(ctx,
		`SELECT coalesce(max(version),0) FROM segment_version WHERE tenant_id=$1 AND segment_id=$2`,
		tenantID, segmentID).Scan(&maxVer)
	if err != nil {
		return Segment{}, fmt.Errorf("max version: %w", err)
	}
	if maxVer == 0 {
		return Segment{}, ErrNotFound
	}
	verID := uuid.New()
	isStateful, hasStateless, events, maxWindow := analyzeRule(rule)
	if _, err := tx.Exec(ctx,
		`INSERT INTO segment_version (id, tenant_id, segment_id, version, rule_json, status, is_stateful, has_stateless_leaves, referenced_event_names, max_window_seconds)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		verID, tenantID, segmentID, maxVer+1, ruleJSON, VersionActive, isStateful, hasStateless, events, int64(maxWindow.Seconds())); err != nil {
		return Segment{}, fmt.Errorf("insert version: %w", err)
	}
	// Editing a segment also reactivates it (an edit implies it should be live) — this
	// is the reactivation path for a retired segment and prevents a dead version on an
	// inactive one (finding #25).
	if _, err := tx.Exec(ctx,
		`UPDATE segment SET current_version_id=$1, description=coalesce($2, description), status=$5, updated_at=now() WHERE tenant_id=$3 AND id=$4`,
		verID, nullString(description), tenantID, segmentID, SegmentActive); err != nil {
		return Segment{}, fmt.Errorf("repoint current: %w", err)
	}
	// Re-evaluate current members against the new rule (finding #24): enqueue each
	// active member due-now so the sweeper re-checks and exits any who no longer match.
	// Only sweep-safe rules — an event-gated rule cannot be swept, so its members
	// re-evaluate at their next inbound event instead (no wasted enqueue). Newly
	// qualifying profiles are covered by the seed on update.
	if !referencesEvent(rule) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason)
			SELECT tenant_id, segment_id, customer_profile_id, now(), 'version_change'
			FROM segment_membership WHERE tenant_id=$1 AND segment_id=$2 AND status=$3
			ON CONFLICT (tenant_id, segment_id, customer_profile_id)
			DO UPDATE SET due_at = LEAST(segment_pending_eval.due_at, now()), reason='version_change', claimed_at=NULL`,
			tenantID, segmentID, MembershipActive); err != nil {
			return Segment{}, fmt.Errorf("enqueue version_change: %w", err)
		}
		// Durably re-seed the whole population so newly-qualifying non-members of a
		// loosened rule enter (the version_change enqueue above only covers members).
		if err := r.EnqueueSeedJobTx(ctx, tx, tenantID, segmentID, "version_change", time.Now().UTC()); err != nil {
			return Segment{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Segment{}, err
	}
	return r.GetSegment(ctx, tenantID, segmentID)
}

// GetSegment loads a segment with its current rule.
func (r *Repo) GetSegment(ctx context.Context, tenantID, segmentID uuid.UUID) (Segment, error) {
	var s Segment
	var ruleJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT s.id, s.tenant_id, s.name, COALESCE(s.description, ''), s.status, s.current_version_id,
		       v.version, v.rule_json, s.created_at, s.updated_at
		FROM segment s
		JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.tenant_id=$1 AND s.id=$2`,
		tenantID, segmentID).
		Scan(&s.ID, &s.TenantID, &s.Name, &s.Description, &s.Status, &s.CurrentVersionID,
			&s.CurrentVersion, &ruleJSON, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Segment{}, ErrNotFound
	}
	if err != nil {
		return Segment{}, fmt.Errorf("get segment: %w", err)
	}
	_ = json.Unmarshal(ruleJSON, &s.Rule)
	return s, nil
}

// ActiveSegmentVersions returns the current rule version of every active segment.
func (r *Repo) ActiveSegmentVersions(ctx context.Context, tenantID uuid.UUID) ([]ActiveVersion, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, v.id, v.version, v.rule_json, v.is_stateful, v.has_stateless_leaves, v.referenced_event_names
		FROM segment s
		JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.tenant_id=$1 AND s.status=$2`,
		tenantID, SegmentActive)
	if err != nil {
		return nil, fmt.Errorf("active segments: %w", err)
	}
	defer rows.Close()
	var out []ActiveVersion
	for rows.Next() {
		var av ActiveVersion
		var ruleJSON []byte
		if err := rows.Scan(&av.SegmentID, &av.VersionID, &av.Version, &ruleJSON, &av.IsStateful, &av.HasStateless, &av.ReferencedNames); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(ruleJSON, &av.Rule); err != nil {
			return nil, fmt.Errorf("unmarshal rule: %w", err)
		}
		out = append(out, av)
	}
	return out, rows.Err()
}

// SegmentsEpoch is a cheap per-tenant fingerprint of the active-segment set that the
// worker reads per event to invalidate its parsed-rule cache cross-process (no notify
// channel). It combines count + newest updated_at with sum(current version) — the
// version sum strictly increases on every create/update (a new version = maxVer+1 is
// repointed), so it detects a change even when two out-of-order commits leave count
// and max(updated_at) unchanged.
func (r *Repo) SegmentsEpoch(ctx context.Context, tenantID uuid.UUID) (count int64, maxUpdated time.Time, versionSum int64, err error) {
	var updated *time.Time
	err = r.pool.QueryRow(ctx, `
		SELECT count(*), max(s.updated_at), COALESCE(sum(v.version), 0)
		FROM segment s JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.tenant_id=$1 AND s.status=$2`,
		tenantID, SegmentActive).Scan(&count, &updated, &versionSum)
	if err != nil {
		return 0, time.Time{}, 0, fmt.Errorf("segments epoch: %w", err)
	}
	if updated != nil {
		maxUpdated = *updated
	}
	return count, maxUpdated, versionSum, nil
}

// DeactivateSegment retires a segment (finding #25): flips it to inactive (so the
// edge no longer evaluates it and the sweeper drops its stranded due-rows) and purges
// its segment_pending_eval rows. Buckets are kept (rule-agnostic, shared across
// segments). Membership rows are frozen as historical — a drain-with-exit-emit is a
// separate policy the service can apply. Idempotent.
func (r *Repo) DeactivateSegment(ctx context.Context, tenantID, segmentID uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ct, err := tx.Exec(ctx,
		`UPDATE segment SET status=$3, updated_at=now() WHERE tenant_id=$1 AND id=$2`,
		tenantID, segmentID, SegmentInactive)
	if err != nil {
		return fmt.Errorf("deactivate segment: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2`, tenantID, segmentID); err != nil {
		return fmt.Errorf("purge pending on retire: %w", err)
	}
	return tx.Commit(ctx)
}

// ActiveVersionForSegment returns the current active rule version of one segment.
func (r *Repo) ActiveVersionForSegment(ctx context.Context, tenantID, segmentID uuid.UUID) (ActiveVersion, bool, error) {
	var av ActiveVersion
	var ruleJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT s.id, v.id, v.version, v.rule_json
		FROM segment s JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.tenant_id=$1 AND s.id=$2 AND s.status=$3`,
		tenantID, segmentID, SegmentActive).Scan(&av.SegmentID, &av.VersionID, &av.Version, &ruleJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return ActiveVersion{}, false, nil
	}
	if err != nil {
		return ActiveVersion{}, false, fmt.Errorf("active version for segment: %w", err)
	}
	if err := json.Unmarshal(ruleJSON, &av.Rule); err != nil {
		return ActiveVersion{}, false, fmt.Errorf("unmarshal rule: %w", err)
	}
	return av, true, nil
}

// PendingEval is a claimed deadline row the sweeper must re-evaluate.
type PendingEval struct {
	TenantID          uuid.UUID
	SegmentID         uuid.UUID
	CustomerProfileID uuid.UUID
	Reason            string
	DueAt             time.Time
}

// PendingBacklog counts due deadline rows that are claimable now — unclaimed OR with a
// stale claim past the reclaim window (crashed claims) — a sweeper-lag SLI gauge.
func (r *Repo) PendingBacklog(ctx context.Context, now time.Time, reclaim time.Duration) (int64, error) {
	var n int64
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_pending_eval WHERE due_at <= $1 AND (claimed_at IS NULL OR claimed_at < $2)`,
		now, now.Add(-reclaim)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("pending backlog: %w", err)
	}
	return n, nil
}

// UpsertPendingTx arms/re-arms a deadline for (segment, profile). Re-arming clears
// claimed_at so the sweeper can pick it up again at the new due_at.
func (r *Repo) UpsertPendingTx(ctx context.Context, tx pgx.Tx, tenantID, segmentID, profileID uuid.UUID, dueAt time.Time, reason string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (tenant_id, segment_id, customer_profile_id)
		DO UPDATE SET due_at=$4, reason=$5, claimed_at=NULL`,
		tenantID, segmentID, profileID, dueAt, reason)
	if err != nil {
		return fmt.Errorf("upsert pending: %w", err)
	}
	return nil
}

// DeferPending pushes a deadline forward and clears its claim, so a row whose sweep
// keeps failing backs off instead of tight-looping on the reclaim and keeps its (now
// later) due_at from monopolizing the tenant's fair-claim slots.
func (r *Repo) DeferPending(ctx context.Context, tenantID, segmentID, profileID uuid.UUID, dueAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE segment_pending_eval SET due_at=$4, claimed_at=NULL WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID, dueAt)
	if err != nil {
		return fmt.Errorf("defer pending: %w", err)
	}
	return nil
}

// DeletePendingTx removes a deadline (no future elapse transition remains).
func (r *Repo) DeletePendingTx(ctx context.Context, tx pgx.Tx, tenantID, segmentID, profileID uuid.UUID) error {
	_, err := tx.Exec(ctx,
		`DELETE FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID)
	if err != nil {
		return fmt.Errorf("delete pending: %w", err)
	}
	return nil
}

// CurrentDueAt returns the stored due_at for a pending row (ok=false if none), so
// the caller can coalesce a near-identical re-arm.
func (r *Repo) CurrentDueAt(ctx context.Context, tenantID, segmentID, profileID uuid.UUID) (time.Time, bool, error) {
	var t time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT due_at FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("current due_at: %w", err)
	}
	return t, true, nil
}

// ClaimDuePending atomically claims up to batchSize due rows, fairly across tenants
// (ROW_NUMBER per tenant, capped at perTenantCap) so one busy tenant cannot starve
// others. Rows claimed longer than reclaim ago are re-claimable (crash recovery).
func (r *Repo) ClaimDuePending(ctx context.Context, now time.Time, batchSize, perTenantCap int, reclaim time.Duration) ([]PendingEval, error) {
	rows, err := r.pool.Query(ctx, `
		WITH ranked AS (
			SELECT tenant_id, segment_id, customer_profile_id, due_at,
			       ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY due_at) AS rn
			FROM segment_pending_eval
			WHERE due_at <= $1 AND (claimed_at IS NULL OR claimed_at < $2)
		),
		picked AS (
			SELECT tenant_id, segment_id, customer_profile_id
			FROM ranked WHERE rn <= $3 ORDER BY due_at LIMIT $4
		),
		locked AS (
			SELECT p.ctid
			FROM segment_pending_eval p
			JOIN picked USING (tenant_id, segment_id, customer_profile_id)
			WHERE p.due_at <= $1 AND (p.claimed_at IS NULL OR p.claimed_at < $2)
			FOR UPDATE SKIP LOCKED
		)
		UPDATE segment_pending_eval p SET claimed_at=$1
		FROM locked l WHERE p.ctid = l.ctid
		RETURNING p.tenant_id, p.segment_id, p.customer_profile_id, p.reason, p.due_at`,
		now, now.Add(-reclaim), perTenantCap, batchSize)
	if err != nil {
		return nil, fmt.Errorf("claim due pending: %w", err)
	}
	defer rows.Close()
	var out []PendingEval
	for rows.Next() {
		var p PendingEval
		if err := rows.Scan(&p.TenantID, &p.SegmentID, &p.CustomerProfileID, &p.Reason, &p.DueAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

const seedPageSize = 1000

// SeedPendingForSegment enqueues a due-at deadline for every profile of a tenant
// (dormant "did-not-do" profiles included), so a newly active/updated stateful
// segment evaluates the existing population without an inbound event. Idempotent
// (an existing earlier deadline is preserved) and PAGED over customer_profile by id
// in bounded per-page transactions — never one unbounded insert (doc 16 §Backfill).
// Call it off the request path (see handler.seedIfSweepable) for large tenants.
func (r *Repo) SeedPendingForSegment(ctx context.Context, tenantID, segmentID uuid.UUID, dueAt time.Time, reason string) (int, error) {
	cursor := uuid.Nil // smallest UUID: id > cursor starts at the first profile
	total := 0
	for {
		var maxID *uuid.UUID
		var pageCount int
		err := r.pool.QueryRow(ctx, `
			WITH page AS (
				SELECT id FROM customer_profile WHERE tenant_id=$1 AND id > $2 ORDER BY id LIMIT $3
			), ins AS (
				INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason)
				SELECT $1, $4, id, $5, $6 FROM page
				ON CONFLICT (tenant_id, segment_id, customer_profile_id)
				DO UPDATE SET due_at = LEAST(segment_pending_eval.due_at, $5), reason=$6, claimed_at=NULL
			)
			SELECT (SELECT id FROM page ORDER BY id DESC LIMIT 1), (SELECT count(*) FROM page)`,
			tenantID, cursor, seedPageSize, segmentID, dueAt, reason).Scan(&maxID, &pageCount)
		if err != nil {
			return total, fmt.Errorf("seed pending: %w", err)
		}
		total += pageCount
		if maxID == nil || pageCount < seedPageSize {
			break
		}
		cursor = *maxID
	}
	return total, nil
}

// SeedJob is a durable, resumable population-seed request the seed runner drains.
type SeedJob struct {
	TenantID  uuid.UUID
	SegmentID uuid.UUID
	Reason    string
	DueAt     time.Time
	Cursor    uuid.UUID
}

// EnqueueSeedJobTx records (or supersedes) a durable seed job in tx, so a crash after
// commit still seeds the population. Re-arming restarts the cursor and unclaims.
func (r *Repo) EnqueueSeedJobTx(ctx context.Context, tx pgx.Tx, tenantID, segmentID uuid.UUID, reason string, dueAt time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO segment_seed_job (tenant_id, segment_id, reason, due_at)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (tenant_id, segment_id)
		DO UPDATE SET reason=$3, due_at=$4, cursor=$5, claimed_at=NULL, created_at=now()`,
		tenantID, segmentID, reason, dueAt, uuid.Nil)
	if err != nil {
		return fmt.Errorf("enqueue seed job: %w", err)
	}
	return nil
}

// ClaimSeedJob claims one drainable seed job (unclaimed, or a claim older than
// reclaim — crash recovery), marking claimed_at=now. ok=false if none.
func (r *Repo) ClaimSeedJob(ctx context.Context, now time.Time, reclaim time.Duration) (SeedJob, bool, error) {
	var j SeedJob
	err := r.pool.QueryRow(ctx, `
		UPDATE segment_seed_job SET claimed_at=$1
		WHERE (tenant_id, segment_id) IN (
			SELECT tenant_id, segment_id FROM segment_seed_job
			WHERE claimed_at IS NULL OR claimed_at < $2
			ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
		RETURNING tenant_id, segment_id, reason, due_at, cursor`,
		now, now.Add(-reclaim)).Scan(&j.TenantID, &j.SegmentID, &j.Reason, &j.DueAt, &j.Cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return SeedJob{}, false, nil
	}
	if err != nil {
		return SeedJob{}, false, fmt.Errorf("claim seed job: %w", err)
	}
	return j, true, nil
}

// SeedJobPage enqueues one page of pending deadlines for profiles after cursor and
// returns the new cursor + whether the population is fully seeded.
func (r *Repo) SeedJobPage(ctx context.Context, j SeedJob) (nextCursor uuid.UUID, done bool, err error) {
	var maxID *uuid.UUID
	var pageCount int
	err = r.pool.QueryRow(ctx, `
		WITH page AS (
			SELECT id FROM customer_profile WHERE tenant_id=$1 AND id > $2 ORDER BY id LIMIT $3
		), ins AS (
			INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason)
			SELECT $1, $4, id, $5, $6 FROM page
			ON CONFLICT (tenant_id, segment_id, customer_profile_id)
			DO UPDATE SET due_at = LEAST(segment_pending_eval.due_at, $5), reason=$6, claimed_at=NULL
		)
		SELECT (SELECT id FROM page ORDER BY id DESC LIMIT 1), (SELECT count(*) FROM page)`,
		j.TenantID, j.Cursor, seedPageSize, j.SegmentID, j.DueAt, j.Reason).Scan(&maxID, &pageCount)
	if err != nil {
		return j.Cursor, false, fmt.Errorf("seed job page: %w", err)
	}
	if maxID == nil || pageCount < seedPageSize {
		return j.Cursor, true, nil
	}
	return *maxID, false, nil
}

// SetSeedJobCursor persists mid-drain progress (keeps the claim) so a crash resumes.
func (r *Repo) SetSeedJobCursor(ctx context.Context, tenantID, segmentID, cursor uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE segment_seed_job SET cursor=$3 WHERE tenant_id=$1 AND segment_id=$2`, tenantID, segmentID, cursor)
	if err != nil {
		return fmt.Errorf("set seed cursor: %w", err)
	}
	return nil
}

// ReleaseSeedJob unclaims a partially-drained job so the next tick continues it.
func (r *Repo) ReleaseSeedJob(ctx context.Context, tenantID, segmentID uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE segment_seed_job SET claimed_at=NULL WHERE tenant_id=$1 AND segment_id=$2`, tenantID, segmentID)
	if err != nil {
		return fmt.Errorf("release seed job: %w", err)
	}
	return nil
}

// CompleteSeedJob removes a fully-drained job.
func (r *Repo) CompleteSeedJob(ctx context.Context, tenantID, segmentID uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM segment_seed_job WHERE tenant_id=$1 AND segment_id=$2`, tenantID, segmentID)
	if err != nil {
		return fmt.Errorf("complete seed job: %w", err)
	}
	return nil
}

// SafetyReEnqueue re-arms a bounded page of active memberships that currently have
// no pending deadline (due-now, reason='safety_sweep'), so a mis-computed or lost
// due_at self-heals. Bounded per call and called at a low rate → a rolling sweep.
// Event-gated / stateless rows the sweeper picks up are harmlessly dropped by
// SweepEvaluate. Returns rows re-enqueued.
func (r *Repo) SafetyReEnqueue(ctx context.Context, dueAt time.Time, limit int) (int, error) {
	ct, err := r.pool.Exec(ctx, `
		INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason)
		SELECT m.tenant_id, m.segment_id, m.customer_profile_id, $1, 'safety_sweep'
		FROM segment_membership m
		JOIN segment sg ON sg.id = m.segment_id AND sg.status = 'active'
		WHERE m.status='active'
		  AND NOT EXISTS (
		      SELECT 1 FROM segment_pending_eval p
		      WHERE p.tenant_id=m.tenant_id AND p.segment_id=m.segment_id AND p.customer_profile_id=m.customer_profile_id)
		LIMIT $2
		ON CONFLICT (tenant_id, segment_id, customer_profile_id) DO NOTHING`,
		dueAt, limit)
	if err != nil {
		return 0, fmt.Errorf("safety re-enqueue: %w", err)
	}
	return int(ct.RowsAffected()), nil
}

// MembershipStatus returns the current status of a membership ("" if none).
func (r *Repo) MembershipStatus(ctx context.Context, tenantID, segmentID, profileID uuid.UUID) (string, error) {
	var status string
	err := r.pool.QueryRow(ctx,
		`SELECT status FROM segment_membership WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("membership status: %w", err)
	}
	return status, nil
}

// EnterTx conditionally flips a membership to active within tx, bumping
// transition_seq only when it actually flips (a brand-new row, or one not already
// active). It returns the new transition_seq and whether a flip occurred; when
// already active it reports flipped=false and emits nothing. Atomic: the caller
// inserts the outbox row in the same tx.
func (r *Repo) EnterTx(ctx context.Context, tx pgx.Tx, tenantID, segmentID, profileID uuid.UUID, version int) (seq int64, flipped bool, err error) {
	err = tx.QueryRow(ctx, `
		INSERT INTO segment_membership
			(tenant_id, segment_id, customer_profile_id, status, entered_at, exited_at, last_evaluated_at, version, transition_seq)
		VALUES ($1,$2,$3,'active', now(), NULL, now(), $4, 1)
		ON CONFLICT (tenant_id, segment_id, customer_profile_id) DO UPDATE
			SET status='active', entered_at=now(), exited_at=NULL, last_evaluated_at=now(), version=$4,
			    transition_seq = segment_membership.transition_seq + 1
			WHERE segment_membership.status IS DISTINCT FROM 'active'
		RETURNING transition_seq`,
		tenantID, segmentID, profileID, version).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil // already active — conflict with the WHERE guard false
	}
	if err != nil {
		return 0, false, fmt.Errorf("enter membership: %w", err)
	}
	return seq, true, nil
}

// ExitTx conditionally flips a membership to exited within tx, bumping
// transition_seq only when it flips. A missing or already-exited row reports
// flipped=false (nothing to emit).
func (r *Repo) ExitTx(ctx context.Context, tx pgx.Tx, tenantID, segmentID, profileID uuid.UUID) (seq int64, flipped bool, err error) {
	err = tx.QueryRow(ctx, `
		UPDATE segment_membership
		SET status='exited', exited_at=now(), last_evaluated_at=now(), transition_seq = transition_seq + 1
		WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3 AND status IS DISTINCT FROM 'exited'
		RETURNING transition_seq`,
		tenantID, segmentID, profileID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("exit membership: %w", err)
	}
	return seq, true, nil
}

// TouchEvaluatedTx refreshes last_evaluated_at (and the rule version, so a
// continuously-active member still tracks later rule versions) for a still-matching,
// already-active membership. No status change, no emit.
func (r *Repo) TouchEvaluatedTx(ctx context.Context, tx pgx.Tx, tenantID, segmentID, profileID uuid.UUID, version int) error {
	_, err := tx.Exec(ctx,
		`UPDATE segment_membership SET last_evaluated_at=now(), version=$4 WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID, version)
	if err != nil {
		return fmt.Errorf("touch membership: %w", err)
	}
	return nil
}

// InsertMembershipOutbox stages a segment_membership_changed emit in the same tx as
// the flip, so flip + emit commit atomically (a relay drains it at-least-once).
func (r *Repo) InsertMembershipOutbox(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, partitionKey string, payload []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO segment_membership_outbox (tenant_id, partition_key, payload_json) VALUES ($1,$2,$3)`,
		tenantID, partitionKey, payload)
	if err != nil {
		return fmt.Errorf("insert membership outbox: %w", err)
	}
	return nil
}

// Begin opens a transaction on the repo's pool.
func (r *Repo) Begin(ctx context.Context) (pgx.Tx, error) { return r.pool.Begin(ctx) }

// ListMembers returns active memberships of a segment.
func (r *Repo) ListMembers(ctx context.Context, tenantID, segmentID uuid.UUID) ([]Membership, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT segment_id, customer_profile_id, status, entered_at, exited_at, last_evaluated_at, version
		FROM segment_membership
		WHERE tenant_id=$1 AND segment_id=$2 AND status=$3
		ORDER BY entered_at DESC LIMIT 500`,
		tenantID, segmentID, MembershipActive)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	defer rows.Close()
	out := []Membership{}
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.SegmentID, &m.CustomerProfileID, &m.Status, &m.EnteredAt, &m.ExitedAt, &m.LastEvaluatedAt, &m.Version); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
