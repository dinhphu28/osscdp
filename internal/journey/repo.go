package journey

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

// Errors.
var (
	ErrNotFound      = errors.New("journey not found")
	ErrDuplicateName = errors.New("journey name already exists for tenant")
)

// Repo persists journeys, versions, and enrollments.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

const journeyCols = `id, tenant_id, name, COALESCE(description,''), status, entry_segment_id, COALESCE(entry_event_name,''), exit_on_segment_leave, current_version, created_at, updated_at`

func scanJourney(row pgx.Row) (Journey, error) {
	var j Journey
	err := row.Scan(&j.ID, &j.TenantID, &j.Name, &j.Description, &j.Status,
		&j.EntrySegmentID, &j.EntryEventName, &j.ExitOnSegmentLeave, &j.CurrentVersion, &j.CreatedAt, &j.UpdatedAt)
	return j, err
}

// CreateJourney inserts a SEGMENT-entry journey (enter on entry-segment membership) and
// enqueues a durable population-backfill seed job in the same tx, so the entry segment's
// current members are enrolled — not just future joiners. The definition is assumed
// already validated by the caller.
func (r *Repo) CreateJourney(ctx context.Context, tenantID uuid.UUID, name, description string, entrySegmentID uuid.UUID, exitOnSegmentLeave bool, def Definition) (Journey, error) {
	return r.create(ctx, tenantID, name, description, &entrySegmentID, "", exitOnSegmentLeave, def)
}

// CreateEventJourney inserts an EVENT-entry journey (enter when the profile emits
// entryEventName). Event-entry journeys have no existing population, so no seed job.
func (r *Repo) CreateEventJourney(ctx context.Context, tenantID uuid.UUID, name, description, entryEventName string, exitOnSegmentLeave bool, def Definition) (Journey, error) {
	return r.create(ctx, tenantID, name, description, nil, entryEventName, exitOnSegmentLeave, def)
}

// create inserts a journey (status active) with its version-1 definition in one tx.
// Exactly one of entrySegmentID / entryEventName must be set (enforced by the DB XOR
// check). A segment-entry journey also enqueues a backfill seed job in-tx.
func (r *Repo) create(ctx context.Context, tenantID uuid.UUID, name, description string, entrySegmentID *uuid.UUID, entryEventName string, exitOnSegmentLeave bool, def Definition) (Journey, error) {
	defJSON, err := json.Marshal(def)
	if err != nil {
		return Journey{}, fmt.Errorf("marshal definition: %w", err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Journey{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	id := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO journey (id, tenant_id, name, description, status, entry_segment_id, entry_event_name, exit_on_segment_leave, current_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1)`,
		id, tenantID, name, nullString(description), StatusActive, entrySegmentID, nullString(entryEventName), exitOnSegmentLeave)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Journey{}, ErrDuplicateName
		}
		return Journey{}, fmt.Errorf("insert journey: %w", err)
	}
	events, maxWindow := analyzeDefinition(def)
	if _, err := tx.Exec(ctx, `
		INSERT INTO journey_version (id, tenant_id, journey_id, version, definition_json, referenced_event_names, max_window_seconds)
		VALUES ($1,$2,$3,1,$4,$5,$6)`,
		uuid.New(), tenantID, id, defJSON, events, int64(maxWindow.Seconds())); err != nil {
		return Journey{}, fmt.Errorf("insert journey version: %w", err)
	}
	// Backfill the entry segment's current population (durable seed job drained by the
	// journey SeedRunner). Enrollments pin version 1, due now.
	if entrySegmentID != nil {
		if err := r.EnqueueSeedJobTx(ctx, tx, tenantID, id, *entrySegmentID, 1, "seed", time.Now().UTC()); err != nil {
			return Journey{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Journey{}, fmt.Errorf("commit create journey: %w", err)
	}
	return r.GetJourney(ctx, tenantID, id)
}

// UpdateJourney mints a new immutable version (N+1), bumps current_version, and
// updates the description. In-flight enrollments keep their pinned version.
func (r *Repo) UpdateJourney(ctx context.Context, tenantID, journeyID uuid.UUID, description string, exitOnSegmentLeave bool, def Definition) (Journey, error) {
	defJSON, err := json.Marshal(def)
	if err != nil {
		return Journey{}, fmt.Errorf("marshal definition: %w", err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Journey{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var cur int
	err = tx.QueryRow(ctx,
		`SELECT current_version FROM journey WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
		tenantID, journeyID).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return Journey{}, ErrNotFound
	}
	if err != nil {
		return Journey{}, fmt.Errorf("lock journey: %w", err)
	}
	next := cur + 1
	events, maxWindow := analyzeDefinition(def)
	if _, err := tx.Exec(ctx, `
		INSERT INTO journey_version (id, tenant_id, journey_id, version, definition_json, referenced_event_names, max_window_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		uuid.New(), tenantID, journeyID, next, defJSON, events, int64(maxWindow.Seconds())); err != nil {
		return Journey{}, fmt.Errorf("insert journey version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE journey SET current_version=$3, description=$4, exit_on_segment_leave=$5, updated_at=now() WHERE tenant_id=$1 AND id=$2`,
		tenantID, journeyID, next, nullString(description), exitOnSegmentLeave); err != nil {
		return Journey{}, fmt.Errorf("update journey: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Journey{}, fmt.Errorf("commit update journey: %w", err)
	}
	return r.GetJourney(ctx, tenantID, journeyID)
}

// GetJourney loads a journey with its current-version definition.
func (r *Repo) GetJourney(ctx context.Context, tenantID, journeyID uuid.UUID) (Journey, error) {
	j, err := scanJourney(r.pool.QueryRow(ctx,
		`SELECT `+journeyCols+` FROM journey WHERE tenant_id=$1 AND id=$2`, tenantID, journeyID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Journey{}, ErrNotFound
	}
	if err != nil {
		return Journey{}, fmt.Errorf("get journey: %w", err)
	}
	def, err := r.GetVersion(ctx, tenantID, journeyID, j.CurrentVersion)
	if err != nil {
		return Journey{}, err
	}
	j.Definition = def
	return j, nil
}

// ListJourneys returns a tenant's journeys, newest first (heads only, no definition).
func (r *Repo) ListJourneys(ctx context.Context, tenantID uuid.UUID) ([]Journey, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+journeyCols+` FROM journey WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 500`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list journeys: %w", err)
	}
	defer rows.Close()
	out := []Journey{}
	for rows.Next() {
		j, err := scanJourney(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// DeactivateJourney archives a journey (status='archived'): the enrollment consumer
// stops entering new customers. In-flight enrollments continue on their pinned
// version (they are drained by the runner, not gated on journey status). Any pending
// backfill seed job is dropped in the same tx (mirrors segment.DeactivateSegment) so
// the seed runner does not keep re-claiming an inert job.
func (r *Repo) DeactivateJourney(ctx context.Context, tenantID, journeyID uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ct, err := tx.Exec(ctx,
		`UPDATE journey SET status=$3, updated_at=now() WHERE tenant_id=$1 AND id=$2`,
		tenantID, journeyID, StatusArchived)
	if err != nil {
		return fmt.Errorf("deactivate journey: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM journey_seed_job WHERE tenant_id=$1 AND journey_id=$2`, tenantID, journeyID); err != nil {
		return fmt.Errorf("drop journey seed job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit deactivate journey: %w", err)
	}
	return nil
}

// GetVersion loads a pinned journey version's definition.
func (r *Repo) GetVersion(ctx context.Context, tenantID, journeyID uuid.UUID, version int) (Definition, error) {
	var raw []byte
	err := r.pool.QueryRow(ctx,
		`SELECT definition_json FROM journey_version WHERE tenant_id=$1 AND journey_id=$2 AND version=$3`,
		tenantID, journeyID, version).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return Definition{}, ErrNotFound
	}
	if err != nil {
		return Definition{}, fmt.Errorf("get journey version: %w", err)
	}
	var def Definition
	if err := json.Unmarshal(raw, &def); err != nil {
		return Definition{}, fmt.Errorf("unmarshal definition: %w", err)
	}
	return def, nil
}

// JourneysEnteringOn returns the active journeys whose entry segment is segmentID.
func (r *Repo) JourneysEnteringOn(ctx context.Context, tenantID, segmentID uuid.UUID) ([]Journey, error) {
	return r.journeysWhere(ctx, `tenant_id=$1 AND entry_segment_id=$2 AND status=$3`, tenantID, segmentID, StatusActive)
}

// JourneysEnteringOnEvent returns the active journeys that enter when a profile emits
// eventName. Event names are matched exactly (case-sensitive, as ingested).
func (r *Repo) JourneysEnteringOnEvent(ctx context.Context, tenantID uuid.UUID, eventName string) ([]Journey, error) {
	if eventName == "" {
		return nil, nil
	}
	return r.journeysWhere(ctx, `tenant_id=$1 AND entry_event_name=$2 AND status=$3`, tenantID, eventName, StatusActive)
}

func (r *Repo) journeysWhere(ctx context.Context, where string, args ...any) ([]Journey, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+journeyCols+` FROM journey WHERE `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("journeys where: %w", err)
	}
	defer rows.Close()
	var out []Journey
	for rows.Next() {
		j, err := scanJourney(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Enroll idempotently creates a live enrollment pinned to version at current step 0,
// due now. ON CONFLICT on the PK (…, enrollment_seq) makes this ONCE-ONLY: a
// redelivered entry is a no-op, and — crucially — a re-entry AFTER a terminal
// (completed/exited) enrollment is also a no-op rather than a primary-key error
// (enrollment_seq is 0 until Phase 5 allocates a new run). The partial-unique-active
// index remains an independent invariant (never two active rows). Returns whether a
// row was created.
func (r *Repo) Enroll(ctx context.Context, tenantID, journeyID, profileID uuid.UUID, version int, dueAt time.Time) (bool, error) {
	ct, err := r.pool.Exec(ctx, `
		INSERT INTO journey_enrollment
			(tenant_id, journey_id, customer_profile_id, enrollment_seq, journey_version, status, current_step_index, step_seq, due_at)
		VALUES ($1,$2,$3,0,$4,$5,0,0,$6)
		ON CONFLICT (tenant_id, journey_id, customer_profile_id, enrollment_seq) DO NOTHING`,
		tenantID, journeyID, profileID, version, EnrollmentActive, dueAt)
	if err != nil {
		return false, fmt.Errorf("enroll: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// ExitActiveEnrollmentsForSegment terminates (status='exited') a profile's active
// enrollments in journeys that (a) enter on segmentID, (b) are ACTIVE, and (c) have
// exit_on_segment_leave set — used when the profile LEAVES that segment (Phase 2). It
// clears the claim and dead-letter so no runner touches the row again. A parked-but-
// still-active enrollment is exited too (the customer no longer qualifies). The
// journey status='active' filter mirrors the entry path (JourneysEnteringOn) and
// honors the "archived journeys drain in-flight enrollments to completion" contract
// (DeactivateJourney): a leave does not exit enrollments of an archived journey.
// Returns how many enrollments were exited.
func (r *Repo) ExitActiveEnrollmentsForSegment(ctx context.Context, tenantID, segmentID, profileID uuid.UUID) (int64, error) {
	ct, err := r.pool.Exec(ctx, `
		UPDATE journey_enrollment je
		SET status='exited', claimed_at=NULL, parked_at=NULL, updated_at=now()
		WHERE je.tenant_id=$1 AND je.customer_profile_id=$2 AND je.status='active'
		  AND je.journey_id IN (
		      SELECT id FROM journey
		      WHERE tenant_id=$1 AND entry_segment_id=$3 AND exit_on_segment_leave=true AND status=$4
		  )`,
		tenantID, profileID, segmentID, StatusActive)
	if err != nil {
		return 0, fmt.Errorf("exit enrollments on leave: %w", err)
	}
	return ct.RowsAffected(), nil
}

// ClaimDueEnrollments atomically claims up to batchSize due enrollments, fairly across
// tenants (ROW_NUMBER per tenant, capped at perTenantCap). claimed_at=now is the fence
// a later Advance must match. Rows claimed longer than reclaim ago are re-claimable
// (crash recovery). Verbatim clone of segment.Repo.ClaimDuePending.
func (r *Repo) ClaimDueEnrollments(ctx context.Context, now time.Time, batchSize, perTenantCap int, reclaim time.Duration) ([]Enrollment, error) {
	rows, err := r.pool.Query(ctx, `
		WITH ranked AS (
			SELECT tenant_id, journey_id, customer_profile_id, enrollment_seq, due_at,
			       ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY due_at) AS rn
			FROM journey_enrollment
			WHERE status='active' AND due_at <= $1 AND parked_at IS NULL
			  AND (claimed_at IS NULL OR claimed_at < $2)
		),
		picked AS (
			SELECT tenant_id, journey_id, customer_profile_id, enrollment_seq
			FROM ranked WHERE rn <= $3 ORDER BY due_at LIMIT $4
		),
		locked AS (
			SELECT e.ctid
			FROM journey_enrollment e
			JOIN picked USING (tenant_id, journey_id, customer_profile_id, enrollment_seq)
			WHERE e.status='active' AND e.due_at <= $1 AND e.parked_at IS NULL
			  AND (e.claimed_at IS NULL OR e.claimed_at < $2)
			FOR UPDATE SKIP LOCKED
		)
		UPDATE journey_enrollment e SET claimed_at=$1
		FROM locked l WHERE e.ctid = l.ctid
		RETURNING e.tenant_id, e.journey_id, e.customer_profile_id, e.enrollment_seq,
		          e.journey_version, e.current_step_index, e.step_seq, e.due_at`,
		now, now.Add(-reclaim), perTenantCap, batchSize)
	if err != nil {
		return nil, fmt.Errorf("claim due enrollments: %w", err)
	}
	defer rows.Close()
	var out []Enrollment
	for rows.Next() {
		var e Enrollment
		if err := rows.Scan(&e.TenantID, &e.JourneyID, &e.CustomerProfileID, &e.EnrollmentSeq,
			&e.JourneyVersion, &e.CurrentStepIndex, &e.StepSeq, &e.DueAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Advance moves a claimed enrollment to newIndex with a new due_at and status. It is a
// single-table CLAIM-FENCED UPDATE: it applies only if the row is still active AND
// claimed_at still equals the fence AND step_seq equals the expected value (all
// captured at claim), so a reclaimed slow runner writes zero rows — no rewind, no
// double-advance — and a concurrent exit-on-segment-leave that flipped the row to
// 'exited' WINS (the advance no-ops). It bumps step_seq, clears the claim, and resets
// the retry budget. Returns whether it applied.
func (r *Repo) Advance(ctx context.Context, e Enrollment, fence time.Time, newIndex int, newDueAt time.Time, newStatus string) (bool, error) {
	ct, err := r.pool.Exec(ctx, `
		UPDATE journey_enrollment SET
			current_step_index=$5, due_at=$6, status=$7,
			step_seq=step_seq+1, claimed_at=NULL, attempts=0, last_error=NULL, updated_at=now()
		WHERE tenant_id=$1 AND journey_id=$2 AND customer_profile_id=$3 AND enrollment_seq=$4
		  AND status='active' AND claimed_at=$8 AND step_seq=$9`,
		e.TenantID, e.JourneyID, e.CustomerProfileID, e.EnrollmentSeq,
		newIndex, newDueAt, newStatus, fence, e.StepSeq)
	if err != nil {
		return false, fmt.Errorf("advance enrollment: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// FailEnrollment records a failed advance: it bumps attempts, stores the (truncated)
// error, and either backs the row off exponentially or — once attempts reach
// maxAttempts — PARKS it. It always clears claimed_at. Clone of segment.Repo.FailPending.
// The status='active' guard mirrors Advance: a concurrent exit-on-segment-leave that
// terminated the row WINS, so an exited enrollment is never (re-)parked — the fail
// no-ops (returns not-parked, no error).
func (r *Repo) FailEnrollment(ctx context.Context, tenantID, journeyID, profileID uuid.UUID, enrollmentSeq int,
	now time.Time, errMsg string, base, cap time.Duration, maxAttempts int) (attempts int, parked bool, err error) {

	const maxErrLen = 500
	if len(errMsg) > maxErrLen {
		errMsg = errMsg[:maxErrLen]
	}
	err = r.pool.QueryRow(ctx, `
		UPDATE journey_enrollment SET
			attempts   = attempts + 1,
			last_error = $6,
			claimed_at = NULL,
			parked_at  = CASE WHEN attempts + 1 >= $7 THEN $5::timestamptz ELSE NULL END,
			due_at     = CASE WHEN attempts + 1 >= $7 THEN due_at
			                  ELSE $5::timestamptz + interval '1 second' * LEAST($9::double precision, $8::double precision * power(2, attempts))
			             END
		WHERE tenant_id=$1 AND journey_id=$2 AND customer_profile_id=$3 AND enrollment_seq=$4
		  AND status='active'
		RETURNING attempts, parked_at IS NOT NULL`,
		tenantID, journeyID, profileID, enrollmentSeq, now, errMsg, maxAttempts,
		int64(base.Seconds()), int64(cap.Seconds())).Scan(&attempts, &parked)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil // row concurrently exited/removed — nothing to fail
	}
	if err != nil {
		return 0, false, fmt.Errorf("fail enrollment: %w", err)
	}
	return attempts, parked, nil
}

// ParkedCount counts currently dead-lettered enrollments — the gauge depth.
func (r *Repo) ParkedCount(ctx context.Context) (int64, error) {
	var n int64
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM journey_enrollment WHERE parked_at IS NOT NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("parked count: %w", err)
	}
	return n, nil
}

// ListParked returns a journey's parked enrollments (most-recently parked first).
func (r *Repo) ListParked(ctx context.Context, tenantID, journeyID uuid.UUID, limit int) ([]ParkedEnrollment, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT journey_id, customer_profile_id, current_step_index, COALESCE(last_error,''), attempts, due_at, parked_at
		FROM journey_enrollment
		WHERE tenant_id=$1 AND journey_id=$2 AND parked_at IS NOT NULL
		ORDER BY parked_at DESC LIMIT $3`, tenantID, journeyID, limit)
	if err != nil {
		return nil, fmt.Errorf("list parked: %w", err)
	}
	defer rows.Close()
	var out []ParkedEnrollment
	for rows.Next() {
		var p ParkedEnrollment
		if err := rows.Scan(&p.JourneyID, &p.CustomerProfileID, &p.CurrentStepIndex, &p.LastError, &p.Attempts, &p.DueAt, &p.ParkedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UnparkEnrollment clears the dead-letter and re-arms the enrollment for an immediate
// retry with a fresh budget. Returns false if no parked row matched.
func (r *Repo) UnparkEnrollment(ctx context.Context, tenantID, journeyID, profileID uuid.UUID, now time.Time) (bool, error) {
	ct, err := r.pool.Exec(ctx, `
		UPDATE journey_enrollment
		SET parked_at=NULL, attempts=0, last_error=NULL, due_at=$4, claimed_at=NULL, updated_at=now()
		WHERE tenant_id=$1 AND journey_id=$2 AND customer_profile_id=$3 AND parked_at IS NOT NULL`,
		tenantID, journeyID, profileID, now)
	if err != nil {
		return false, fmt.Errorf("unpark enrollment: %w", err)
	}
	return ct.RowsAffected() > 0, nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
