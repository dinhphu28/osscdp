package journey

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// seedPageSize bounds one backfill page (profiles enrolled per SeedJobPage).
const seedPageSize = 1000

// EnqueueSeedJobTx records (or supersedes) a durable population-backfill job in tx, so
// a crash after commit still backfills. Re-arming restarts the cursor and unclaims.
// Clone of segment.Repo.EnqueueSeedJobTx, keyed by (tenant, journey).
func (r *Repo) EnqueueSeedJobTx(ctx context.Context, tx pgx.Tx, tenantID, journeyID, entrySegmentID uuid.UUID, version int, reason string, dueAt time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO journey_seed_job (tenant_id, journey_id, entry_segment_id, journey_version, reason, due_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id, journey_id)
		DO UPDATE SET entry_segment_id=$3, journey_version=$4, reason=$5, due_at=$6, cursor=$7, claimed_at=NULL, created_at=now()`,
		tenantID, journeyID, entrySegmentID, version, reason, dueAt, uuid.Nil)
	if err != nil {
		return fmt.Errorf("enqueue journey seed job: %w", err)
	}
	return nil
}

// ClaimJourneySeedJob claims one drainable seed job (unclaimed, or a claim older than
// reclaim — crash recovery), marking claimed_at=now. ok=false if none.
func (r *Repo) ClaimJourneySeedJob(ctx context.Context, now time.Time, reclaim time.Duration) (JourneySeedJob, bool, error) {
	var j JourneySeedJob
	err := r.pool.QueryRow(ctx, `
		UPDATE journey_seed_job SET claimed_at=$1
		WHERE (tenant_id, journey_id) IN (
			SELECT tenant_id, journey_id FROM journey_seed_job
			WHERE claimed_at IS NULL OR claimed_at < $2
			ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
		RETURNING tenant_id, journey_id, entry_segment_id, journey_version, reason, due_at, cursor, claimed_at`,
		now, now.Add(-reclaim)).Scan(&j.TenantID, &j.JourneyID, &j.EntrySegmentID, &j.JourneyVersion, &j.Reason, &j.DueAt, &j.Cursor, &j.ClaimedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return JourneySeedJob{}, false, nil
	}
	if err != nil {
		return JourneySeedJob{}, false, fmt.Errorf("claim journey seed job: %w", err)
	}
	return j, true, nil
}

// SeedJobPage enrolls one page (pageSize active entry-segment members after the cursor)
// and returns the new cursor + whether the population is fully drained. Enrollment is
// idempotent (ON CONFLICT DO NOTHING) and gated on the journey still being active, so a
// journey archived mid-drain stops admitting new members. A member joining during the
// drain whose id sorts below the cursor is enrolled by the live membership-entry path
// instead (mirrors the segment seed behaviour).
func (r *Repo) SeedJobPage(ctx context.Context, j JourneySeedJob, pageSize int) (nextCursor uuid.UUID, done bool, err error) {
	var maxID *uuid.UUID
	var pageCount int
	err = r.pool.QueryRow(ctx, `
		WITH page AS (
			SELECT customer_profile_id AS id FROM segment_membership
			WHERE tenant_id=$1 AND segment_id=$2 AND status='active' AND customer_profile_id > $3
			ORDER BY customer_profile_id LIMIT $4
		), ins AS (
			INSERT INTO journey_enrollment
				(tenant_id, journey_id, customer_profile_id, enrollment_seq, journey_version, status, current_step_index, step_seq, due_at)
			SELECT $1, $5, p.id, 0, $6, 'active', 0, 0, $7
			FROM page p
			WHERE EXISTS (SELECT 1 FROM journey jr WHERE jr.tenant_id=$1 AND jr.id=$5 AND jr.status='active')
			ON CONFLICT (tenant_id, journey_id, customer_profile_id, enrollment_seq) DO NOTHING
		)
		SELECT (SELECT id FROM page ORDER BY id DESC LIMIT 1), (SELECT count(*) FROM page)`,
		j.TenantID, j.EntrySegmentID, j.Cursor, pageSize, j.JourneyID, j.JourneyVersion, j.DueAt).Scan(&maxID, &pageCount)
	if err != nil {
		return j.Cursor, false, fmt.Errorf("journey seed job page: %w", err)
	}
	if maxID == nil || pageCount < pageSize {
		return j.Cursor, true, nil
	}
	return *maxID, false, nil
}

// SetSeedJobCursor persists mid-drain progress (keeps the claim) so a crash resumes.
// Fenced on claimedAt: a no-op if the job was re-enqueued or reclaimed meanwhile.
func (r *Repo) SetSeedJobCursor(ctx context.Context, tenantID, journeyID, cursor uuid.UUID, claimedAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE journey_seed_job SET cursor=$3 WHERE tenant_id=$1 AND journey_id=$2 AND claimed_at=$4`,
		tenantID, journeyID, cursor, claimedAt)
	if err != nil {
		return fmt.Errorf("set journey seed cursor: %w", err)
	}
	return nil
}

// ReleaseSeedJob unclaims a partially-drained job so the next tick continues it, and
// rotates it behind others (fairness). Fenced on claimedAt.
func (r *Repo) ReleaseSeedJob(ctx context.Context, tenantID, journeyID uuid.UUID, claimedAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE journey_seed_job SET claimed_at=NULL, created_at=now() WHERE tenant_id=$1 AND journey_id=$2 AND claimed_at=$3`,
		tenantID, journeyID, claimedAt)
	if err != nil {
		return fmt.Errorf("release journey seed job: %w", err)
	}
	return nil
}

// CompleteSeedJob removes a fully-drained job. Fenced: a job re-enqueued or reclaimed
// during the drain is NOT deleted (so a mid-drain re-seed is not silently lost).
func (r *Repo) CompleteSeedJob(ctx context.Context, tenantID, journeyID uuid.UUID, claimedAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM journey_seed_job WHERE tenant_id=$1 AND journey_id=$2 AND claimed_at=$3`,
		tenantID, journeyID, claimedAt)
	if err != nil {
		return fmt.Errorf("complete journey seed job: %w", err)
	}
	return nil
}
