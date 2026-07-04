package foundationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

func countSeedJobs(t *testing.T, f fixture, tid, segID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM segment_seed_job WHERE tenant_id=$1 AND segment_id=$2`, tid, segID).Scan(&n))
	return n
}

// TestSeedJob_EnqueuedForSweepSafeOnly: creating a sweep-safe segment records a durable
// seed job; an event-gated one does not (it re-evaluates at the edge).
func TestSeedJob_EnqueuedForSweepSafeOnly(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)

	behavioral, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	require.Equal(t, 1, countSeedJobs(t, f, tid, behavioral.ID), "sweep-safe rule enqueues a seed job")

	eventGated, err := repo.CreateSegment(ctx, tid, "vn-viewers", "", vnPhoneRule()) // has event.event_name leaf
	require.NoError(t, err)
	require.Equal(t, 0, countSeedJobs(t, f, tid, eventGated.ID), "event-gated rule enqueues no seed job")
}

// TestSeedJob_DrainsAndEntersDormant is the durable-seed end-to-end: a seed job seeds
// the whole population, and the sweep then enters dormant profiles.
func TestSeedJob_DrainsAndEntersDormant(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	for i := 0; i < 3; i++ {
		seedProfile(t, f, tid, sid, fmt.Sprintf("e%d", i), "page_view", fmt.Sprintf("u%d", i), "")
	}
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	require.Equal(t, 1, countSeedJobs(t, f, tid, seg.ID))

	sr := segment.NewSeedRunner(repo, 10, time.Minute, time.Second, testLogger())
	idle, err := sr.RunOnce(ctx)
	require.NoError(t, err)
	require.False(t, idle, "the seed job is claimed and drained")
	require.Equal(t, 0, countSeedJobs(t, f, tid, seg.ID), "job removed once fully drained")

	var pending int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2`, tid, seg.ID).Scan(&pending))
	require.Equal(t, 3, pending, "every profile is enqueued for evaluation")

	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger())
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 3, "all dormant profiles entered via the durable seed + sweep")
}

// TestSeedJob_ResumesFromCursor: a partially-drained job resumes at its persisted
// cursor — profiles at/below the cursor are not re-processed, those above are.
func TestSeedJob_ResumesFromCursor(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)

	for i := 0; i < 3; i++ {
		seedProfile(t, f, tid, sid, fmt.Sprintf("e%d", i), "page_view", fmt.Sprintf("u%d", i), "")
	}
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)

	// Profiles ordered by id (the cursor order).
	rows, err := f.pool.Query(ctx, `SELECT id FROM customer_profile WHERE tenant_id=$1 ORDER BY id`, tid)
	require.NoError(t, err)
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	rows.Close()
	require.Len(t, ids, 3)

	// Simulate progress: cursor already past the first two profiles.
	_, err = f.pool.Exec(ctx, `UPDATE segment_seed_job SET cursor=$3 WHERE tenant_id=$1 AND segment_id=$2`, tid, seg.ID, ids[1])
	require.NoError(t, err)

	sr := segment.NewSeedRunner(repo, 10, time.Minute, time.Second, testLogger())
	_, err = sr.RunOnce(ctx)
	require.NoError(t, err)

	var third, firstTwo int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`, tid, seg.ID, ids[2]).Scan(&third))
	require.Equal(t, 1, third, "the profile past the cursor is enqueued")
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id = ANY($3)`,
		tid, seg.ID, []uuid.UUID{ids[0], ids[1]}).Scan(&firstTwo))
	require.Zero(t, firstTwo, "profiles at/below the cursor are not re-processed")
}

// TestSeedJob_ReclaimsStaleClaim: a crashed claim (stale claimed_at) is re-claimable;
// a fresh claim is skipped.
func TestSeedJob_ReclaimsStaleClaim(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	sr := segment.NewSeedRunner(repo, 10, time.Minute, time.Second, testLogger())

	// Fresh claim → skipped.
	_, err = f.pool.Exec(ctx, `UPDATE segment_seed_job SET claimed_at=now() WHERE tenant_id=$1 AND segment_id=$2`, tid, seg.ID)
	require.NoError(t, err)
	idle, err := sr.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, idle, "a fresh claim is not reclaimed within the window")

	// Stale claim → reclaimed and drained.
	_, err = f.pool.Exec(ctx, `UPDATE segment_seed_job SET claimed_at=$3 WHERE tenant_id=$1 AND segment_id=$2`,
		tid, seg.ID, time.Now().Add(-2*time.Minute))
	require.NoError(t, err)
	idle, err = sr.RunOnce(ctx)
	require.NoError(t, err)
	require.False(t, idle, "a claim older than reclaim is re-claimed")
	require.Equal(t, 0, countSeedJobs(t, f, tid, seg.ID), "reclaimed job drained to completion")
}
