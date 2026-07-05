package foundationtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

var errBoom = errors.New("store boom")

// errStore implements segment.BehaviorStore, failing every read so SweepEvaluate of a
// behavioral rule errors deterministically (doc 18 §B poison-row tests).
type errStore struct{}

func (errStore) Count(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (int64, error) {
	return 0, errBoom
}
func (errStore) Recent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) Absent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) CorrelatedAbsent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) Sequence(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) SumValue(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (float64, error) {
	return 0, errBoom
}
func (errStore) LastAt(context.Context, uuid.UUID, uuid.UUID, string, time.Time) (time.Time, bool, error) {
	return time.Time{}, false, errBoom
}
func (errStore) NthNewestInWindow(context.Context, uuid.UUID, uuid.UUID, string, time.Duration, int, time.Time) (time.Time, bool, error) {
	return time.Time{}, false, errBoom
}

// poisonStore delegates to a real behavior.Store but fails every read for one profile,
// so a poison row and a healthy row can coexist under one sweeper (doc 18 §B test #4).
type poisonStore struct {
	real   *behavior.Store
	poison uuid.UUID
}

func (p poisonStore) Count(ctx context.Context, tid, pid uuid.UUID, s behavior.Spec, at time.Time) (int64, error) {
	if pid == p.poison {
		return 0, errBoom
	}
	return p.real.Count(ctx, tid, pid, s, at)
}
func (p poisonStore) Recent(ctx context.Context, tid, pid uuid.UUID, s behavior.Spec, at time.Time) (bool, error) {
	if pid == p.poison {
		return false, errBoom
	}
	return p.real.Recent(ctx, tid, pid, s, at)
}
func (p poisonStore) Absent(ctx context.Context, tid, pid uuid.UUID, s behavior.Spec, at time.Time) (bool, error) {
	if pid == p.poison {
		return false, errBoom
	}
	return p.real.Absent(ctx, tid, pid, s, at)
}
func (p poisonStore) CorrelatedAbsent(ctx context.Context, tid, pid uuid.UUID, s behavior.Spec, at time.Time) (bool, error) {
	if pid == p.poison {
		return false, errBoom
	}
	return p.real.CorrelatedAbsent(ctx, tid, pid, s, at)
}
func (p poisonStore) Sequence(ctx context.Context, tid, pid uuid.UUID, s behavior.Spec, at time.Time) (bool, error) {
	if pid == p.poison {
		return false, errBoom
	}
	return p.real.Sequence(ctx, tid, pid, s, at)
}
func (p poisonStore) SumValue(ctx context.Context, tid, pid uuid.UUID, s behavior.Spec, at time.Time) (float64, error) {
	if pid == p.poison {
		return 0, errBoom
	}
	return p.real.SumValue(ctx, tid, pid, s, at)
}
func (p poisonStore) LastAt(ctx context.Context, tid, pid uuid.UUID, ev string, at time.Time) (time.Time, bool, error) {
	if pid == p.poison {
		return time.Time{}, false, errBoom
	}
	return p.real.LastAt(ctx, tid, pid, ev, at)
}
func (p poisonStore) NthNewestInWindow(ctx context.Context, tid, pid uuid.UUID, ev string, w time.Duration, n int, at time.Time) (time.Time, bool, error) {
	if pid == p.poison {
		return time.Time{}, false, errBoom
	}
	return p.real.NthNewestInWindow(ctx, tid, pid, ev, w, n, at)
}

func parkState(t *testing.T, f fixture, tid, segID, profID uuid.UUID) (attempts int, parked bool, lastErr string, due time.Time) {
	t.Helper()
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT attempts, parked_at IS NOT NULL, COALESCE(last_error,''), due_at
		 FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tid, segID, profID).Scan(&attempts, &parked, &lastErr, &due))
	return
}

// TestSweep_PoisonRowParks: a deadline whose sweep persistently errors backs off
// exponentially and, past the ceiling, dead-letters itself — excluded from the claim
// and the backlog SLI, surfaced by ParkedCount, and recoverable via UnparkPending.
func TestSweep_PoisonRowParks(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), errStore{})

	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "order_completed", "u1", "")
	pid := pu.CustomerProfileID

	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	insertPending(t, f, tid, seg.ID, pid, t0, nil)

	nowVal := t0
	var parked int
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger()).
		WithParkPolicy(time.Second, 8*time.Second, 3).
		WithClock(func() time.Time { return nowVal })
	runner.OnParked = func() { parked++ }

	// Attempt 1 → attempts=1, backoff base*2^0 = 1s (not parked).
	nowVal = t0.Add(time.Second)
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	att, pk, le, due := parkState(t, f, tid, seg.ID, pid)
	require.Equal(t, 1, att)
	require.False(t, pk)
	require.Contains(t, le, "boom")
	require.WithinDuration(t, nowVal.Add(time.Second), due, time.Millisecond, "first backoff is base")

	// Reclaim isolation: a backing-off row (future due_at, claimed_at=NULL) is not
	// picked up by the time-boxed reclaim before its due_at — the two timers never overlap.
	gap, err := repo.ClaimDuePending(ctx, nowVal.Add(500*time.Millisecond), 100, 50, time.Minute)
	require.NoError(t, err)
	require.Empty(t, gap, "a backing-off row is not claimed within its backoff gap")

	// Attempt 2 → attempts=2, backoff base*2^1 = 2s (not parked).
	nowVal = t0.Add(3 * time.Second)
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	att, pk, _, due = parkState(t, f, tid, seg.ID, pid)
	require.Equal(t, 2, att)
	require.False(t, pk)
	require.WithinDuration(t, nowVal.Add(2*time.Second), due, time.Millisecond, "backoff doubles")

	// Attempt 3 → attempts=3 >= maxAttempts → PARK.
	nowVal = t0.Add(6 * time.Second)
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	att, pk, _, _ = parkState(t, f, tid, seg.ID, pid)
	require.Equal(t, 3, att)
	require.True(t, pk, "row parked after exhausting retries")
	require.Equal(t, 1, parked, "OnParked fired exactly once")

	// Parked row is excluded from the claim + backlog even far past its due_at.
	future := t0.Add(time.Hour)
	rows, err := repo.ClaimDuePending(ctx, future, 100, 50, time.Minute)
	require.NoError(t, err)
	require.Empty(t, rows, "a parked row is never claimed")
	pc, err := repo.ParkedCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), pc)
	bl, err := repo.PendingBacklog(ctx, future, time.Minute)
	require.NoError(t, err)
	require.Zero(t, bl, "a parked row does not masquerade as sweeper lag")

	// Surfacing: it shows up in the admin list with its last_error.
	list, err := repo.ListParked(ctx, tid, seg.ID, 50)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Contains(t, list[0].LastError, "boom")
	require.Equal(t, 3, list[0].Attempts)

	// Manual retry: unpark, then it is claimable again with a fresh budget.
	found, err := repo.UnparkPending(ctx, tid, seg.ID, pid, future)
	require.NoError(t, err)
	require.True(t, found)
	att, pk, le, _ = parkState(t, f, tid, seg.ID, pid)
	require.Equal(t, 0, att)
	require.False(t, pk)
	require.Empty(t, le)
	rows, err = repo.ClaimDuePending(ctx, future.Add(time.Second), 100, 50, time.Minute)
	require.NoError(t, err)
	require.Len(t, rows, 1, "an unparked row is re-claimed")
}

// TestSweep_ActiveReArmUnparks: an active edge re-arm (UpsertPendingTx) revives a
// parked row with a fresh retry budget, while a fresh claim of a backing-off row is
// not stolen by the time-boxed reclaim before its due_at.
func TestSweep_ActiveReArmUnparks(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "order_completed", "u1", "")
	pid := pu.CustomerProfileID

	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	insertPending(t, f, tid, seg.ID, pid, t0, nil)
	// Force it parked directly.
	_, err = f.pool.Exec(ctx, `UPDATE segment_pending_eval SET parked_at=$4, attempts=9, last_error='boom'
		WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`, tid, seg.ID, pid, t0)
	require.NoError(t, err)

	// An active re-arm (edge) must clear the dead-letter.
	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.UpsertPendingTx(ctx, tx, tid, seg.ID, pid, t0.Add(time.Hour), "edge"))
	require.NoError(t, tx.Commit(ctx))

	att, pk, le, _ := parkState(t, f, tid, seg.ID, pid)
	require.False(t, pk, "an active re-arm unparks")
	require.Equal(t, 0, att)
	require.Empty(t, le)
	pc, err := repo.ParkedCount(ctx)
	require.NoError(t, err)
	require.Zero(t, pc)
}

// TestSweep_HealthyRowUnaffectedByPoison: a poison (segment,profile) never blocks or
// parks a healthy sibling in the same tenant — the healthy row is evaluated and enters
// while the poison row backs off and parks (doc 18 §B test #4).
func TestSweep_HealthyRowUnaffectedByPoison(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	healthy := seedProfile(t, f, tid, sid, "eh", "order_completed", "uh", "")
	poison := seedProfile(t, f, tid, sid, "ep", "order_completed", "up", "")
	store := poisonStore{real: behavior.NewStore(f.pool), poison: poison.CustomerProfileID}
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), store)

	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	insertPending(t, f, tid, seg.ID, healthy.CustomerProfileID, t0, nil)
	insertPending(t, f, tid, seg.ID, poison.CustomerProfileID, t0, nil)

	nowVal := t0
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger()).
		WithParkPolicy(time.Second, 8*time.Second, 2).
		WithClock(func() time.Time { return nowVal })

	// Tick 1: both claimed — healthy (never ordered → absent) ENTERS; poison errors.
	nowVal = t0.Add(time.Second)
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1, "the healthy sibling entered despite a poison row in the same tenant")
	_, pkP, _, _ := parkState(t, f, tid, seg.ID, poison.CustomerProfileID)
	require.False(t, pkP, "poison not yet parked after one failure")

	// Tick 2: poison reaches the ceiling and parks; the healthy membership is untouched.
	nowVal = t0.Add(4 * time.Second)
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	pc, err := repo.ParkedCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), pc, "only the poison row parked")
	members, err = repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1, "the healthy membership is unaffected by the sibling parking")
}

// TestSweep_ParkedRowNotCoalescedOnActiveEdge: an active edge re-arm whose computed
// deadline is within coalesceGranularity of a parked row's due_at still unparks it
// (planDeadline must never coalesce a parked row — doc 18 §B, load-bearing).
func TestSweep_ParkedRowNotCoalescedOnActiveEdge(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "order_completed", "u1", "")
	pid := pu.CustomerProfileID
	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	_, err = f.pool.Exec(ctx,
		`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,'ord1','order_completed',$3)`,
		tid, pid, t0)
	require.NoError(t, err)

	// Edge eval arms due = t0+24h (unparked).
	pu.Event.Timestamp = t0
	pu.Event.ReceivedAt = t0
	require.NoError(t, svc.Evaluate(ctx, pu))
	_, _, ok, err := repo.CurrentDueAt(ctx, tid, seg.ID, pid)
	require.NoError(t, err)
	require.True(t, ok)

	// Force it parked, then re-run the SAME edge (computed due == stored due → coalesce).
	_, err = f.pool.Exec(ctx, `UPDATE segment_pending_eval SET parked_at=$4, attempts=5, last_error='boom'
		WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`, tid, seg.ID, pid, t0)
	require.NoError(t, err)
	require.NoError(t, svc.Evaluate(ctx, pu))

	att, parked, le, _ := parkState(t, f, tid, seg.ID, pid)
	require.False(t, parked, "an active edge re-arm within coalesce of a parked row must unpark it, not coalesce")
	require.Equal(t, 0, att)
	require.Empty(t, le)
}

// TestSweep_SeedJobDrainUnparks: the production durable seed path (SeedJobPage, driven
// by the SeedRunner from create/republish) unparks a previously-parked row — the
// documented republish-based recovery (doc 18 §B).
func TestSweep_SeedJobDrainUnparks(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)

	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule()) // enqueues a seed job
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "order_completed", "u1", "")
	pid := pu.CustomerProfileID
	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	insertPending(t, f, tid, seg.ID, pid, t0, nil)
	_, err = f.pool.Exec(ctx, `UPDATE segment_pending_eval SET parked_at=$4, attempts=9, last_error='boom'
		WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`, tid, seg.ID, pid, t0)
	require.NoError(t, err)

	// Drain the durable seed job (the real production re-seed path).
	sr := segment.NewSeedRunner(repo, 10, time.Minute, time.Second, testLogger())
	_, err = sr.RunOnce(ctx)
	require.NoError(t, err)

	att, parked, le, _ := parkState(t, f, tid, seg.ID, pid)
	require.False(t, parked, "a durable seed-job drain unparks a previously-parked row")
	require.Equal(t, 0, att)
	require.Empty(t, le)
}
