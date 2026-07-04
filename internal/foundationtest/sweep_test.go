package foundationtest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

func absenceRule() segment.Rule {
	return segment.Rule{Behavior: &segment.BehaviorSpec{Kind: segment.BehaviorAbsence, EventName: "order_completed", Window: "24h"}}
}

func insertPending(t *testing.T, f fixture, tid, segID, profID uuid.UUID, due time.Time, claimed *time.Time) {
	t.Helper()
	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason, claimed_at)
		 VALUES ($1,$2,$3,$4,'seed',$5)`, tid, segID, profID, due, claimed)
	require.NoError(t, err)
}

// TestSweep_AbsenceFiresWithoutEvent is the flagship: a profile that just ordered is
// not yet absent; the edge arms a deadline; when the sweeper fires past the deadline
// it enters the profile with NO inbound event.
func TestSweep_AbsenceFiresWithoutEvent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	seg, err := repo.CreateSegment(ctx, tid, "no-recent-order", "", absenceRule())
	require.NoError(t, err)

	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	pu := seedProfile(t, f, tid, sid, "e1", "order_completed", "u1", "")
	_, err = f.pool.Exec(ctx,
		`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,'ord1','order_completed',$3)`,
		tid, pu.CustomerProfileID, t0)
	require.NoError(t, err)

	// Edge eval at t0: just ordered → absence false → no membership; arms due_at = t0+24h.
	pu.Event.Timestamp = t0
	pu.Event.ReceivedAt = t0
	require.NoError(t, svc.Evaluate(ctx, pu))
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 0, "just ordered → not yet absent")
	due, ok, err := repo.CurrentDueAt(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, due.Equal(t0.Add(24*time.Hour)), "deadline armed 24h after the order")

	// Sweep past the deadline → absence true → ENTER, with no inbound event.
	sweepAt := t0.Add(24*time.Hour + time.Minute)
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger()).
		WithClock(func() time.Time { return sweepAt })
	n, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	members2, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members2, 1, "the sweeper enters the profile with no inbound event")
	require.Equal(t, 1, countMembershipOutbox(t, f, tid))

	var token string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT payload_json->>'reason_event_id' FROM segment_membership_outbox WHERE tenant_id=$1`, tid).Scan(&token))
	require.True(t, strings.HasPrefix(token, "sweep:"), "sweep emit carries a sweep token, got %q", token)
}

// TestSweep_DormantSeedEnters: a profile that never emits the event is seeded and
// evaluated by the sweeper (finding #32) — dormant "did-not-do" profiles enter.
func TestSweep_DormantSeedEnters(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	seg, err := repo.CreateSegment(ctx, tid, "no-recent-order", "", absenceRule())
	require.NoError(t, err)

	// A profile that never orders (seedProfile records no behavioral events).
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", "")

	seedAt := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	nSeed, err := svc.SeedSegment(ctx, tid, seg.ID, seedAt, "seed")
	require.NoError(t, err)
	require.GreaterOrEqual(t, nSeed, 1, "seed enqueues the dormant profile")

	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger()).
		WithClock(func() time.Time { return seedAt.Add(time.Minute) })
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)

	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1, "never-ordered dormant profile is absent → entered via seed")
	require.Equal(t, pu.CustomerProfileID, members[0].CustomerProfileID)
}

// TestSweep_CompositeReArmsToLaterDeadline proves finding #6 end-to-end: a rule with
// two elapse deadlines, swept at the earlier one (a no-op wake), re-arms to the later
// deadline instead of dropping it.
func TestSweep_CompositeReArmsToLaterDeadline(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	// AND(recency(login,24h), absence(order,48h)) — recency deadline is earlier.
	rule := segment.Rule{Operator: segment.OpAnd, Conditions: []segment.Rule{
		{Behavior: &segment.BehaviorSpec{Kind: segment.BehaviorRecency, EventName: "login", Window: "24h"}},
		{Behavior: &segment.BehaviorSpec{Kind: segment.BehaviorAbsence, EventName: "order", Window: "48h"}},
	}}
	seg, err := repo.CreateSegment(ctx, tid, "composite", "", rule)
	require.NoError(t, err)

	t0 := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	pu := seedProfile(t, f, tid, sid, "e1", "login", "u1", "")
	for _, ev := range []struct{ id, name string }{{"lg1", "login"}, {"or1", "order"}} {
		_, err := f.pool.Exec(ctx,
			`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,$3,$4,$5)`,
			tid, pu.CustomerProfileID, ev.id, ev.name, t0)
		require.NoError(t, err)
	}

	// Edge at t0 arms the earliest deadline: recency flips at t0+24h.
	pu.Event.Timestamp, pu.Event.ReceivedAt = t0, t0
	require.NoError(t, svc.Evaluate(ctx, pu))
	due, ok, err := repo.CurrentDueAt(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, due.Equal(t0.Add(24*time.Hour)), "armed to the earlier (recency) deadline")

	// Sweep just past the recency deadline: still not matched (a no-op wake), but the
	// later absence deadline (t0+48h) must survive as the re-armed due_at.
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger()).
		WithClock(func() time.Time { return t0.Add(24*time.Hour + time.Minute) })
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)

	due2, ok, err := repo.CurrentDueAt(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.True(t, ok, "deadline re-armed, not dropped")
	require.True(t, due2.Equal(t0.Add(48*time.Hour)), "re-armed to the later (absence) deadline, got %v", due2)
}

// TestSweep_SafetyReEnqueuesActiveMember: an active membership with no pending row is
// re-enqueued by the safety sweep so a lost deadline self-heals.
func TestSweep_SafetyReEnqueuesActiveMember(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	seg, err := repo.CreateSegment(ctx, tid, "s", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", "")

	_, err = f.pool.Exec(ctx,
		`INSERT INTO segment_membership (tenant_id, segment_id, customer_profile_id, status, last_evaluated_at, transition_seq)
		 VALUES ($1,$2,$3,'active',now(),1)`, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	_, ok, err := repo.CurrentDueAt(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.False(t, ok, "no deadline yet")

	n, err := repo.SafetyReEnqueue(ctx, time.Now().UTC(), 100)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)
	_, ok, err = repo.CurrentDueAt(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.True(t, ok, "active member with no deadline is re-enqueued by the safety sweep")
}

// TestSweep_MetricHooksFire: the sweep-lag histogram and backlog gauge hooks fire
// with sensible values (Phase 8 observability).
func TestSweep_MetricHooksFire(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))
	seg, err := repo.CreateSegment(ctx, tid, "s", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", "")
	insertPending(t, f, tid, seg.ID, pu.CustomerProfileID, time.Now().Add(-2*time.Minute), nil)

	var lag float64
	var backlog, claimed int
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger())
	runner.OnSweepLag = func(s float64) { lag = s }
	runner.OnBacklog = func(n int) { backlog = n }
	runner.OnClaimed = func() { claimed++ }

	n, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)
	require.GreaterOrEqual(t, claimed, 1)
	require.Greater(t, lag, 0.0, "sweep lag = now - due_at is observed")
	require.GreaterOrEqual(t, backlog, 1, "backlog gauge counts the due row")
}

// TestSweep_FairClaimAcrossTenants: one tenant with many overdue rows must not starve
// another tenant's due row in the same claim (finding #8).
func TestSweep_FairClaimAcrossTenants(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	repo := segment.NewRepo(f.pool)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")
	segA, err := repo.CreateSegment(ctx, tidA, "s", "", absenceRule())
	require.NoError(t, err)
	segB, err := repo.CreateSegment(ctx, tidB, "s", "", absenceRule())
	require.NoError(t, err)

	now := time.Now().UTC()
	past := now.Add(-time.Hour)
	for i := 0; i < 100; i++ {
		insertPending(t, f, tidA, segA.ID, uuid.New(), past, nil)
	}
	insertPending(t, f, tidB, segB.ID, uuid.New(), past, nil)

	claimed, err := repo.ClaimDuePending(ctx, now, 20, 5, time.Minute)
	require.NoError(t, err)

	var aCount, bCount int
	for _, c := range claimed {
		switch c.TenantID {
		case tidA:
			aCount++
		case tidB:
			bCount++
		}
	}
	require.Equal(t, 1, bCount, "tenant B's due row is claimed, not starved")
	require.LessOrEqual(t, aCount, 5, "tenant A is capped at the per-tenant cap")
}

// TestSweep_ReclaimsStaleClaim: a row claimed longer than the reclaim window ago is
// re-claimable (crash recovery, finding #29); a freshly claimed row is not.
func TestSweep_ReclaimsStaleClaim(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	repo := segment.NewRepo(f.pool)
	tid, _ := mkTenant(t, f, "acme")
	seg, err := repo.CreateSegment(ctx, tid, "s", "", absenceRule())
	require.NoError(t, err)

	now := time.Now().UTC()
	prof := uuid.New()
	stale := now.Add(-2 * time.Minute)
	insertPending(t, f, tid, seg.ID, prof, now.Add(-time.Hour), &stale)

	claimed, err := repo.ClaimDuePending(ctx, now, 10, 10, time.Minute) // reclaim = 1m
	require.NoError(t, err)
	require.Len(t, claimed, 1, "a stale claim (2m old) is reclaimed")

	// It is now freshly claimed (claimed_at=now); a second claim must skip it.
	claimed2, err := repo.ClaimDuePending(ctx, now, 10, 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed2, 0, "a fresh claim is not reclaimed within the window")
}
