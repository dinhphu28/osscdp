package foundationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/consent"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/governance"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

// TestGovernance_ErasesBehavioralData: erasure removes the Level-3 behavioral tables
// (behavioral_event carries PII) and commits with no FK crash (findings #22, #24).
func TestGovernance_ErasesBehavioralData(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "ev", "order_completed", "u1", `{"country":"VN"}`)

	_, err = f.pool.Exec(ctx,
		`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,'be1','order_completed',now())`,
		tid, pu.CustomerProfileID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx,
		`INSERT INTO profile_behavior_bucket (tenant_id, customer_profile_id, event_name, bucket_start, count, first_at, last_at) VALUES ($1,$2,'order_completed', date_trunc('hour', now()), 1, now(), now())`,
		tid, pu.CustomerProfileID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx,
		`INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason) VALUES ($1,$2,$3, now(), 'seed')`,
		tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	// An outbox emit whose payload/partition_key carry the profile id + canonical.
	_, err = f.pool.Exec(ctx,
		`INSERT INTO segment_membership_outbox (tenant_id, partition_key, payload_json) VALUES ($1,$2,$3)`,
		tid, tid.String()+"|"+pu.CanonicalUserID, fmt.Sprintf(`{"customer_profile_id":"%s","change":"entered"}`, pu.CustomerProfileID))
	require.NoError(t, err)

	gov := governance.NewService(f.pool, audit.NewRecorder(f.pool), nil)
	counts, err := gov.Delete(ctx, tid, pu.CanonicalUserID)
	require.NoError(t, err, "erasure must commit across the new tables")
	require.EqualValues(t, 1, counts.BehavioralEvent)
	require.EqualValues(t, 1, counts.BehaviorBucket)
	require.EqualValues(t, 1, counts.PendingEval)
	require.EqualValues(t, 1, counts.OutboxEmits)

	for _, tbl := range []string{"behavioral_event", "profile_behavior_bucket", "segment_pending_eval"} {
		var n int
		require.NoError(t, f.pool.QueryRow(ctx,
			`SELECT count(*) FROM `+tbl+` WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, pu.CustomerProfileID).Scan(&n))
		require.Zero(t, n, "%s must be erased", tbl)
	}
	var outbox int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_membership_outbox WHERE tenant_id=$1`, tid).Scan(&outbox))
	require.Zero(t, outbox, "the outbox emit carrying the erased identifier must be gone")
}

// TestBehavior_ConsentGatesProps: a profile that denied analytics consent still has
// its events counted, but the verbatim props_json PII is dropped (finding: opt-out
// resurrection channel closed).
func TestBehavior_ConsentGatesProps(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "seed", "page_view", "u1", "")
	require.NoError(t, consent.NewRepo(f.pool).Set(ctx, tid, pu.CustomerProfileID, "webhook", "analytics", consent.StatusDenied, "test"))

	rec := behavior.NewRecorder()
	rec.PropsGate = behavior.ConsentPropsGate{}
	env := events.Envelope{
		TenantID: tid, Type: events.TypeTrack, EventName: "product_viewed", EventID: "cg1",
		Properties: []byte(`{"email":"x@y.com"}`), Timestamp: time.Now(), ReceivedAt: time.Now(),
	}
	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, rec.Append(ctx, tx, pu.CustomerProfileID, env))
	require.NoError(t, tx.Commit(ctx))

	var props *string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT props_json FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_id='cg1'`,
		tid, pu.CustomerProfileID).Scan(&props))
	require.Nil(t, props, "denied analytics consent nulls props_json")
	require.Equal(t, 1, countBehavioral(t, f, tid, pu.CustomerProfileID), "the event is still counted")
	_ = sid
}

// TestSegment_LoosenedTraitAdmitsNewMember: loosening a trait-only rule admits a
// previously non-matching profile via the seed + sweep (finding #24).
func TestSegment_LoosenedTraitAdmitsNewMember(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	tight := segment.Rule{Operator: segment.OpAnd, Conditions: []segment.Rule{
		{Field: "profile.traits.country", Op: segment.OpEq, Value: "US"},
		{Field: "profile.traits.tier", Op: segment.OpEq, Value: "gold"},
	}}
	seg, err := repo.CreateSegment(ctx, tid, "us-gold", "", tight)
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", `{"country":"US","tier":"silver"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 0, "US+silver does not match US AND gold")

	// Loosen to just country=US; the seed enumerates the population (widened gate).
	_, err = repo.UpdateSegment(ctx, tid, seg.ID, "", segment.Rule{Field: "profile.traits.country", Op: segment.OpEq, Value: "US"})
	require.NoError(t, err)
	n, err := svc.SeedSegment(ctx, tid, seg.ID, time.Now(), "version_change")
	require.NoError(t, err)
	require.Greater(t, n, 0, "a loosened trait-only rule seeds the population")

	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger())
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	members, err = repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1, "the newly-qualifying profile enters on recompute")
}

// TestSegment_VersionChangeExitsStaleMember: UpdateSegment enqueues active members
// (reason=version_change); the sweep re-evaluates and exits a member who no longer
// matches the tightened rule (finding #24).
func TestSegment_VersionChangeExitsStaleMember(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	// Sweep-safe (no event.* leaf) trait rule so the sweeper can re-evaluate it.
	seg, err := repo.CreateSegment(ctx, tid, "us", "", segment.Rule{Field: "profile.traits.country", Op: segment.OpEq, Value: "US"})
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", `{"country":"US"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1)

	// Tighten to country=JP: the active member is enqueued for recompute.
	_, err = repo.UpdateSegment(ctx, tid, seg.ID, "", segment.Rule{Field: "profile.traits.country", Op: segment.OpEq, Value: "JP"})
	require.NoError(t, err)
	var reason string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT reason FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tid, seg.ID, pu.CustomerProfileID).Scan(&reason))
	require.Equal(t, "version_change", reason)

	// The sweep re-evaluates the US profile against JP → exits it.
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger())
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	status, err := repo.MembershipStatus(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.Equal(t, segment.MembershipExited, status, "tightened rule exits the stale member via recompute")
}

// TestSegment_DeactivateRetires: deactivating a segment flips it inactive, purges its
// pending rows, and the sweep drops any stranded due-row without mis-firing (#25).
func TestSegment_DeactivateRetires(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "", absenceRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", "")

	_, err = f.pool.Exec(ctx,
		`INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason) VALUES ($1,$2,$3, now(), 'seed')`,
		tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)

	require.NoError(t, repo.DeactivateSegment(ctx, tid, seg.ID))

	got, err := repo.GetSegment(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Equal(t, segment.SegmentInactive, got.Status)

	active, err := repo.ActiveSegmentVersions(ctx, tid)
	require.NoError(t, err)
	require.Len(t, active, 0, "retired segment is not evaluated at the edge")

	var pending int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2`, tid, seg.ID).Scan(&pending))
	require.Zero(t, pending, "retire purges the segment's due-rows")

	// A stranded due-row (e.g. armed concurrently) must be dropped by the sweep, not
	// mis-fired against the retired rule.
	_, err = f.pool.Exec(ctx,
		`INSERT INTO segment_pending_eval (tenant_id, segment_id, customer_profile_id, due_at, reason) VALUES ($1,$2,$3, now(), 'seed')`,
		tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	runner := segment.NewRunner(svc, 100, 50, 0, time.Minute, time.Second, testLogger())
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM segment_pending_eval WHERE tenant_id=$1 AND segment_id=$2`, tid, seg.ID).Scan(&pending))
	require.Zero(t, pending, "the sweep drops a stranded due-row for an inactive segment")
	require.Equal(t, 0, countMembershipOutbox(t, f, tid), "no spurious membership emit on a retired segment")
}
