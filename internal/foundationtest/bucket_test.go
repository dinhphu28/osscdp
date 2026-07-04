package foundationtest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/identity"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

func bucketSum(t *testing.T, f fixture, tid, profileID uuid.UUID, name string) int64 {
	t.Helper()
	var n *int64
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT SUM(count) FROM profile_behavior_bucket WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3`,
		tid, profileID, name).Scan(&n))
	if n == nil {
		return 0
	}
	return *n
}

// TestBucket_MergeRebuildsFromLog: after a cluster merge, the survivor's buckets are
// rebuilt from the deduped log, so the total equals the survivor's log count and a
// shared event_id is not double-counted (finding #21).
func TestBucket_MergeRebuildsFromLog(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := behaviorSvc(f)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// Two clusters, each recording a track event (populates per-profile buckets).
	ir1 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "v1", events.Identifiers{AnonymousID: "a1"}, "", base))
	ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "v2", events.Identifiers{UserID: "u1"}, "", base.Add(time.Hour)))
	sp1, err := prepo.GetByCanonical(ctx, tid, ir1.CanonicalUserID)
	require.NoError(t, err)
	sp2, err := prepo.GetByCanonical(ctx, tid, ir2.CanonicalUserID)
	require.NoError(t, err)

	// A shared event_id delivered to both profiles (same event, two deliveries).
	shared := mergeEnv(t, tid, sid, "shared", events.Identifiers{UserID: "u1"}, "", base.Add(90*time.Minute))
	require.NoError(t, profSvc.Update(ctx, ir1.CanonicalUserID, sp1.IdentityClusterID, nil, shared))
	require.NoError(t, profSvc.Update(ctx, ir2.CanonicalUserID, sp2.IdentityClusterID, nil, shared))

	// Merge: event carrying both identifiers.
	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "v3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	require.True(t, ir3.MergeOccurred)
	survivor, err := prepo.GetByCanonical(ctx, tid, ir3.CanonicalUserID)
	require.NoError(t, err)

	// Survivor's bucket total equals its deduped log count (rebuild-from-log), and the
	// shared event_id is counted once: v1,v2,shared,v3 across profiles -> 4 unique.
	require.Equal(t, 4, countBehavioral(t, f, tid, survivor.ID), "deduped log = 4 unique events")
	require.EqualValues(t, 4, bucketSum(t, f, tid, survivor.ID, "track"),
		"rebuilt buckets equal the deduped log; shared event_id not double-counted")
}

// TestSegment_PrefilterKeepsStatelessSegment: a mixed rule (stateless country=US AND
// behavioural count>=3) must still be evaluated — and enter — on an event NOT in its
// referenced_event_names, because a stateless leaf can newly match (findings #15/#30).
func TestSegment_PrefilterKeepsStatelessSegment(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	val := 3.0
	rule := segment.Rule{Operator: segment.OpAnd, Conditions: []segment.Rule{
		{Field: "profile.traits.country", Op: segment.OpEq, Value: "US"},
		{Behavior: &segment.BehaviorSpec{Kind: segment.BehaviorCount, EventName: "product_viewed", Window: "7d", Op: segment.OpGte, Value: &val}},
	}}
	seg, err := repo.CreateSegment(ctx, tid, "us-power-viewers", "", rule)
	require.NoError(t, err)

	// A US profile with 3 product_viewed already recorded, but the triggering event is
	// a page_view (NOT product_viewed → not in referenced_event_names).
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", `{"country":"US"}`)
	for i := 0; i < 3; i++ {
		occ := pu.Event.Timestamp.Add(-time.Duration(i+1) * time.Hour)
		_, err := f.pool.Exec(ctx,
			`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,$3,'product_viewed',$4)`,
			tid, pu.CustomerProfileID, "pv"+string(rune('a'+i)), occ)
		require.NoError(t, err)
		// The count leaf is non-exact → served from buckets, so seed the bucket too.
		_, err = f.pool.Exec(ctx,
			`INSERT INTO profile_behavior_bucket (tenant_id, customer_profile_id, event_name, bucket_start, count, first_at, last_at)
			 VALUES ($1,$2,'product_viewed', date_trunc('hour', $3::timestamptz), 1, $3, $3)
			 ON CONFLICT (tenant_id, customer_profile_id, event_name, bucket_start)
			 DO UPDATE SET count = profile_behavior_bucket.count + 1`,
			tid, pu.CustomerProfileID, occ)
		require.NoError(t, err)
	}

	require.NoError(t, svc.Evaluate(ctx, pu))
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1, "stateless-leaf segment must be evaluated on a non-referenced event and enter")
}

// TestSegment_PrefilterSkipsPureBehavioral: a pure-behavioural segment (no stateless
// leaf) is NOT evaluated on an event it does not reference — the positive prefilter
// win — so no deadline is armed for that event.
func TestSegment_PrefilterSkipsPureBehavioral(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), behavior.NewStore(f.pool))

	// absence(order_completed, 24h): references only "order_completed".
	seg, err := repo.CreateSegment(ctx, tid, "no-order", "",
		segment.Rule{Behavior: &segment.BehaviorSpec{Kind: segment.BehaviorAbsence, EventName: "order_completed", Window: "24h"}})
	require.NoError(t, err)

	// A page_view event (not referenced) must be skipped → no deadline armed by it.
	pu := seedProfile(t, f, tid, sid, "e1", "page_view", "u1", "")
	require.NoError(t, svc.Evaluate(ctx, pu))

	_, _, ok, err := repo.CurrentDueAt(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.False(t, ok, "the unreferenced event must not evaluate the pure-behavioural segment")
}

// TestSegment_CacheInvalidatesOnUpdate: UpdateSegment must be visible to the next
// Evaluate (the epoch-keyed cache invalidates cross-write).
func TestSegment_CacheInvalidatesOnUpdate(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), nil)

	seg, err := repo.CreateSegment(ctx, tid, "s", "", vnPhoneRule()) // country=VN AND event=product_viewed
	require.NoError(t, err)

	// VN profile matches v1 → enters (also primes the cache).
	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1)

	// Update the rule to require country=JP; the same VN profile now fails → exits,
	// proving the next Evaluate saw the new rule (cache invalidated on the update).
	jpRule := segment.Rule{Operator: segment.OpAnd, Conditions: []segment.Rule{
		{Field: "profile.traits.country", Op: segment.OpEq, Value: "JP"},
		{Field: "event.event_name", Op: segment.OpEq, Value: "product_viewed"},
	}}
	_, err = repo.UpdateSegment(ctx, tid, seg.ID, "", jpRule)
	require.NoError(t, err)

	pu2 := seedProfile(t, f, tid, sid, "e2", "product_viewed", "u1", `{"country":"VN"}`)
	require.NoError(t, svc.Evaluate(ctx, pu2))
	status, err := repo.MembershipStatus(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.Equal(t, segment.MembershipExited, status, "next Evaluate saw the updated (JP) rule and exited the VN profile")
}
