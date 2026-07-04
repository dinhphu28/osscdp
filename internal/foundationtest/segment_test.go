package foundationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/identity"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

// seedProfile resolves identity + builds a profile for a user, returning the
// profile id, canonical id, and a profile_updated message for that event.
func seedProfile(t *testing.T, f fixture, tid, sid uuid.UUID, eventID, name, userID, traits string) profile.ProfileUpdated {
	t.Helper()
	idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	env := profEnv(t, tid, sid, eventID, name, userID, traits, time.Now())
	canonical, cluster := resolveAndProfile(t, f, idSvc, profSvc, env)

	p, err := profile.NewRepo(f.pool).GetByCanonical(context.Background(), tid, canonical)
	require.NoError(t, err)
	_ = cluster
	return profile.ProfileUpdated{
		TenantID:          tid,
		EventID:           eventID,
		CustomerProfileID: p.ID,
		CanonicalUserID:   canonical,
		Event:             env,
	}
}

func vnPhoneRule() segment.Rule {
	return segment.Rule{Operator: segment.OpAnd, Conditions: []segment.Rule{
		{Field: "profile.traits.country", Op: segment.OpEq, Value: "VN"},
		{Field: "event.event_name", Op: segment.OpEq, Value: "product_viewed"},
	}}
}

func TestSegment_EnterThenExit(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), noopPub{}, bus.TopicSegmentMembershipChanged, nil)

	seg, err := repo.CreateSegment(ctx, tid, "vn-phone-viewers", "", vnPhoneRule())
	require.NoError(t, err)
	require.Equal(t, 1, seg.CurrentVersion)

	// Matching event → entered.
	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))

	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, pu.CustomerProfileID, members[0].CustomerProfileID)

	// Non-matching event (different event name) → exited.
	pu2 := seedProfile(t, f, tid, sid, "e2", "page_viewed", "u1", "")
	require.NoError(t, svc.Evaluate(ctx, pu2))

	members2, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members2, 0)

	status, err := repo.MembershipStatus(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.Equal(t, segment.MembershipExited, status)
}

func TestSegment_Idempotent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)

	var emits int
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), countingPub{&emits}, bus.TopicSegmentMembershipChanged, nil)
	seg, err := repo.CreateSegment(ctx, tid, "s1", "", vnPhoneRule())
	require.NoError(t, err)

	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))
	require.NoError(t, svc.Evaluate(ctx, pu)) // re-deliver
	require.NoError(t, svc.Evaluate(ctx, pu))

	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, 1, emits, "membership change must emit only on the entering transition")
}

func TestSegment_VersioningAndEdit(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)

	seg, err := repo.CreateSegment(ctx, tid, "s1", "first", vnPhoneRule())
	require.NoError(t, err)

	newRule := segment.Rule{Field: "profile.computed_attributes.total_orders", Op: segment.OpGt, Value: float64(3)}
	updated, err := repo.UpdateSegment(ctx, tid, seg.ID, "second", newRule)
	require.NoError(t, err)
	require.Equal(t, 2, updated.CurrentVersion)
	require.NotEqual(t, *seg.CurrentVersionID, *updated.CurrentVersionID)
	require.Equal(t, segment.OpGt, updated.Rule.Op)
}

func TestSegment_TenantIsolation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tidA, sidA := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), noopPub{}, bus.TopicSegmentMembershipChanged, nil)

	segA, err := repo.CreateSegment(ctx, tidA, "s", "", vnPhoneRule())
	require.NoError(t, err)
	// Tenant B has no segments → evaluating B's profile touches nothing in A.
	pu := seedProfile(t, f, tidA, sidA, "e1", "product_viewed", "u1", `{"country":"VN"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))

	// B's active segments are empty.
	bSegs, err := repo.ActiveSegmentVersions(ctx, tidB)
	require.NoError(t, err)
	require.Len(t, bSegs, 0)

	membersA, err := repo.ListMembers(ctx, tidA, segA.ID)
	require.NoError(t, err)
	require.Len(t, membersA, 1)
}

func TestSegment_EmitsMembershipChanged(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)

	brokers := []string{broker}
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, bus.TopicSegmentMembershipChanged))
	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	sink := &captureSink{}
	consumer, err := bus.NewConsumer(brokers, "grp-seg-changed", []string{bus.TopicSegmentMembershipChanged}, 1, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Run(runCtx, func(_ context.Context, r bus.Record) error { sink.add(r); return nil }, nil)
	}()

	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), prod, bus.TopicSegmentMembershipChanged, nil)
	seg, err := repo.CreateSegment(ctx, tid, "s1", "", vnPhoneRule())
	require.NoError(t, err)

	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`)
	require.NoError(t, svc.Evaluate(ctx, pu))

	require.Eventually(t, func() bool { return sink.len() >= 1 }, 30*time.Second, 200*time.Millisecond)
	var mc segment.MembershipChanged
	require.NoError(t, json.Unmarshal(sink.first().Value, &mc))
	require.Equal(t, "segment_membership_changed", mc.EventType)
	require.Equal(t, segment.ChangeEntered, mc.Change)
	require.Equal(t, seg.ID, mc.SegmentID)
	require.Equal(t, "e1", mc.ReasonEventID)
	require.Equal(t, tid.String()+"|"+pu.CanonicalUserID, string(sink.first().Key))
}

type countingPub struct{ n *int }

func (c countingPub) Publish(context.Context, string, string, []byte) error { *c.n++; return nil }

// TestSegment_StatefulEnterViaBehavioralEvents is the Phase-3 end-to-end proof:
// seeded behavioral_event rows drive a count-in-window segment to Enter through
// the real Service.Evaluate membership switch (with a real behavior.Store).
func TestSegment_StatefulEnterViaBehavioralEvents(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), noopPub{}, bus.TopicSegmentMembershipChanged, behavior.NewStore(f.pool))

	val := 3.0
	rule := segment.Rule{Behavior: &segment.BehaviorSpec{Kind: segment.BehaviorCount, EventName: "product_viewed", Window: "7d", Op: segment.OpGte, Value: &val}}
	seg, err := repo.CreateSegment(ctx, tid, "power-viewers", "", rule)
	require.NoError(t, err)

	seedViews := func(pu profile.ProfileUpdated, n int) {
		for i := 0; i < n; i++ {
			_, err := f.pool.Exec(ctx,
				`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,$3,'product_viewed',$4)`,
				tid, pu.CustomerProfileID, fmt.Sprintf("bv-%s-%d", pu.CustomerProfileID, i), pu.Event.Timestamp.Add(-time.Duration(i+1)*time.Hour))
			require.NoError(t, err)
		}
	}

	// 3 qualifying views -> enters through the stateful count leaf.
	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", "")
	seedViews(pu, 3)
	require.NoError(t, svc.Evaluate(ctx, pu))
	members, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, pu.CustomerProfileID, members[0].CustomerProfileID)

	// Only 2 views -> below threshold -> does not enter.
	pu2 := seedProfile(t, f, tid, sid, "e2", "product_viewed", "u2", "")
	seedViews(pu2, 2)
	require.NoError(t, svc.Evaluate(ctx, pu2))
	members2, err := repo.ListMembers(ctx, tid, seg.ID)
	require.NoError(t, err)
	require.Len(t, members2, 1, "the 2-view profile must not enter")
	require.Equal(t, pu.CustomerProfileID, members2[0].CustomerProfileID)
}
