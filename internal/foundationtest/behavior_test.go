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
)

func behaviorSvc(f fixture) *profile.Service {
	s := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	s.Behavior = behavior.NewRecorder()
	return s
}

func countBehavioral(t *testing.T, f fixture, tid, profileID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, profileID).Scan(&n))
	return n
}

// TestBehavioralEventRecorder groups the Phase 2 behavioral-event cases under one
// testcontainer (each sub-test uses its own tenant, so data stays isolated).
func TestBehavioralEventRecorder(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// The append sits behind the profile idempotency ledger: a redelivered event
	// writes exactly one behavioral row.
	t.Run("AppendsOnceUnderRedelivery", func(t *testing.T) {
		tid, sid := mkTenant(t, f, "acme")
		idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
		profSvc := behaviorSvc(f)
		prepo := profile.NewRepo(f.pool)

		e := mergeEnv(t, tid, sid, "be1", events.Identifiers{UserID: "u1"}, "", base)
		canonical, cluster := resolveAndProfile(t, f, idSvc, profSvc, e)
		require.NoError(t, profSvc.Update(ctx, canonical, cluster, nil, e)) // redelivery

		p, err := prepo.GetByCanonical(ctx, tid, canonical)
		require.NoError(t, err)
		require.Equal(t, 1, countBehavioral(t, f, tid, p.ID), "append must be idempotent under redelivery")
	})

	// identify/alias carry no behavior.
	t.Run("SkipsIdentifyAndAlias", func(t *testing.T) {
		tid, sid := mkTenant(t, f, "acme")
		idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
		profSvc := behaviorSvc(f)
		prepo := profile.NewRepo(f.pool)

		idEnv, err := events.Normalize(events.IncomingEvent{EventID: "id1", UserID: "u1"}, tid, sid, events.TypeIdentify, time.Now())
		require.NoError(t, err)
		canonical, cluster := resolveAndProfile(t, f, idSvc, profSvc, idEnv)
		// alias — even carrying a non-empty event_name, the Type gate must skip it.
		aliasEnv, err := events.Normalize(events.IncomingEvent{EventID: "al1", EventName: "x", UserID: "u1", PreviousID: "a1"}, tid, sid, events.TypeAlias, time.Now())
		require.NoError(t, err)
		require.NoError(t, profSvc.Update(ctx, canonical, cluster, nil, aliasEnv))

		p, err := prepo.GetByCanonical(ctx, tid, canonical)
		require.NoError(t, err)
		require.Zero(t, countBehavioral(t, f, tid, p.ID), "identify/alias must not append a behavioral_event")
	})

	// occurred_at = LEAST(Timestamp, ReceivedAt): future clamps down; past is preserved.
	t.Run("ClampsTimestamp", func(t *testing.T) {
		tid, sid := mkTenant(t, f, "acme")
		idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
		profSvc := behaviorSvc(f)
		prepo := profile.NewRepo(f.pool)
		received := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

		occ := func(clientTS time.Time, userID, evID string) time.Time {
			in := events.IncomingEvent{EventID: evID, EventName: "product_viewed", UserID: userID, Timestamp: clientTS.Format(time.RFC3339)}
			env, err := events.Normalize(in, tid, sid, events.TypeTrack, received)
			require.NoError(t, err)
			canonical, _ := resolveAndProfile(t, f, idSvc, profSvc, env)
			p, err := prepo.GetByCanonical(ctx, tid, canonical)
			require.NoError(t, err)
			var got time.Time
			require.NoError(t, f.pool.QueryRow(ctx,
				`SELECT occurred_at FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_id=$3`, tid, p.ID, evID).Scan(&got))
			return got
		}
		require.True(t, occ(received.Add(72*time.Hour), "fut", "fut1").Equal(received), "future must clamp to received_at")
		past := received.Add(-72 * time.Hour)
		require.True(t, occ(past, "old", "old1").Equal(past), "past client timestamp must be preserved")
	})

	// props_json round-trips for a track event with properties.
	t.Run("StoresProps", func(t *testing.T) {
		tid, sid := mkTenant(t, f, "acme")
		idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
		profSvc := behaviorSvc(f)
		prepo := profile.NewRepo(f.pool)

		in := events.IncomingEvent{EventID: "p1", EventName: "product_viewed", UserID: "u1", Properties: []byte(`{"product_id":"p001"}`)}
		env, err := events.Normalize(in, tid, sid, events.TypeTrack, time.Now())
		require.NoError(t, err)
		canonical, _ := resolveAndProfile(t, f, idSvc, profSvc, env)
		p, err := prepo.GetByCanonical(ctx, tid, canonical)
		require.NoError(t, err)
		var prod string
		require.NoError(t, f.pool.QueryRow(ctx,
			`SELECT props_json->>'product_id' FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, p.ID).Scan(&prod))
		require.Equal(t, "p001", prod)
	})

	// Two appends of the same event_id at DIFFERENT occurred_at (future-clamp re-ingest)
	// yield one row — the append dedups by event_id, not the occurred_at-bearing PK.
	t.Run("AppendIdempotentByEventID", func(t *testing.T) {
		tid, sid := mkTenant(t, f, "acme")
		rec := behavior.NewRecorder()
		profID := uuid.New()
		future := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		mk := func(recv time.Time) events.Envelope {
			in := events.IncomingEvent{EventID: "dup", EventName: "product_viewed", UserID: "u1", Timestamp: future.Format(time.RFC3339)}
			e, err := events.Normalize(in, tid, sid, events.TypeTrack, recv)
			require.NoError(t, err)
			return e
		}
		tx, err := f.pool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, rec.Append(ctx, tx, profID, mk(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))))
		require.NoError(t, rec.Append(ctx, tx, profID, mk(time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC))))
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, 1, countBehavioral(t, f, tid, profID), "same event_id at differing occurred_at must dedup to one row")
	})

	// A cluster merge folds the loser's behavioral rows into the survivor (union, no
	// double-count of a shared event_id) and commits — proving 00011 rows do not
	// FK-crash the merge (doc 16 finding #23).
	t.Run("MergeFoldsBehavioralEvents", func(t *testing.T) {
		tid, sid := mkTenant(t, f, "acme")
		pub := &reparentPub{}
		idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
		profSvc := behaviorSvc(f)
		prepo := profile.NewRepo(f.pool)

		ir1 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "be1", events.Identifiers{AnonymousID: "a1"}, "", base))
		ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "be2", events.Identifiers{UserID: "u1"}, "", base.Add(time.Hour)))
		k1, k2 := ir1.CanonicalUserID, ir2.CanonicalUserID
		sp1, err := prepo.GetByCanonical(ctx, tid, k1)
		require.NoError(t, err)
		sp2, err := prepo.GetByCanonical(ctx, tid, k2)
		require.NoError(t, err)

		shared := mergeEnv(t, tid, sid, "shared", events.Identifiers{UserID: "u1"}, "", base.Add(90*time.Minute))
		require.NoError(t, profSvc.Update(ctx, k1, sp1.IdentityClusterID, nil, shared))
		require.NoError(t, profSvc.Update(ctx, k2, sp2.IdentityClusterID, nil, shared))
		require.Equal(t, 2, countBehavioral(t, f, tid, sp1.ID))
		require.Equal(t, 2, countBehavioral(t, f, tid, sp2.ID))

		ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "be3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
		require.True(t, ir3.MergeOccurred)
		require.Equal(t, k1, ir3.CanonicalUserID)

		_, err = prepo.GetByCanonical(ctx, tid, k2)
		require.ErrorIs(t, err, profile.ErrNotFound)
		require.Zero(t, countBehavioral(t, f, tid, sp2.ID), "loser behavioral rows must be gone")
		require.Equal(t, 4, countBehavioral(t, f, tid, sp1.ID), "union: be1, shared, be2, be3")
		var sharedCount int
		require.NoError(t, f.pool.QueryRow(ctx,
			`SELECT count(*) FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_id='shared'`, tid, sp1.ID).Scan(&sharedCount))
		require.Equal(t, 1, sharedCount, "shared event_id must not be double-counted after the fold")
	})
}
