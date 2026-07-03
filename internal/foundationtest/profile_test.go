package foundationtest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/identity"
	"github.com/dinhphu28/osscdp/internal/profile"
)

// resolveAndProfile resolves an event's identity (to get a real cluster +
// canonical id, satisfying the profile FK) then applies it to the profile.
func resolveAndProfile(t *testing.T, f fixture, idSvc *identity.Service, profSvc *profile.Service, env events.Envelope) (string, uuid.UUID) {
	t.Helper()
	require.NoError(t, idSvc.Resolve(context.Background(), env))
	var canonical string
	var cluster uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(), `
		SELECT c.canonical_user_id, c.id
		FROM identity_node n
		JOIN identity_cluster_member m ON m.identity_node_id=n.id
		JOIN identity_cluster c ON c.id=m.cluster_id
		WHERE n.tenant_id=$1 AND n.namespace=$2 AND n.value_hash=$3`,
		env.TenantID, identity.NSUserID, identity.ValueHash(env.TenantID, identity.NSUserID, env.Identifiers.UserID)).
		Scan(&canonical, &cluster))
	require.NoError(t, profSvc.Update(context.Background(), canonical, cluster, nil, env))
	return canonical, cluster
}

func profEnv(t *testing.T, tid, sid uuid.UUID, eventID, name, userID, traits string, ts time.Time) events.Envelope {
	t.Helper()
	in := events.IncomingEvent{EventID: eventID, EventName: name, UserID: userID, Timestamp: ts.Format(time.RFC3339)}
	if traits != "" {
		in.Traits = json.RawMessage(traits)
	}
	if name == "product_viewed" {
		in.Properties = json.RawMessage(`{"product_id":"p001"}`)
	}
	env, err := events.Normalize(in, tid, sid, events.TypeTrack, ts)
	require.NoError(t, err)
	return env
}

func newSvcs(f fixture) (*identity.Service, *profile.Service) {
	return identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved),
		profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
}

func TestProfile_CreateThenUpdate(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	idSvc, profSvc := newSvcs(f)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	e1 := profEnv(t, tid, sid, "e1", "page_viewed", "u1", `{"email":"u@x.com","name":"Ann","country":"VN"}`, base)
	canonical, _ := resolveAndProfile(t, f, idSvc, profSvc, e1)

	p, err := prepo.GetByCanonical(ctx, tid, canonical)
	require.NoError(t, err)
	require.Equal(t, int64(1), p.Version)
	require.Equal(t, "u@x.com", p.Traits["email"])
	require.Equal(t, "Ann", p.Traits["name"])
	require.EqualValues(t, 1, asIntJSON(p.ComputedAttributes["total_events"]))
	require.NotNil(t, p.FirstSeenAt)

	e2 := profEnv(t, tid, sid, "e2", "product_viewed", "u1", "", base.Add(24*time.Hour))
	resolveAndProfile(t, f, idSvc, profSvc, e2)

	p2, err := prepo.GetByCanonical(ctx, tid, canonical)
	require.NoError(t, err)
	require.Equal(t, int64(2), p2.Version)
	require.EqualValues(t, 2, asIntJSON(p2.ComputedAttributes["total_events"]))
	require.Equal(t, "p001", p2.ComputedAttributes["last_product_viewed"])
	require.True(t, p2.LastSeenAt.After(*p2.FirstSeenAt))
}

func TestProfile_IdempotentByEventID(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	idSvc, profSvc := newSvcs(f)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	e1 := profEnv(t, tid, sid, "dup1", "product_viewed", "u1", "", base)
	canonical, cluster := resolveAndProfile(t, f, idSvc, profSvc, e1)

	require.NoError(t, profSvc.Update(ctx, canonical, cluster, nil, e1))
	require.NoError(t, profSvc.Update(ctx, canonical, cluster, nil, e1))

	p, err := prepo.GetByCanonical(ctx, tid, canonical)
	require.NoError(t, err)
	require.EqualValues(t, 1, asIntJSON(p.ComputedAttributes["total_events"]), "total_events must not double-count")

	var hist int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM customer_profile_history WHERE tenant_id=$1`, tid).Scan(&hist))
	require.Equal(t, 1, hist)
}

// reparentPub captures every identity_resolved payload the identity service emits.
type reparentPub struct{ msgs [][]byte }

func (p *reparentPub) Publish(_ context.Context, _, _ string, v []byte) error {
	p.msgs = append(p.msgs, append([]byte(nil), v...))
	return nil
}

// TestProfile_ReparentOnMerge exercises Enhancement D end-to-end: when two
// clusters merge, the loser's profile is folded into the survivor and deleted,
// leaving no orphan row.
func TestProfile_ReparentOnMerge(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	profSvc.Audit = audit.NewRecorder(f.pool)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	mkEnv := func(eventID string, ids events.Identifiers, traits string, ts time.Time) events.Envelope {
		in := events.IncomingEvent{
			EventID: eventID, EventName: "track", UserID: ids.UserID, AnonymousID: ids.AnonymousID,
			Email: ids.Email, Phone: ids.Phone, Timestamp: ts.Format(time.RFC3339),
		}
		if traits != "" {
			in.Traits = json.RawMessage(traits)
		}
		env, err := events.Normalize(in, tid, sid, events.TypeTrack, ts)
		require.NoError(t, err)
		return env
	}

	// resolve mirrors the worker: resolve identity, then apply to the profile
	// using the merged canonical ids the resolution just emitted.
	resolve := func(env events.Envelope) identity.IdentityResolved {
		pub.msgs = nil
		require.NoError(t, idSvc.Resolve(ctx, env))
		require.Len(t, pub.msgs, 1)
		var ir identity.IdentityResolved
		require.NoError(t, json.Unmarshal(pub.msgs[0], &ir))
		require.NoError(t, profSvc.Update(ctx, ir.CanonicalUserID, ir.IdentityClusterID, ir.MergedCanonicalUserIDs, env))
		return ir
	}

	// 1) Anonymous person -> cluster K1 with a name.
	ir1 := resolve(mkEnv("e1", events.Identifiers{AnonymousID: "a1"}, `{"name":"Ann","country":"VN"}`, base))
	k1 := ir1.CanonicalUserID
	require.False(t, ir1.MergeOccurred)

	// 2) Known user -> cluster K2 with phone + email.
	ir2 := resolve(mkEnv("e2", events.Identifiers{UserID: "u1"}, `{"phone":"+8490","email":"u@x.com"}`, base.Add(time.Hour)))
	k2 := ir2.CanonicalUserID
	require.NotEqual(t, k1, k2)
	_, err := prepo.GetByCanonical(ctx, tid, k2)
	require.NoError(t, err, "loser profile exists before merge")

	// 3) Linking event carries both identifiers -> merge; survivor is the older K1.
	ir3 := resolve(mkEnv("e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	require.True(t, ir3.MergeOccurred)
	require.Equal(t, k1, ir3.CanonicalUserID)
	require.Equal(t, []string{k2}, ir3.MergedCanonicalUserIDs)

	// Loser profile is gone — no orphan row.
	_, err = prepo.GetByCanonical(ctx, tid, k2)
	require.ErrorIs(t, err, profile.ErrNotFound)

	// Survivor kept its own name, folded in the loser's phone/email, summed events.
	sp, err := prepo.GetByCanonical(ctx, tid, k1)
	require.NoError(t, err)
	require.Equal(t, "Ann", sp.Traits["name"])
	require.Equal(t, "+8490", sp.Traits["phone"])
	require.Equal(t, "u@x.com", sp.Traits["email"])
	require.EqualValues(t, 3, asIntJSON(sp.ComputedAttributes["total_events"]), "1 (K1) + 1 (folded K2) + 1 (merge event)")

	// The reparent was audited.
	var audits int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_log WHERE tenant_id=$1 AND action='reparent'`, tid).Scan(&audits))
	require.Equal(t, 1, audits)
}

func TestProfile_QueryByEmail(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	idSvc, profSvc := newSvcs(f)
	prepo := profile.NewRepo(f.pool)

	e1 := profEnv(t, tid, sid, "e1", "page_viewed", "u1", `{"email":"find@x.com"}`, time.Now())
	resolveAndProfile(t, f, idSvc, profSvc, e1)

	got, err := prepo.ListByTrait(ctx, tid, profile.TraitEmail, "find@x.com")
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestProfile_EmitsProfileUpdated(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)

	brokers := []string{broker}
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, bus.TopicProfileUpdated))
	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	sink := &captureSink{}
	consumer, err := bus.NewConsumer(brokers, "grp-profile-updated", []string{bus.TopicProfileUpdated}, 1, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Run(runCtx, func(_ context.Context, r bus.Record) error { sink.add(r); return nil }, nil)
	}()

	profSvc := profile.NewService(f.pool, prod, bus.TopicProfileUpdated)
	e1 := profEnv(t, tid, sid, "e1", "product_viewed", "u1", "", time.Now())
	resolveAndProfileWith(t, f, idSvc, profSvc, e1)

	require.Eventually(t, func() bool { return sink.len() >= 1 }, 30*time.Second, 200*time.Millisecond)
	var pu profile.ProfileUpdated
	require.NoError(t, json.Unmarshal(sink.first().Value, &pu))
	require.Equal(t, "profile_updated", pu.EventType)
	require.Equal(t, "e1", pu.EventID)
	require.EqualValues(t, 1, pu.ProfileVersion)
	require.Equal(t, tid.String()+"|"+pu.CanonicalUserID, string(sink.first().Key))
}

// resolveAndProfileWith mirrors resolveAndProfile but uses the provided profile
// service (with a real producer) for the update.
func resolveAndProfileWith(t *testing.T, f fixture, idSvc *identity.Service, profSvc *profile.Service, env events.Envelope) {
	t.Helper()
	require.NoError(t, idSvc.Resolve(context.Background(), env))
	var canonical string
	var cluster uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(), `
		SELECT c.canonical_user_id, c.id FROM identity_node n
		JOIN identity_cluster_member m ON m.identity_node_id=n.id
		JOIN identity_cluster c ON c.id=m.cluster_id
		WHERE n.tenant_id=$1 AND n.namespace=$2 AND n.value_hash=$3`,
		env.TenantID, identity.NSUserID, identity.ValueHash(env.TenantID, identity.NSUserID, env.Identifiers.UserID)).
		Scan(&canonical, &cluster))
	require.NoError(t, profSvc.Update(context.Background(), canonical, cluster, nil, env))
}

func asIntJSON(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	default:
		return -1
	}
}
