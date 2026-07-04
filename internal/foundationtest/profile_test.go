package foundationtest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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

// --- reparent (Enhancement D) test helpers ---

func mergeEnv(t *testing.T, tid, sid uuid.UUID, eventID string, ids events.Identifiers, traits string, ts time.Time) events.Envelope {
	t.Helper()
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

// resolveCapture resolves an event's identity and returns the emitted
// identity_resolved payload without applying it to a profile.
func resolveCapture(t *testing.T, idSvc *identity.Service, pub *reparentPub, env events.Envelope) identity.IdentityResolved {
	t.Helper()
	pub.msgs = nil
	require.NoError(t, idSvc.Resolve(context.Background(), env))
	require.Len(t, pub.msgs, 1)
	var ir identity.IdentityResolved
	require.NoError(t, json.Unmarshal(pub.msgs[0], &ir))
	return ir
}

func applyProfile(t *testing.T, profSvc *profile.Service, ir identity.IdentityResolved, env events.Envelope) {
	t.Helper()
	require.NoError(t, profSvc.Update(context.Background(), ir.CanonicalUserID, ir.IdentityClusterID, ir.MergedCanonicalUserIDs, env))
}

func resolveAndUpdate(t *testing.T, idSvc *identity.Service, pub *reparentPub, profSvc *profile.Service, env events.Envelope) identity.IdentityResolved {
	t.Helper()
	ir := resolveCapture(t, idSvc, pub, env)
	applyProfile(t, profSvc, ir, env)
	return ir
}

// TestProfile_ReparentThreeWayMerge verifies a single event linking three
// previously-distinct clusters folds and deletes both losers.
func TestProfile_ReparentThreeWayMerge(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	ir1 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, `{"name":"Ann"}`, base))
	ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, `{"phone":"+8490"}`, base.Add(time.Hour)))
	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e3", events.Identifiers{Email: "e@x.com"}, `{"country":"VN"}`, base.Add(2*time.Hour)))
	k1, k2, k3 := ir1.CanonicalUserID, ir2.CanonicalUserID, ir3.CanonicalUserID
	require.NotEqual(t, k1, k2)
	require.NotEqual(t, k1, k3)
	require.NotEqual(t, k2, k3)

	// One event carrying all three identifiers -> two-loser merge; survivor = oldest K1.
	ir4 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e4", events.Identifiers{AnonymousID: "a1", UserID: "u1", Email: "e@x.com"}, "", base.Add(3*time.Hour)))
	require.True(t, ir4.MergeOccurred)
	require.Equal(t, k1, ir4.CanonicalUserID)
	require.ElementsMatch(t, []string{k2, k3}, ir4.MergedCanonicalUserIDs)

	for _, loser := range []string{k2, k3} {
		_, err := prepo.GetByCanonical(ctx, tid, loser)
		require.ErrorIs(t, err, profile.ErrNotFound, "loser %s must be deleted", loser)
	}
	sp, err := prepo.GetByCanonical(ctx, tid, k1)
	require.NoError(t, err)
	require.Equal(t, "Ann", sp.Traits["name"])
	require.Equal(t, "+8490", sp.Traits["phone"])
	require.Equal(t, "VN", sp.Traits["country"])
	require.EqualValues(t, 4, asIntJSON(sp.ComputedAttributes["total_events"]), "1 per cluster (3) + merge event")
}

// TestProfile_ReparentRedirectsReorderedLoserEvent verifies the alias-redirect:
// when the merge event is applied to the profile store before the loser's own
// creation event (as can happen across Kafka partitions), the loser event folds
// into the survivor instead of resurrecting a zombie.
func TestProfile_ReparentRedirectsReorderedLoserEvent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	e1 := mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, `{"name":"Ann"}`, base)
	e2 := mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, `{"phone":"+8490"}`, base.Add(time.Hour))
	e3 := mergeEnv(t, tid, sid, "e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour))

	// Resolve identity in order (produces the merge), capturing each payload.
	ir1 := resolveCapture(t, idSvc, pub, e1)
	ir2 := resolveCapture(t, idSvc, pub, e2)
	ir3 := resolveCapture(t, idSvc, pub, e3)
	k1, k2 := ir1.CanonicalUserID, ir2.CanonicalUserID
	require.True(t, ir3.MergeOccurred)
	require.Equal(t, k1, ir3.CanonicalUserID)
	require.Equal(t, []string{k2}, ir3.MergedCanonicalUserIDs)

	// Apply to the profile store OUT OF ORDER: survivor create, then the merge
	// (loser profile absent -> reparent loop skips), then the loser's own event.
	applyProfile(t, profSvc, ir1, e1)
	applyProfile(t, profSvc, ir3, e3)
	applyProfile(t, profSvc, ir2, e2)

	// No zombie for the retired loser canonical.
	_, err := prepo.GetByCanonical(ctx, tid, k2)
	require.ErrorIs(t, err, profile.ErrNotFound)
	// The reordered loser event folded into the survivor via the redirect.
	sp, err := prepo.GetByCanonical(ctx, tid, k1)
	require.NoError(t, err)
	require.Equal(t, "+8490", sp.Traits["phone"])
	require.EqualValues(t, 3, asIntJSON(sp.ComputedAttributes["total_events"]))
}

// TestProfile_ReparentDedupsRedeliveredLoserEvent verifies the loser's
// idempotency records survive the merge (re-keyed to the survivor), so an
// at-least-once redelivery of a loser event is not re-applied.
func TestProfile_ReparentDedupsRedeliveredLoserEvent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	e2 := mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, `{"phone":"+8490"}`, base.Add(time.Hour))
	ir1 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, `{"name":"Ann"}`, base))
	ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, e2)
	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	k1, k2 := ir1.CanonicalUserID, ir2.CanonicalUserID
	require.True(t, ir3.MergeOccurred)

	sp, err := prepo.GetByCanonical(ctx, tid, k1)
	require.NoError(t, err)
	total := asIntJSON(sp.ComputedAttributes["total_events"])

	// Redeliver the loser's own event (already applied before the merge). It must
	// be recognized as applied (history re-keyed to survivor) and not double-count.
	applyProfile(t, profSvc, ir2, e2)

	_, err = prepo.GetByCanonical(ctx, tid, k2)
	require.ErrorIs(t, err, profile.ErrNotFound, "redelivery must not resurrect the loser")
	sp2, err := prepo.GetByCanonical(ctx, tid, k1)
	require.NoError(t, err)
	require.EqualValues(t, total, asIntJSON(sp2.ComputedAttributes["total_events"]), "redelivery must not double-count")
}

// TestProfile_ReparentPreservesConsentAndMembership verifies the loser's consent
// (with denied-wins) and segment memberships migrate to the survivor rather than
// being silently dropped.
func TestProfile_ReparentPreservesConsentAndMembership(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	ir1 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, `{"name":"Ann"}`, base))
	ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, `{"phone":"+8490"}`, base.Add(time.Hour)))
	k1, k2 := ir1.CanonicalUserID, ir2.CanonicalUserID
	sp1, err := prepo.GetByCanonical(ctx, tid, k1)
	require.NoError(t, err)
	sp2, err := prepo.GetByCanonical(ctx, tid, k2)
	require.NoError(t, err)
	survivorID, loserID := sp1.ID, sp2.ID

	// Survivor GRANTED email/marketing; loser DENIED the same -> denied must win.
	_, err = f.pool.Exec(ctx, `INSERT INTO customer_consent (id, tenant_id, customer_profile_id, channel, purpose, status) VALUES (gen_random_uuid(),$1,$2,'email','marketing','granted')`, tid, survivorID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `INSERT INTO customer_consent (id, tenant_id, customer_profile_id, channel, purpose, status) VALUES (gen_random_uuid(),$1,$2,'email','marketing','denied')`, tid, loserID)
	require.NoError(t, err)
	// Loser is an active member of a segment.
	var segID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `INSERT INTO segment (id, tenant_id, name, status) VALUES (gen_random_uuid(),$1,'seg','active') RETURNING id`, tid).Scan(&segID))
	_, err = f.pool.Exec(ctx, `INSERT INTO segment_membership (tenant_id, segment_id, customer_profile_id, status, last_evaluated_at) VALUES ($1,$2,$3,'active',now())`, tid, segID, loserID)
	require.NoError(t, err)

	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	require.True(t, ir3.MergeOccurred)
	require.Equal(t, k1, ir3.CanonicalUserID)

	// Loser gone with no orphan child rows.
	_, err = prepo.GetByCanonical(ctx, tid, k2)
	require.ErrorIs(t, err, profile.ErrNotFound)
	for _, tbl := range []string{"customer_consent", "segment_membership", "customer_profile_history"} {
		var n int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl+` WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, loserID).Scan(&n))
		require.Zero(t, n, "orphan rows left in %s", tbl)
	}
	// Consent denied-wins on the survivor; membership migrated.
	var status string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2 AND channel='email' AND purpose='marketing'`, tid, survivorID).Scan(&status))
	require.Equal(t, "denied", status, "loser opt-out must win over survivor's grant")
	var members int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM segment_membership WHERE tenant_id=$1 AND customer_profile_id=$2 AND segment_id=$3`, tid, survivorID, segID).Scan(&members))
	require.Equal(t, 1, members, "loser membership must migrate to the survivor")
}

// TestProfile_ReparentAuditFailureStillEmits verifies a failing reparent audit
// does not gate the profile_updated emit (which the segment/activation workers
// depend on) — the audit is best-effort.
func TestProfile_ReparentAuditFailureStillEmits(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	emitPub := &reparentPub{}
	profSvc := profile.NewService(f.pool, emitPub, bus.TopicProfileUpdated)
	// Audit recorder on a closed pool -> Record fails.
	badPool, err := pgxpool.NewWithConfig(ctx, f.pool.Config())
	require.NoError(t, err)
	badPool.Close()
	profSvc.Audit = audit.NewRecorder(badPool)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, `{"name":"Ann"}`, base))
	resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, `{"phone":"+8490"}`, base.Add(time.Hour)))
	emitPub.msgs = nil
	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	require.True(t, ir3.MergeOccurred)

	// The merge committed (loser deleted) AND profile_updated was still emitted.
	_, err = prepo.GetByCanonical(ctx, tid, ir3.MergedCanonicalUserIDs[0])
	require.ErrorIs(t, err, profile.ErrNotFound)
	require.NotEmpty(t, emitPub.msgs, "profile_updated must still emit when the reparent audit fails")
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
