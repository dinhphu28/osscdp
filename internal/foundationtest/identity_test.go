package foundationtest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/identity"
)

type noopPub struct{}

func (noopPub) Publish(context.Context, string, string, []byte) error { return nil }

func identityEnv(t *testing.T, tid, sid uuid.UUID, typ, eventID string, ids events.Identifiers, previousID string) events.Envelope {
	t.Helper()
	in := events.IncomingEvent{
		EventID: eventID, UserID: ids.UserID, AnonymousID: ids.AnonymousID,
		Email: ids.Email, Phone: ids.Phone, ExternalID: ids.ExternalID, DeviceID: ids.DeviceID,
		PreviousID: previousID, EventName: "x",
	}
	env, err := events.Normalize(in, tid, sid, typ, time.Now())
	require.NoError(t, err)
	return env
}

func clusterCount(t *testing.T, f fixture, tid uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM identity_cluster WHERE tenant_id=$1 AND status='active'`, tid).Scan(&n))
	return n
}

func canonicalForNode(t *testing.T, f fixture, tid uuid.UUID, namespace, value string) string {
	t.Helper()
	vh := identity.ValueHash(tid, namespace, value)
	var canonical string
	err := f.pool.QueryRow(context.Background(), `
		SELECT c.canonical_user_id
		FROM identity_node n
		JOIN identity_cluster_member m ON m.tenant_id=n.tenant_id AND m.identity_node_id=n.id
		JOIN identity_cluster c ON c.tenant_id=m.tenant_id AND c.id=m.cluster_id
		WHERE n.tenant_id=$1 AND n.namespace=$2 AND n.value_hash=$3`,
		tid, namespace, vh).Scan(&canonical)
	require.NoError(t, err)
	return canonical
}

func newIdentitySvc(f fixture) *identity.Service {
	return identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
}

func TestIdentity_FirstEventCreatesCluster(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")
	svc := newIdentitySvc(f)

	env := identityEnv(t, tid, sid, events.TypeTrack, "e1", events.Identifiers{UserID: "u1"}, "")
	require.NoError(t, svc.Resolve(context.Background(), env))
	require.Equal(t, 1, clusterCount(t, f, tid))

	c := canonicalForNode(t, f, tid, identity.NSUserID, "u1")
	require.Contains(t, c, "customer_")
}

func TestIdentity_IdentifyLinksAnonAndUserIntoOneCluster(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")
	svc := newIdentitySvc(f)

	env := identityEnv(t, tid, sid, events.TypeIdentify, "e1", events.Identifiers{UserID: "u1", AnonymousID: "a1"}, "")
	require.NoError(t, svc.Resolve(context.Background(), env))

	require.Equal(t, 1, clusterCount(t, f, tid))
	require.Equal(t,
		canonicalForNode(t, f, tid, identity.NSUserID, "u1"),
		canonicalForNode(t, f, tid, identity.NSAnonymousID, "a1"))
}

func TestIdentity_MergeTwoClusters(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	svc := newIdentitySvc(f)

	// Two separate clusters.
	require.NoError(t, svc.Resolve(ctx, identityEnv(t, tid, sid, events.TypeTrack, "e1", events.Identifiers{AnonymousID: "a1"}, "")))
	require.NoError(t, svc.Resolve(ctx, identityEnv(t, tid, sid, events.TypeTrack, "e2", events.Identifiers{UserID: "u1"}, "")))
	require.Equal(t, 2, clusterCount(t, f, tid))

	// An alias links them → merge to one.
	require.NoError(t, svc.Resolve(ctx, identityEnv(t, tid, sid, events.TypeAlias, "e3", events.Identifiers{UserID: "u1"}, "a1")))
	require.Equal(t, 1, clusterCount(t, f, tid))

	require.Equal(t,
		canonicalForNode(t, f, tid, identity.NSUserID, "u1"),
		canonicalForNode(t, f, tid, identity.NSAnonymousID, "a1"))

	var merges int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM identity_merge_history WHERE tenant_id=$1`, tid).Scan(&merges))
	require.Equal(t, 1, merges)

	var merged int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM identity_cluster WHERE tenant_id=$1 AND status='merged'`, tid).Scan(&merged))
	require.Equal(t, 1, merged)
}

func TestIdentity_Idempotent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	svc := newIdentitySvc(f)

	env := identityEnv(t, tid, sid, events.TypeIdentify, "e1", events.Identifiers{UserID: "u1", AnonymousID: "a1"}, "")
	require.NoError(t, svc.Resolve(ctx, env))
	require.NoError(t, svc.Resolve(ctx, env)) // reprocess
	require.NoError(t, svc.Resolve(ctx, env))

	require.Equal(t, 1, clusterCount(t, f, tid))
	var nodes int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM identity_node WHERE tenant_id=$1`, tid).Scan(&nodes))
	require.Equal(t, 2, nodes) // u1, a1 — not duplicated
}

func TestIdentity_EmitsResolvedEvent(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")

	brokers := []string{broker}
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, bus.TopicIdentityResolved))
	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	sink := &captureSink{}
	consumer, err := bus.NewConsumer(brokers, "grp-identity-resolved", []string{bus.TopicIdentityResolved}, 1, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Run(runCtx, func(_ context.Context, r bus.Record) error { sink.add(r); return nil }, nil)
	}()

	svc := identity.NewService(f.pool, prod, bus.TopicIdentityResolved)
	env := identityEnv(t, tid, sid, events.TypeIdentify, "e1", events.Identifiers{UserID: "u1", AnonymousID: "a1"}, "")
	require.NoError(t, svc.Resolve(ctx, env))

	require.Eventually(t, func() bool { return sink.len() >= 1 }, 30*time.Second, 200*time.Millisecond)
	rec := sink.first()

	var resolved identity.IdentityResolved
	require.NoError(t, json.Unmarshal(rec.Value, &resolved))
	require.Equal(t, "identity_resolved", resolved.EventType)
	require.Equal(t, "e1", resolved.EventID)
	require.Contains(t, resolved.CanonicalUserID, "customer_")
	require.Equal(t, tid.String()+"|"+resolved.CanonicalUserID, string(rec.Key))
	require.Equal(t, "e1", resolved.Event.EventID) // original envelope embedded
}

func TestIdentity_TenantIsolation_SameEmailNeverMerges(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tidA, sidA := mkTenant(t, f, "tenant-a")
	tidB, sidB := mkTenant(t, f, "tenant-b")
	svc := newIdentitySvc(f)

	require.NoError(t, svc.Resolve(ctx, identityEnv(t, tidA, sidA, events.TypeIdentify, "a1", events.Identifiers{Email: "shared@x.com"}, "")))
	require.NoError(t, svc.Resolve(ctx, identityEnv(t, tidB, sidB, events.TypeIdentify, "b1", events.Identifiers{Email: "shared@x.com"}, "")))

	require.Equal(t, 1, clusterCount(t, f, tidA))
	require.Equal(t, 1, clusterCount(t, f, tidB))
	require.NotEqual(t,
		canonicalForNode(t, f, tidA, identity.NSEmail, "shared@x.com"),
		canonicalForNode(t, f, tidB, identity.NSEmail, "shared@x.com"))
}
