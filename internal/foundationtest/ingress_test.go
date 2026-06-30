package foundationtest

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/events"
)

// mkTenant creates a tenant + source and returns their IDs. The real source ID
// is required because event_outbox.source_id has a foreign key to source(id).
func mkTenant(t *testing.T, f fixture, name string) (tenantID, sourceID uuid.UUID) {
	t.Helper()
	tn, err := f.tenantSvc.Create(context.Background(), name)
	require.NoError(t, err)
	src, err := f.sourceSvc.Create(context.Background(), tn.ID, "web", "server")
	require.NoError(t, err)
	return src.Source.TenantID, src.Source.ID
}

func countOutbox(t *testing.T, f fixture, tenantID uuid.UUID, eventID string) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM event_outbox WHERE tenant_id = $1 AND event_id = $2`,
		tenantID, eventID).Scan(&n))
	return n
}

func TestIngest_TrackHappyPath(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")

	res, err := f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "product_viewed"},
		tid, sid, events.TypeTrack)
	require.NoError(t, err)
	require.Equal(t, events.StatusAccepted, res.Status)

	var status, idKey string
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT status, identifier_key FROM event_outbox WHERE tenant_id=$1 AND event_id=$2`,
		tid, "e1").Scan(&status, &idKey))
	require.Equal(t, events.StatusPending, status)
	require.Equal(t, "user_id:u1", idKey)
}

func TestIngest_DuplicateSamePayloadIsIdempotent(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")
	in := events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "product_viewed"}

	r1, err := f.eventsSvc.Ingest(context.Background(), in, tid, sid, events.TypeTrack)
	require.NoError(t, err)
	require.Equal(t, events.StatusAccepted, r1.Status)

	r2, err := f.eventsSvc.Ingest(context.Background(), in, tid, sid, events.TypeTrack)
	require.NoError(t, err)
	require.Equal(t, events.StatusDuplicate, r2.Status)

	require.Equal(t, 1, countOutbox(t, f, tid, "e1"), "duplicate must not create a second row")
}

func TestIngest_DuplicateDifferentPayloadConflicts(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")

	_, err := f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "product_viewed"},
		tid, sid, events.TypeTrack)
	require.NoError(t, err)

	_, err = f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "checkout_started"},
		tid, sid, events.TypeTrack)
	require.ErrorIs(t, err, events.ErrConflict)

	// Original row unchanged.
	var name string
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT event_name FROM event_outbox WHERE tenant_id=$1 AND event_id=$2`, tid, "e1").Scan(&name))
	require.Equal(t, "product_viewed", name)
}

func TestIngest_TenantIsolation_SameEventID(t *testing.T) {
	f := setup(t)
	tidA, sidA := mkTenant(t, f, "tenant-a")
	tidB, sidB := mkTenant(t, f, "tenant-b")
	in := events.IncomingEvent{EventID: "shared", UserID: "u1", EventName: "x"}

	rA, err := f.eventsSvc.Ingest(context.Background(), in, tidA, sidA, events.TypeTrack)
	require.NoError(t, err)
	require.Equal(t, events.StatusAccepted, rA.Status)

	// Same event_id under a different tenant is a distinct event, not a duplicate.
	rB, err := f.eventsSvc.Ingest(context.Background(), in, tidB, sidB, events.TypeTrack)
	require.NoError(t, err)
	require.Equal(t, events.StatusAccepted, rB.Status)

	require.Equal(t, 1, countOutbox(t, f, tidA, "shared"))
	require.Equal(t, 1, countOutbox(t, f, tidB, "shared"))
}

func TestIngest_ValidationRejected(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")

	// track without event_name.
	_, err := f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "e1", UserID: "u1"}, tid, sid, events.TypeTrack)
	var ve *events.ValidationError
	require.ErrorAs(t, err, &ve)
}

func TestIngestBatch_MixedResults(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")

	// Pre-insert e1 so the batch's e1 is a duplicate.
	_, err := f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "product_viewed"},
		tid, sid, events.TypeTrack)
	require.NoError(t, err)

	batch := []events.IncomingEvent{
		{EventID: "e1", Type: events.TypeTrack, UserID: "u1", EventName: "product_viewed"},   // duplicate
		{EventID: "e2", Type: events.TypeTrack, UserID: "u2", EventName: "checkout_started"}, // accepted
		{EventID: "e3", Type: events.TypeTrack, UserID: "u3"},                                // rejected (no event_name)
	}
	res := f.eventsSvc.IngestBatch(context.Background(), batch, tid, sid)
	require.Equal(t, 1, res.Accepted)
	require.Equal(t, 1, res.Duplicate)
	require.Equal(t, 1, res.Rejected)
	require.Len(t, res.Results, 3)
}

func TestIngest_IdentifyAndAliasPersist(t *testing.T) {
	f := setup(t)
	tid, sid := mkTenant(t, f, "acme")

	_, err := f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "id1", UserID: "u1", AnonymousID: "a1",
			Traits: []byte(`{"email":"u@x.com"}`)}, tid, sid, events.TypeIdentify)
	require.NoError(t, err)

	_, err = f.eventsSvc.Ingest(context.Background(),
		events.IncomingEvent{EventID: "al1", PreviousID: "a1", UserID: "u1"},
		tid, sid, events.TypeAlias)
	require.NoError(t, err)

	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM event_outbox WHERE tenant_id=$1 AND type IN ($2,$3)`,
		tid, events.TypeIdentify, events.TypeAlias).Scan(&n))
	require.Equal(t, 2, n)
}
