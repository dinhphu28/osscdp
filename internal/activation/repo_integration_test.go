// Integration tests for the activation repo against a real PostgreSQL
// (testcontainers), following the convention in internal/foundationtest.
package activation

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
	"github.com/dinhphu28/osscdp/internal/tenant"
)

func TestMain(m *testing.M) {
	// Ryuk (the resource-reaper sidecar) is not available in every Docker setup;
	// cleanup is handled explicitly via t.Cleanup below, so disable it.
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	os.Exit(m.Run())
}

// newPool spins up a fresh migrated PostgreSQL and returns a pool bound to it.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("cdp"),
		tcpostgres.WithUsername("cdp"),
		tcpostgres.WithPassword("cdp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, migrate.Up(dsn))

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// seedSubscription creates a tenant, a webhook destination, and one active
// subscription on segmentID, returning the ids needed to exercise disable.
func seedSubscription(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (tenantID, destID, subID, segmentID uuid.UUID) {
	t.Helper()
	repo := NewRepo(pool)

	tn, err := tenant.NewService(tenant.NewRepository(pool), audit.NewRecorder(pool)).Create(ctx, "acme")
	require.NoError(t, err)

	dest, err := repo.CreateDestination(ctx, tn.ID, TypeWebhook, "test-dest", json.RawMessage(`{"url":"http://example.test"}`), "")
	require.NoError(t, err)

	segmentID = uuid.New()
	sub, err := repo.CreateSubscription(ctx, tn.ID, dest.ID, TriggerSegmentMembership, &segmentID)
	require.NoError(t, err)

	return tn.ID, dest.ID, sub.ID, segmentID
}

func TestDisableSubscription(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	repo := NewRepo(pool)
	tenantID, destID, subID, segmentID := seedSubscription(t, ctx, pool)

	// The active subscription is dispatched before disabling.
	active, err := repo.ActiveSubscriptionsForSegment(ctx, tenantID, segmentID)
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, subID, active[0].ID)

	// Disabling returns the updated row with status=disabled.
	disabled, err := repo.DisableSubscription(ctx, tenantID, destID, subID)
	require.NoError(t, err)
	require.Equal(t, subID, disabled.ID)
	require.Equal(t, StatusDisabled, disabled.Status)

	// The sender no longer sees it.
	active, err = repo.ActiveSubscriptionsForSegment(ctx, tenantID, segmentID)
	require.NoError(t, err)
	require.Empty(t, active)

	// The row still exists (soft-disable, not delete).
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM destination_subscription WHERE id=$1`, subID).Scan(&status))
	require.Equal(t, StatusDisabled, status)

	// Disabling again is idempotent.
	again, err := repo.DisableSubscription(ctx, tenantID, destID, subID)
	require.NoError(t, err)
	require.Equal(t, StatusDisabled, again.Status)
}

func TestSubscriptionsBySegment(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	repo := NewRepo(pool)
	tenantID, _, subID, segmentID := seedSubscription(t, ctx, pool)

	// A second destination + subscription on the same segment.
	dest2, err := repo.CreateDestination(ctx, tenantID, TypeWebhook, "test-dest-2", json.RawMessage(`{"url":"http://example.test/2"}`), "")
	require.NoError(t, err)
	sub2, err := repo.CreateSubscription(ctx, tenantID, dest2.ID, TriggerSegmentMembership, &segmentID)
	require.NoError(t, err)

	// Both destinations are listed, active.
	got, err := repo.SubscriptionsBySegment(ctx, tenantID, segmentID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	byName := map[string]SegmentDestination{}
	for _, sd := range got {
		byName[sd.Name] = sd
	}
	require.Equal(t, subID, byName["test-dest"].SubscriptionID)
	require.Equal(t, StatusActive, byName["test-dest"].SubscriptionStatus)
	require.Equal(t, TypeWebhook, byName["test-dest"].Type)
	require.Equal(t, sub2.ID, byName["test-dest-2"].SubscriptionID)

	// Disabling one keeps it in the list (unlike ActiveSubscriptionsForSegment).
	_, err = repo.DisableSubscription(ctx, tenantID, byName["test-dest-2"].DestinationID, sub2.ID)
	require.NoError(t, err)
	got, err = repo.SubscriptionsBySegment(ctx, tenantID, segmentID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, sd := range got {
		if sd.SubscriptionID == sub2.ID {
			require.Equal(t, StatusDisabled, sd.SubscriptionStatus)
		}
	}

	// A segment with no subscriptions yields an empty slice, no error.
	empty, err := repo.SubscriptionsBySegment(ctx, tenantID, uuid.New())
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestDisableSubscription_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	repo := NewRepo(pool)
	tenantID, destID, _, _ := seedSubscription(t, ctx, pool)

	// Unknown subscription id.
	_, err := repo.DisableSubscription(ctx, tenantID, destID, uuid.New())
	require.ErrorIs(t, err, ErrNotFound)

	// Wrong destination for a real subscription is also not found (destination-scoped).
	_, _, subID, _ := seedSubscription(t, ctx, pool)
	_, err = repo.DisableSubscription(ctx, tenantID, uuid.New(), subID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDisableSubscription_TenantIsolation(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	repo := NewRepo(pool)
	tenantA, destA, subA, segA := seedSubscription(t, ctx, pool)
	tenantB, _, _, _ := seedSubscription(t, ctx, pool)

	// Tenant B cannot disable tenant A's subscription.
	_, err := repo.DisableSubscription(ctx, tenantB, destA, subA)
	require.ErrorIs(t, err, ErrNotFound)

	// A's subscription is untouched (still dispatched).
	active, err := repo.ActiveSubscriptionsForSegment(ctx, tenantA, segA)
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, subA, active[0].ID)
}

func TestSubscriptionsBySegment_TenantIsolation(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	repo := NewRepo(pool)
	tenantA, _, subA, segA := seedSubscription(t, ctx, pool)

	// A second tenant with a subscription on the SAME segment id.
	tnB, err := tenant.NewService(tenant.NewRepository(pool), audit.NewRecorder(pool)).Create(ctx, "acmeB")
	require.NoError(t, err)
	destB, err := repo.CreateDestination(ctx, tnB.ID, TypeWebhook, "destB", json.RawMessage(`{"url":"http://b.test"}`), "")
	require.NoError(t, err)
	_, err = repo.CreateSubscription(ctx, tnB.ID, destB.ID, TriggerSegmentMembership, &segA)
	require.NoError(t, err)

	// Tenant A sees only its own subscription on that segment.
	got, err := repo.SubscriptionsBySegment(ctx, tenantA, segA)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, subA, got[0].SubscriptionID)
}
