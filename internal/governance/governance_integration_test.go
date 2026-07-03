// Integration tests for governance against a real PostgreSQL (testcontainers),
// following the convention in internal/foundationtest.
package governance

import (
	"context"
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
	// Ryuk (the reaper sidecar) is not available in every Docker setup; cleanup
	// is handled explicitly via t.Cleanup, so disable it.
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	os.Exit(m.Run())
}

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

// seedPerson creates a tenant, an identity cluster, a customer profile keyed to
// it, and identity nodes in the given namespaces (namespace -> count of distinct
// values). Returns the tenant id and the canonical user id.
func seedPerson(t *testing.T, ctx context.Context, pool *pgxpool.Pool, nodes map[string]int) (uuid.UUID, string) {
	t.Helper()

	tn, err := tenant.NewService(tenant.NewRepository(pool), audit.NewRecorder(pool)).Create(ctx, "acme")
	require.NoError(t, err)

	clusterID := uuid.New()
	canonical := "customer_" + uuid.New().String()
	_, err = pool.Exec(ctx,
		`INSERT INTO identity_cluster (id, tenant_id, canonical_user_id, status) VALUES ($1,$2,$3,'active')`,
		clusterID, tn.ID, canonical)
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`INSERT INTO customer_profile (id, tenant_id, canonical_user_id, identity_cluster_id) VALUES ($1,$2,$3,$4)`,
		uuid.New(), tn.ID, canonical, clusterID)
	require.NoError(t, err)

	for ns, count := range nodes {
		for i := 0; i < count; i++ {
			nodeID := uuid.New()
			// value_hash need only be unique within (tenant, namespace); the id makes it so.
			_, err = pool.Exec(ctx,
				`INSERT INTO identity_node (id, tenant_id, namespace, value_hash) VALUES ($1,$2,$3,$4)`,
				nodeID, tn.ID, ns, "hash-"+nodeID.String())
			require.NoError(t, err)
			_, err = pool.Exec(ctx,
				`INSERT INTO identity_cluster_member (tenant_id, identity_node_id, cluster_id, source) VALUES ($1,$2,$3,'test')`,
				tn.ID, nodeID, clusterID)
			require.NoError(t, err)
		}
	}
	return tn.ID, canonical
}

func TestIdentifiers(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	svc := NewService(pool, audit.NewRecorder(pool))

	// A person with two emails, three phones, one user_id.
	tenantID, canonical := seedPerson(t, ctx, pool, map[string]int{"email": 2, "phone": 3, "user_id": 1})

	inv, err := svc.Identifiers(ctx, tenantID, canonical)
	require.NoError(t, err)
	require.Equal(t, canonical, inv.CanonicalUserID)
	require.Equal(t, 6, inv.Total)
	require.Equal(t, map[string]int{"email": 2, "phone": 3, "user_id": 1}, inv.ByNamespace)
}

func TestIdentifiers_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t)
	svc := NewService(pool, audit.NewRecorder(pool))

	// Seed a person so the tenant exists, then query an unknown canonical id.
	tenantID, _ := seedPerson(t, ctx, pool, map[string]int{"email": 1})

	_, err := svc.Identifiers(ctx, tenantID, "customer_"+uuid.New().String())
	require.ErrorIs(t, err, ErrNotFound)
}
