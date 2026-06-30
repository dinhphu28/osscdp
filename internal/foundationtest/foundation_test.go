// Package foundationtest holds Phase 1 integration tests that exercise tenant,
// source, auth, and audit together against a real PostgreSQL (testcontainers).
package foundationtest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
	"github.com/dinhphu28/osscdp/internal/source"
	"github.com/dinhphu28/osscdp/internal/tenant"
)

func TestMain(m *testing.M) {
	// The Ryuk resource-reaper sidecar is not available in every Docker setup;
	// container cleanup is handled explicitly via t.Cleanup below, so disable it.
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	os.Exit(m.Run())
}

type fixture struct {
	pool      *pgxpool.Pool
	tenantSvc *tenant.Service
	sourceSvc *source.Service
	eventsSvc *events.Service
}

func setup(t *testing.T) fixture {
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

	recorder := audit.NewRecorder(pool)
	return fixture{
		pool:      pool,
		tenantSvc: tenant.NewService(tenant.NewRepository(pool), recorder),
		sourceSvc: source.NewService(source.NewRepository(pool), recorder),
		eventsSvc: events.NewService(events.NewRepository(pool)),
	}
}

func TestCreateTenantAndSource_AuditedAndAuthenticates(t *testing.T) {
	f := setup(t)
	ctx := context.Background()

	tn, err := f.tenantSvc.Create(ctx, "acme")
	require.NoError(t, err)
	require.Equal(t, tenant.StatusActive, tn.Status)

	res, err := f.sourceSvc.Create(ctx, tn.ID, "web", "server")
	require.NoError(t, err)
	require.NotEmpty(t, res.APIKey)
	require.Equal(t, tn.ID, res.Source.TenantID)

	// The plaintext key authenticates back to the same source/tenant.
	got, err := f.sourceSvc.Authenticate(ctx, res.APIKey)
	require.NoError(t, err)
	require.Equal(t, res.Source.ID, got.ID)
	require.Equal(t, tn.ID, got.TenantID)

	// Audit log recorded both creations.
	var actions []string
	rows, err := f.pool.Query(ctx, `SELECT resource_type FROM audit_log ORDER BY created_at`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var rt string
		require.NoError(t, rows.Scan(&rt))
		actions = append(actions, rt)
	}
	require.ElementsMatch(t, []string{"tenant", "source"}, actions)
}

func TestAuthenticate_RejectsUnknownKey(t *testing.T) {
	f := setup(t)
	_, err := f.sourceSvc.Authenticate(context.Background(), "cdp_does_not_exist")
	require.ErrorIs(t, err, source.ErrNotFound)
}

func TestSource_DuplicateNameRejected(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tn, err := f.tenantSvc.Create(ctx, "acme")
	require.NoError(t, err)

	_, err = f.sourceSvc.Create(ctx, tn.ID, "web", "server")
	require.NoError(t, err)
	_, err = f.sourceSvc.Create(ctx, tn.ID, "web", "server")
	require.ErrorIs(t, err, source.ErrDuplicateName)
}

func TestSource_UnknownTenantRejected(t *testing.T) {
	f := setup(t)
	tn, err := f.tenantSvc.Create(context.Background(), "acme")
	require.NoError(t, err)
	// A different (non-existent) tenant id must fail the FK.
	other := tn.ID
	other[0] ^= 0xFF
	_, err = f.sourceSvc.Create(context.Background(), other, "web", "server")
	require.ErrorIs(t, err, source.ErrTenantNotFound)
}

// TestTenantIsolation verifies a source's API key never resolves to another
// tenant's source.
func TestTenantIsolation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()

	tnA, err := f.tenantSvc.Create(ctx, "tenant-a")
	require.NoError(t, err)
	tnB, err := f.tenantSvc.Create(ctx, "tenant-b")
	require.NoError(t, err)

	srcA, err := f.sourceSvc.Create(ctx, tnA.ID, "web", "server")
	require.NoError(t, err)
	srcB, err := f.sourceSvc.Create(ctx, tnB.ID, "web", "server")
	require.NoError(t, err)

	gotA, err := f.sourceSvc.Authenticate(ctx, srcA.APIKey)
	require.NoError(t, err)
	require.Equal(t, tnA.ID, gotA.TenantID)
	require.NotEqual(t, tnB.ID, gotA.TenantID)

	gotB, err := f.sourceSvc.Authenticate(ctx, srcB.APIKey)
	require.NoError(t, err)
	require.Equal(t, tnB.ID, gotB.TenantID)
}

func TestAuthMiddleware(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tn, err := f.tenantSvc.Create(ctx, "acme")
	require.NoError(t, err)
	src, err := f.sourceSvc.Create(ctx, tn.ID, "web", "server")
	require.NoError(t, err)

	var gotTenant string
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := auth.TenantID(r.Context())
		require.True(t, ok)
		gotTenant = id.String()
		w.WriteHeader(http.StatusOK)
	})
	h := auth.APIKey(f.sourceSvc)(probe)

	// Valid key -> 200, context populated.
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	req.Header.Set("Authorization", "Bearer "+src.APIKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, tn.ID.String(), gotTenant)

	// Invalid key -> 401.
	bad := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	bad.Header.Set("Authorization", "Bearer cdp_bogus")
	badRec := httptest.NewRecorder()
	h.ServeHTTP(badRec, bad)
	require.Equal(t, http.StatusUnauthorized, badRec.Code)

	// Missing key -> 401.
	none := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	noneRec := httptest.NewRecorder()
	h.ServeHTTP(noneRec, none)
	require.Equal(t, http.StatusUnauthorized, noneRec.Code)
}
