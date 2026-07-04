// Package foundationtest holds Phase 1 integration tests that exercise tenant,
// source, auth, and audit together against a real PostgreSQL (testcontainers).
package foundationtest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
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

// Package-level shared PostgreSQL: one container is started for the whole package
// (in TestMain) with the schema migrated into the "cdp" template database. Each
// setup(t) clones that template into a fresh per-test database via CREATE DATABASE
// … TEMPLATE — a fast file copy that gives every test a pristine schema without
// paying the ~8s container spin-up per test.
var (
	pgTemplateDSN string        // DSN of the migrated "cdp" template database
	adminPool     *pgxpool.Pool // connected to the "postgres" maintenance db for CREATE/DROP DATABASE
	dbSeq         atomic.Uint64
)

func TestMain(m *testing.M) { os.Exit(runMain(m)) }

// runMain owns the shared container so defers run on every bootstrap failure path
// (Ryuk is disabled, so an orphaned container would otherwise leak).
func runMain(m *testing.M) int {
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("cdp"),
		tcpostgres.WithUsername("cdp"),
		tcpostgres.WithPassword("cdp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() { _ = container.Terminate(ctx) }()

	pgTemplateDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	// Migrate the "cdp" database once; it becomes the clone template.
	if err := migrate.Up(pgTemplateDSN); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	adminCfg, err := pgxpool.ParseConfig(pgTemplateDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse dsn: %v\n", err)
		return 1
	}
	adminCfg.ConnConfig.Database = "postgres" // never connect to the template itself
	adminPool, err = pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin pool: %v\n", err)
		return 1
	}
	defer adminPool.Close()

	// migrate.Up closes its client connection, but the PostgreSQL backend detaches
	// asynchronously; CREATE DATABASE … TEMPLATE cdp refuses a template with any live
	// backend. Wait for "cdp" to be fully idle before the first clone.
	if err := waitTemplateIdle(ctx, adminPool); err != nil {
		fmt.Fprintf(os.Stderr, "template not idle: %v\n", err)
		return 1
	}

	return m.Run()
}

// waitTemplateIdle blocks until no backend is attached to the "cdp" template.
func waitTemplateIdle(ctx context.Context, admin *pgxpool.Pool) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		var n int
		if err := admin.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = 'cdp'`).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d backend(s) still attached to template cdp", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
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

	dbName := fmt.Sprintf("t_%d_%d", os.Getpid(), dbSeq.Add(1))
	// Clone the migrated template into a fresh database for this test. Identifier is
	// composed only of integers, so interpolation is safe (DDL cannot bind params).
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+dbName+" TEMPLATE cdp"); err != nil {
		t.Fatalf("create test database %s: %v", dbName, err)
	}
	// LIFO cleanup: pool.Close runs first (below), then DROP … WITH (FORCE) drops the
	// db even if a stray connection lingers.
	t.Cleanup(func() {
		if _, err := adminPool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)"); err != nil {
			t.Logf("drop test database %s: %v", dbName, err)
		}
	})

	cfg, err := pgxpool.ParseConfig(pgTemplateDSN)
	require.NoError(t, err)
	cfg.ConnConfig.Database = dbName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
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
