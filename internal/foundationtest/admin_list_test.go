package foundationtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/internal/segment"
	"github.com/dinhphu28/osscdp/internal/source"
	"github.com/dinhphu28/osscdp/internal/stats"
	"github.com/dinhphu28/osscdp/internal/tenant"
)

// adminListRouter wires the 7 new admin console read endpoints exactly as
// cmd/cdp-api/main.go does (same middleware + perms), so the tests exercise the
// real auth path end-to-end against a live database.
func adminListRouter(f fixture) http.Handler {
	authRepo := rbac.NewRepo(f.pool)
	sourceHandler := source.NewHandler(source.NewService(source.NewRepository(f.pool), audit.NewRecorder(f.pool)))
	segmentRepo := segment.NewRepo(f.pool)
	segmentHandler := segment.NewHandler(segmentRepo)
	activationHandler := activation.NewHandler(activation.NewRepo(f.pool), nil)
	tenantHandler := tenant.NewHandler(tenant.NewService(tenant.NewRepository(f.pool), audit.NewRecorder(f.pool)))
	auditHandler := audit.NewHandler(audit.NewReader(f.pool))
	statsHandler := stats.NewHandler(f.pool, segmentRepo)

	r := chi.NewRouter()
	r.Group(func(a chi.Router) {
		a.Use(auth.Authenticate(staticSuperToken, authRepo))
		a.Get("/admin/v1/whoami", auth.Whoami)
		a.With(auth.RequireSuperAdmin()).Get("/admin/v1/tenants", tenantHandler.List)
		a.With(auth.Require(rbac.PermSourceRead)).Get("/admin/v1/tenants/{tenantID}/sources", sourceHandler.List)
		a.With(auth.Require(rbac.PermSegmentRead)).Get("/admin/v1/tenants/{tenantID}/segments", segmentHandler.List)
		a.With(auth.Require(rbac.PermDestinationRead)).Get("/admin/v1/tenants/{tenantID}/destinations", activationHandler.ListDestinations)
		a.With(auth.Require(rbac.PermAuditRead)).Get("/admin/v1/tenants/{tenantID}/audit", auditHandler.List)
		a.With(auth.Require(rbac.PermSourceRead)).Get("/admin/v1/tenants/{tenantID}/stats", statsHandler.Stats)
	})
	return r
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

func TestAdminWhoami(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminListRouter(f)
	tid, _ := mkTenant(t, f, "acme")

	// Static token → cross-tenant super-admin.
	rec := do(t, h, "GET", "/admin/v1/whoami", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	var super struct {
		Role         string  `json:"role"`
		TenantID     *string `json:"tenant_id"`
		IsSuperAdmin bool    `json:"is_super_admin"`
	}
	decodeBody(t, rec, &super)
	require.Equal(t, rbac.RoleSuperAdmin, super.Role)
	require.Nil(t, super.TenantID)
	require.True(t, super.IsSuperAdmin)

	// A pinned tenant-admin token → its role + tenant, not super.
	token, err := rbacRepo(f).CreateToken(ctx, &tid, "ta", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	rec = do(t, h, "GET", "/admin/v1/whoami", token)
	require.Equal(t, 200, rec.Code)
	var ta struct {
		Role         string  `json:"role"`
		TenantID     *string `json:"tenant_id"`
		IsSuperAdmin bool    `json:"is_super_admin"`
	}
	decodeBody(t, rec, &ta)
	require.Equal(t, rbac.RoleTenantAdmin, ta.Role)
	require.NotNil(t, ta.TenantID)
	require.Equal(t, tid.String(), *ta.TenantID)
	require.False(t, ta.IsSuperAdmin)
}

func TestAdminListSources_IsolationAndEmpty(t *testing.T) {
	f := setup(t)
	h := adminListRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")

	// Each tenant has exactly its own source (created by mkTenant) — A never sees B's.
	var a struct {
		Sources []source.Source `json:"sources"`
	}
	rec := do(t, h, "GET", "/admin/v1/tenants/"+tidA.String()+"/sources", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	decodeBody(t, rec, &a)
	require.Len(t, a.Sources, 1)
	require.Equal(t, tidA, a.Sources[0].TenantID)

	// A source with no rows for a brand-new tenant → 200 {"sources":[]}, never null/404.
	empty, _ := mkTenantNoSource(t, f, "tenant-empty")
	rec = do(t, h, "GET", "/admin/v1/tenants/"+empty.String()+"/sources", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	require.JSONEq(t, `{"sources":[]}`, rec.Body.String())

	// Secrets never leak: the api_key_hash column is not in the payload.
	require.NotContains(t, rec.Body.String(), "api_key")
	_ = tidB
}

func TestAdminListSegments_IsolationAndEmpty(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminListRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")

	repo := segment.NewRepo(f.pool)
	rule := segment.Rule{Field: "profile.computed_attributes.total_orders", Op: segment.OpGt, Value: float64(3)}
	_, err := repo.CreateSegment(ctx, tidA, "big-spenders", "", rule)
	require.NoError(t, err)

	var a struct {
		Segments []segment.Segment `json:"segments"`
	}
	rec := do(t, h, "GET", "/admin/v1/tenants/"+tidA.String()+"/segments", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	decodeBody(t, rec, &a)
	require.Len(t, a.Segments, 1)
	require.Equal(t, "big-spenders", a.Segments[0].Name)

	// Tenant B (no segments) → empty list.
	rec = do(t, h, "GET", "/admin/v1/tenants/"+tidB.String()+"/segments", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	require.JSONEq(t, `{"segments":[]}`, rec.Body.String())
}

func TestAdminListDestinations_IsolationAndEmpty(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminListRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")

	repo := activation.NewRepo(f.pool)
	_, err := repo.CreateDestination(ctx, tidA, activation.TypeWebhook, "hook",
		json.RawMessage(`{"url":"http://example.test"}`), "ciphertext-secret")
	require.NoError(t, err)

	var a struct {
		Destinations []activation.Destination `json:"destinations"`
	}
	rec := do(t, h, "GET", "/admin/v1/tenants/"+tidA.String()+"/destinations", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	decodeBody(t, rec, &a)
	require.Len(t, a.Destinations, 1)
	require.Equal(t, "hook", a.Destinations[0].Name)
	// The secret_ref is unexported/`json:"-"` → never serialized.
	require.NotContains(t, rec.Body.String(), "ciphertext-secret")
	require.NotContains(t, rec.Body.String(), "secret_ref")

	rec = do(t, h, "GET", "/admin/v1/tenants/"+tidB.String()+"/destinations", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	require.JSONEq(t, `{"destinations":[]}`, rec.Body.String())
}

func TestAdminListTenants_SuperAdminOnly(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminListRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	mkTenant(t, f, "tenant-b")

	// Super-admin sees all tenants.
	var out struct {
		Tenants []tenant.Tenant `json:"tenants"`
	}
	rec := do(t, h, "GET", "/admin/v1/tenants", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	decodeBody(t, rec, &out)
	require.GreaterOrEqual(t, len(out.Tenants), 2)

	// A pinned tenant-admin is not super → 403.
	token, err := rbacRepo(f).CreateToken(ctx, &tidA, "ta", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	require.Equal(t, 403, do(t, h, "GET", "/admin/v1/tenants", token).Code)
}

func TestAdminList_TenantScopeAndPermDenied(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminListRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")

	// A tenant-admin pinned to A may read A's sources but not B's (tenant scope 403).
	token, err := rbacRepo(f).CreateToken(ctx, &tidA, "ta", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	require.Equal(t, 200, do(t, h, "GET", "/admin/v1/tenants/"+tidA.String()+"/sources", token).Code)
	require.Equal(t, 403, do(t, h, "GET", "/admin/v1/tenants/"+tidB.String()+"/sources", token).Code)

	// An OPERATOR lacks segment... actually holds read perms; use audit as the gate:
	// a role without audit:read is denied. MARKETER has no audit:read? It does via
	// readPerms. Every defined role holds the read perms, so the meaningful denial is
	// the tenant-scope one above; assert a missing token is unauthorized here too.
	require.Equal(t, 401, do(t, h, "GET", "/admin/v1/tenants/"+tidA.String()+"/sources", "").Code)
}

func TestAdminAudit_KeysetPagingAndNoBodyLeak(t *testing.T) {
	f := setup(t)
	h := adminListRouter(f)
	// mkTenant records a "create tenant" + "create source" audit entry per call, all
	// scoped to the tenant. Create several sources to have >1 page.
	tid, _ := mkTenant(t, f, "acme")
	svc := source.NewService(source.NewRepository(f.pool), audit.NewRecorder(f.pool))
	for i := 0; i < 5; i++ {
		_, err := svc.Create(context.Background(), tid, "src-"+uuid.NewString()[:8], "server")
		require.NoError(t, err)
	}

	// Page 1 (limit=3) returns a next_cursor; metadata only, no before/after.
	rec := do(t, h, "GET", "/admin/v1/tenants/"+tid.String()+"/audit?limit=3", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	require.NotContains(t, body, "before")
	require.NotContains(t, body, "after")
	require.NotContains(t, body, "before_json")

	var p1 struct {
		Entries []audit.LogEntry `json:"entries"`
		Next    string           `json:"next_cursor"`
	}
	decodeBody(t, rec, &p1)
	require.Len(t, p1.Entries, 3)
	require.NotEmpty(t, p1.Next)
	require.NotEmpty(t, p1.Entries[0].Action)
	require.NotEmpty(t, p1.Entries[0].ResourceType)

	// Page 2 continues past the cursor with no overlap.
	rec = do(t, h, "GET", "/admin/v1/tenants/"+tid.String()+"/audit?limit=3&cursor="+p1.Next, staticSuperToken)
	require.Equal(t, 200, rec.Code)
	var p2 struct {
		Entries []audit.LogEntry `json:"entries"`
		Next    string           `json:"next_cursor"`
	}
	decodeBody(t, rec, &p2)
	require.NotEmpty(t, p2.Entries)
	seen := map[uuid.UUID]bool{}
	for _, e := range p1.Entries {
		seen[e.ID] = true
	}
	for _, e := range p2.Entries {
		require.False(t, seen[e.ID], "cursor page overlap")
	}

	// A malformed cursor (non-empty, undecodable) is a 400.
	require.Equal(t, 400, do(t, h, "GET", "/admin/v1/tenants/"+tid.String()+"/audit?cursor=@@@", staticSuperToken).Code)
}

func TestAdminStats_CountsAndBestEffort(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminListRouter(f)
	tid, _ := mkTenant(t, f, "acme")

	// One extra source (mkTenant already made one), one segment, one destination.
	svc := source.NewService(source.NewRepository(f.pool), audit.NewRecorder(f.pool))
	_, err := svc.Create(ctx, tid, "src2", "server")
	require.NoError(t, err)
	rule := segment.Rule{Field: "profile.computed_attributes.total_orders", Op: segment.OpGt, Value: float64(3)}
	_, err = segment.NewRepo(f.pool).CreateSegment(ctx, tid, "seg", "", rule)
	require.NoError(t, err)
	_, err = activation.NewRepo(f.pool).CreateDestination(ctx, tid, activation.TypeWebhook, "dest",
		json.RawMessage(`{"url":"http://example.test"}`), "")
	require.NoError(t, err)

	rec := do(t, h, "GET", "/admin/v1/tenants/"+tid.String()+"/stats", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	var s struct {
		DLQOpen      int64 `json:"dlq_open"`
		Sources      int64 `json:"sources"`
		Segments     int64 `json:"segments"`
		Destinations int64 `json:"destinations"`
		Profiles     int64 `json:"profiles"`
	}
	decodeBody(t, rec, &s)
	require.Equal(t, int64(2), s.Sources)
	require.Equal(t, int64(1), s.Segments)
	require.Equal(t, int64(1), s.Destinations)
	require.Equal(t, int64(0), s.DLQOpen)
	require.GreaterOrEqual(t, s.Profiles, int64(0))
}

// rbacRepo builds an rbac repo bound to the fixture pool for minting test tokens.
func rbacRepo(f fixture) *rbac.Repo { return rbac.NewRepo(f.pool) }

// mkTenantNoSource creates a tenant only (no source), so source/segment/destination
// lists are genuinely empty for it.
func mkTenantNoSource(t *testing.T, f fixture, name string) (tenantID uuid.UUID, _ struct{}) {
	t.Helper()
	tn, err := f.tenantSvc.Create(context.Background(), name)
	require.NoError(t, err)
	return tn.ID, struct{}{}
}
