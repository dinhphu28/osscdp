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

	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/rbac"
)

const staticSuperToken = "static-super"

func okHandler(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func rbacRouter(f fixture) http.Handler {
	repo := rbac.NewRepo(f.pool)
	profileHandler := profile.NewHandler(profile.NewRepo(f.pool))
	r := chi.NewRouter()
	r.Group(func(a chi.Router) {
		a.Use(auth.Authenticate(staticSuperToken, repo))
		a.With(auth.RequireSuperAdmin()).Post("/admin/v1/tenants", okHandler)
		a.With(auth.Require(rbac.PermSegmentRead)).Get("/admin/v1/tenants/{tenantID}/segments/{id}", okHandler)
		a.With(auth.Require(rbac.PermSegmentWrite)).Post("/admin/v1/tenants/{tenantID}/segments", okHandler)
		a.With(auth.Require(rbac.PermProfileRead)).Get("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}", profileHandler.Get)
	})
	return r
}

func do(t *testing.T, h http.Handler, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRBAC_StaticTokenIsSuperAdmin(t *testing.T) {
	f := setup(t)
	h := rbacRouter(f)
	tid := uuid.New()
	require.Equal(t, 200, do(t, h, "POST", "/admin/v1/tenants", staticSuperToken).Code)
	require.Equal(t, 200, do(t, h, "POST", "/admin/v1/tenants/"+tid.String()+"/segments", staticSuperToken).Code)
}

func TestRBAC_ViewerPermissions(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := rbacRouter(f)
	tid, _ := mkTenant(t, f, "acme")
	token, err := rbac.NewRepo(f.pool).CreateToken(ctx, &tid, "v", rbac.RoleViewer)
	require.NoError(t, err)

	base := "/admin/v1/tenants/" + tid.String()
	require.Equal(t, 200, do(t, h, "GET", base+"/segments/x", token).Code)   // segment:read ok
	require.Equal(t, 403, do(t, h, "POST", base+"/segments", token).Code)    // segment:write denied
	require.Equal(t, 403, do(t, h, "POST", "/admin/v1/tenants", token).Code) // not super-admin
}

func TestRBAC_TenantScope(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := rbacRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")
	token, err := rbac.NewRepo(f.pool).CreateToken(ctx, &tidA, "ta", rbac.RoleTenantAdmin)
	require.NoError(t, err)

	require.Equal(t, 200, do(t, h, "GET", "/admin/v1/tenants/"+tidA.String()+"/segments/x", token).Code)
	require.Equal(t, 403, do(t, h, "GET", "/admin/v1/tenants/"+tidB.String()+"/segments/x", token).Code)
}

func TestRBAC_UnknownAndMissingToken(t *testing.T) {
	f := setup(t)
	h := rbacRouter(f)
	require.Equal(t, 401, do(t, h, "POST", "/admin/v1/tenants", "cdpadm_bogus").Code)
	require.Equal(t, 401, do(t, h, "POST", "/admin/v1/tenants", "").Code)
}

func TestRBAC_PIIMasking(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := rbacRouter(f)
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev", "page_viewed", "u1", `{"email":"user@example.com"}`)

	viewer, err := rbac.NewRepo(f.pool).CreateToken(ctx, &tid, "v", rbac.RoleViewer)
	require.NoError(t, err)

	path := "/admin/v1/tenants/" + tid.String() + "/profiles/" + pu.CanonicalUserID

	// Viewer (no pii:read) → masked.
	masked := do(t, h, "GET", path, viewer)
	require.Equal(t, 200, masked.Code)
	require.Equal(t, "u***@example.com", traitEmail(t, masked))

	// Super-admin (static) → raw.
	raw := do(t, h, "GET", path, staticSuperToken)
	require.Equal(t, "user@example.com", traitEmail(t, raw))
}

func traitEmail(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Traits map[string]any `json:"traits"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	s, _ := p.Traits["email"].(string)
	return s
}
