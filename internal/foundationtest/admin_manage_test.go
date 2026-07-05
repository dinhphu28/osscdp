package foundationtest

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/internal/source"
)

// adminManageRouter wires the source-disable and admin-token list/revoke routes
// exactly as cmd/cdp-api/main.go does (same middleware + perms), exercising the
// real auth path end-to-end against a live database.
func adminManageRouter(f fixture) http.Handler {
	authRepo := rbac.NewRepo(f.pool)
	sourceHandler := source.NewHandler(source.NewService(source.NewRepository(f.pool), audit.NewRecorder(f.pool)))
	tokenHandler := auth.NewTokenHandler(authRepo, audit.NewRecorder(f.pool))

	r := chi.NewRouter()
	r.Group(func(a chi.Router) {
		a.Use(auth.Authenticate(staticSuperToken, authRepo))
		a.With(auth.Require(rbac.PermAdminWrite)).Get("/admin/v1/admin-tokens", tokenHandler.List)
		a.With(auth.Require(rbac.PermAdminWrite)).Post("/admin/v1/admin-tokens/{tokenID}/revoke", tokenHandler.Revoke)
		a.With(auth.Require(rbac.PermSourceWrite)).Post("/admin/v1/tenants/{tenantID}/sources/{sourceID}/disable", sourceHandler.Disable)
	})
	return r
}

func TestAdminDisableSource_BlocksIngest(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminManageRouter(f)
	tid, _ := mkTenant(t, f, "acme")

	// Create a fresh source whose plaintext key we keep to verify it stops working.
	created, err := f.sourceSvc.Create(ctx, tid, "web-2", "server")
	require.NoError(t, err)
	sid := created.Source.ID

	// The key authenticates while active.
	_, err = f.sourceSvc.Authenticate(ctx, created.APIKey)
	require.NoError(t, err)

	// Disable → 200 {"id":...,"status":"disabled"}.
	rec := do(t, h, "POST", "/admin/v1/tenants/"+tid.String()+"/sources/"+sid.String()+"/disable", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	var body struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	decodeBody(t, rec, &body)
	require.Equal(t, sid.String(), body.ID)
	require.Equal(t, source.StatusDisabled, body.Status)

	// The key no longer authenticates (auth resolves only active sources).
	_, err = f.sourceSvc.Authenticate(ctx, created.APIKey)
	require.ErrorIs(t, err, source.ErrNotFound)

	// Disabling an unknown source → 404.
	rec = do(t, h, "POST", "/admin/v1/tenants/"+tid.String()+"/sources/"+uuid.NewString()+"/disable", staticSuperToken)
	require.Equal(t, 404, rec.Code)
}

func TestAdminDisableSource_TenantScope(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminManageRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, sidB := mkTenant(t, f, "tenant-b")

	// Tenant-A admin cannot disable tenant-B's source (tenant-scope 403).
	token, err := rbacRepo(f).CreateToken(ctx, &tidA, "ta", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	rec := do(t, h, "POST", "/admin/v1/tenants/"+tidB.String()+"/sources/"+sidB.String()+"/disable", token)
	require.Equal(t, 403, rec.Code)

	// Super-admin can disable cross-tenant.
	rec = do(t, h, "POST", "/admin/v1/tenants/"+tidB.String()+"/sources/"+sidB.String()+"/disable", staticSuperToken)
	require.Equal(t, 200, rec.Code)
}

func TestAdminListTokens_TenantScopedNoHash(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminManageRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")

	_, err := rbacRepo(f).CreateToken(ctx, &tidA, "a-token", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	_, err = rbacRepo(f).CreateToken(ctx, &tidB, "b-token", rbac.RoleViewer)
	require.NoError(t, err)
	tokenA, err := rbacRepo(f).CreateToken(ctx, &tidA, "a-admin", rbac.RoleTenantAdmin)
	require.NoError(t, err)

	// Super-admin sees all tenants' tokens; never any hash.
	rec := do(t, h, "GET", "/admin/v1/admin-tokens", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	require.NotContains(t, rec.Body.String(), "token_hash")
	require.NotContains(t, rec.Body.String(), "hash")
	var all struct {
		Tokens []rbac.TokenSummary `json:"tokens"`
	}
	decodeBody(t, rec, &all)
	require.GreaterOrEqual(t, len(all.Tokens), 3)

	// Tenant-A admin sees only tenant-A tokens (never tenant-B's).
	rec = do(t, h, "GET", "/admin/v1/admin-tokens", tokenA)
	require.Equal(t, 200, rec.Code)
	var scoped struct {
		Tokens []rbac.TokenSummary `json:"tokens"`
	}
	decodeBody(t, rec, &scoped)
	require.NotEmpty(t, scoped.Tokens)
	for _, tk := range scoped.Tokens {
		require.NotNil(t, tk.TenantID)
		require.Equal(t, tidA, *tk.TenantID)
	}
}

func TestAdminRevokeToken_InvalidatesAndTenantScoped(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminManageRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")
	tidB, _ := mkTenant(t, f, "tenant-b")

	// A viewer token in tenant B that we will revoke; capture its id via list.
	victimPlain, err := rbacRepo(f).CreateToken(ctx, &tidB, "victim", rbac.RoleViewer)
	require.NoError(t, err)
	tokensB, err := rbacRepo(f).ListTokens(ctx, &tidB)
	require.NoError(t, err)
	require.Len(t, tokensB, 1)
	victimID := tokensB[0].ID

	// The victim token resolves while active.
	_, err = rbacRepo(f).FindByTokenHash(ctx, rbac.HashToken(victimPlain))
	require.NoError(t, err)

	// Tenant-A admin cannot revoke tenant-B's token → 403.
	tokenA, err := rbacRepo(f).CreateToken(ctx, &tidA, "a-admin", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	rec := do(t, h, "POST", "/admin/v1/admin-tokens/"+victimID.String()+"/revoke", tokenA)
	require.Equal(t, 403, rec.Code)
	// Still active after the forbidden attempt.
	_, err = rbacRepo(f).FindByTokenHash(ctx, rbac.HashToken(victimPlain))
	require.NoError(t, err)

	// Super-admin revokes cross-tenant → 200 {"id":...,"status":"revoked"}.
	rec = do(t, h, "POST", "/admin/v1/admin-tokens/"+victimID.String()+"/revoke", staticSuperToken)
	require.Equal(t, 200, rec.Code)
	var body struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	decodeBody(t, rec, &body)
	require.Equal(t, victimID.String(), body.ID)
	require.Equal(t, rbac.StatusRevoked, body.Status)

	// The revoked token no longer resolves.
	_, err = rbacRepo(f).FindByTokenHash(ctx, rbac.HashToken(victimPlain))
	require.ErrorIs(t, err, rbac.ErrNotFound)

	// Revoking an unknown token → 404.
	rec = do(t, h, "POST", "/admin/v1/admin-tokens/"+uuid.NewString()+"/revoke", staticSuperToken)
	require.Equal(t, 404, rec.Code)
}

func TestAdminRevokeToken_TenantAdminOwnTenant(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	h := adminManageRouter(f)
	tidA, _ := mkTenant(t, f, "tenant-a")

	// Tenant-A admin revokes a token in its own tenant → allowed.
	admin, err := rbacRepo(f).CreateToken(ctx, &tidA, "a-admin", rbac.RoleTenantAdmin)
	require.NoError(t, err)
	victim, err := rbacRepo(f).CreateToken(ctx, &tidA, "a-viewer", rbac.RoleViewer)
	require.NoError(t, err)

	var victimID uuid.UUID
	tokens, err := rbacRepo(f).ListTokens(ctx, &tidA)
	require.NoError(t, err)
	for _, tk := range tokens {
		if tk.Name == "a-viewer" {
			victimID = tk.ID
		}
	}
	require.NotEqual(t, uuid.Nil, victimID)

	rec := do(t, h, "POST", "/admin/v1/admin-tokens/"+victimID.String()+"/revoke", admin)
	require.Equal(t, 200, rec.Code)
	_, err = rbacRepo(f).FindByTokenHash(ctx, rbac.HashToken(victim))
	require.ErrorIs(t, err, rbac.ErrNotFound)
}
