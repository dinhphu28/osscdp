package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Whoami handles GET /admin/v1/whoami. It reports the authenticated admin
// principal's role and tenant scope so the console can drop its "declare your
// own role" flow. Any authenticated admin may call it (no permission required);
// it never takes a {tenantID}. tenant_id is null for cross-tenant (super-admin).
func Whoami(w http.ResponseWriter, r *http.Request) {
	p, ok := PrincipalFromContext(r.Context())
	if !ok {
		apierror.Unauthorized(w, "not authenticated")
		return
	}
	var tenantID *string
	if p.TenantID != nil {
		s := p.TenantID.String()
		tenantID = &s
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"role":           p.Role,
		"tenant_id":      tenantID,
		"is_super_admin": p.IsSuperAdmin(),
	})
}

// AdminTokenCreator mints and manages admin tokens. ListTokens/GetTokenTenant
// never expose the token hash.
type AdminTokenCreator interface {
	CreateToken(ctx context.Context, tenantID *uuid.UUID, name, role string) (string, error)
	ListTokens(ctx context.Context, tenantID *uuid.UUID) ([]rbac.TokenSummary, error)
	GetTokenTenant(ctx context.Context, id uuid.UUID) (*uuid.UUID, error)
	RevokeToken(ctx context.Context, id uuid.UUID) error
}

// TokenHandler exposes admin-token management.
type TokenHandler struct {
	repo  AdminTokenCreator
	audit *audit.Recorder
}

// NewTokenHandler constructs a TokenHandler.
func NewTokenHandler(repo AdminTokenCreator, recorder *audit.Recorder) *TokenHandler {
	return &TokenHandler{repo: repo, audit: recorder}
}

type createTokenRequest struct {
	Name     string `json:"name"`
	Role     string `json:"role"`
	TenantID string `json:"tenant_id"`
}

// Create handles POST /admin/v1/admin-tokens. SUPER_ADMIN may mint any role/tenant;
// TENANT_ADMIN may mint only non-super roles scoped to its own tenant.
func (h *TokenHandler) Create(w http.ResponseWriter, r *http.Request) {
	caller, ok := PrincipalFromContext(r.Context())
	if !ok {
		apierror.Unauthorized(w, "not authenticated")
		return
	}
	var req createTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if req.Name == "" || !rbac.ValidRole(req.Role) {
		apierror.BadRequest(w, "name and a valid role are required")
		return
	}

	var tenantID *uuid.UUID
	if caller.IsSuperAdmin() {
		if req.Role != rbac.RoleSuperAdmin {
			id, err := uuid.Parse(req.TenantID)
			if err != nil {
				apierror.BadRequest(w, "tenant_id is required for non-super roles")
				return
			}
			tenantID = &id
		}
	} else {
		// Tenant-admin: cannot mint super-admin; forced to its own tenant.
		if req.Role == rbac.RoleSuperAdmin {
			apierror.Forbidden(w, "cannot mint a SUPER_ADMIN token")
			return
		}
		tenantID = caller.TenantID
	}

	plaintext, err := h.repo.CreateToken(r.Context(), tenantID, req.Name, req.Role)
	if err != nil {
		apierror.Internal(w)
		return
	}
	if err := h.audit.Record(r.Context(), audit.Entry{
		TenantID: tenantID, ActorType: audit.ActorAdmin, Action: "create",
		ResourceType: "admin_token", After: map[string]string{"name": req.Name, "role": req.Role},
	}); err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"api_token": plaintext, "role": req.Role})
}

type listTokensResponse struct {
	Tokens []rbac.TokenSummary `json:"tokens"`
}

// List handles GET /admin/v1/admin-tokens. SUPER_ADMIN sees every tenant's
// tokens; a non-super principal sees only its own tenant's. The token hash is
// never returned.
func (h *TokenHandler) List(w http.ResponseWriter, r *http.Request) {
	caller, ok := PrincipalFromContext(r.Context())
	if !ok {
		apierror.Unauthorized(w, "not authenticated")
		return
	}
	var scope *uuid.UUID
	if !caller.IsSuperAdmin() {
		scope = caller.TenantID
	}
	tokens, err := h.repo.ListTokens(r.Context(), scope)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listTokensResponse{Tokens: tokens})
}

// Revoke handles POST /admin/v1/admin-tokens/{tokenID}/revoke. A non-super
// principal may revoke only tokens belonging to its own tenant; SUPER_ADMIN may
// revoke any. Revoking immediately invalidates the token (auth resolves only
// active tokens).
func (h *TokenHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	caller, ok := PrincipalFromContext(r.Context())
	if !ok {
		apierror.Unauthorized(w, "not authenticated")
		return
	}
	tokenID, err := uuid.Parse(chi.URLParam(r, "tokenID"))
	if err != nil {
		apierror.BadRequest(w, "invalid token id")
		return
	}

	targetTenant, err := h.repo.GetTokenTenant(r.Context(), tokenID)
	if errors.Is(err, rbac.ErrNotFound) {
		apierror.NotFound(w, "admin token not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	// Tenant scope: a non-super principal may only revoke its own tenant's tokens.
	if !caller.IsSuperAdmin() {
		if targetTenant == nil || caller.TenantID == nil || *targetTenant != *caller.TenantID {
			apierror.Forbidden(w, "tenant scope violation")
			return
		}
	}

	if err := h.repo.RevokeToken(r.Context(), tokenID); errors.Is(err, rbac.ErrNotFound) {
		apierror.NotFound(w, "admin token not found")
		return
	} else if err != nil {
		apierror.Internal(w)
		return
	}
	if err := h.audit.Record(r.Context(), audit.Entry{
		TenantID: targetTenant, ActorType: audit.ActorAdmin, Action: "revoke",
		ResourceType: "admin_token", ResourceID: tokenID.String(),
		After: map[string]string{"status": rbac.StatusRevoked},
	}); err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": tokenID.String(), "status": rbac.StatusRevoked})
}
