package auth

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// AdminTokenCreator mints admin tokens.
type AdminTokenCreator interface {
	CreateToken(ctx context.Context, tenantID *uuid.UUID, name, role string) (string, error)
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
