package governance

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes admin export/delete endpoints.
type Handler struct {
	svc *Service
}

// NewHandler constructs a Handler.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Export handles GET /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/export.
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	bundle, err := h.svc.Export(r.Context(), tenantID, chi.URLParam(r, "canonicalUserID"))
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "profile not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	if principal, ok := auth.PrincipalFromContext(r.Context()); !ok || !principal.Can(rbac.PermPIIRead) {
		bundle.Profile.Traits = rbac.MaskTraits(bundle.Profile.Traits)
	}
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

// Identifiers handles GET /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/identifiers.
// It returns a per-namespace count of every identity node linked to the person
// (e.g. how many phones/emails they have), without exposing the values.
func (h *Handler) Identifiers(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	inv, err := h.svc.Identifiers(r.Context(), tenantID, chi.URLParam(r, "canonicalUserID"))
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "profile not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, inv)
}

// Delete handles DELETE /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}.
func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	counts, err := h.svc.Delete(r.Context(), tenantID, chi.URLParam(r, "canonicalUserID"))
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "profile not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": counts})
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}
