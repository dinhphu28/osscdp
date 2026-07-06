package profile

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

// maskTraits masks PII in a profile's traits unless the caller holds pii:read.
func maskTraits(r *http.Request, p *Profile) {
	if principal, ok := auth.PrincipalFromContext(r.Context()); ok && principal.Can(rbac.PermPIIRead) {
		return
	}
	p.Traits = rbac.MaskTraits(p.Traits)
}

// Handler exposes the admin profile query endpoints.
type Handler struct {
	repo *Repo
}

// NewHandler constructs a Handler.
func NewHandler(repo *Repo) *Handler { return &Handler{repo: repo} }

// Get handles GET /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	p, err := h.repo.GetByCanonical(r.Context(), tenantID, chi.URLParam(r, "canonicalUserID"))
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "profile not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	maskTraits(r, &p)
	httpx.WriteJSON(w, http.StatusOK, p)
}

type listResponse struct {
	Profiles []Profile `json:"profiles"`
}

// List handles GET /admin/v1/tenants/{tenantID}/profiles?email=&phone=.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	key, value := TraitEmail, r.URL.Query().Get("email")
	if value == "" {
		key, value = TraitPhone, r.URL.Query().Get("phone")
	}
	if value == "" {
		apierror.BadRequest(w, "email or phone query parameter is required")
		return
	}
	profiles, err := h.repo.ResolveByIdentifier(r.Context(), tenantID, key, value)
	if err != nil {
		apierror.Internal(w)
		return
	}
	for i := range profiles {
		maskTraits(r, &profiles[i])
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Profiles: profiles})
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}
