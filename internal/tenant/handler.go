package tenant

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes tenant admin endpoints.
type Handler struct {
	svc *Service
}

// NewHandler constructs a Handler.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

type createRequest struct {
	Name string `json:"name"`
}

type listResponse struct {
	Tenants []Tenant `json:"tenants"`
}

// List handles GET /admin/v1/tenants (super-admin only).
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.svc.List(r.Context())
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Tenants: tenants})
}

// Create handles POST /admin/v1/tenants.
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	t, err := h.svc.Create(r.Context(), req.Name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			apierror.NotFound(w, "tenant not found")
			return
		}
		apierror.BadRequest(w, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, t)
}
