package source

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes source admin endpoints.
type Handler struct {
	svc *Service
}

// NewHandler constructs a Handler.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

type createRequest struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type listResponse struct {
	Sources []Source `json:"sources"`
}

// List handles GET /admin/v1/tenants/{tenantID}/sources.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return
	}
	sources, err := h.svc.List(r.Context(), tenantID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Sources: sources})
}

// RotateKey handles POST /admin/v1/tenants/{tenantID}/sources/{sourceID}/rotate-key.
func (h *Handler) RotateKey(w http.ResponseWriter, r *http.Request) {
	tenantID, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return
	}
	sourceID, err := uuid.Parse(chi.URLParam(r, "sourceID"))
	if err != nil {
		apierror.BadRequest(w, "invalid source id")
		return
	}
	key, err := h.svc.RotateKey(r.Context(), tenantID, sourceID)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "source not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"api_key": key})
}

type createResponse struct {
	ID       uuid.UUID `json:"id"`
	TenantID uuid.UUID `json:"tenant_id"`
	Name     string    `json:"name"`
	Type     string    `json:"type"`
	Status   string    `json:"status"`
	// APIKey is shown exactly once at creation and never stored or logged.
	APIKey string `json:"api_key"`
}

// Create handles POST /admin/v1/tenants/{tenantID}/sources.
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return
	}
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	res, err := h.svc.Create(r.Context(), tenantID, req.Name, req.Type)
	if err != nil {
		switch {
		case errors.Is(err, ErrDuplicateName):
			apierror.Conflict(w, "source name already exists for tenant")
		case errors.Is(err, ErrTenantNotFound):
			apierror.NotFound(w, "tenant not found")
		default:
			apierror.BadRequest(w, err.Error())
		}
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, createResponse{
		ID:       res.Source.ID,
		TenantID: res.Source.TenantID,
		Name:     res.Source.Name,
		Type:     res.Source.Type,
		Status:   res.Source.Status,
		APIKey:   res.APIKey,
	})
}
