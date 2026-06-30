package dlq

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes the admin DLQ endpoints.
type Handler struct {
	repo    *Repo
	retrier *Retrier
	audit   *audit.Recorder
}

// NewHandler constructs a Handler.
func NewHandler(repo *Repo, retrier *Retrier, recorder *audit.Recorder) *Handler {
	return &Handler{repo: repo, retrier: retrier, audit: recorder}
}

type listResponse struct {
	Events []Event `json:"events"`
}

// List handles GET /admin/v1/tenants/{tenantID}/dlq?status=.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	events, err := h.repo.List(r.Context(), tenantID, r.URL.Query().Get("status"), 0)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Events: events})
}

// Retry handles POST /admin/v1/tenants/{tenantID}/dlq/{id}/retry.
func (h *Handler) Retry(w http.ResponseWriter, r *http.Request) {
	tenantID, id, ok := parseTenantAndID(w, r)
	if !ok {
		return
	}
	err := h.retrier.Retry(r.Context(), tenantID, id)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "dlq event not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	h.record(r, tenantID, id, "retry")
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": id.String(), "status": StatusRetried})
}

// Discard handles POST /admin/v1/tenants/{tenantID}/dlq/{id}/discard.
func (h *Handler) Discard(w http.ResponseWriter, r *http.Request) {
	tenantID, id, ok := parseTenantAndID(w, r)
	if !ok {
		return
	}
	found, err := h.repo.MarkStatus(r.Context(), tenantID, id, StatusDiscarded)
	if err != nil {
		apierror.Internal(w)
		return
	}
	if !found {
		apierror.NotFound(w, "dlq event not found")
		return
	}
	h.record(r, tenantID, id, "discard")
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": id.String(), "status": StatusDiscarded})
}

func (h *Handler) record(r *http.Request, tenantID, id uuid.UUID, action string) {
	tid := tenantID
	_ = h.audit.Record(r.Context(), audit.Entry{
		TenantID: &tid, ActorType: audit.ActorAdmin, Action: action,
		ResourceType: "dlq_event", ResourceID: id.String(),
	})
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}

func parseTenantAndID(w http.ResponseWriter, r *http.Request) (uuid.UUID, uuid.UUID, bool) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return uuid.Nil, uuid.Nil, false
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		apierror.BadRequest(w, "invalid dlq id")
		return uuid.Nil, uuid.Nil, false
	}
	return tenantID, id, true
}
