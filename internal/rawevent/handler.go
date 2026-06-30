package rawevent

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes the admin event query + replay endpoints.
type Handler struct {
	repo     *Repo
	replayer *Replayer
}

// NewHandler constructs a Handler.
func NewHandler(repo *Repo, replayer *Replayer) *Handler {
	return &Handler{repo: repo, replayer: replayer}
}

// Get handles GET /admin/v1/tenants/{tenantID}/events/{eventID}.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	ev, err := h.repo.GetByEventID(r.Context(), tenantID, chi.URLParam(r, "eventID"))
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "event not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, ev)
}

type listResponse struct {
	Events     []RawEvent `json:"events"`
	NextCursor string     `json:"next_cursor"`
}

// List handles GET /admin/v1/tenants/{tenantID}/events.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	events, next, err := h.repo.List(r.Context(), ListQuery{
		TenantID:      tenantID,
		IdentifierKey: q.Get("identifier_key"),
		EventName:     q.Get("event_name"),
		Limit:         limit,
		Cursor:        q.Get("cursor"),
	})
	if errors.Is(err, ErrInvalidCursor) {
		apierror.BadRequest(w, "invalid cursor")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Events: events, NextCursor: next})
}

// ReplayOne handles POST /admin/v1/tenants/{tenantID}/events/{eventID}/replay.
func (h *Handler) ReplayOne(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	eventID := chi.URLParam(r, "eventID")
	err := h.replayer.ReplayOne(r.Context(), tenantID, eventID)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "event not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]string{"event_id": eventID, "status": "replayed"})
}

// ReplayByIdentifier handles POST /admin/v1/tenants/{tenantID}/replay?identifier_key=.
func (h *Handler) ReplayByIdentifier(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	identifierKey := r.URL.Query().Get("identifier_key")
	if identifierKey == "" {
		apierror.BadRequest(w, "identifier_key is required")
		return
	}
	max, _ := strconv.Atoi(r.URL.Query().Get("max"))
	n, err := h.replayer.ReplayByIdentifier(r.Context(), tenantID, identifierKey, max)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"identifier_key": identifierKey, "replayed": n})
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}
