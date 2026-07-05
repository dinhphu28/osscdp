package audit

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes the admin audit-log read endpoint.
type Handler struct {
	reader *Reader
}

// NewHandler constructs a Handler.
func NewHandler(reader *Reader) *Handler { return &Handler{reader: reader} }

type listResponse struct {
	Entries    []LogEntry `json:"entries"`
	NextCursor string     `json:"next_cursor"`
}

// List handles GET /admin/v1/tenants/{tenantID}/audit. Keyset-paged, newest-first,
// metadata only (before/after bodies are never returned).
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, next, err := h.reader.List(r.Context(), tenantID, r.URL.Query().Get("cursor"), limit)
	if errors.Is(err, ErrInvalidCursor) {
		apierror.BadRequest(w, "invalid cursor")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Entries: entries, NextCursor: next})
}
