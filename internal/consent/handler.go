package consent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// ProfileResolver maps a canonical_user_id to a profile.
type ProfileResolver interface {
	GetByCanonical(ctx context.Context, tenantID uuid.UUID, canonicalUserID string) (profile.Profile, error)
}

// Handler exposes admin consent endpoints.
type Handler struct {
	repo     *Repo
	profiles ProfileResolver
}

// NewHandler constructs a Handler.
func NewHandler(repo *Repo, profiles ProfileResolver) *Handler {
	return &Handler{repo: repo, profiles: profiles}
}

func (h *Handler) resolve(w http.ResponseWriter, r *http.Request) (uuid.UUID, profile.Profile, bool) {
	tenantID, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, profile.Profile{}, false
	}
	p, err := h.profiles.GetByCanonical(r.Context(), tenantID, chi.URLParam(r, "canonicalUserID"))
	if errors.Is(err, profile.ErrNotFound) {
		apierror.NotFound(w, "profile not found")
		return uuid.Nil, profile.Profile{}, false
	}
	if err != nil {
		apierror.Internal(w)
		return uuid.Nil, profile.Profile{}, false
	}
	return tenantID, p, true
}

type setRequest struct {
	Channel string `json:"channel"`
	Purpose string `json:"purpose"`
	Status  string `json:"status"`
	Source  string `json:"source"`
}

var validStatus = map[string]bool{StatusGranted: true, StatusDenied: true, StatusUnknown: true}

// Set handles PUT /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/consent.
func (h *Handler) Set(w http.ResponseWriter, r *http.Request) {
	tenantID, p, ok := h.resolve(w, r)
	if !ok {
		return
	}
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if req.Channel == "" || req.Purpose == "" {
		apierror.BadRequest(w, "channel and purpose are required")
		return
	}
	if !validStatus[req.Status] {
		apierror.BadRequest(w, "status must be granted, denied, or unknown")
		return
	}
	if err := h.repo.Set(r.Context(), tenantID, p.ID, req.Channel, req.Purpose, req.Status, req.Source); err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type listResponse struct {
	Consent []Record `json:"consent"`
}

// List handles GET /admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/consent.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, p, ok := h.resolve(w, r)
	if !ok {
		return
	}
	records, err := h.repo.ListForProfile(r.Context(), tenantID, p.ID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Consent: records})
}
