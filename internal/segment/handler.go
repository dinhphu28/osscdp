package segment

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes the admin segment management endpoints.
type Handler struct {
	repo *Repo
}

// NewHandler constructs a Handler.
func NewHandler(repo *Repo) *Handler { return &Handler{repo: repo} }

type createRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Rule        Rule   `json:"rule"`
}

// Create handles POST /admin/v1/tenants/{tenantID}/segments.
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if req.Name == "" {
		apierror.BadRequest(w, "name is required")
		return
	}
	if err := Validate(req.Rule); err != nil {
		apierror.BadRequest(w, "invalid rule: "+err.Error())
		return
	}
	seg, err := h.repo.CreateSegment(r.Context(), tenantID, req.Name, req.Description, req.Rule)
	if errors.Is(err, ErrDuplicateName) {
		apierror.Conflict(w, "segment name already exists for tenant")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	h.seedIfSweepable(tenantID, seg.ID, req.Rule, "seed")
	httpx.WriteJSON(w, http.StatusCreated, seg)
}

// seedIfSweepable enqueues due-now deadlines for the tenant's profiles when the
// segment is a sweep-safe stateful rule, so the existing population (including
// dormant "did-not-do" profiles) is evaluated by the sweeper without an inbound
// event. Runs OFF the request path (own context, paged) so a large population never
// blocks or is cancelled with the admin response. Best-effort; re-issuing
// create/update re-seeds. (A durable job queue is the production-grade follow-up.)
func (h *Handler) seedIfSweepable(tenantID, segmentID uuid.UUID, rule Rule, reason string) {
	if !hasBehavior(rule) || referencesEvent(rule) {
		return
	}
	go func() {
		bg, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		_, _ = h.repo.SeedPendingForSegment(bg, tenantID, segmentID, time.Now().UTC(), reason)
	}()
}

type updateRequest struct {
	Description string `json:"description"`
	Rule        Rule   `json:"rule"`
}

// Update handles PUT /admin/v1/tenants/{tenantID}/segments/{segmentID}.
func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segmentID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	var req updateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if err := Validate(req.Rule); err != nil {
		apierror.BadRequest(w, "invalid rule: "+err.Error())
		return
	}
	seg, err := h.repo.UpdateSegment(r.Context(), tenantID, segmentID, req.Description, req.Rule)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "segment not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	h.seedIfSweepable(tenantID, seg.ID, req.Rule, "version_change")
	httpx.WriteJSON(w, http.StatusOK, seg)
}

// Get handles GET /admin/v1/tenants/{tenantID}/segments/{segmentID}.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segmentID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	seg, err := h.repo.GetSegment(r.Context(), tenantID, segmentID)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "segment not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, seg)
}

type membersResponse struct {
	Members []Membership `json:"members"`
}

// Members handles GET /admin/v1/tenants/{tenantID}/segments/{segmentID}/members.
func (h *Handler) Members(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segmentID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	members, err := h.repo.ListMembers(r.Context(), tenantID, segmentID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, membersResponse{Members: members})
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}
