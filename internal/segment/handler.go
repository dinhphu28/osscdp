package segment

import (
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

type listResponse struct {
	Segments []Segment `json:"segments"`
}

// List handles GET /admin/v1/tenants/{tenantID}/segments.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segments, err := h.repo.ListSegments(r.Context(), tenantID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Segments: segments})
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
	// CreateSegment durably records a seed job in-tx for sweep-safe rules (drained by
	// the seed runner), so no fire-and-forget goroutine is needed here.
	httpx.WriteJSON(w, http.StatusCreated, seg)
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
	httpx.WriteJSON(w, http.StatusOK, seg)
}

// Deactivate handles DELETE /admin/v1/tenants/{tenantID}/segments/{segmentID}:
// retires the segment (edge + sweeper stop touching it; stranded due-rows purged).
func (h *Handler) Deactivate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segmentID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	if err := h.repo.DeactivateSegment(r.Context(), tenantID, segmentID); errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "segment not found")
		return
	} else if err != nil {
		apierror.Internal(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

type parkedResponse struct {
	Parked []ParkedRow `json:"parked"`
}

// ListParked handles GET /admin/v1/tenants/{tenantID}/segments/{segmentID}/pending/parked:
// the dead-lettered deadline rows (with last_error) an operator can inspect and retry.
func (h *Handler) ListParked(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segmentID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	parked, err := h.repo.ListParked(r.Context(), tenantID, segmentID, 200)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, parkedResponse{Parked: parked})
}

// RetryParked handles POST /admin/v1/tenants/{tenantID}/segments/{segmentID}/pending/{profileID}/retry:
// clears the dead-letter and re-arms the row for an immediate retry with a fresh budget.
func (h *Handler) RetryParked(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segmentID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	profileID, err := uuid.Parse(chi.URLParam(r, "profileID"))
	if err != nil {
		apierror.BadRequest(w, "invalid profile id")
		return
	}
	found, err := h.repo.UnparkPending(r.Context(), tenantID, segmentID, profileID, time.Now().UTC())
	if err != nil {
		apierror.Internal(w)
		return
	}
	if !found {
		apierror.NotFound(w, "no parked deadline for that profile")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}
