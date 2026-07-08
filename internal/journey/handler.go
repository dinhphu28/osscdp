package journey

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

// Handler exposes the admin journey management endpoints.
type Handler struct {
	repo *Repo
}

// NewHandler constructs a Handler.
func NewHandler(repo *Repo) *Handler { return &Handler{repo: repo} }

type createRequest struct {
	Name               string     `json:"name"`
	Description        string     `json:"description"`
	EntrySegmentID     uuid.UUID  `json:"entry_segment_id"`
	EntryEventName     string     `json:"entry_event_name"`
	ExitOnSegmentLeave bool       `json:"exit_on_segment_leave"`
	Definition         Definition `json:"definition"`
}

type listResponse struct {
	Journeys []Journey `json:"journeys"`
}

// List handles GET /admin/v1/tenants/{tenantID}/journeys.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	journeys, err := h.repo.ListJourneys(r.Context(), tenantID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Journeys: journeys})
}

// Create handles POST /admin/v1/tenants/{tenantID}/journeys.
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
	// Exactly one entry mode: segment membership XOR event name.
	hasSegment := req.EntrySegmentID != uuid.Nil
	hasEvent := req.EntryEventName != ""
	if hasSegment == hasEvent {
		apierror.BadRequest(w, "exactly one of entry_segment_id or entry_event_name is required")
		return
	}
	if err := Validate(req.Definition); err != nil {
		apierror.BadRequest(w, "invalid definition: "+err.Error())
		return
	}
	var j Journey
	var err error
	if hasSegment {
		j, err = h.repo.CreateJourney(r.Context(), tenantID, req.Name, req.Description, req.EntrySegmentID, req.ExitOnSegmentLeave, req.Definition)
	} else {
		j, err = h.repo.CreateEventJourney(r.Context(), tenantID, req.Name, req.Description, req.EntryEventName, req.ExitOnSegmentLeave, req.Definition)
	}
	if errors.Is(err, ErrDuplicateName) {
		apierror.Conflict(w, "journey name already exists for tenant")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, j)
}

type updateRequest struct {
	Description        string     `json:"description"`
	ExitOnSegmentLeave bool       `json:"exit_on_segment_leave"`
	Definition         Definition `json:"definition"`
}

// Update handles PUT /admin/v1/tenants/{tenantID}/journeys/{journeyID}: mints a new
// version. In-flight enrollments keep their pinned version.
func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	journeyID, err := uuid.Parse(chi.URLParam(r, "journeyID"))
	if err != nil {
		apierror.BadRequest(w, "invalid journey id")
		return
	}
	var req updateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if err := Validate(req.Definition); err != nil {
		apierror.BadRequest(w, "invalid definition: "+err.Error())
		return
	}
	j, err := h.repo.UpdateJourney(r.Context(), tenantID, journeyID, req.Description, req.ExitOnSegmentLeave, req.Definition)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "journey not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, j)
}

// Get handles GET /admin/v1/tenants/{tenantID}/journeys/{journeyID}.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	journeyID, err := uuid.Parse(chi.URLParam(r, "journeyID"))
	if err != nil {
		apierror.BadRequest(w, "invalid journey id")
		return
	}
	j, err := h.repo.GetJourney(r.Context(), tenantID, journeyID)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "journey not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, j)
}

// Deactivate handles DELETE /admin/v1/tenants/{tenantID}/journeys/{journeyID}:
// archives the journey (no new enrollments; in-flight ones drain to completion).
func (h *Handler) Deactivate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	journeyID, err := uuid.Parse(chi.URLParam(r, "journeyID"))
	if err != nil {
		apierror.BadRequest(w, "invalid journey id")
		return
	}
	if err := h.repo.DeactivateJourney(r.Context(), tenantID, journeyID); errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "journey not found")
		return
	} else if err != nil {
		apierror.Internal(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type parkedResponse struct {
	Parked []ParkedEnrollment `json:"parked"`
}

// ListParked handles GET /admin/v1/tenants/{tenantID}/journeys/{journeyID}/enrollments/parked.
func (h *Handler) ListParked(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	journeyID, err := uuid.Parse(chi.URLParam(r, "journeyID"))
	if err != nil {
		apierror.BadRequest(w, "invalid journey id")
		return
	}
	parked, err := h.repo.ListParked(r.Context(), tenantID, journeyID, 200)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, parkedResponse{Parked: parked})
}

// RetryParked handles POST /admin/v1/tenants/{tenantID}/journeys/{journeyID}/enrollments/{profileID}/retry.
func (h *Handler) RetryParked(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	journeyID, err := uuid.Parse(chi.URLParam(r, "journeyID"))
	if err != nil {
		apierror.BadRequest(w, "invalid journey id")
		return
	}
	profileID, err := uuid.Parse(chi.URLParam(r, "profileID"))
	if err != nil {
		apierror.BadRequest(w, "invalid profile id")
		return
	}
	found, err := h.repo.UnparkEnrollment(r.Context(), tenantID, journeyID, profileID, time.Now().UTC())
	if err != nil {
		apierror.Internal(w)
		return
	}
	if !found {
		apierror.NotFound(w, "no parked enrollment for that profile")
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
