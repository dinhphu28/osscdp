package events

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes the ingress endpoints.
type Handler struct {
	svc *Service
}

// NewHandler constructs a Handler.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// Track handles POST /v1/events/track.
func (h *Handler) Track(w http.ResponseWriter, r *http.Request) { h.ingestSingle(w, r, TypeTrack) }

// Identify handles POST /v1/identify.
func (h *Handler) Identify(w http.ResponseWriter, r *http.Request) {
	h.ingestSingle(w, r, TypeIdentify)
}

// Alias handles POST /v1/alias.
func (h *Handler) Alias(w http.ResponseWriter, r *http.Request) { h.ingestSingle(w, r, TypeAlias) }

func (h *Handler) ingestSingle(w http.ResponseWriter, r *http.Request, forcedType string) {
	tenantID, sourceID, ok := identity(r)
	if !ok {
		apierror.Unauthorized(w, "missing authenticated source")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxEventBytes)

	var in IncomingEvent
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeDecodeError(w, err, "event exceeds size limit")
		return
	}

	res, err := h.svc.Ingest(r.Context(), in, tenantID, sourceID, forcedType)
	if err != nil {
		var ve *ValidationError
		switch {
		case errors.As(err, &ve):
			apierror.BadRequest(w, ve.Error())
		case errors.Is(err, ErrConflict):
			apierror.Conflict(w, "event_id already exists with different payload")
		default:
			apierror.Internal(w)
		}
		return
	}

	logging.AddFields(r.Context(), slog.String("event_id", res.EventID))
	httpx.WriteJSON(w, http.StatusAccepted, res)
}

// Batch handles POST /v1/events/batch.
func (h *Handler) Batch(w http.ResponseWriter, r *http.Request) {
	tenantID, sourceID, ok := identity(r)
	if !ok {
		apierror.Unauthorized(w, "missing authenticated source")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBatchBytes)

	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeDecodeError(w, err, "batch exceeds size limit")
		return
	}
	if len(req.Events) == 0 {
		apierror.BadRequest(w, "events array is required and must be non-empty")
		return
	}
	if len(req.Events) > MaxBatchSize {
		apierror.BadRequest(w, "batch exceeds the maximum of 500 events")
		return
	}

	result := h.svc.IngestBatch(r.Context(), req.Events, tenantID, sourceID)
	httpx.WriteJSON(w, http.StatusAccepted, result)
}

func identity(r *http.Request) (tenantID, sourceID uuid.UUID, ok bool) {
	tenantID, okT := auth.TenantID(r.Context())
	sourceID, okS := auth.SourceID(r.Context())
	return tenantID, sourceID, okT && okS
}

func writeDecodeError(w http.ResponseWriter, err error, tooLargeMsg string) {
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		apierror.PayloadTooLarge(w, tooLargeMsg)
		return
	}
	apierror.BadRequest(w, "invalid JSON body")
}
