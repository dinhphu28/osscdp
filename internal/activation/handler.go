package activation

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/crypto"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes admin destination/subscription/delivery endpoints.
type Handler struct {
	repo   *Repo
	cipher *crypto.Cipher
}

// NewHandler constructs a Handler. cipher encrypts destination secrets at rest.
func NewHandler(repo *Repo, cipher *crypto.Cipher) *Handler {
	return &Handler{repo: repo, cipher: cipher}
}

type destinationsResponse struct {
	Destinations []Destination `json:"destinations"`
}

// ListDestinations handles GET /admin/v1/tenants/{tenantID}/destinations.
func (h *Handler) ListDestinations(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	dests, err := h.repo.ListDestinations(r.Context(), tenantID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, destinationsResponse{Destinations: dests})
}

type createDestinationRequest struct {
	Type    string          `json:"type"`
	Name    string          `json:"name"`
	Config  json.RawMessage `json:"config"`
	Secret  string          `json:"secret"`
	Channel string          `json:"channel"`
	Purpose string          `json:"purpose"`
}

// CreateDestination handles POST /admin/v1/tenants/{tenantID}/destinations.
func (h *Handler) CreateDestination(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	var req createDestinationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if req.Name == "" {
		apierror.BadRequest(w, "name is required")
		return
	}
	if err := validateConfig(req.Type, req.Config); err != nil {
		apierror.BadRequest(w, err.Error())
		return
	}
	config, err := mergeConsentTarget(req.Config, req.Channel, req.Purpose)
	if err != nil {
		apierror.BadRequest(w, "invalid config JSON")
		return
	}
	secretRef := ""
	if req.Secret != "" {
		if h.cipher == nil {
			apierror.Internal(w)
			return
		}
		secretRef, err = h.cipher.Encrypt(req.Secret)
		if err != nil {
			apierror.Internal(w)
			return
		}
	}
	dest, err := h.repo.CreateDestination(r.Context(), tenantID, req.Type, req.Name, config, secretRef)
	if errors.Is(err, ErrDuplicateName) {
		apierror.Conflict(w, "destination name already exists for tenant")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, dest)
}

// mergeConsentTarget injects channel/purpose into the config JSON when provided.
func mergeConsentTarget(config json.RawMessage, channel, purpose string) (json.RawMessage, error) {
	m := map[string]any{}
	if len(config) > 0 {
		if err := json.Unmarshal(config, &m); err != nil {
			return nil, err
		}
	}
	if channel != "" {
		m["channel"] = channel
	}
	if purpose != "" {
		m["purpose"] = purpose
	}
	return json.Marshal(m)
}

func validateConfig(typ string, config json.RawMessage) error {
	switch typ {
	case TypeWebhook:
		var c WebhookConfig
		if err := json.Unmarshal(config, &c); err != nil || c.URL == "" {
			return errors.New("webhook destination requires config.url")
		}
	case TypeKafka:
		var c KafkaConfig
		if err := json.Unmarshal(config, &c); err != nil || c.Topic == "" {
			return errors.New("kafka destination requires config.topic")
		}
	default:
		return errors.New("type must be webhook or kafka")
	}
	return nil
}

type updateDestinationRequest struct {
	Status string          `json:"status"`
	Config json.RawMessage `json:"config"`
}

// UpdateDestination handles PUT /admin/v1/tenants/{tenantID}/destinations/{destinationID}.
func (h *Handler) UpdateDestination(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	destID, err := uuid.Parse(chi.URLParam(r, "destinationID"))
	if err != nil {
		apierror.BadRequest(w, "invalid destination id")
		return
	}
	var req updateDestinationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	dest, err := h.repo.UpdateDestination(r.Context(), tenantID, destID, req.Status, req.Config)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "destination not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, dest)
}

// GetDestination handles GET /admin/v1/tenants/{tenantID}/destinations/{destinationID}.
func (h *Handler) GetDestination(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	destID, err := uuid.Parse(chi.URLParam(r, "destinationID"))
	if err != nil {
		apierror.BadRequest(w, "invalid destination id")
		return
	}
	dest, err := h.repo.GetDestination(r.Context(), tenantID, destID)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "destination not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, dest)
}

type createSubscriptionRequest struct {
	TriggerType string `json:"trigger_type"`
	SegmentID   string `json:"segment_id"`
}

// CreateSubscription handles POST /admin/v1/tenants/{tenantID}/destinations/{destinationID}/subscriptions.
func (h *Handler) CreateSubscription(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	destID, err := uuid.Parse(chi.URLParam(r, "destinationID"))
	if err != nil {
		apierror.BadRequest(w, "invalid destination id")
		return
	}
	var req createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.BadRequest(w, "invalid JSON body")
		return
	}
	if req.TriggerType != TriggerSegmentMembership {
		apierror.BadRequest(w, "trigger_type must be segment_membership")
		return
	}
	segID, err := uuid.Parse(req.SegmentID)
	if err != nil {
		apierror.BadRequest(w, "segment_id is required and must be a UUID")
		return
	}
	sub, err := h.repo.CreateSubscription(r.Context(), tenantID, destID, req.TriggerType, &segID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, sub)
}

// DisableSubscription handles DELETE /admin/v1/tenants/{tenantID}/destinations/{destinationID}/subscriptions/{subscriptionID}.
// It soft-disables the subscription (status='disabled'); the destination itself is untouched.
func (h *Handler) DisableSubscription(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	destID, err := uuid.Parse(chi.URLParam(r, "destinationID"))
	if err != nil {
		apierror.BadRequest(w, "invalid destination id")
		return
	}
	subID, err := uuid.Parse(chi.URLParam(r, "subscriptionID"))
	if err != nil {
		apierror.BadRequest(w, "invalid subscription id")
		return
	}
	sub, err := h.repo.DisableSubscription(r.Context(), tenantID, destID, subID)
	if errors.Is(err, ErrNotFound) {
		apierror.NotFound(w, "subscription not found")
		return
	}
	if err != nil {
		apierror.Internal(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, sub)
}

type deliveriesResponse struct {
	Deliveries []deliveryView `json:"deliveries"`
}

type deliveryView struct {
	TaskID         uuid.UUID `json:"activation_task_id"`
	Status         string    `json:"status"`
	HTTPStatus     *int      `json:"http_status,omitempty"`
	ErrorMessage   string    `json:"error_message,omitempty"`
	AttemptCount   int       `json:"attempt_count"`
	IdempotencyKey string    `json:"idempotency_key"`
}

// Deliveries handles GET /admin/v1/tenants/{tenantID}/destinations/{destinationID}/deliveries.
func (h *Handler) Deliveries(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	destID, err := uuid.Parse(chi.URLParam(r, "destinationID"))
	if err != nil {
		apierror.BadRequest(w, "invalid destination id")
		return
	}
	rows, err := h.repo.ListDeliveries(r.Context(), tenantID, destID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	out := deliveriesResponse{Deliveries: make([]deliveryView, 0, len(rows))}
	for _, d := range rows {
		out.Deliveries = append(out.Deliveries, deliveryView{
			TaskID: d.TaskID, Status: d.Status, HTTPStatus: d.HTTPStatus,
			ErrorMessage: d.ErrorMessage, AttemptCount: d.AttemptCount, IdempotencyKey: d.IdempotencyKey,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

type segmentDestinationsResponse struct {
	Destinations []SegmentDestination `json:"destinations"`
}

// ListSegmentDestinations handles GET /admin/v1/tenants/{tenantID}/segments/{segmentID}/destinations.
// It returns every destination wired to the segment, including disabled subscriptions.
func (h *Handler) ListSegmentDestinations(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := parseTenant(w, r)
	if !ok {
		return
	}
	segID, err := uuid.Parse(chi.URLParam(r, "segmentID"))
	if err != nil {
		apierror.BadRequest(w, "invalid segment id")
		return
	}
	rows, err := h.repo.SubscriptionsBySegment(r.Context(), tenantID, segID)
	if err != nil {
		apierror.Internal(w)
		return
	}
	out := segmentDestinationsResponse{Destinations: make([]SegmentDestination, 0, len(rows))}
	out.Destinations = append(out.Destinations, rows...)
	httpx.WriteJSON(w, http.StatusOK, out)
}

func parseTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return uuid.Nil, false
	}
	return id, true
}
