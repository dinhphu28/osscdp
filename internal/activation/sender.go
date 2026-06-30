package activation

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dinhphu28/osscdp/internal/crypto"
)

// Outcome is the result of a send attempt.
type Outcome struct {
	Success          bool
	Retryable        bool
	HTTPStatus       int
	ResponseBodyHash string
	ErrorMessage     string
}

// Sender delivers a task to a destination.
type Sender interface {
	Send(ctx context.Context, dest Destination, task Task) Outcome
}

// --- Webhook ---

const defaultWebhookTimeoutMS = 5000

// WebhookSender posts task payloads over HTTP.
type WebhookSender struct {
	client *http.Client
	cipher *crypto.Cipher // optional; decrypts the destination secret for signing
}

// NewWebhookSender constructs a WebhookSender. cipher may be nil (no signing).
func NewWebhookSender(cipher *crypto.Cipher) *WebhookSender {
	return &WebhookSender{client: &http.Client{}, cipher: cipher}
}

// Send posts the payload and classifies the response.
func (s *WebhookSender) Send(ctx context.Context, dest Destination, task Task) Outcome {
	var cfg WebhookConfig
	_ = json.Unmarshal(dest.Config, &cfg)
	if cfg.URL == "" {
		return Outcome{ErrorMessage: "destination config missing url"} // permanent
	}
	method := cfg.Method
	if method == "" {
		method = http.MethodPost
	}
	timeout := cfg.TimeoutMS
	if timeout <= 0 {
		timeout = defaultWebhookTimeoutMS
	}
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, cfg.URL, bytes.NewReader(task.Payload))
	if err != nil {
		return Outcome{ErrorMessage: "build request: " + err.Error()} // permanent
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", task.IdempotencyKey)
	req.Header.Set("X-CDP-Tenant-Id", task.TenantID.String())
	req.Header.Set("X-CDP-Event-Id", task.SourceEventID)
	req.Header.Set("X-CDP-Destination-Id", task.DestinationID.String())
	if sig := s.signature(dest, task.Payload); sig != "" {
		req.Header.Set("X-CDP-Signature", sig)
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return Outcome{Retryable: true, ErrorMessage: err.Error()} // network/timeout → retryable
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	sum := sha256.Sum256(body)

	out := Outcome{HTTPStatus: resp.StatusCode, ResponseBodyHash: hex.EncodeToString(sum[:])}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		out.Success = true
	case retryableHTTP(resp.StatusCode):
		out.Retryable = true
		out.ErrorMessage = fmt.Sprintf("http %d", resp.StatusCode)
	default:
		out.ErrorMessage = fmt.Sprintf("http %d", resp.StatusCode) // 4xx → permanent
	}
	return out
}

// signature returns "sha256=<hmac>" of the body using the destination's
// decrypted secret, or "" when no secret/cipher is configured.
func (s *WebhookSender) signature(dest Destination, body []byte) string {
	if s.cipher == nil || dest.SecretRef == nil || *dest.SecretRef == "" {
		return ""
	}
	secret, err := s.cipher.Decrypt(*dest.SecretRef)
	if err != nil {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func retryableHTTP(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// --- Kafka ---

// Producer publishes to the bus (satisfied by bus.Producer).
type Producer interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// KafkaSender produces task payloads to a configured topic.
type KafkaSender struct {
	producer Producer
}

// NewKafkaSender constructs a KafkaSender.
func NewKafkaSender(p Producer) *KafkaSender { return &KafkaSender{producer: p} }

// Send produces the payload to the destination's topic.
func (s *KafkaSender) Send(ctx context.Context, dest Destination, task Task) Outcome {
	var cfg KafkaConfig
	_ = json.Unmarshal(dest.Config, &cfg)
	if cfg.Topic == "" {
		return Outcome{ErrorMessage: "destination config missing topic"} // permanent
	}
	if err := s.producer.Publish(ctx, cfg.Topic, task.IdempotencyKey, task.Payload); err != nil {
		return Outcome{Retryable: true, ErrorMessage: err.Error()}
	}
	return Outcome{Success: true}
}
