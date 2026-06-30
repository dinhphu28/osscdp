package activation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIdempotencyKey_DeterministicAndSensitive(t *testing.T) {
	tn, d, s, p := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	k1 := IdempotencyKey(tn, d, s, p, "evt1", "entered")
	k2 := IdempotencyKey(tn, d, s, p, "evt1", "entered")
	if k1 != k2 {
		t.Fatal("idempotency key must be deterministic")
	}
	if IdempotencyKey(tn, d, s, p, "evt1", "exited") == k1 {
		t.Fatal("changing the change kind must change the key")
	}
	if IdempotencyKey(tn, d, s, p, "evt2", "entered") == k1 {
		t.Fatal("changing the source event must change the key")
	}
}

func TestBackoff_MonotonicAndCapped(t *testing.T) {
	prev := time.Duration(0)
	for attempt := 1; attempt <= 10; attempt++ {
		d := Backoff(attempt)
		if d < prev {
			t.Fatalf("backoff decreased at attempt %d: %v < %v", attempt, d, prev)
		}
		if d > backoffMax {
			t.Fatalf("backoff exceeded max at attempt %d: %v", attempt, d)
		}
		prev = d
	}
	if Backoff(20) != backoffMax {
		t.Fatalf("backoff should cap at %v", backoffMax)
	}
}

func webhookDest(t *testing.T, url string) Destination {
	t.Helper()
	cfg, _ := json.Marshal(WebhookConfig{URL: url, TimeoutMS: 2000})
	return Destination{ID: uuid.New(), TenantID: uuid.New(), Type: TypeWebhook, Config: cfg}
}

func TestWebhookSender_Classification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		success   bool
		retryable bool
	}{
		{"2xx success", 200, true, false},
		{"429 retryable", 429, false, true},
		{"500 retryable", 500, false, true},
		{"400 permanent", 400, false, false},
		{"404 permanent", 404, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotKey string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotKey = r.Header.Get("Idempotency-Key")
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			s := NewWebhookSender()
			task := Task{IdempotencyKey: "key123", TenantID: uuid.New(), DestinationID: uuid.New(), Payload: json.RawMessage(`{}`)}
			out := s.Send(context.Background(), webhookDest(t, srv.URL), task)

			if out.Success != tc.success || out.Retryable != tc.retryable {
				t.Fatalf("status %d: got success=%v retryable=%v", tc.status, out.Success, out.Retryable)
			}
			if out.HTTPStatus != tc.status {
				t.Fatalf("http status = %d, want %d", out.HTTPStatus, tc.status)
			}
			if gotKey != "key123" {
				t.Fatalf("Idempotency-Key header not set, got %q", gotKey)
			}
		})
	}
}

func TestWebhookSender_NetworkErrorRetryable(t *testing.T) {
	s := NewWebhookSender()
	// Unroutable URL → connection error → retryable.
	out := s.Send(context.Background(), webhookDest(t, "http://127.0.0.1:1"), Task{Payload: json.RawMessage(`{}`)})
	if out.Success || !out.Retryable {
		t.Fatalf("network error should be retryable, got %+v", out)
	}
}

func TestWebhookSender_MissingURLPermanent(t *testing.T) {
	s := NewWebhookSender()
	dest := Destination{Type: TypeWebhook, Config: json.RawMessage(`{}`)}
	out := s.Send(context.Background(), dest, Task{Payload: json.RawMessage(`{}`)})
	if out.Success || out.Retryable {
		t.Fatalf("missing url should be permanent, got %+v", out)
	}
}
