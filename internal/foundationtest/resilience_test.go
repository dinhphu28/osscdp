package foundationtest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/dlq"
)

func seedDLQ(t *testing.T, f fixture, tid uuid.UUID, payload []byte) {
	t.Helper()
	rec := dlq.NewRecorder(f.pool)
	require.NoError(t, rec.Record(context.Background(), dlq.Entry{
		TenantID: &tid, EventID: "e1", Component: "cdp-worker",
		ErrorCode: "processing_failed", ErrorMessage: "boom", Payload: payload,
		RetryCount: 5, FailedAt: time.Now().UTC(),
	}))
}

func dlqRow(t *testing.T, f fixture, tid uuid.UUID) (id uuid.UUID, status string) {
	t.Helper()
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT id, status FROM dlq_event WHERE tenant_id=$1`, tid).Scan(&id, &status))
	return
}

func TestDLQ_ListRetryDiscard(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	seedDLQ(t, f, tid, []byte(`{"event_id":"e1","tenant_id":"`+tid.String()+`"}`))

	repo := dlq.NewRepo(f.pool)
	events, err := repo.List(ctx, tid, "", 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, dlq.StatusOpen, events[0].Status)

	// Retry republishes to cdp.events and marks retried.
	pub := &stubPublisher{}
	retrier := dlq.NewRetrier(repo, pub, bus.TopicEvents)
	id, _ := dlqRow(t, f, tid)
	require.NoError(t, retrier.Retry(ctx, tid, id))
	require.Equal(t, 1, pub.calls)
	require.Equal(t, bus.TopicEvents, pub.topic)

	_, status := dlqRow(t, f, tid)
	require.Equal(t, dlq.StatusRetried, status)

	// Discard.
	ok, err := repo.MarkStatus(ctx, tid, id, dlq.StatusDiscarded)
	require.NoError(t, err)
	require.True(t, ok)
	_, status = dlqRow(t, f, tid)
	require.Equal(t, dlq.StatusDiscarded, status)
}

func TestDLQ_TenantIsolation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tidA, _ := mkTenant(t, f, "a")
	tidB, _ := mkTenant(t, f, "b")
	seedDLQ(t, f, tidA, []byte(`{"event_id":"e1"}`))

	got, err := dlq.NewRepo(f.pool).List(ctx, tidB, "", 0)
	require.NoError(t, err)
	require.Len(t, got, 0)
}

// flakySender always fails (retryable), tracking how many real sends happened.
type flakySender struct{ sends int }

func (s *flakySender) Send(context.Context, activation.Destination, activation.Task) activation.Outcome {
	s.sends++
	return activation.Outcome{Retryable: true, ErrorMessage: "500"}
}

func TestCircuitBreaker_DefersAfterThreshold(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"country":"VN"}`)

	arepo := activation.NewRepo(f.pool)
	cfg, _ := json.Marshal(map[string]any{"url": "http://127.0.0.1:9"})
	dest, err := arepo.CreateDestination(ctx, tid, activation.TypeWebhook, "wh", cfg, "")
	require.NoError(t, err)
	segID := uuid.New()
	sub, err := arepo.CreateSubscription(ctx, tid, dest.ID, activation.TriggerSegmentMembership, &segID)
	require.NoError(t, err)

	// Create several tasks for the same destination (distinct idempotency keys).
	for i := 0; i < 6; i++ {
		_, err := arepo.CreateTask(ctx, activation.Task{
			TenantID: tid, DestinationID: dest.ID, SubscriptionID: sub.ID,
			CustomerProfileID: pu.CustomerProfileID, IdempotencyKey: "k" + string(rune('a'+i)),
			Payload: []byte(`{}`),
		}, activation.TaskPending, "")
		require.NoError(t, err)
	}

	sender := &flakySender{}
	breaker := activation.NewBreaker(3, time.Minute, time.Hour) // long cooldown so it stays open
	runner := activation.NewRunner(f.pool, map[string]activation.Sender{
		activation.TypeWebhook: sender,
	}, 50, time.Second, testLogger()).WithBreaker(breaker)

	circuitOpens := 0
	runner.OnCircuitOpen = func() { circuitOpens++ }

	// One pass claims all 6 due tasks: 3 real sends trip the breaker, the rest defer.
	n, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, 3, sender.sends, "breaker should stop real sends after the threshold")
	require.Equal(t, 3, circuitOpens, "remaining tasks should be deferred by the open breaker")
}
