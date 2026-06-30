package foundationtest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/dlq"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/rawevent"
	"github.com/dinhphu28/osscdp/internal/relay"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// setupPipeline brings up Postgres (via setup) plus a Redpanda broker.
func setupPipeline(t *testing.T) (fixture, string) {
	t.Helper()
	f := setup(t)
	ctx := context.Background()
	rp, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rp.Terminate(ctx) })
	broker, err := rp.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return f, broker
}

func rawCount(t *testing.T, f fixture, tenantID interface{}, eventID string) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM raw_event WHERE tenant_id=$1 AND event_id=$2`, tenantID, eventID).Scan(&n))
	return n
}

func rawHandler(repo *rawevent.Repo) bus.Handler {
	return func(ctx context.Context, r bus.Record) error {
		var env events.Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			return err
		}
		return repo.Store(ctx, env, r.Value)
	}
}

func TestPipeline_EndToEnd(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")

	_, err := f.eventsSvc.Ingest(ctx,
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "product_viewed"},
		tid, sid, events.TypeTrack)
	require.NoError(t, err)

	brokers := []string{broker}
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, bus.TopicEvents))
	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	// Relay drains the outbox.
	rel := relay.New(f.pool, prod, bus.TopicEvents, 100, time.Second, testLogger())
	n, err := rel.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var status string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT status FROM event_outbox WHERE tenant_id=$1 AND event_id=$2`, tid, "e1").Scan(&status))
	require.Equal(t, "published", status)

	// Consumer persists to raw_event.
	repo := rawevent.NewRepo(f.pool)
	consumer, err := bus.NewConsumer(brokers, "grp-e2e", []string{bus.TopicEvents}, 2, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = consumer.Run(runCtx, rawHandler(repo), nil) }()

	require.Eventually(t, func() bool { return rawCount(t, f, tid, "e1") == 1 }, 30*time.Second, 200*time.Millisecond)

	var ps string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT processing_status FROM raw_event WHERE tenant_id=$1 AND event_id=$2`, tid, "e1").Scan(&ps))
	require.Equal(t, rawevent.StatusStored, ps)
}

type stubPublisher struct {
	topic, key string
	value      []byte
	calls      int
}

func (s *stubPublisher) Publish(_ context.Context, topic, key string, value []byte) error {
	s.calls++
	s.topic, s.key, s.value = topic, key, value
	return nil
}

func TestRelay_StubPublisherMarksPublished(t *testing.T) {
	f := setup(t) // no broker needed
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")

	_, err := f.eventsSvc.Ingest(ctx,
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "x"}, tid, sid, events.TypeTrack)
	require.NoError(t, err)

	pub := &stubPublisher{}
	rel := relay.New(f.pool, pub, bus.TopicEvents, 100, time.Second, testLogger())
	n, err := rel.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 1, pub.calls)
	require.Equal(t, bus.TopicEvents, pub.topic)
	require.Equal(t, tid.String()+"|user_id:u1", pub.key)

	var status string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT status FROM event_outbox WHERE tenant_id=$1 AND event_id=$2`, tid, "e1").Scan(&status))
	require.Equal(t, "published", status)

	// Second run has nothing left to publish.
	n2, err := rel.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n2)
}

func TestRawEvent_StoreIdempotent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := rawevent.NewRepo(f.pool)

	env, err := events.Normalize(
		events.IncomingEvent{EventID: "e1", UserID: "u1", EventName: "x"}, tid, sid, events.TypeTrack, time.Now())
	require.NoError(t, err)
	payload, err := env.PayloadJSON()
	require.NoError(t, err)

	require.NoError(t, repo.Store(ctx, env, payload))
	require.NoError(t, repo.Store(ctx, env, payload)) // duplicate delivery
	require.Equal(t, 1, rawCount(t, f, tid, "e1"))
}

func TestPipeline_PoisonMessageDeadLetters(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	brokers := []string{broker}
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, bus.TopicEvents))

	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	require.NoError(t, prod.Publish(ctx, bus.TopicEvents, "k", []byte("not-json")))

	rec := dlq.NewRecorder(f.pool)
	handler := func(_ context.Context, r bus.Record) error {
		var env events.Envelope
		return json.Unmarshal(r.Value, &env) // fails on poison message
	}
	deadLetter := func(ctx context.Context, r bus.Record, retries int, cause error) {
		_ = rec.Record(ctx, dlq.Entry{
			Component: "test", ErrorCode: "processing_failed", ErrorMessage: cause.Error(),
			Payload: r.Value, RetryCount: retries, FailedAt: time.Now().UTC(),
		})
	}

	consumer, err := bus.NewConsumer(brokers, "grp-dlq", []string{bus.TopicEvents}, 2, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = consumer.Run(runCtx, handler, deadLetter) }()

	require.Eventually(t, func() bool {
		var n int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM dlq_event`).Scan(&n))
		return n > 0
	}, 30*time.Second, 200*time.Millisecond)
}
