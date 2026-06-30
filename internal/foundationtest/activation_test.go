package foundationtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

type webhookSink struct {
	mu      sync.Mutex
	hits    int
	lastKey string
	lastErr string
	body    []byte
}

func (s *webhookSink) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits++
	s.lastKey = r.Header.Get("Idempotency-Key")
}
func (s *webhookSink) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.hits }

func webhookDestConfig(url string) json.RawMessage {
	c, _ := json.Marshal(activation.WebhookConfig{URL: url, TimeoutMS: 2000})
	return c
}

func mc(tid, segID, profileID uuid.UUID, change string) segment.MembershipChanged {
	return segment.MembershipChanged{
		TenantID: tid, SegmentID: segID, CustomerProfileID: profileID,
		Change: change, ReasonEventID: "e1", ChangedAt: time.Now(),
	}
}

func taskStatus(t *testing.T, f fixture, tid uuid.UUID) (status string, attempt int, next *time.Time) {
	t.Helper()
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT status, attempt_count, next_attempt_at FROM activation_task WHERE tenant_id=$1`, tid).
		Scan(&status, &attempt, &next))
	return
}

func taskCount(t *testing.T, f fixture, tid uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(), `SELECT count(*) FROM activation_task WHERE tenant_id=$1`, tid).Scan(&n))
	return n
}

func setupActivation(t *testing.T, f fixture, status int) (uuid.UUID, uuid.UUID, *webhookSink, *httptest.Server) {
	t.Helper()
	sink := &webhookSink{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sink.record(r)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	tid, sid := mkTenant(t, f, "acme")
	seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"country":"VN"}`)

	arepo := activation.NewRepo(f.pool)
	dest, err := arepo.CreateDestination(context.Background(), tid, activation.TypeWebhook, "wh", webhookDestConfig(srv.URL), "")
	require.NoError(t, err)
	segID := uuid.New()
	_, err = arepo.CreateSubscription(context.Background(), tid, dest.ID, activation.TriggerSegmentMembership, &segID)
	require.NoError(t, err)
	return tid, segID, sink, srv
}

func newRunner(f fixture) *activation.Runner {
	return activation.NewRunner(f.pool, map[string]activation.Sender{
		activation.TypeWebhook: activation.NewWebhookSender(),
	}, 50, time.Second, testLogger())
}

func TestActivation_EnterDeliversAndIdempotent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, segID, sink, _ := setupActivation(t, f, 200)
	pid := profileIDFor(t, f, tid)
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))

	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))
	require.Equal(t, 1, taskCount(t, f, tid))
	// Duplicate membership change → no second task.
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))
	require.Equal(t, 1, taskCount(t, f, tid))

	n, err := newRunner(f).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	status, _, _ := taskStatus(t, f, tid)
	require.Equal(t, activation.TaskSucceeded, status)
	require.Equal(t, 1, sink.count())
	require.NotEmpty(t, sink.lastKey)

	var delivered, httpStatus int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*), coalesce(max(http_status),0) FROM activation_delivery WHERE tenant_id=$1`, tid).Scan(&delivered, &httpStatus))
	require.Equal(t, 1, delivered)
	require.Equal(t, 200, httpStatus)
}

func TestActivation_RetryableSchedulesRetry(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, segID, _, _ := setupActivation(t, f, 500)
	pid := profileIDFor(t, f, tid)
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))

	_, err := newRunner(f).RunOnce(ctx)
	require.NoError(t, err)

	status, attempt, next := taskStatus(t, f, tid)
	require.Equal(t, activation.TaskFailedRetryable, status)
	require.Equal(t, 1, attempt)
	require.NotNil(t, next)
	require.True(t, next.After(time.Now()))
}

func TestActivation_PermanentFailsImmediately(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, segID, _, _ := setupActivation(t, f, 400)
	pid := profileIDFor(t, f, tid)
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))

	_, err := newRunner(f).RunOnce(ctx)
	require.NoError(t, err)
	status, attempt, _ := taskStatus(t, f, tid)
	require.Equal(t, activation.TaskFailedPermanent, status)
	require.Equal(t, 1, attempt)
}

func TestActivation_ExhaustionToPermanent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, segID, _, _ := setupActivation(t, f, 500)
	pid := profileIDFor(t, f, tid)
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))
	// Simulate 4 prior attempts so the next one exhausts (max 5).
	_, err := f.pool.Exec(ctx, `UPDATE activation_task SET attempt_count=4, next_attempt_at=now() WHERE tenant_id=$1`, tid)
	require.NoError(t, err)

	_, err = newRunner(f).RunOnce(ctx)
	require.NoError(t, err)
	status, attempt, _ := taskStatus(t, f, tid)
	require.Equal(t, activation.TaskFailedPermanent, status)
	require.Equal(t, 5, attempt)
}

func TestActivation_DisabledDestinationNoTask(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, segID, _, _ := setupActivation(t, f, 200)
	pid := profileIDFor(t, f, tid)

	arepo := activation.NewRepo(f.pool)
	// Disable the destination.
	var destID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT id FROM destination WHERE tenant_id=$1`, tid).Scan(&destID))
	_, err := arepo.UpdateDestination(ctx, tid, destID, activation.StatusDisabled, nil)
	require.NoError(t, err)

	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))
	require.Equal(t, 0, taskCount(t, f, tid))
}

func TestActivation_TenantIsolation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tidA, segID, _, _ := setupActivation(t, f, 200)
	tidB, _ := mkTenant(t, f, "tenant-b")
	pidA := profileIDFor(t, f, tidA)
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))

	// A membership change for tenant B referencing A's segment id → no subs in B.
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tidB, segID, pidA, "entered")))
	require.Equal(t, 0, taskCount(t, f, tidB))
}

func TestActivation_KafkaDestination(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"country":"VN"}`)

	brokers := []string{broker}
	const topic = "cdp.activation-test"
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, topic))
	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	arepo := activation.NewRepo(f.pool)
	cfg, _ := json.Marshal(activation.KafkaConfig{Topic: topic})
	dest, err := arepo.CreateDestination(ctx, tid, activation.TypeKafka, "kf", cfg, "")
	require.NoError(t, err)
	segID := uuid.New()
	_, err = arepo.CreateSubscription(ctx, tid, dest.ID, activation.TriggerSegmentMembership, &segID)
	require.NoError(t, err)

	sink := &captureSink{}
	consumer, err := bus.NewConsumer(brokers, "grp-activation-test", []string{topic}, 1, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Run(runCtx, func(_ context.Context, r bus.Record) error { sink.add(r); return nil }, nil)
	}()

	svc := activation.NewService(f.pool, profile.NewRepo(f.pool))
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pu.CustomerProfileID, "entered")))

	runner := activation.NewRunner(f.pool, map[string]activation.Sender{
		activation.TypeKafka: activation.NewKafkaSender(prod),
	}, 50, time.Second, testLogger())
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sink.len() >= 1 }, 30*time.Second, 200*time.Millisecond)
}

func profileIDFor(t *testing.T, f fixture, tid uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(), `SELECT id FROM customer_profile WHERE tenant_id=$1 LIMIT 1`, tid).Scan(&id))
	return id
}
