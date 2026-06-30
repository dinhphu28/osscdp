package foundationtest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/rawevent"
)

func seedRaw(t *testing.T, repo *rawevent.Repo, tid, sid uuid.UUID, eventID, eventName, userID string, now time.Time) {
	t.Helper()
	env, err := events.Normalize(
		events.IncomingEvent{EventID: eventID, EventName: eventName, UserID: userID},
		tid, sid, events.TypeTrack, now)
	require.NoError(t, err)
	payload, err := env.PayloadJSON()
	require.NoError(t, err)
	require.NoError(t, repo.Store(context.Background(), env, payload))
}

func TestRawEventQuery_GetAndTenantIsolation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	tidB, _ := mkTenant(t, f, "other")
	repo := rawevent.NewRepo(f.pool)

	seedRaw(t, repo, tid, sid, "e1", "product_viewed", "u1", time.Now())

	got, err := repo.GetByEventID(ctx, tid, "e1")
	require.NoError(t, err)
	require.Equal(t, "e1", got.EventID)

	_, err = repo.GetByEventID(ctx, tidB, "e1") // other tenant cannot see it
	require.ErrorIs(t, err, rawevent.ErrNotFound)

	_, err = repo.GetByEventID(ctx, tid, "missing")
	require.ErrorIs(t, err, rawevent.ErrNotFound)
}

func TestRawEventQuery_ListFilterAndPaginate(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := rawevent.NewRepo(f.pool)

	base := time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		seedRaw(t, repo, tid, sid, "u1-"+string(rune('a'+i)), "product_viewed", "u1", base.Add(time.Duration(i)*time.Minute))
	}
	seedRaw(t, repo, tid, sid, "u2-x", "checkout_started", "u2", base.Add(10*time.Minute))

	// Filter by identifier_key.
	byUser, _, err := repo.List(ctx, rawevent.ListQuery{TenantID: tid, IdentifierKey: "user_id:u1"})
	require.NoError(t, err)
	require.Len(t, byUser, 5)

	// Filter by event_name.
	byName, _, err := repo.List(ctx, rawevent.ListQuery{TenantID: tid, EventName: "checkout_started"})
	require.NoError(t, err)
	require.Len(t, byName, 1)
	require.Equal(t, "u2-x", byName[0].EventID)

	// Paginate through the u1 events, 2 at a time.
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		page, next, err := repo.List(ctx, rawevent.ListQuery{TenantID: tid, IdentifierKey: "user_id:u1", Limit: 2, Cursor: cursor})
		require.NoError(t, err)
		for _, e := range page {
			require.False(t, seen[e.EventID], "duplicate across pages: %s", e.EventID)
			seen[e.EventID] = true
		}
		pages++
		if next == "" {
			break
		}
		cursor = next
		require.LessOrEqual(t, pages, 5, "pagination did not terminate")
	}
	require.Len(t, seen, 5)
}

type captureSink struct {
	mu   sync.Mutex
	recs []bus.Record
}

func (c *captureSink) add(r bus.Record)  { c.mu.Lock(); c.recs = append(c.recs, r); c.mu.Unlock() }
func (c *captureSink) len() int          { c.mu.Lock(); defer c.mu.Unlock(); return len(c.recs) }
func (c *captureSink) first() bus.Record { c.mu.Lock(); defer c.mu.Unlock(); return c.recs[0] }

func TestReplay_EndToEnd(t *testing.T) {
	f, broker := setupPipeline(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := rawevent.NewRepo(f.pool)
	seedRaw(t, repo, tid, sid, "e1", "product_viewed", "u1", time.Now())

	brokers := []string{broker}
	require.NoError(t, bus.EnsureTopics(ctx, brokers, 1, bus.TopicEvents))
	prod, err := bus.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	sink := &captureSink{}
	consumer, err := bus.NewConsumer(brokers, "grp-replay", []string{bus.TopicEvents}, 1, testLogger())
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Run(runCtx, func(_ context.Context, r bus.Record) error { sink.add(r); return nil }, nil)
	}()

	replayer := rawevent.NewReplayer(repo, prod, bus.TopicEvents, testLogger())
	require.NoError(t, replayer.ReplayOne(ctx, tid, "e1"))

	require.Eventually(t, func() bool { return sink.len() >= 1 }, 30*time.Second, 200*time.Millisecond)
	rec := sink.first()
	require.Equal(t, tid.String()+"|user_id:u1", string(rec.Key))

	var env events.Envelope
	require.NoError(t, json.Unmarshal(rec.Value, &env))
	require.Equal(t, "e1", env.EventID)
}

func TestReplay_NotFound(t *testing.T) {
	f := setup(t)
	tid, _ := mkTenant(t, f, "acme")
	replayer := rawevent.NewReplayer(rawevent.NewRepo(f.pool), &captureNoopPub{}, bus.TopicEvents, testLogger())
	err := replayer.ReplayOne(context.Background(), tid, "missing")
	require.ErrorIs(t, err, rawevent.ErrNotFound)
}

type captureNoopPub struct{}

func (captureNoopPub) Publish(context.Context, string, string, []byte) error { return nil }
