package behavior

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/events"
)

func TestBuckets(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	rec := NewRecorder()
	store := NewStore(pool)

	// appendEvent records one track event through the recorder (log + bucket, one tx),
	// pinning occurred_at to occ (Timestamp==ReceivedAt so the clamp preserves it).
	appendEvent := func(t *testing.T, tid, pid uuid.UUID, evID, name string, occ time.Time) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, rec.Append(ctx, tx, pid, events.Envelope{
			TenantID: tid, Type: events.TypeTrack, EventName: name, EventID: evID,
			Timestamp: occ, ReceivedAt: occ,
		}))
		require.NoError(t, tx.Commit(ctx))
	}

	t.Run("UpsertAccumulates", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		h := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC) // hour bucket start
		appendEvent(t, tid, pid, "b1", "view", h.Add(5*time.Minute))
		appendEvent(t, tid, pid, "b2", "view", h.Add(45*time.Minute))

		var count int64
		var first, last time.Time
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count, first_at, last_at FROM profile_behavior_bucket
			 WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name='view' AND bucket_start=$3`,
			tid, pid, h).Scan(&count, &first, &last))
		require.EqualValues(t, 2, count)
		require.True(t, first.Equal(h.Add(5*time.Minute)), "first_at is the earliest")
		require.True(t, last.Equal(h.Add(45*time.Minute)), "last_at is the latest")

		// Redelivery of b1: the log insert is a no-op, so the bucket must not re-increment.
		appendEvent(t, tid, pid, "b1", "view", h.Add(5*time.Minute))
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count FROM profile_behavior_bucket WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name='view' AND bucket_start=$3`,
			tid, pid, h).Scan(&count))
		require.EqualValues(t, 2, count, "a redelivered event must not double-count the bucket")
	})

	// The bucket-backed count must equal an exact log count even when the window's
	// lower boundary splits an hour bucket (finding #5: no leading-edge over-inclusion).
	t.Run("BucketCountMatchesExactAtBoundary", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		at := time.Date(2026, 6, 10, 12, 30, 0, 0, time.UTC)
		times := []time.Time{
			at.Add(-4 * time.Hour),                    // out (before window)
			at.Add(-3 * time.Hour).Add(-time.Minute),  // out — boundary hour, before at-3h
			at.Add(-3 * time.Hour).Add(time.Minute),   // in  — boundary hour, after at-3h
			at.Add(-2 * time.Hour),                    // in
			at.Add(-1 * time.Hour),                    // in
			at.Add(-time.Minute),                      // in — upper boundary hour
		}
		for i, tm := range times {
			appendEvent(t, tid, pid, fmt.Sprintf("e%d", i), "view", tm)
		}

		from := at.Add(-3 * time.Hour)
		var exact int64
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM behavioral_event
			 WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name='view' AND occurred_at >= $3 AND occurred_at <= $4`,
			tid, pid, from, at).Scan(&exact))
		require.EqualValues(t, 4, exact, "sanity: 4 events fall inside [at-3h, at]")

		n, err := store.Count(ctx, tid, pid, Spec{EventName: "view", Window: 3 * time.Hour}, at)
		require.NoError(t, err)
		require.EqualValues(t, exact, n, "bucket count equals exact; the split boundary hour is counted exactly")
	})

	// The bucket count must equal the exact count for every window placement,
	// including a sub-hour (single-hour) window and full middle hours holding many
	// events (count>1 per bucket).
	t.Run("BucketCountEqualsExactAcrossPlacements", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		h := time.Date(2026, 6, 10, 14, 0, 0, 0, time.UTC)
		seed := []time.Time{
			h.Add(-3*time.Hour + 10*time.Minute), h.Add(-3*time.Hour + 20*time.Minute), h.Add(-3*time.Hour + 50*time.Minute), // 3 in hour h-3
			h.Add(-1*time.Hour + 5*time.Minute), // 1 in hour h-1
			h.Add(15 * time.Minute), h.Add(45 * time.Minute), // 2 in hour h
		}
		for i, tm := range seed {
			appendEvent(t, tid, pid, fmt.Sprintf("c%d", i), "click", tm)
		}
		cases := []struct {
			window time.Duration
			at     time.Time
		}{
			{40 * time.Minute, h.Add(30 * time.Minute)}, // single-hour window
			{90 * time.Minute, h.Add(30 * time.Minute)}, // two hours
			{4 * time.Hour, h.Add(50 * time.Minute)},    // spans the count=3 middle bucket
			{10 * time.Hour, h.Add(59 * time.Minute)},   // wide
		}
		for _, tc := range cases {
			bucket, err := store.Count(ctx, tid, pid, Spec{EventName: "click", Window: tc.window}, tc.at)
			require.NoError(t, err)
			exact, err := store.Count(ctx, tid, pid, Spec{EventName: "click", Window: tc.window, Exact: true}, tc.at)
			require.NoError(t, err)
			require.Equal(t, exact, bucket, "window=%v at=%v: bucket count must equal exact", tc.window, tc.at)
		}
	})
}
