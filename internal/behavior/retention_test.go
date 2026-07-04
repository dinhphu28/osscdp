package behavior

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRetention(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	ret := NewRetention(pool, 40*24*time.Hour, time.Hour, slog.Default()).
		WithClock(func() time.Time { return now })

	insertEvent := func(t *testing.T, tid, pid uuid.UUID, id string, occ time.Time) {
		t.Helper()
		_, err := pool.Exec(ctx,
			`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at) VALUES ($1,$2,$3,'view',$4)`,
			tid, pid, id, occ)
		require.NoError(t, err)
	}
	partExists := func(t *testing.T, name string) bool {
		t.Helper()
		var ok bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname=$1)`, name).Scan(&ok))
		return ok
	}

	t.Run("DeletesResidueKeepsInWindow", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		insertEvent(t, tid, pid, "old", now.AddDate(0, 0, -60))    // outside 40d horizon (DEFAULT)
		insertEvent(t, tid, pid, "recent", now.AddDate(0, 0, -10)) // in horizon
		// A bucket row on each side of the horizon too (bucket residue is also pruned).
		for _, b := range []struct {
			ev string
			at time.Time
		}{{"vold", now.AddDate(0, 0, -60)}, {"vrec", now.AddDate(0, 0, -10)}} {
			_, err := pool.Exec(ctx,
				`INSERT INTO profile_behavior_bucket (tenant_id, customer_profile_id, event_name, bucket_start, count, first_at, last_at) VALUES ($1,$2,$3,$4,1,$4,$4)`,
				tid, pid, b.ev, b.at)
			require.NoError(t, err)
		}

		_, err := ret.PruneOnce(ctx, now)
		require.NoError(t, err)

		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, pid).Scan(&n))
		require.Equal(t, 1, n, "old residue deleted, in-window kept")
		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT event_id FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, pid).Scan(&id))
		require.Equal(t, "recent", id)
		var buckets int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM profile_behavior_bucket WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, pid).Scan(&buckets))
		require.Equal(t, 1, buckets, "old bucket residue deleted, in-window bucket kept")
	})

	// A long active window must extend the effective horizon so its in-window data
	// (older than the 40d config horizon) is NOT pruned (findings #9/#19).
	t.Run("HonorsLongestActiveWindow", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		segID, verID := uuid.New(), uuid.New()
		_, err := pool.Exec(ctx, `INSERT INTO segment (id, tenant_id, name, status) VALUES ($1,$2,'longwin','active')`, segID, tid)
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`INSERT INTO segment_version (id, tenant_id, segment_id, version, rule_json, status, max_window_seconds) VALUES ($1,$2,$3,1,'{}','active',$4)`,
			verID, tid, segID, int64((90 * 24 * time.Hour).Seconds()))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE segment SET current_version_id=$1 WHERE id=$2`, verID, segID)
		require.NoError(t, err)
		// Don't leak the active window or the DEFAULT row to other subtests.
		defer pool.Exec(ctx, `UPDATE segment SET status='inactive' WHERE id=$1`, segID)
		defer pool.Exec(ctx, `DELETE FROM behavioral_event WHERE event_id='w60'`)

		pid := uuid.New()
		insertEvent(t, tid, pid, "w60", now.AddDate(0, 0, -60)) // >40d but <90d window

		_, err = ret.PruneOnce(ctx, now)
		require.NoError(t, err)
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM behavioral_event WHERE event_id='w60'`).Scan(&n))
		require.Equal(t, 1, n, "data within the longest active window survives retention")
	})

	t.Run("DropsOldWeeklyPartition", func(t *testing.T) {
		oldWeek := weekStart(now.AddDate(0, 0, -60))
		part := "behavioral_event_w" + oldWeek.Format("20060102")
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF behavioral_event FOR VALUES FROM ('%s') TO ('%s')`,
			part, oldWeek.Format(time.RFC3339), oldWeek.AddDate(0, 0, 7).Format(time.RFC3339)))
		require.NoError(t, err)
		tid := seedTenant(t, ctx, pool)
		insertEvent(t, tid, uuid.New(), "p1", oldWeek.Add(24*time.Hour))
		require.True(t, partExists(t, part))

		_, err = ret.PruneOnce(ctx, now)
		require.NoError(t, err)
		require.False(t, partExists(t, part), "the whole old partition is dropped")
	})

	t.Run("CreatesFuturePartitions", func(t *testing.T) {
		_, err := ret.PruneOnce(ctx, now)
		require.NoError(t, err)
		nextWeek := weekStart(now).AddDate(0, 0, 7)
		require.True(t, partExists(t, "behavioral_event_w"+nextWeek.Format("20060102")),
			"next week's partition is created ahead so new writes are droppable")
	})
}
