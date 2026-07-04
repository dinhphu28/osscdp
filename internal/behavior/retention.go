package behavior

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// retentionLockKey guards PruneOnce so only one worker replica prunes at a time
// (advisory lock — avoids duplicated scans + concurrent DDL across replicas).
const retentionLockKey int64 = 0x0B1A_5EED

// retentionMargin is added past the longest active window so a boundary read never
// races a prune, and covers the weekly partition granularity.
const retentionMargin = 8 * 24 * time.Hour

// partitionedTables map a range-partitioned table to its range key column and its
// DEFAULT partition (residue is deleted only from DEFAULT; whole weekly partitions
// are DROPped).
var partitionedTables = []struct{ table, col, deflt string }{
	{"behavioral_event", "occurred_at", "behavioral_event_default"},
	{"profile_behavior_bucket", "bucket_start", "profile_behavior_bucket_default"},
}

// Retention prunes aged Level-3 behavioral data. Primary mechanism is DROP of whole
// weekly range partitions (cheap, no bloat); it creates upcoming weekly partitions
// ahead of time so new writes land in droppable partitions, and DELETEs residue from
// the DEFAULT partition (the bootstrap week) as a correctness fallback. The effective
// horizon is extended to cover the longest active window, so retention never prunes
// data a live rule still needs (findings #9/#19).
type Retention struct {
	pool       *pgxpool.Pool
	horizon    time.Duration
	interval   time.Duration
	aheadWeeks int
	logger     *slog.Logger
	now        func() time.Time

	OnPruned func(int) // partitions dropped + residue rows deleted per run (nil-safe)
}

// NewRetention constructs a Retention. horizon is the minimum age kept.
func NewRetention(pool *pgxpool.Pool, horizon, interval time.Duration, logger *slog.Logger) *Retention {
	return &Retention{
		pool: pool, horizon: horizon, interval: interval, aheadWeeks: 2, logger: logger,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests).
func (r *Retention) WithClock(now func() time.Time) *Retention { r.now = now; return r }

// Run prunes on a low-frequency ticker until ctx is canceled.
func (r *Retention) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	if _, err := r.PruneOnce(ctx, r.now()); err != nil && ctx.Err() == nil {
		r.logger.Error("behavior retention failed", "error", err.Error())
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.PruneOnce(ctx, r.now()); err != nil && ctx.Err() == nil {
				r.logger.Error("behavior retention failed", "error", err.Error())
			}
		}
	}
}

// effectiveHorizon = max(configured horizon, longest active window + margin), so
// retention never prunes data an active rule's window still reads.
func (r *Retention) effectiveHorizon(ctx context.Context) (time.Duration, error) {
	var maxWin *int64
	err := r.pool.QueryRow(ctx, `
		SELECT max(v.max_window_seconds)
		FROM segment s JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.status = 'active'`).Scan(&maxWin)
	if err != nil {
		return r.horizon, fmt.Errorf("max window: %w", err)
	}
	h := r.horizon
	if maxWin != nil {
		if w := time.Duration(*maxWin)*time.Second + retentionMargin; w > h {
			h = w
		}
	}
	return h, nil
}

// PruneOnce (leader-guarded) ensures upcoming partitions exist, drops partitions
// entirely older than the effective horizon, and deletes DEFAULT-partition residue.
// Returns dropped partitions + deleted residue rows.
func (r *Retention) PruneOnce(ctx context.Context, now time.Time) (int, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, retentionLockKey).Scan(&got); err != nil {
		return 0, fmt.Errorf("retention lock: %w", err)
	}
	if !got {
		return 0, nil // another replica is pruning
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, retentionLockKey) }()

	horizon, err := r.effectiveHorizon(ctx)
	if err != nil {
		return 0, err
	}
	cutoff := now.Add(-horizon)

	pruned := 0
	var firstErr error
	for _, pt := range partitionedTables {
		if err := r.ensureFuturePartitions(ctx, pt.table, now); err != nil && firstErr == nil {
			firstErr = err
		}
		dropped, err := r.dropOldPartitions(ctx, pt.table, cutoff)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		residue, err := r.pruneResidue(ctx, pt.deflt, pt.col, cutoff)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		pruned += dropped + int(residue)
	}
	if r.OnPruned != nil {
		r.OnPruned(pruned)
	}
	return pruned, firstErr
}

func weekStart(t time.Time) time.Time {
	d := t.UTC().Truncate(24 * time.Hour)
	daysSinceMonday := (int(d.Weekday()) + 6) % 7 // Mon->0 ... Sun->6
	return d.AddDate(0, 0, -daysSinceMonday)
}

// ensureFuturePartitions creates weekly partitions for this week + the next aheadWeeks
// so new writes land in a droppable partition. A create that overlaps existing DEFAULT
// rows (the current week's bootstrap data) is expected and logged at debug; those rows
// are pruned by the DEFAULT residue DELETE instead.
func (r *Retention) ensureFuturePartitions(ctx context.Context, table string, now time.Time) error {
	ws := weekStart(now)
	for i := 0; i <= r.aheadWeeks; i++ {
		start := ws.AddDate(0, 0, 7*i)
		end := start.AddDate(0, 0, 7)
		part := fmt.Sprintf("%s_w%s", table, start.Format("20060102"))
		sql := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
			part, table, start.Format(time.RFC3339), end.Format(time.RFC3339))
		if _, err := r.pool.Exec(ctx, sql); err != nil {
			// Benign for the current week (DEFAULT already holds overlapping rows); a
			// real failure just means writes stay in DEFAULT and are DELETE-pruned.
			r.logger.Debug("skip partition create", "partition", part, "error", err.Error())
		}
	}
	return nil
}

// dropOldPartitions drops weekly partitions whose entire range is older than cutoff.
// Each DROP runs with a short lock_timeout so it can never stall live inserts behind
// its brief ACCESS EXCLUSIVE lock.
func (r *Retention) dropOldPartitions(ctx context.Context, table string, cutoff time.Time) (int, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = $1 AND c.relname LIKE $2`,
		table, table+`_w%`)
	if err != nil {
		return 0, fmt.Errorf("list partitions: %w", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return 0, err
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	dropped := 0
	for _, name := range names {
		suffix := name[len(table)+2:] // strip "<table>_w"
		start, perr := time.Parse("20060102", suffix)
		if perr != nil {
			continue
		}
		if start.AddDate(0, 0, 7).After(cutoff) {
			continue // partition still holds in-horizon data
		}
		if err := r.dropPartition(ctx, name); err != nil {
			r.logger.Warn("drop partition failed (will retry)", "partition", name, "error", err.Error())
			continue
		}
		dropped++
	}
	return dropped, nil
}

// dropPartition drops one partition under a short lock_timeout (SET LOCAL, tx-scoped)
// so it never stalls live inserts behind its brief ACCESS EXCLUSIVE lock.
func (r *Retention) dropPartition(ctx context.Context, name string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DROP TABLE IF EXISTS `+name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// pruneResidue deletes rows older than cutoff from the DEFAULT partition only (weekly
// partitions are dropped whole), so the hot table is never churned with dead tuples.
func (r *Retention) pruneResidue(ctx context.Context, defaultPart, col string, cutoff time.Time) (int64, error) {
	ct, err := r.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s < $1`, defaultPart, col), cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune residue %s: %w", defaultPart, err)
	}
	return ct.RowsAffected(), nil
}
