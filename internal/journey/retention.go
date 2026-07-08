package journey

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// PruneTerminalEnrollments deletes terminal (completed/exited) enrollments last touched
// before olderThan, bounded to `limit` rows per call so a large backlog spreads over
// ticks. Active enrollments are never touched (only status IN completed/exited).
// journey_enrollment is small and non-partitioned, so a bounded DELETE is the right tool
// (unlike behavioral_event's partition DROP). Returns rows deleted.
func (r *Repo) PruneTerminalEnrollments(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	ct, err := r.pool.Exec(ctx, `
		DELETE FROM journey_enrollment
		WHERE ctid IN (
			SELECT ctid FROM journey_enrollment
			WHERE status IN ('completed','exited') AND updated_at < $1
			LIMIT $2
		)`, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("prune terminal enrollments: %w", err)
	}
	return ct.RowsAffected(), nil
}

// RetentionSweeper periodically prunes aged terminal journey_enrollment rows so the
// table does not grow without bound as enrollments complete/exit.
type RetentionSweeper struct {
	repo      *Repo
	retention time.Duration
	interval  time.Duration
	batch     int
	logger    *slog.Logger
	now       func() time.Time

	OnPruned func(int) // rows deleted per run (nil-safe)
}

// NewRetentionSweeper constructs a sweeper. retention is the minimum age a terminal row
// is kept before pruning.
func NewRetentionSweeper(repo *Repo, retention, interval time.Duration, logger *slog.Logger) *RetentionSweeper {
	return &RetentionSweeper{
		repo: repo, retention: retention, interval: interval, batch: 1000, logger: logger,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests).
func (s *RetentionSweeper) WithClock(now func() time.Time) *RetentionSweeper { s.now = now; return s }

// WithBatch overrides the per-run delete cap (tests).
func (s *RetentionSweeper) WithBatch(n int) *RetentionSweeper { s.batch = n; return s }

// Run prunes on a low-frequency ticker until ctx is canceled. It prunes once at
// startup (matching behavior.Retention) so a restart does not wait a full interval.
func (s *RetentionSweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	if _, err := s.PruneOnce(ctx); err != nil && ctx.Err() == nil {
		s.logger.Error("journey enrollment retention failed", "error", err.Error())
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.PruneOnce(ctx); err != nil && ctx.Err() == nil {
				s.logger.Error("journey enrollment retention failed", "error", err.Error())
			}
		}
	}
}

// PruneOnce deletes one bounded batch of aged terminal rows. Returns rows deleted.
func (s *RetentionSweeper) PruneOnce(ctx context.Context) (int, error) {
	cutoff := s.now().Add(-s.retention)
	n, err := s.repo.PruneTerminalEnrollments(ctx, cutoff, s.batch)
	if err != nil {
		return 0, err
	}
	if s.OnPruned != nil {
		s.OnPruned(int(n))
	}
	return int(n), nil
}
