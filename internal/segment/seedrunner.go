package segment

import (
	"context"
	"log/slog"
	"time"
)

// SeedRunner drains durable segment_seed_job rows: it claims a job, pages over the
// tenant's profiles enqueuing segment_pending_eval, persists a cursor per page so a
// crash RESUMES from the last processed profile, and completes when the population is
// fully seeded. Restart-safe; concurrent-safe (claim uses FOR UPDATE SKIP LOCKED + a
// time-boxed reclaim).
type SeedRunner struct {
	repo          *Repo
	pagesPerClaim int
	pageSize      int
	reclaim       time.Duration
	interval      time.Duration
	logger        *slog.Logger
	now           func() time.Time

	// Metric hooks (nil-safe).
	OnSeededPage func()
	OnJobDone    func()
}

// NewSeedRunner constructs a SeedRunner. pagesPerClaim bounds the pages drained per
// tick (so one huge population spreads over ticks); reclaim time-boxes a crashed claim.
func NewSeedRunner(repo *Repo, pagesPerClaim int, reclaim, interval time.Duration, logger *slog.Logger) *SeedRunner {
	return &SeedRunner{
		repo: repo, pagesPerClaim: pagesPerClaim, pageSize: seedPageSize, reclaim: reclaim, interval: interval, logger: logger,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests).
func (r *SeedRunner) WithClock(now func() time.Time) *SeedRunner { r.now = now; return r }

// WithPageSize overrides the per-page profile count (tests exercise real multi-page drains).
func (r *SeedRunner) WithPageSize(n int) *SeedRunner { r.pageSize = n; return r }

// Run drains seed jobs each tick until ctx is canceled.
func (r *SeedRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("seed runner failed", "error", err.Error())
			}
		}
	}
}

// RunOnce claims one job and drains up to pagesPerClaim pages, persisting the cursor
// per page. Returns idle=true when no job was available.
func (r *SeedRunner) RunOnce(ctx context.Context) (idle bool, err error) {
	now := r.now()
	job, ok, err := r.repo.ClaimSeedJob(ctx, now, r.reclaim)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}

	for p := 0; p < r.pagesPerClaim; p++ {
		nextCursor, done, err := r.repo.SeedJobPage(ctx, job, r.pageSize)
		if err != nil {
			// Leave the claim set; the time-boxed reclaim resumes from the last cursor.
			return false, err
		}
		if r.OnSeededPage != nil {
			r.OnSeededPage()
		}
		job.Cursor = nextCursor
		if done {
			if err := r.repo.CompleteSeedJob(ctx, job.TenantID, job.SegmentID, job.ClaimedAt); err != nil {
				return false, err
			}
			if r.OnJobDone != nil {
				r.OnJobDone()
			}
			return false, nil
		}
		if err := r.repo.SetSeedJobCursor(ctx, job.TenantID, job.SegmentID, job.Cursor, job.ClaimedAt); err != nil {
			return false, err
		}
	}
	// Bounded pages reached; more remain — unclaim so the next tick continues it.
	return false, r.repo.ReleaseSeedJob(ctx, job.TenantID, job.SegmentID, job.ClaimedAt)
}
