package segment

import (
	"context"
	"log/slog"
	"time"
)

// Runner is the deadline sweeper: each tick it fairly claims due segment_pending_eval
// rows across tenants (per-tenant cap so one busy tenant cannot starve others) and
// re-evaluates each at now() with no inbound event — firing absence/expiry and
// re-entry transitions for idle and dormant profiles. Restart-safe; safe to run
// concurrently (the claim uses FOR UPDATE SKIP LOCKED + a time-boxed reclaim).
type Runner struct {
	svc          *Service
	batchSize    int
	perTenantCap int
	safetyBatch  int
	reclaim      time.Duration
	interval     time.Duration
	logger       *slog.Logger
	now          func() time.Time

	// Metric hooks (nil-safe).
	OnClaimed    func()
	OnTransition func()
	OnError      func()
}

// NewRunner constructs a sweeper. reclaim time-boxes a crashed claim; safetyBatch
// bounds the rolling safety re-enqueue per tick (0 disables it).
func NewRunner(svc *Service, batchSize, perTenantCap, safetyBatch int, reclaim, interval time.Duration, logger *slog.Logger) *Runner {
	return &Runner{
		svc: svc, batchSize: batchSize, perTenantCap: perTenantCap, safetyBatch: safetyBatch,
		reclaim: reclaim, interval: interval, logger: logger,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests advance the injected instant).
func (r *Runner) WithClock(now func() time.Time) *Runner { r.now = now; return r }

// Run sweeps due deadlines each tick until ctx is canceled.
func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("segment sweep failed", "error", err.Error())
			}
		}
	}
}

// RunOnce claims and re-evaluates one fair batch of due deadlines, then rolls the
// safety re-enqueue forward. Returns how many deadlines were processed.
func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	now := r.now()
	rows, err := r.svc.Repo().ClaimDuePending(ctx, now, r.batchSize, r.perTenantCap, r.reclaim)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if r.OnClaimed != nil {
			r.OnClaimed()
		}
		if err := r.svc.SweepEvaluate(ctx, row, now); err != nil {
			if r.OnError != nil {
				r.OnError()
			}
			r.logger.Error("sweep eval failed",
				"tenant", row.TenantID.String(), "segment", row.SegmentID.String(), "error", err.Error())
			// Back off: push the deadline forward (and unclaim) so a persistently
			// failing row neither tight-loops on the reclaim nor keeps its oldest
			// due_at and starves the tenant's healthy deadlines.
			if derr := r.svc.Repo().DeferPending(ctx, row.TenantID, row.SegmentID, row.CustomerProfileID, now.Add(r.reclaim)); derr != nil && ctx.Err() == nil {
				r.logger.Error("defer pending failed", "error", derr.Error())
			}
			continue
		}
		if r.OnTransition != nil {
			r.OnTransition()
		}
	}

	if r.safetyBatch > 0 {
		if _, err := r.svc.Repo().SafetyReEnqueue(ctx, now, r.safetyBatch); err != nil && ctx.Err() == nil {
			r.logger.Error("safety re-enqueue failed", "error", err.Error())
		}
	}
	return len(rows), nil
}
