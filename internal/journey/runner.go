package journey

import (
	"context"
	"log/slog"
	"time"
)

// Runner is the journey step sweeper: each tick it fairly claims due
// journey_enrollment rows across tenants (per-tenant cap so one busy tenant cannot
// starve others) and advances each one step. Restart-safe; safe to run concurrently
// (the claim uses FOR UPDATE SKIP LOCKED + a time-boxed reclaim, and each advance is
// claim-fenced). A clone of segment.Runner.
type Runner struct {
	svc          *Service
	batchSize    int
	perTenantCap int
	reclaim      time.Duration
	interval     time.Duration
	logger       *slog.Logger
	now          func() time.Time

	// Dead-letter park policy. Defaults if WithParkPolicy is not called.
	backoffBase time.Duration
	backoffCap  time.Duration
	maxAttempts int

	// Metric hooks (nil-safe).
	OnClaimed       func()
	OnAdvanced      func()
	OnError         func()
	OnSweepLag      func(seconds float64)
	OnParked        func()
	OnParkedBacklog func(depth int)
}

// NewRunner constructs a journey step sweeper.
func NewRunner(svc *Service, batchSize, perTenantCap int, reclaim, interval time.Duration, logger *slog.Logger) *Runner {
	return &Runner{
		svc: svc, batchSize: batchSize, perTenantCap: perTenantCap,
		reclaim: reclaim, interval: interval, logger: logger,
		backoffBase: 30 * time.Second, backoffCap: time.Hour, maxAttempts: 10,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests advance the injected instant).
func (r *Runner) WithClock(now func() time.Time) *Runner { r.now = now; return r }

// WithParkPolicy sets the exponential-backoff base/cap and park-after-N threshold.
func (r *Runner) WithParkPolicy(base, cap time.Duration, maxAttempts int) *Runner {
	r.backoffBase, r.backoffCap, r.maxAttempts = base, cap, maxAttempts
	return r
}

// Run advances due enrollments each tick until ctx is canceled.
func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("journey sweep failed", "error", err.Error())
			}
		}
	}
}

// RunOnce claims and advances one fair batch of due enrollments. Returns how many were
// processed.
func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	now := r.now()
	if r.OnParkedBacklog != nil {
		if n, err := r.svc.Repo().ParkedCount(ctx); err == nil {
			r.OnParkedBacklog(int(n))
		} else if ctx.Err() == nil {
			r.logger.Warn("journey parked count query failed", "error", err.Error())
		}
	}
	rows, err := r.svc.Repo().ClaimDueEnrollments(ctx, now, r.batchSize, r.perTenantCap, r.reclaim)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if r.OnClaimed != nil {
			r.OnClaimed()
		}
		if r.OnSweepLag != nil {
			r.OnSweepLag(now.Sub(row.DueAt).Seconds())
		}
		if err := r.svc.Advance(ctx, row, now); err != nil {
			if r.OnError != nil {
				r.OnError()
			}
			r.logger.Error("journey advance failed",
				"tenant", row.TenantID.String(), "journey", row.JourneyID.String(),
				"profile", row.CustomerProfileID.String(), "error", err.Error())
			attempts, parked, ferr := r.svc.Repo().FailEnrollment(ctx,
				row.TenantID, row.JourneyID, row.CustomerProfileID, row.EnrollmentSeq, now,
				err.Error(), r.backoffBase, r.backoffCap, r.maxAttempts)
			if ferr != nil && ctx.Err() == nil {
				r.logger.Error("fail enrollment failed", "error", ferr.Error())
			} else if parked {
				if r.OnParked != nil {
					r.OnParked()
				}
				r.logger.Warn("journey enrollment parked (dead-letter)",
					"tenant", row.TenantID.String(), "journey", row.JourneyID.String(),
					"profile", row.CustomerProfileID.String(), "attempts", attempts, "error", err.Error())
			}
			continue
		}
		if r.OnAdvanced != nil {
			r.OnAdvanced()
		}
	}
	return len(rows), nil
}
