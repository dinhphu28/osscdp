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

	// Dead-letter park policy (doc 18 §B). Defaults if WithParkPolicy is not called.
	backoffBase time.Duration // first-failure backoff (default 30s)
	backoffCap  time.Duration // backoff ceiling (default 1h)
	maxAttempts int           // park after this many failures (default 10)

	// Metric hooks (nil-safe).
	OnClaimed       func()
	OnTransition    func()
	OnError         func()
	OnSweepLag      func(seconds float64) // now - due_at at claim
	OnBacklog       func(due int)         // due, unclaimed rows this tick
	OnParked        func()                // a row crossed into the dead-letter
	OnParkedBacklog func(depth int)       // current parked-row depth this tick
}

// NewRunner constructs a sweeper. reclaim time-boxes a crashed claim; safetyBatch
// bounds the rolling safety re-enqueue per tick (0 disables it).
func NewRunner(svc *Service, batchSize, perTenantCap, safetyBatch int, reclaim, interval time.Duration, logger *slog.Logger) *Runner {
	return &Runner{
		svc: svc, batchSize: batchSize, perTenantCap: perTenantCap, safetyBatch: safetyBatch,
		reclaim: reclaim, interval: interval, logger: logger,
		backoffBase: 30 * time.Second, backoffCap: time.Hour, maxAttempts: 10,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests advance the injected instant).
func (r *Runner) WithClock(now func() time.Time) *Runner { r.now = now; return r }

// WithParkPolicy sets the exponential-backoff base/cap and the park-after-N threshold
// for a persistently-failing deadline row (doc 18 §B).
func (r *Runner) WithParkPolicy(base, cap time.Duration, maxAttempts int) *Runner {
	r.backoffBase, r.backoffCap, r.maxAttempts = base, cap, maxAttempts
	return r
}

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
	if r.OnBacklog != nil {
		if n, err := r.svc.Repo().PendingBacklog(ctx, now, r.reclaim); err == nil {
			r.OnBacklog(int(n))
		} else if ctx.Err() == nil {
			r.logger.Warn("pending backlog query failed", "error", err.Error())
		}
	}
	if r.OnParkedBacklog != nil {
		if n, err := r.svc.Repo().ParkedCount(ctx); err == nil {
			r.OnParkedBacklog(int(n))
		} else if ctx.Err() == nil {
			r.logger.Warn("parked count query failed", "error", err.Error())
		}
	}
	rows, err := r.svc.Repo().ClaimDuePending(ctx, now, r.batchSize, r.perTenantCap, r.reclaim)
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
		if err := r.svc.SweepEvaluate(ctx, row, now); err != nil {
			if r.OnError != nil {
				r.OnError()
			}
			r.logger.Error("sweep eval failed",
				"tenant", row.TenantID.String(), "segment", row.SegmentID.String(), "error", err.Error())
			// Back off exponentially (and unclaim); past the ceiling, park (dead-letter)
			// the row so a persistent poison stops churning, stops distorting the lag/
			// backlog SLIs, and stops crowding the tenant's fair-claim slots.
			attempts, parked, ferr := r.svc.Repo().FailPending(ctx,
				row.TenantID, row.SegmentID, row.CustomerProfileID, now,
				err.Error(), r.backoffBase, r.backoffCap, r.maxAttempts)
			if ferr != nil && ctx.Err() == nil {
				r.logger.Error("fail pending failed", "error", ferr.Error())
			} else if parked {
				if r.OnParked != nil {
					r.OnParked()
				}
				r.logger.Warn("sweep deadline parked (dead-letter)",
					"tenant", row.TenantID.String(), "segment", row.SegmentID.String(),
					"profile", row.CustomerProfileID.String(), "attempts", attempts, "error", err.Error())
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
