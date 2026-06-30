package activation

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Runner is the DB-backed sender loop: it claims due tasks, sends them, and
// reschedules retries with backoff. Restart-safe; safe to run concurrently.
type Runner struct {
	repo      *Repo
	senders   map[string]Sender
	batchSize int
	interval  time.Duration
	logger    *slog.Logger

	// Metric hooks (nil-safe).
	OnSent   func()
	OnFailed func()
}

// NewRunner constructs a Runner. senders maps destination type → Sender.
func NewRunner(pool *pgxpool.Pool, senders map[string]Sender, batchSize int, interval time.Duration, logger *slog.Logger) *Runner {
	return &Runner{repo: NewRepo(pool), senders: senders, batchSize: batchSize, interval: interval, logger: logger}
}

// Run drains due tasks each tick until ctx is canceled.
func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("activation run failed", "error", err.Error())
			}
		}
	}
}

// RunOnce claims and sends up to batchSize due tasks; returns how many processed.
func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	tasks, err := r.repo.ClaimDueTasks(ctx, r.batchSize)
	if err != nil {
		return 0, err
	}
	for _, task := range tasks {
		r.process(ctx, task)
	}
	return len(tasks), nil
}

func (r *Runner) process(ctx context.Context, task Task) {
	attempt := task.AttemptCount + 1

	dest, err := r.repo.GetDestination(ctx, task.TenantID, task.DestinationID)
	if err != nil {
		r.finish(ctx, task, attempt, Outcome{ErrorMessage: "destination unavailable: " + err.Error()})
		return
	}
	sender := r.senders[dest.Type]
	if sender == nil {
		r.finish(ctx, task, attempt, Outcome{ErrorMessage: "unknown destination type: " + dest.Type})
		return
	}
	r.finish(ctx, task, attempt, sender.Send(ctx, dest, task))
}

func (r *Runner) finish(ctx context.Context, task Task, attempt int, out Outcome) {
	now := time.Now().UTC()
	deliveryStatus := DeliveryFailed
	if out.Success {
		deliveryStatus = DeliverySuccess
	}
	var httpStatus *int
	if out.HTTPStatus != 0 {
		httpStatus = &out.HTTPStatus
	}
	if err := r.repo.InsertDelivery(ctx, Delivery{
		TenantID:          task.TenantID,
		TaskID:            task.ID,
		DestinationID:     task.DestinationID,
		CustomerProfileID: task.CustomerProfileID,
		SourceEventID:     task.SourceEventID,
		IdempotencyKey:    task.IdempotencyKey,
		Status:            deliveryStatus,
		HTTPStatus:        httpStatus,
		ResponseBodyHash:  out.ResponseBodyHash,
		ErrorMessage:      out.ErrorMessage,
		AttemptCount:      attempt,
		SentAt:            &now,
	}); err != nil {
		r.logger.Error("insert delivery failed", "error", err.Error())
	}

	switch {
	case out.Success:
		_ = r.repo.MarkResult(ctx, task.ID, TaskSucceeded, attempt, nil, "")
		if r.OnSent != nil {
			r.OnSent()
		}
	case out.Retryable && attempt < maxRetries:
		next := now.Add(withJitter(Backoff(attempt)))
		_ = r.repo.MarkResult(ctx, task.ID, TaskFailedRetryable, attempt, &next, out.ErrorMessage)
		if r.OnFailed != nil {
			r.OnFailed()
		}
	default: // permanent, or retryable exhausted
		_ = r.repo.MarkResult(ctx, task.ID, TaskFailedPermanent, attempt, nil, out.ErrorMessage)
		if r.OnFailed != nil {
			r.OnFailed()
		}
	}
}

// withJitter adds up to +20% jitter to a backoff delay.
func withJitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d)/5+1))
}
