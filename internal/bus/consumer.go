package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is a consumed message.
type Record struct {
	Topic string
	Key   []byte
	Value []byte
}

// Handler processes a record. Returning an error triggers retry.
type Handler func(ctx context.Context, r Record) error

// DeadLetter is called once a record exhausts its retries.
type DeadLetter func(ctx context.Context, r Record, retries int, cause error)

// Consumer consumes a topic as part of a consumer group, committing offsets only
// after a record is processed (or dead-lettered), giving at-least-once delivery.
type Consumer struct {
	cl         *kgo.Client
	maxRetries int
	logger     *slog.Logger

	// OnRetry, if set, is invoked on each retry attempt (for metrics).
	OnRetry func()
}

// NewConsumer joins the group and subscribes to topics.
func NewConsumer(brokers []string, group string, topics []string, maxRetries int, logger *slog.Logger) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(), // commit explicitly after processing
		// A brand-new group reads the backlog rather than only new events.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{cl: cl, maxRetries: maxRetries, logger: logger}, nil
}

// Run polls and processes records until ctx is canceled.
func (c *Consumer) Run(ctx context.Context, handler Handler, dlq DeadLetter) error {
	defer c.cl.Close()
	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				c.logger.Error("fetch error", "topic", e.Topic, "error", e.Err.Error())
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			c.process(ctx, Record{Topic: rec.Topic, Key: rec.Key, Value: rec.Value}, handler, dlq)
		})

		if err := c.cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.logger.Error("commit offsets failed", "error", err.Error())
		}
	}
}

// process runs the handler with bounded retries; on exhaustion it dead-letters.
func (c *Consumer) process(ctx context.Context, r Record, handler Handler, dlq DeadLetter) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			if c.OnRetry != nil {
				c.OnRetry()
			}
			backoff := time.Duration(attempt) * 100 * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		if err := handler(ctx, r); err != nil {
			lastErr = err
			continue
		}
		return // success
	}
	c.logger.Error("record dead-lettered", "topic", r.Topic, "retries", c.maxRetries, "error", lastErr.Error())
	if dlq != nil {
		dlq(ctx, r, c.maxRetries, lastErr)
	}
}
