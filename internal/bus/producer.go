package bus

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes records to the bus. franz-go defaults to acks=all with an
// idempotent producer, so retried publishes don't duplicate within a session.
type Producer struct {
	cl *kgo.Client
}

// NewProducer connects a producer to the given brokers.
func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Publish synchronously produces one record, blocking until it is acknowledged.
// The key drives partitioning (use partition_key to preserve per-customer order).
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// Close flushes and closes the producer.
func (p *Producer) Close() { p.cl.Close() }
