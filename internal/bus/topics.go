// Package bus provides producer and consumer abstractions over Redpanda/Kafka
// (franz-go) for the CDP event pipeline.
package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic names. Only TopicEvents is used in Phase 3; the rest are declared for
// the phases that introduce them.
const (
	TopicEvents                   = "cdp.events"
	TopicIdentityResolved         = "cdp.identity-resolved"
	TopicProfileUpdated           = "cdp.profile-updated"
	TopicSegmentMembershipChanged = "cdp.segment-membership-changed"
	TopicActivation               = "cdp.activation"
)

// EnsureTopics creates the given topics with the requested partition count if
// they do not already exist. Existing topics are left untouched.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	// replicationFactor -1 → use the broker default.
	resp, err := adm.CreateTopics(ctx, partitions, -1, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, t := range resp {
		if t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.Topic, t.Err)
		}
	}
	return nil
}
