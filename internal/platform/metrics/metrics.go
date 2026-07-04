// Package metrics exposes Prometheus collectors for the event pipeline.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the pipeline collectors over a private registry.
type Metrics struct {
	reg *prometheus.Registry

	EventsPublished       prometheus.Counter
	EventsConsumed        prometheus.Counter
	ProcessingRetries     prometheus.Counter
	DLQTotal              prometheus.Counter
	KafkaPublishFailed    prometheus.Counter
	ProcessingLagSecond   prometheus.Histogram
	IdentityResolved      prometheus.Counter
	IdentityMerge         prometheus.Counter
	ProfileUpdated        prometheus.Counter
	SegmentEvaluated      prometheus.Counter
	SegmentMatched        prometheus.Counter
	StatefulEvaluated     prometheus.Counter
	StatefulMatched       prometheus.Counter
	MembershipPublished   prometheus.Counter
	MembershipPublishFail prometheus.Counter
	ActivationSent        prometheus.Counter
	ActivationFailed      prometheus.Counter
	ActivationSkipped     prometheus.Counter
	ActivationCircuitOpen prometheus.Counter

	// Ingress (cdp-api).
	EventsReceived    prometheus.Counter
	EventsValidated   prometheus.Counter
	EventsRejected    prometheus.Counter
	EventsRateLimited prometheus.Counter
}

// New constructs and registers the collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		EventsPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_published_total", Help: "Events published from the outbox to the bus.",
		}),
		EventsConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_consumed_total", Help: "Events consumed and processed by the worker.",
		}),
		ProcessingRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "processing_retries_total", Help: "Handler retries across all consumed events.",
		}),
		DLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dlq_total", Help: "Events dead-lettered after exhausting retries.",
		}),
		KafkaPublishFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_publish_failed_total", Help: "Failed publish attempts to the bus.",
		}),
		ProcessingLagSecond: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "processing_lag_seconds",
			Help:    "Seconds between event received_at and worker processing.",
			Buckets: prometheus.ExponentialBuckets(0.05, 3, 10),
		}),
		IdentityResolved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "identity_resolved_total", Help: "Events resolved to an identity cluster.",
		}),
		IdentityMerge: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "identity_merge_total", Help: "Identity cluster merges performed.",
		}),
		ProfileUpdated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "profile_updated_total", Help: "Customer profile updates applied.",
		}),
		SegmentEvaluated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "segment_evaluated_total", Help: "Segment rule evaluations performed.",
		}),
		SegmentMatched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "segment_matched_total", Help: "Segment rule evaluations that matched.",
		}),
		StatefulEvaluated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "segment_stateful_evaluated_total", Help: "Behavioral (Level 3) segment evaluations performed.",
		}),
		StatefulMatched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "segment_stateful_matched_total", Help: "Behavioral segment evaluations that matched.",
		}),
		MembershipPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "segment_membership_published_total", Help: "Membership-change emits relayed from the outbox to the bus.",
		}),
		MembershipPublishFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "segment_membership_publish_failed_total", Help: "Membership-change outbox publishes that failed.",
		}),
		ActivationSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_sent_total", Help: "Activation deliveries that succeeded.",
		}),
		ActivationFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_failed_total", Help: "Activation delivery attempts that failed.",
		}),
		ActivationSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_skipped_total", Help: "Activations skipped due to denied consent.",
		}),
		ActivationCircuitOpen: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_circuit_open_total", Help: "Activation sends deferred by an open circuit breaker.",
		}),
		EventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_received_total", Help: "Events received by ingress.",
		}),
		EventsValidated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_validated_total", Help: "Events accepted (valid) by ingress.",
		}),
		EventsRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_rejected_total", Help: "Events rejected by ingress validation.",
		}),
		EventsRateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_rate_limited_total", Help: "Ingress requests rejected by rate limiting.",
		}),
	}
	reg.MustRegister(m.EventsPublished, m.EventsConsumed, m.ProcessingRetries,
		m.DLQTotal, m.KafkaPublishFailed, m.ProcessingLagSecond, m.IdentityResolved, m.IdentityMerge,
		m.ProfileUpdated, m.SegmentEvaluated, m.SegmentMatched, m.StatefulEvaluated, m.StatefulMatched, m.MembershipPublished, m.MembershipPublishFail, m.ActivationSent, m.ActivationFailed,
		m.ActivationSkipped, m.ActivationCircuitOpen,
		m.EventsReceived, m.EventsValidated, m.EventsRejected, m.EventsRateLimited)
	return m
}

// Handler returns the HTTP handler that serves the metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
