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

	EventsPublished     prometheus.Counter
	EventsConsumed      prometheus.Counter
	ProcessingRetries   prometheus.Counter
	DLQTotal            prometheus.Counter
	KafkaPublishFailed  prometheus.Counter
	ProcessingLagSecond prometheus.Histogram
	IdentityResolved    prometheus.Counter
	IdentityMerge       prometheus.Counter
	ProfileUpdated      prometheus.Counter
	SegmentEvaluated    prometheus.Counter
	SegmentMatched      prometheus.Counter
	ActivationSent      prometheus.Counter
	ActivationFailed    prometheus.Counter
	ActivationSkipped   prometheus.Counter
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
		ActivationSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_sent_total", Help: "Activation deliveries that succeeded.",
		}),
		ActivationFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_failed_total", Help: "Activation delivery attempts that failed.",
		}),
		ActivationSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "activation_skipped_total", Help: "Activations skipped due to denied consent.",
		}),
	}
	reg.MustRegister(m.EventsPublished, m.EventsConsumed, m.ProcessingRetries,
		m.DLQTotal, m.KafkaPublishFailed, m.ProcessingLagSecond, m.IdentityResolved, m.IdentityMerge,
		m.ProfileUpdated, m.SegmentEvaluated, m.SegmentMatched, m.ActivationSent, m.ActivationFailed,
		m.ActivationSkipped)
	return m
}

// Handler returns the HTTP handler that serves the metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
