// Command cdp-worker runs the event pipeline: it relays the outbox to the bus,
// consumes events into the raw event store, dead-letters poison messages, and
// exposes Prometheus metrics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/config"
	"github.com/dinhphu28/osscdp/internal/consent"
	"github.com/dinhphu28/osscdp/internal/crypto"
	"github.com/dinhphu28/osscdp/internal/dlq"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/identity"
	"github.com/dinhphu28/osscdp/internal/platform/database"
	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/internal/platform/metrics"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/rawevent"
	"github.com/dinhphu28/osscdp/internal/relay"
	"github.com/dinhphu28/osscdp/internal/segment"
)

const eventTopicPartitions = 6

func main() {
	if err := run(); err != nil {
		println("fatal:", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := logging.Component(logging.New(cfg.LogLevel), "cdp-worker")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := migrate.Up(cfg.DatabaseURL); err != nil {
		return err
	}
	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := bus.EnsureTopics(ctx, cfg.KafkaBrokers, eventTopicPartitions, bus.TopicEvents, bus.TopicIdentityResolved, bus.TopicProfileUpdated, bus.TopicSegmentMembershipChanged); err != nil {
		return err
	}
	logger.Info("topics ensured", "brokers", cfg.KafkaBrokers)

	cipher, err := crypto.New(cfg.EncryptionKey)
	if err != nil {
		return err
	}

	m := metrics.New()

	producer, err := bus.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	rel := relay.New(pool, producer, bus.TopicEvents, cfg.RelayBatchSize, cfg.RelayPollInterval, logger)
	rel.OnPublished = m.EventsPublished.Inc
	rel.OnPublishFail = m.KafkaPublishFailed.Inc

	// Second relay: drains the segment membership outbox (Phase 4) to its topic, so
	// membership flip + emit commit atomically and publish at-least-once.
	memRelay := relay.New(pool, producer, bus.TopicSegmentMembershipChanged, cfg.RelayBatchSize, cfg.RelayPollInterval, logger).
		WithTable("segment_membership_outbox")
	memRelay.OnPublished = m.MembershipPublished.Inc
	memRelay.OnPublishFail = m.MembershipPublishFail.Inc

	rawRepo := rawevent.NewRepo(pool)
	dlqRec := dlq.NewRecorder(pool)

	consumer, err := bus.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, []string{bus.TopicEvents}, cfg.MaxRetries, logger)
	if err != nil {
		return err
	}
	consumer.OnRetry = m.ProcessingRetries.Inc

	handler := makeHandler(rawRepo, m, logger)
	deadLetter := makeDeadLetter(dlqRec, m, logger)

	// Identity resolution: a second, independent consumer group on cdp.events.
	identitySvc := identity.NewService(pool, producer, bus.TopicIdentityResolved)
	identitySvc.OnResolved = m.IdentityResolved.Inc
	identitySvc.OnMerge = m.IdentityMerge.Inc
	identitySvc.Cipher = cipher // Tier 2: encrypt identifier plaintext at ingest
	identityConsumer, err := bus.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup+"-identity", []string{bus.TopicEvents}, cfg.MaxRetries, logger)
	if err != nil {
		return err
	}
	identityConsumer.OnRetry = m.ProcessingRetries.Inc
	identityHandler := makeIdentityHandler(identitySvc)

	// Profile unification: consumes identity_resolved, builds customer profiles.
	profileSvc := profile.NewService(pool, producer, bus.TopicProfileUpdated)
	profileSvc.OnUpdated = m.ProfileUpdated.Inc
	profileSvc.Audit = audit.NewRecorder(pool)
	profileSvc.Logger = logger
	behaviorRec := behavior.NewRecorder() // Phase 2: durable behavioral_event log
	behaviorRec.PropsGate = behavior.ConsentPropsGate{} // Phase 7: gate props under analytics consent
	profileSvc.Behavior = behaviorRec
	profileConsumer, err := bus.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup+"-profile", []string{bus.TopicIdentityResolved}, cfg.MaxRetries, logger)
	if err != nil {
		return err
	}
	profileConsumer.OnRetry = m.ProcessingRetries.Inc
	profileHandler := makeProfileHandler(profileSvc)

	// Segmentation: consumes profile_updated, maintains segment membership.
	behaviorStore := behavior.NewStore(pool)
	behaviorStore.OnSchemaDrift = m.SchemaDrift.Inc // finding #33: in-window event-property type drift (doc 18 §A)
	segmentSvc := segment.NewService(pool, profile.NewRepo(pool), behaviorStore)
	segmentSvc.OnEvaluated = m.SegmentEvaluated.Inc
	segmentSvc.OnMatched = m.SegmentMatched.Inc
	segmentSvc.OnStatefulEvaluated = m.StatefulEvaluated.Inc
	segmentSvc.OnStatefulMatched = m.StatefulMatched.Inc
	segmentConsumer, err := bus.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup+"-segment", []string{bus.TopicProfileUpdated}, cfg.MaxRetries, logger)
	if err != nil {
		return err
	}
	segmentConsumer.OnRetry = m.ProcessingRetries.Inc
	segmentHandler := makeSegmentHandler(segmentSvc)

	// Deadline sweeper (Phase 5): fires absence/expiry/re-entry transitions with no
	// inbound event by re-evaluating due segment_pending_eval rows fairly per tenant.
	segmentRunner := segment.NewRunner(segmentSvc, cfg.SegmentSweepBatchSize, cfg.SegmentSweepPerTenantCap,
		cfg.SegmentSweepSafetyBatch, cfg.SegmentSweepReclaimTimeout, cfg.SegmentSweepInterval, logger).
		WithParkPolicy(cfg.SegmentSweepBackoffBase, cfg.SegmentSweepBackoffCap, cfg.SegmentSweepMaxAttempts)
	segmentRunner.OnClaimed = m.SweepClaimed.Inc
	segmentRunner.OnTransition = m.SweepTransition.Inc
	segmentRunner.OnError = m.SweepError.Inc
	segmentRunner.OnSweepLag = m.SweepLagSeconds.Observe
	segmentRunner.OnBacklog = func(n int) { m.PendingBacklog.Set(float64(n)) }
	segmentRunner.OnParked = m.SweepParked.Inc
	segmentRunner.OnParkedBacklog = func(n int) { m.PendingParked.Set(float64(n)) }

	// Retention (Phase 8): prune aged behavioral_event / bucket partitions.
	retention := behavior.NewRetention(pool, cfg.BehaviorRetention, cfg.BehaviorRetentionInterval, logger)
	retention.OnPruned = func(n int) { m.BehaviorRetention.Add(float64(n)) }

	// Durable population-seed runner: drains segment_seed_job (resumable), seeding the
	// existing population for newly created/updated sweep-safe segments.
	seedRunner := segment.NewSeedRunner(segmentSvc.Repo(), cfg.SeedJobPagesPerClaim, cfg.SeedJobReclaimTimeout, cfg.SeedJobInterval, logger)
	seedRunner.OnSeededPage = m.SeedPages.Inc
	seedRunner.OnJobDone = m.SeedJobsDone.Inc

	// Activation: consumes segment_membership_changed → creates tasks; a sender
	// loop delivers them with retry/backoff. The consent gate skips denied sends.
	activationSvc := activation.NewService(pool, profile.NewRepo(pool), consent.NewRepo(pool))
	activationSvc.OnSkipped = m.ActivationSkipped.Inc
	activationConsumer, err := bus.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup+"-activation", []string{bus.TopicSegmentMembershipChanged}, cfg.MaxRetries, logger)
	if err != nil {
		return err
	}
	activationConsumer.OnRetry = m.ProcessingRetries.Inc
	activationHandler := makeActivationHandler(activationSvc)

	senders := map[string]activation.Sender{
		activation.TypeWebhook: activation.NewWebhookSender(cipher),
		activation.TypeKafka:   activation.NewKafkaSender(producer),
	}
	activationRunner := activation.NewRunner(pool, senders, cfg.ActivationBatchSize, cfg.ActivationPollInterval, logger).
		WithBreaker(activation.NewBreaker(cfg.CircuitThreshold, cfg.CircuitWindow, cfg.CircuitCooldown))
	activationRunner.OnSent = m.ActivationSent.Inc
	activationRunner.OnFailed = m.ActivationFailed.Inc
	activationRunner.OnCircuitOpen = m.ActivationCircuitOpen.Inc

	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsMux(m),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(12)
	go func() { defer wg.Done(); rel.Run(ctx) }()
	go func() { defer wg.Done(); memRelay.Run(ctx) }()
	go func() { defer wg.Done(); segmentRunner.Run(ctx) }()
	go func() { defer wg.Done(); retention.Run(ctx) }()
	go func() { defer wg.Done(); seedRunner.Run(ctx) }()
	go func() { defer wg.Done(); activationRunner.Run(ctx) }()
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx, handler, deadLetter); err != nil {
			logger.Error("consumer stopped", "error", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		if err := identityConsumer.Run(ctx, identityHandler, deadLetter); err != nil {
			logger.Error("identity consumer stopped", "error", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		if err := profileConsumer.Run(ctx, profileHandler, deadLetter); err != nil {
			logger.Error("profile consumer stopped", "error", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		if err := segmentConsumer.Run(ctx, segmentHandler, deadLetter); err != nil {
			logger.Error("segment consumer stopped", "error", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		if err := activationConsumer.Run(ctx, activationHandler, deadLetter); err != nil {
			logger.Error("activation consumer stopped", "error", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", err.Error())
			stop()
		}
	}()

	logger.Info("worker started")
	<-ctx.Done()
	logger.Info("worker shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	wg.Wait()
	return nil
}

func metricsMux(m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// makeHandler stores each consumed event and records lag. A malformed payload
// returns an error so it is retried and ultimately dead-lettered.
func makeHandler(repo *rawevent.Repo, m *metrics.Metrics, _ *slog.Logger) bus.Handler {
	return func(ctx context.Context, r bus.Record) error {
		var env events.Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			return err
		}
		if err := repo.Store(ctx, env, r.Value); err != nil {
			return err
		}
		m.EventsConsumed.Inc()
		m.ProcessingLagSecond.Observe(time.Since(env.ReceivedAt).Seconds())
		return nil
	}
}

// makeIdentityHandler unmarshals the event and resolves its identity. A
// malformed payload returns an error so it is retried and dead-lettered.
func makeIdentityHandler(svc *identity.Service) bus.Handler {
	return func(ctx context.Context, r bus.Record) error {
		var env events.Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			return err
		}
		return svc.Resolve(ctx, env)
	}
}

// makeProfileHandler unmarshals identity_resolved and updates the customer profile.
func makeProfileHandler(svc *profile.Service) bus.Handler {
	return func(ctx context.Context, r bus.Record) error {
		var ir identity.IdentityResolved
		if err := json.Unmarshal(r.Value, &ir); err != nil {
			return err
		}
		return svc.Update(ctx, ir.CanonicalUserID, ir.IdentityClusterID, ir.MergedCanonicalUserIDs, ir.Event)
	}
}

// makeSegmentHandler unmarshals profile_updated and updates segment membership.
func makeSegmentHandler(svc *segment.Service) bus.Handler {
	return func(ctx context.Context, r bus.Record) error {
		var pu profile.ProfileUpdated
		if err := json.Unmarshal(r.Value, &pu); err != nil {
			return err
		}
		return svc.Evaluate(ctx, pu)
	}
}

// makeActivationHandler unmarshals segment_membership_changed and creates tasks.
func makeActivationHandler(svc *activation.Service) bus.Handler {
	return func(ctx context.Context, r bus.Record) error {
		var mc segment.MembershipChanged
		if err := json.Unmarshal(r.Value, &mc); err != nil {
			return err
		}
		return svc.OnMembershipChanged(ctx, mc)
	}
}

func makeDeadLetter(rec *dlq.Recorder, m *metrics.Metrics, logger *slog.Logger) bus.DeadLetter {
	return func(ctx context.Context, r bus.Record, retries int, cause error) {
		var tid, sid *uuid.UUID
		var eventID string
		var env events.Envelope
		if json.Unmarshal(r.Value, &env) == nil {
			t, s := env.TenantID, env.SourceID
			tid, sid, eventID = &t, &s, env.EventID
		}
		entry := dlq.Entry{
			TenantID:     tid,
			SourceID:     sid,
			EventID:      eventID,
			Component:    "cdp-worker",
			ErrorCode:    "processing_failed",
			ErrorMessage: cause.Error(),
			Payload:      r.Value,
			RetryCount:   retries,
			FailedAt:     time.Now().UTC(),
		}
		if err := rec.Record(ctx, entry); err != nil {
			logger.Error("dlq record failed", "error", err.Error())
		}
		m.DLQTotal.Inc()
	}
}
