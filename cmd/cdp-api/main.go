// Command cdp-api serves the admin API and the ingress auth probe.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/config"
	"github.com/dinhphu28/osscdp/internal/consent"
	"github.com/dinhphu28/osscdp/internal/crypto"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/governance"
	"github.com/dinhphu28/osscdp/internal/platform/database"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/rawevent"
	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/internal/segment"
	"github.com/dinhphu28/osscdp/internal/source"
	"github.com/dinhphu28/osscdp/internal/tenant"
)

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

	base := logging.New(cfg.LogLevel)
	logger := logging.Component(base, "cdp-api")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Apply migrations on boot.
	if err := migrate.Up(cfg.DatabaseURL); err != nil {
		return err
	}
	logger.Info("migrations applied")

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.Info("database connected")

	cipher, err := crypto.New(cfg.EncryptionKey)
	if err != nil {
		return err
	}

	// Wire dependencies.
	recorder := audit.NewRecorder(pool)
	tenantSvc := tenant.NewService(tenant.NewRepository(pool), recorder)
	sourceSvc := source.NewService(source.NewRepository(pool), recorder)
	tenantHandler := tenant.NewHandler(tenantSvc)
	sourceHandler := source.NewHandler(sourceSvc)
	eventsHandler := events.NewHandler(events.NewService(events.NewRepository(pool)))

	// Producer for replay (franz-go dials lazily, so cdp-api still starts if the
	// bus is down — only replay fails in that case).
	producer, err := bus.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	rawRepo := rawevent.NewRepo(pool)
	rawHandler := rawevent.NewHandler(rawRepo, rawevent.NewReplayer(rawRepo, producer, bus.TopicEvents, logger))
	profileHandler := profile.NewHandler(profile.NewRepo(pool))
	segmentHandler := segment.NewHandler(segment.NewRepo(pool))
	activationHandler := activation.NewHandler(activation.NewRepo(pool), cipher)
	consentHandler := consent.NewHandler(consent.NewRepo(pool), profile.NewRepo(pool))
	governanceHandler := governance.NewHandler(governance.NewService(pool, recorder))
	rbacRepo := rbac.NewRepo(pool)
	tokenHandler := auth.NewTokenHandler(rbacRepo, recorder)

	r := httpx.NewRouter(base)
	httpx.Health(r, pool)

	// Admin API: authenticate (static token = SUPER_ADMIN, else admin_token) then
	// authorize per-route by permission + tenant scope (RBAC, Phase 9b).
	r.Group(func(admin chi.Router) {
		admin.Use(auth.Authenticate(cfg.AdminAPIToken, rbacRepo))

		admin.With(auth.RequireSuperAdmin()).Post("/admin/v1/tenants", tenantHandler.Create)
		admin.With(auth.Require(rbac.PermAdminWrite)).Post("/admin/v1/admin-tokens", tokenHandler.Create)

		admin.With(auth.Require(rbac.PermSourceWrite)).Post("/admin/v1/tenants/{tenantID}/sources", sourceHandler.Create)
		admin.With(auth.Require(rbac.PermSourceWrite)).Post("/admin/v1/tenants/{tenantID}/sources/{sourceID}/rotate-key", sourceHandler.RotateKey)
		// Raw event query + replay (Phase 4).
		admin.With(auth.Require(rbac.PermEventRead)).Get("/admin/v1/tenants/{tenantID}/events", rawHandler.List)
		admin.With(auth.Require(rbac.PermEventRead)).Get("/admin/v1/tenants/{tenantID}/events/{eventID}", rawHandler.Get)
		admin.With(auth.Require(rbac.PermEventReplay)).Post("/admin/v1/tenants/{tenantID}/events/{eventID}/replay", rawHandler.ReplayOne)
		admin.With(auth.Require(rbac.PermEventReplay)).Post("/admin/v1/tenants/{tenantID}/replay", rawHandler.ReplayByIdentifier)
		// Customer profile query (Phase 6).
		admin.With(auth.Require(rbac.PermProfileRead)).Get("/admin/v1/tenants/{tenantID}/profiles", profileHandler.List)
		admin.With(auth.Require(rbac.PermProfileRead)).Get("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}", profileHandler.Get)
		// Consent + data governance (Phase 9a).
		admin.With(auth.Require(rbac.PermConsentWrite)).Put("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/consent", consentHandler.Set)
		admin.With(auth.Require(rbac.PermProfileRead)).Get("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/consent", consentHandler.List)
		admin.With(auth.Require(rbac.PermProfileRead)).Get("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}/export", governanceHandler.Export)
		admin.With(auth.Require(rbac.PermProfileDelete)).Delete("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}", governanceHandler.Delete)
		// Segment management (Phase 7).
		admin.With(auth.Require(rbac.PermSegmentWrite)).Post("/admin/v1/tenants/{tenantID}/segments", segmentHandler.Create)
		admin.With(auth.Require(rbac.PermSegmentWrite)).Put("/admin/v1/tenants/{tenantID}/segments/{segmentID}", segmentHandler.Update)
		admin.With(auth.Require(rbac.PermSegmentRead)).Get("/admin/v1/tenants/{tenantID}/segments/{segmentID}", segmentHandler.Get)
		admin.With(auth.Require(rbac.PermSegmentRead)).Get("/admin/v1/tenants/{tenantID}/segments/{segmentID}/members", segmentHandler.Members)
		// Activation: destinations, subscriptions, delivery log (Phase 8).
		admin.With(auth.Require(rbac.PermDestinationWrite)).Post("/admin/v1/tenants/{tenantID}/destinations", activationHandler.CreateDestination)
		admin.With(auth.Require(rbac.PermDestinationWrite)).Put("/admin/v1/tenants/{tenantID}/destinations/{destinationID}", activationHandler.UpdateDestination)
		admin.With(auth.Require(rbac.PermDestinationRead)).Get("/admin/v1/tenants/{tenantID}/destinations/{destinationID}", activationHandler.GetDestination)
		admin.With(auth.Require(rbac.PermDestinationWrite)).Post("/admin/v1/tenants/{tenantID}/destinations/{destinationID}/subscriptions", activationHandler.CreateSubscription)
		admin.With(auth.Require(rbac.PermActivationRead)).Get("/admin/v1/tenants/{tenantID}/destinations/{destinationID}/deliveries", activationHandler.Deliveries)
	})

	// Ingress API (API-key guard). Validates, normalizes, and enqueues to the
	// outbox; no heavy processing in the request path.
	r.Group(func(ingress chi.Router) {
		ingress.Use(auth.APIKey(sourceSvc))
		ingress.Get("/v1/auth/whoami", whoami)
		ingress.Post("/v1/events/track", eventsHandler.Track)
		ingress.Post("/v1/events/batch", eventsHandler.Batch)
		ingress.Post("/v1/identify", eventsHandler.Identify)
		ingress.Post("/v1/alias", eventsHandler.Alias)
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err.Error())
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func whoami(w http.ResponseWriter, r *http.Request) {
	tenantID, _ := auth.TenantID(r.Context())
	sourceID, _ := auth.SourceID(r.Context())
	httpx.WriteJSON(w, http.StatusOK, map[string]string{
		"tenant_id": tenantID.String(),
		"source_id": sourceID.String(),
	})
}
