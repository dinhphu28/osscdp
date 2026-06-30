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

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/config"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/platform/database"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/rawevent"
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

	r := httpx.NewRouter(base)
	httpx.Health(r, pool)

	// Admin API (static token guard; RBAC in Phase 9).
	r.Group(func(admin chi.Router) {
		admin.Use(auth.AdminToken(cfg.AdminAPIToken))
		admin.Post("/admin/v1/tenants", tenantHandler.Create)
		admin.Post("/admin/v1/tenants/{tenantID}/sources", sourceHandler.Create)
		// Raw event query + replay (Phase 4).
		admin.Get("/admin/v1/tenants/{tenantID}/events", rawHandler.List)
		admin.Get("/admin/v1/tenants/{tenantID}/events/{eventID}", rawHandler.Get)
		admin.Post("/admin/v1/tenants/{tenantID}/events/{eventID}/replay", rawHandler.ReplayOne)
		admin.Post("/admin/v1/tenants/{tenantID}/replay", rawHandler.ReplayByIdentifier)
		// Customer profile query (Phase 6).
		admin.Get("/admin/v1/tenants/{tenantID}/profiles", profileHandler.List)
		admin.Get("/admin/v1/tenants/{tenantID}/profiles/{canonicalUserID}", profileHandler.Get)
		// Segment management (Phase 7).
		admin.Post("/admin/v1/tenants/{tenantID}/segments", segmentHandler.Create)
		admin.Put("/admin/v1/tenants/{tenantID}/segments/{segmentID}", segmentHandler.Update)
		admin.Get("/admin/v1/tenants/{tenantID}/segments/{segmentID}", segmentHandler.Get)
		admin.Get("/admin/v1/tenants/{tenantID}/segments/{segmentID}/members", segmentHandler.Members)
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
