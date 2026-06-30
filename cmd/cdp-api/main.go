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
	"github.com/dinhphu28/osscdp/internal/config"
	"github.com/dinhphu28/osscdp/internal/platform/database"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
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

	r := httpx.NewRouter(base)
	httpx.Health(r, pool)

	// Admin API (static token guard; RBAC in Phase 9).
	r.Group(func(admin chi.Router) {
		admin.Use(auth.AdminToken(cfg.AdminAPIToken))
		admin.Post("/admin/v1/tenants", tenantHandler.Create)
		admin.Post("/admin/v1/tenants/{tenantID}/sources", sourceHandler.Create)
	})

	// Ingress auth probe (API-key guard).
	r.Group(func(ingress chi.Router) {
		ingress.Use(auth.APIKey(sourceSvc))
		ingress.Get("/v1/auth/whoami", whoami)
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
