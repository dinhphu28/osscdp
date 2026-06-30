// Command cdp-worker is the asynchronous processing worker.
//
// Phase 1 is a stub: it boots, establishes structured logging and config, and
// idles until interrupted. Kafka/Redpanda consumers arrive in Phase 3.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/dinhphu28/osscdp/internal/config"
	"github.com/dinhphu28/osscdp/internal/platform/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// No logger yet; write to stderr and exit.
		println("config error:", err.Error())
		os.Exit(1)
	}

	logger := logging.Component(logging.New(cfg.LogLevel), "cdp-worker")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("worker started", "note", "no consumers yet (Phase 3)")
	<-ctx.Done()
	logger.Info("worker shutting down")
}
