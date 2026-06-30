// Package logging provides a structured JSON logger and context helpers.
//
// Logs must never contain PII (raw email, phone, names) or secrets (API keys,
// destination credentials). Callers attach safe identifiers — tenant_id,
// source_id, request_id, component — via the helpers below.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
)

type ctxKey int

const holderKey ctxKey = iota

// holder is a request-scoped, mutable bag of log attributes. Storing a pointer
// in the context lets inner middleware/handlers add fields (e.g. tenant_id)
// that an outer middleware (the access log) can still observe afterwards.
type holder struct {
	mu    sync.Mutex
	attrs []slog.Attr
}

// New builds a JSON slog.Logger writing to stdout at the given level.
func New(level string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(h)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Component returns a logger tagged with the component name.
func Component(l *slog.Logger, name string) *slog.Logger {
	return l.With(slog.String("component", name))
}

// WithFields installs a request-scoped field holder on the context if one is not
// already present. Call this once per request (the access-log middleware does).
func WithFields(ctx context.Context) context.Context {
	if ctx.Value(holderKey) != nil {
		return ctx
	}
	return context.WithValue(ctx, holderKey, &holder{})
}

// AddFields appends log attributes to the request's field holder. Safe to call
// from any middleware/handler; a no-op if no holder is installed.
func AddFields(ctx context.Context, attrs ...slog.Attr) {
	h, _ := ctx.Value(holderKey).(*holder)
	if h == nil {
		return
	}
	h.mu.Lock()
	h.attrs = append(h.attrs, attrs...)
	h.mu.Unlock()
}

// FromContext returns the logger enriched with any attributes stored on ctx.
func FromContext(ctx context.Context, l *slog.Logger) *slog.Logger {
	h, _ := ctx.Value(holderKey).(*holder)
	if h == nil {
		return l
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.attrs) == 0 {
		return l
	}
	args := make([]any, 0, len(h.attrs))
	for _, a := range h.attrs {
		args = append(args, a)
	}
	return l.With(args...)
}
