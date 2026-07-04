// Package database wires up the PostgreSQL connection pool.
package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// minMaxConns floors the pool size so the worker's many goroutines (relays,
// consumers, the activation runner, and the Phase-5 segment sweeper) plus the HTTP
// server don't contend on pgx's small default (max(4, numCPU)).
const minMaxConns = 20

// Connect builds a pgx pool from the connection URL and verifies connectivity.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse pgx config: %w", err)
	}
	// Floor the pool size. Note: this also raises an explicitly-configured smaller
	// pool_max_conns from the URL — the worker's goroutines need the headroom.
	if cfg.MaxConns < minMaxConns {
		cfg.MaxConns = minMaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}
