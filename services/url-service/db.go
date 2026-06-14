package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewDBPool creates a pgxpool with project-standard settings.
// Returns fatal error if the pool cannot ping the DB within 10s.
//
// Parameters:
//
//	ctx        - a context with a 10-second timeout applied internally
//	databaseURL - postgres:// DSN from config
//	log        - structured logger for startup messages
//
// Returns:
//
//	*pgxpool.Pool - ready pool, caller must call .Close() on shutdown
//	error         - non-nil if DB unreachable; caller must os.Exit(1)
func NewDBPool(ctx context.Context, databaseURL string, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	log.Info("connected to DB", "max_conns", cfg.MaxConns, "min_conns", cfg.MinConns)
	return pool, nil
}
