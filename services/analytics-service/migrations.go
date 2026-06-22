package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migration.sql
var analyticsSchema string

func runMigrations(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if _, err := pool.Exec(ctx, analyticsSchema); err != nil {
		return fmt.Errorf("run analytics migrations: %w", err)
	}
	log.Info("analytics migrations applied")
	return nil
}
