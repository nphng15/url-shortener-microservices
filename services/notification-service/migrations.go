package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migration.sql
var notificationSchema string

func runMigrations(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if _, err := pool.Exec(ctx, notificationSchema); err != nil {
		return fmt.Errorf("run notification migrations: %w", err)
	}
	log.Info("notification migrations applied")
	return nil
}
