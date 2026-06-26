package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ikniz/url-shortener/shared/logger"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	databaseStartupTimeout = 60 * time.Second
	rabbitMQStartupTimeout = 120 * time.Second
	shutdownTimeout        = 10 * time.Second
	rabbitMQAttempts       = 10
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		logger.New("analytics-service").Error("config error", "error", err)
		os.Exit(1)
	}

	log := logger.New(cfg.ServiceName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := connectDatabase(cfg, log)
	defer pool.Close()

	mqConn := connectRabbitMQ(cfg, log)
	defer mqConn.Close()

	clickStore := NewClickStore(pool)
	milestoneStore := NewMilestoneStore()
	dedupStore := NewDeduplicationStore()
	publisher := NewAnalyticsPublisher(mqConn.Channel, log)
	checker := NewMilestoneChecker(clickStore, milestoneStore, publisher, log)
	consumer := NewClickConsumer(mqConn, pool, clickStore, milestoneStore, dedupStore, checker, log)
	statsHandler := NewStatsHandler(clickStore, log)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", NewHealthHandler(cfg.ServiceName))
	mux.HandleFunc("GET /stats/{code}", statsHandler.Stats)
	mux.HandleFunc("GET /stats/{code}/timeline", statsHandler.TimeLine)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go consumer.Run(ctx)
	go startServer(srv, log, cfg.Port)

	waitForShutdown(cancel, log)
	shutdownServer(srv, log)
	log.Info("server stopped cleanly")
}

func connectDatabase(cfg *Config, log *slog.Logger) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), databaseStartupTimeout)
	defer cancel()

	pool, err := NewDBPool(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	if err := runMigrations(ctx, pool, log); err != nil {
		pool.Close()
		log.Error("failed to run database migrations", "error", err)
		os.Exit(1)
	}
	return pool
}

func connectRabbitMQ(cfg *Config, log *slog.Logger) *RabbitMQConn {
	ctx, cancel := context.WithTimeout(context.Background(), rabbitMQStartupTimeout)
	defer cancel()

	mqConn, err := NewRabbitMQConn(ctx, cfg.RabbitMQURL, log, rabbitMQAttempts)
	if err != nil {
		log.Error("failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	if err := DeclareAnalyticsQueue(mqConn.Channel); err != nil {
		mqConn.Close()
		log.Error("failed to declare analytics queue", "error", err)
		os.Exit(1)
	}
	return mqConn
}

func startServer(srv *http.Server, log *slog.Logger, port string) {
	log.Info("server listening", "port", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server error", "error", err)
		os.Exit(1)
	}
}

func waitForShutdown(cancel context.CancelFunc, log *slog.Logger) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	cancel()
	log.Info("shutdown signal received, draining connections")
}

func shutdownServer(srv *http.Server, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
}
