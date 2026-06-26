package main

import (
	"context"
	_ "embed"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ikniz/url-shortener/shared/auth"
	"github.com/ikniz/url-shortener/shared/logger"
)

//go:embed migration.sql
var migrationSQL string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := LoadConfig()
	if err != nil {
		logger.New("url-service").Error("config error", "error", err)
		os.Exit(1)
	}

	log := logger.New(cfg.ServiceName)

	// Database (fatal on failure)
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dbCancel()
	pool, err := NewDBPool(dbCtx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Database Migration
	if _, err := pool.Exec(context.Background(), migrationSQL); err != nil {
		log.Error("failed to run database migrations", "error", err)
		os.Exit(1)
	}
	log.Info("database migrations applied successfully")

	// Redis
	redisClient, redisOK := NewRedisClient(context.Background(), cfg.RedisURL, log)
	defer redisClient.Close()
	if !redisOK {
		log.Warn("starting without Redis cache; cache will be unavailable until Redis recovers")
	}

	// RabbitMQ (fatal after max retries)
	rmqCtx, rmqCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer rmqCancel()
	rmqConn, err := NewRabbitMQConn(rmqCtx, cfg.RabbitMQURL, log, 10)
	if err != nil {
		log.Error("failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rmqConn.Close()

	urlStore := NewURLStore(pool)
	outboxStore := NewOutboxStore(pool)
	publisher := NewAMQPPublisher(rmqConn.Channel)
	outboxCoordinator := NewOutboxCoordinator(outboxStore, publisher, log)
	cache := NewRedisCache(redisClient)
	cgen := NewShortCodeGenerator()
	handler := NewHTTPHandler(pool, urlStore, outboxStore, cache, cgen, cfg.ShortURLBase)

	// HTTP mux
	authMw := auth.JWTMiddleware(cfg.JWTSecret)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", NewHealthHandler(cfg.ServiceName))
	mux.Handle("POST /shorten", authMw(http.HandlerFunc(handler.HandleShorten)))
	mux.HandleFunc("GET /{code}", handler.HandleRedirect)
	mux.HandleFunc("POST /shorten-anon", handler.HandleShortenAnon)
	mux.HandleFunc("GET /redirect-anon/{code}", handler.HandleRedirectAnon)
	mux.Handle("GET /urls", authMw(http.HandlerFunc(handler.HandleGetUrls)))
	mux.Handle("DELETE /urls/{code}", authMw(http.HandlerFunc(handler.HandleDeactivateUrl)))

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Info("server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()
	go outboxCoordinator.Run(ctx)

	<-quit
	cancel()
	log.Info("shutdown signal received, draining connections…")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}

	log.Info("server stopped cleanly")
}
