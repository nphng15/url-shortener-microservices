package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ikniz/url-shortener/shared/auth"
	"github.com/ikniz/url-shortener/shared/logger"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	log := logger.New(cfg.ServiceName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := NewDBPool(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("db pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := runMigrations(ctx, pool, log); err != nil {
		log.Error("migrations", "error", err)
		os.Exit(1)
	}

	store := NewUserStore(pool)
	hasher := NewPasswordHasher(cfg.BCryptCost)
	issuer := NewTokenIssuer(cfg.JWTSecret, cfg.TokenTTL)
	handler := NewHandler(store, hasher, issuer, log)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", handler.Register)
	mux.HandleFunc("POST /login", handler.Login)
	mux.Handle("GET /me", auth.JWTMiddleware(cfg.JWTSecret)(http.HandlerFunc(handler.Me)))
	mux.HandleFunc("GET /health", NewHealthHandler(cfg.ServiceName))

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		cancel()
		srv.Shutdown(ctx)
	}()

	log.Info("server listening", "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server error", "error", err)
		os.Exit(1)
	}
}