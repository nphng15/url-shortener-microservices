package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ikniz/url-shortener/shared/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.ServiceName)

	upstreams := map[string]string{
		"url-service":          cfg.URLServiceURL,
		"analytics-service":    cfg.AnalyticsServiceURL,
		"user-service":         cfg.UserServiceURL,
		"notification-service": cfg.NotificationServiceURL,
	}

	proxy := NewProxy(upstreams)
	limiter, err := NewRateLimiter(cfg.RedisURL)
	if err != nil {
		log.Error("rate limiter setup failed", "error", err)
		os.Exit(1)
	}
	defer limiter.Close()
	cb := NewCircuitBreaker(
		cfg.CircuitBreaker.MaxFailures,
		time.Duration(cfg.CircuitBreaker.OpenTimeoutSecs)*time.Second,
		time.Duration(cfg.CircuitBreaker.FailureWindowSecs)*time.Second,
	)
	handler := NewHandler(proxy, cfg, limiter, cb, log)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", NewHealthHandler(cfg.ServiceName))
	mux.Handle("GET /metrics", promhttp.Handler()) // Prometheus scrape endpoint
	mux.Handle("/", jwtMiddleware(cfg.JWTSecret, handler))

	app := logger.RequestLogger(log, correlationIDMiddleware(mux))
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: app}

	go func() {
		log.Info("server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutting down gateway")
	srv.Shutdown(context.Background())
}
