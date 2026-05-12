package main

import (
	"context"
	"fmt"
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
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.ServiceName)

	upstreams := map[string]string{
		"url-service":          cfg.URLServiceURL,
		"analytics-service":   cfg.AnalyticsServiceURL,
		"user-service":        cfg.UserServiceURL,
		"notification-service": cfg.NotificationServiceURL,
	}

	proxy := NewProxy(upstreams)
	authMw := auth.JWTMiddleware(cfg.JWTSecret)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", NewHealthHandler(cfg.ServiceName))
	mux.Handle("/", NewHandler(proxy, cfg, authMw))

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: mux}

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