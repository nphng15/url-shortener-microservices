package main

import (
	"net/http"
	"os"

	"github.com/ikniz/url-shortener/shared/logger"
)

func main() {
	serviceName := "user-service"
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log := logger.New(serviceName)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", NewHealthHandler(serviceName))

	log.Info("server listening", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil && err != http.ErrServerClosed {
		log.Error("server error", "error", err)
		os.Exit(1)
	}
}
