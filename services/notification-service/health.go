package main

import (
	"encoding/json"
	"net/http"
)

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// NewHealthHandler returns an http.HandlerFunc for GET /health.
// No authentication required. Responds in < 10ms.
// Does NOT check DB connectivity on every request (too expensive).
func NewHealthHandler(serviceName string) http.HandlerFunc {
	resp := HealthResponse{Status: "ok", Service: serviceName}
	body, _ := json.Marshal(resp) // pre-encoded; never changes
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}
