package main

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/ikniz/url-shortener/shared/logger"
)

const correlationIDHeader = "X-Correlation-ID"

func correlationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := r.Header.Get(correlationIDHeader)
		if correlationID == "" {
			correlationID = newCorrelationID()
			r.Header.Set(correlationIDHeader, correlationID)
		}
		w.Header().Set(correlationIDHeader, correlationID)
		ctx := logger.ContextWithCorrelationID(r.Context(), correlationID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b[:])
}
