package logger

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type correlationIDKey struct{}

// New creates a new structured JSON logger for the given service name.
func New(serviceName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	logger := slog.New(handler)

	// Add default attribute
	return logger.With(slog.String("service", serviceName))
}

func WithCorrelationID(log *slog.Logger, correlationID string) *slog.Logger {
	if correlationID == "" {
		return log
	}
	return log.With(slog.String("correlation_id", correlationID))
}

func ContextWithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, correlationID)
}

func CorrelationIDFromContext(ctx context.Context) string {
	correlationID, _ := ctx.Value(correlationIDKey{}).(string)
	return correlationID
}

func RequestLogger(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(recorder, r)

		correlationID := CorrelationIDFromContext(r.Context())
		if correlationID == "" {
			correlationID = r.Header.Get("X-Correlation-ID")
		}

		WithCorrelationID(log, correlationID).Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
