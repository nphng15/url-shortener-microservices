package logger

import (
	"log/slog"
	"os"
)

// New creates a new structured JSON logger for the given service name.
func New(serviceName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	logger := slog.New(handler)

	// Add default attribute
	return logger.With(slog.String("service", serviceName))
}
