package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates a Redis client and pings the server.
// UNLIKE the DB pool, a failed ping here is NON-FATAL.
// The function always returns a *redis.Client (usable even if server is down).
// The bool return indicates whether the initial ping succeeded.
//
// Parameters:
//
//	ctx      - used for initial ping only
//	redisURL - redis://host:port/0 DSN
//	log      - structured logger
//
// Returns:
//
//	*redis.Client - always non-nil; configured client
//	bool          - true if initial ping succeeded, false otherwise
func NewRedisClient(ctx context.Context, redisURL string, log *slog.Logger) (*redis.Client, bool) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Warn("redis URL parse failed, cache disabled", "error", err)
		return redis.NewClient(&redis.Options{Addr: "localhost:6379"}), false
	}
	client := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		log.Warn("redis unreachable on startup, cache will be disabled until available", "error", err)
		return client, false
	}
	log.Info("connected to Redis cache", "addr", opts.Addr)
	return client, true
}
