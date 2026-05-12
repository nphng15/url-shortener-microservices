package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateLimitConfig struct {
	Limit      int
	WindowSecs int
}

type RateLimiter struct {
	client *redis.Client
}

func NewRateLimiter(redisURL string) (*RateLimiter, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return &RateLimiter{client: redis.NewClient(opts)}, nil
}

func (rl *RateLimiter) Allow(ctx context.Context, key string, limit int, windowSecs int) (bool, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	fullKey := "rl:" + key

	count, err := rl.client.Incr(ctx, fullKey).Result()
	if err != nil {
		return true, 0, err
	}

	if count == 1 {
		if err := rl.client.Expire(ctx, fullKey, time.Duration(windowSecs)*time.Second).Err(); err != nil {
			return true, 0, err
		}
	}

	if count > int64(limit) {
		ttl, err := rl.client.TTL(ctx, fullKey).Result()
		if err != nil {
			return false, 0, nil
		}
		return false, int(ttl.Seconds()), nil
	}

	return true, 0, nil
}

func rateLimitKey(routeKey, ip string) string {
	return fmt.Sprintf("%s:%s", routeKey, ip)
}

func parseInt(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}