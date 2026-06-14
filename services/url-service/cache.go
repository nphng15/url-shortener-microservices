package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// CachedURL is the Redis-stored projection for the redirect path.
// Small enough to fit in a single Redis string value.
// Storing is_active avoids returning 301 for a deactivated URL cached before deletion.
type CachedURL struct {
	OriginalURL string     `json:"original_url"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	IsActive    bool       `json:"is_active"`
}

type Cache interface {
	Get(ctx context.Context, code string) (*CachedURL, error)
	Set(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error
	Delete(ctx context.Context, code string) error
}

type redisCache struct {
	client *redis.Client
}

func NewRedisCache(client *redis.Client) Cache {
	return &redisCache{client: client}
}

func (c *redisCache) Get(ctx context.Context, code string) (*CachedURL, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	data, err := c.client.Get(timeoutCtx, code).Result()
	if err != nil {
		return nil, nil
	}
	var cached CachedURL
	if err := json.Unmarshal([]byte(data), &cached); err != nil {
		return nil, nil
	}
	return &cached, nil
}

func (c *redisCache) Set(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error {
	data, err := json.Marshal(cached)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, code, data, ttl).Err()
}

func (c *redisCache) Delete(ctx context.Context, code string) error {
	return c.client.Del(ctx, code).Err()
}
