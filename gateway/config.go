package main

import (
	"fmt"
	"os"
)

type Config struct {
	URLServiceURL          string
	AnalyticsServiceURL    string
	UserServiceURL         string
	NotificationServiceURL string
	RedisURL               string
	JWTSecret              string
	ShortenRateLimit       RateLimitConfig
	RedirectRateLimit      RateLimitConfig
	CircuitBreaker         CircuitBreakerConfig
	Port                   string
	ServiceName            string
}

type CircuitBreakerConfig struct {
	MaxFailures       int
	OpenTimeoutSecs   int
	FailureWindowSecs int
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		URLServiceURL:          os.Getenv("URL_SERVICE_URL"),
		AnalyticsServiceURL:    os.Getenv("ANALYTICS_SERVICE_URL"),
		UserServiceURL:         os.Getenv("USER_SERVICE_URL"),
		NotificationServiceURL: os.Getenv("NOTIFICATION_SERVICE_URL"),
		RedisURL:               os.Getenv("REDIS_URL"),
		JWTSecret:              os.Getenv("JWT_SECRET"),
		ShortenRateLimit:       RateLimitConfig{Limit: parseInt(envOrDefault("SHORTEN_RATE_LIMIT", "10"), 10), WindowSecs: 60},
		RedirectRateLimit:      RateLimitConfig{Limit: parseInt(envOrDefault("REDIRECT_RATE_LIMIT", "300"), 300), WindowSecs: 60},
		CircuitBreaker: CircuitBreakerConfig{
			MaxFailures:       parseInt(envOrDefault("CB_MAX_FAILURES", "5"), 5),
			OpenTimeoutSecs:   parseInt(envOrDefault("CB_OPEN_TIMEOUT_SECS", "30"), 30),
			FailureWindowSecs: parseInt(envOrDefault("CB_FAILURE_WINDOW_SECS", "10"), 10),
		},
		Port:        envOrDefault("PORT", "8080"),
		ServiceName: "gateway",
	}

	required := map[string]string{
		"URL_SERVICE_URL":          cfg.URLServiceURL,
		"ANALYTICS_SERVICE_URL":    cfg.AnalyticsServiceURL,
		"USER_SERVICE_URL":         cfg.UserServiceURL,
		"NOTIFICATION_SERVICE_URL": cfg.NotificationServiceURL,
		"REDIS_URL":                cfg.RedisURL,
		"JWT_SECRET":               cfg.JWTSecret,
	}

	for k, v := range required {
		if v == "" {
			return nil, fmt.Errorf("%s is required", k)
		}
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
