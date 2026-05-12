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
	Port                   string
	ServiceName            string
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		URLServiceURL:          os.Getenv("URL_SERVICE_URL"),
		AnalyticsServiceURL:    os.Getenv("ANALYTICS_SERVICE_URL"),
		UserServiceURL:         os.Getenv("USER_SERVICE_URL"),
		NotificationServiceURL: os.Getenv("NOTIFICATION_SERVICE_URL"),
		RedisURL:               os.Getenv("REDIS_URL"),
		JWTSecret:              os.Getenv("JWT_SECRET"),
		Port:                   envOrDefault("PORT", "8080"),
		ServiceName:            "gateway",
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