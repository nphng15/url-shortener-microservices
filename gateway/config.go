package main

import "os"

type Config struct {
	URLServiceURL          string
	AnalyticsServiceURL    string
	UserServiceURL         string
	NotificationServiceURL string
	RedisURL               string
	Port                   string
	ServiceName            string
}

func loadConfig() (*Config, error) {
	return &Config{
		URLServiceURL:          envOrDefault("URL_SERVICE_URL", "http://url-service:8080"),
		AnalyticsServiceURL:    envOrDefault("ANALYTICS_SERVICE_URL", "http://analytics-service:8080"),
		UserServiceURL:         envOrDefault("USER_SERVICE_URL", "http://user-service:8080"),
		NotificationServiceURL: envOrDefault("NOTIFICATION_SERVICE_URL", "http://notification-service:8080"),
		RedisURL:               envOrDefault("REDIS_URL", "redis://redis:6379/0"),
		Port:                   envOrDefault("PORT", "8080"),
		ServiceName:            "gateway",
	}, nil
}

func envOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
