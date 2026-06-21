package main

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL       string
	RabbitMQURL       string
	Port              string
	ServiceName       string
	IPHashSalt        string
	AMQPPrefetchCount int
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		RabbitMQURL:       os.Getenv("RABBITMQ_URL"),
		Port:              envOrDefault("PORT", "8080"),
		ServiceName:       "analytics-service",
		IPHashSalt:        os.Getenv("IP_HASH_SALT"),
		AMQPPrefetchCount: 1,
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RabbitMQURL == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is required")
	}
	return cfg, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
