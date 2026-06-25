package main

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL       string
	RabbitMQURL       string
	JWTSecret         string
	Port              string
	ServiceName       string
	AMQPPrefetchCount int
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		RabbitMQURL:       os.Getenv("RABBITMQ_URL"),
		JWTSecret:         os.Getenv("JWT_SECRET"),
		Port:              envOrDefault("PORT", "8080"),
		ServiceName:       "notification-service",
		AMQPPrefetchCount: 1,
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RabbitMQURL == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is required")
	}
	if cfg.JWTSecret == "" {
		return nil, fmt.Errorf("JWT_SECRET is required")
	}
	return cfg, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
