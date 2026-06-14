package main

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL  string // required; fatal if empty
	RedisURL     string // required; non-fatal if unreachable
	RabbitMQURL  string // required; retry with backoff
	JWTSecret    string // required; fatal if empty
	ShortURLBase string // e.g. "http://localhost:8080"; default fallback
	IPHashSalt   string // salt for SHA-256 IP hashing; default fallback
	Port         string // default "8080"
	ServiceName  string // constant "url-service"
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		RedisURL:     os.Getenv("REDIS_URL"),
		RabbitMQURL:  os.Getenv("RABBITMQ_URL"),
		JWTSecret:    os.Getenv("JWT_SECRET"),
		ShortURLBase: envOrDefault("SHORT_URL_BASE", "http://localhost:8080"),
		IPHashSalt:   envOrDefault("IP_HASH_SALT", "default-salt"),
		Port:         envOrDefault("PORT", "8080"),
		ServiceName:  "url-service",
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RabbitMQURL == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is required")
	}
	if cfg.RedisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required") // required env var; non-fatal only if *connection* fails
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
