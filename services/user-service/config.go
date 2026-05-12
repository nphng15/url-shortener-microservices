package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL string
	JWTSecret   string
	BCryptCost  int
	TokenTTL    time.Duration
	Port        string
	ServiceName string
}

func loadConfig() (*Config, error) {
	bcryptCost := 12
	if v := os.Getenv("BCRYPT_COST"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 4 && n <= 12 {
			bcryptCost = n
		}
	}

	tokenTTL := 24 * time.Hour
	if v := os.Getenv("TOKEN_TTL_HOURS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			tokenTTL = time.Duration(n) * time.Hour
		}
	}

	cfg := &Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		JWTSecret:   os.Getenv("JWT_SECRET"),
		BCryptCost:  bcryptCost,
		TokenTTL:    tokenTTL,
		Port:        envOrDefault("PORT", "8080"),
		ServiceName: "user-service",
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
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