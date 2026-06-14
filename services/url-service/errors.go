package main

import (
	"encoding/json"
	"errors"
	"net/http"
)

var (
	ErrNotFound      = errors.New("url not found")
	ErrInvalidURL    = errors.New("invalid URL")
	ErrAlreadyExists = errors.New("short code already exists")
	ErrForbidden     = errors.New("forbidden")
	ErrExpired       = errors.New("url has expired")
	ErrDeactivated   = errors.New("url has been deactivated")
	ErrDatabaseError = errors.New("database error")
	ErrCacheError    = errors.New("cache error")
)

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
