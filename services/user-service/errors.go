package main

import (
	"encoding/json"
	"errors"
	"net/http"
)

var (
	ErrDuplicateEmail    = errors.New("duplicate email")
	ErrUserNotFound      = errors.New("user not found")
	ErrPasswordMismatch  = errors.New("password mismatch")
	ErrTokenInvalid     = errors.New("invalid token")
	ErrInvalidEmail      = errors.New("invalid email")
	ErrInvalidPassword   = errors.New("invalid password")
)

type errorResponse struct {
	Error string `json:"error"`
}

type fieldErrorResponse struct {
	Error string `json:"error"`
	Field string `json:"field,omitempty"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errorResponse{Error: message})
}

func writeFieldError(w http.ResponseWriter, status int, message, field string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(fieldErrorResponse{Error: message, Field: field})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}