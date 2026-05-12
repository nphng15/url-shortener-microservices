package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/ikniz/url-shortener/shared/auth"
)

type Handler struct {
	store  UserRepository
	hasher PasswordHasher
	issuer auth.TokenIssuer
	log    *slog.Logger
}

func NewHandler(store UserRepository, hasher PasswordHasher, issuer auth.TokenIssuer, log *slog.Logger) *Handler {
	return &Handler{
		store:  store,
		hasher: hasher,
		issuer: issuer,
		log:    log,
	}
}

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type registerResponse struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

type userInfoResponse struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "content-type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.log.Warn("malformed JSON request body", "path", r.URL.Path)
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := validateEmail(req.Email); err != nil {
		writeFieldError(w, http.StatusBadRequest, "invalid email format", "email")
		return
	}

	if err := validatePassword(req.Password); err != nil {
		writeFieldError(w, http.StatusBadRequest, "password must be at least 8 characters", "password")
		return
	}

	hash, err := h.hasher.Hash(req.Password)
	if err != nil {
		h.log.Error("bcrypt hash failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	user, err := h.store.Insert(r.Context(), req.Email, hash)
	if err != nil {
		if errors.Is(err, ErrDuplicateEmail) {
			h.log.Info("duplicate email registration attempt", "email", req.Email)
			writeError(w, http.StatusConflict, "email already registered")
			return
		}
		h.log.Error("store insert failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusCreated, registerResponse{
		UserID: user.ID,
		Email:  user.Email,
	})
}

func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "content-type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.log.Warn("malformed JSON request body", "path", r.URL.Path)
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	user, err := h.store.FindByEmail(r.Context(), req.Email)
	if user == nil {
		_ = h.hasher.Verify(req.Password, "$2a$12$invalidhashfortimingsafetyonlyxx")
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if err := h.hasher.Verify(req.Password, user.PasswordHash); err != nil {
		if errors.Is(err, ErrPasswordMismatch) {
			writeError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		h.log.Error("password verify failed", "error", err)
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	tokenStr, expiresAt, err := h.issuer.Issue(user.ID, user.Email)
	if err != nil {
		h.log.Error("token sign failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, loginResponse{
		Token:     tokenStr,
		ExpiresAt: expiresAt.Format("2006-01-02T15:04:05Z07:00"),
	})
}

func (h *Handler) Me(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	writeJSON(w, http.StatusOK, userInfoResponse{
		UserID: claims.Sub,
		Email:  claims.Email,
	})
}