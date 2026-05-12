package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/ikniz/url-shortener/shared/auth"
)

type Handler struct {
	proxy  *Proxy
	cfg    *Config
	authMw func(http.Handler) http.Handler
}

func NewHandler(proxy *Proxy, cfg *Config, authMw func(http.Handler) http.Handler) *Handler {
	return &Handler{
		proxy:  proxy,
		cfg:    cfg,
		authMw: authMw,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := matchRoute(r)
	if route == nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	if route.RequiresAuth {
		_, ok := h.authCheck(w, r)
		if !ok {
			return
		}
	}

	if route.RateLimitKey != "" {
		allowed, retryAfter, err := h.checkRateLimit(r, route.RateLimitKey)
		if err == nil && !allowed {
			w.Header().Set("Retry-After", formatInt(retryAfter))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	upstreamPath := r.URL.Path
	if route.StripPrefix != "" && strings.HasPrefix(upstreamPath, route.StripPrefix) {
		upstreamPath = upstreamPath[len(route.StripPrefix):]
	}

	newReq := r.WithContext(context.WithValue(r.Context(), upstreamPathKey{}, upstreamPath))
	h.proxy.ServeHTTP(w, newReq, route.Upstream)
}

func (h *Handler) checkRateLimit(r *http.Request, key string) (bool, int, error) {
	return true, 0, nil
}

func formatInt(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func (h *Handler) authCheck(w http.ResponseWriter, r *http.Request) (*auth.Claims, bool) {
	authHdr := r.Header.Get("Authorization")
	if authHdr == "" || !strings.HasPrefix(authHdr, "Bearer ") {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return nil, false
	}
	token := strings.TrimPrefix(authHdr, "Bearer ")
	if token == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return nil, false
	}
	claims, err := auth.VerifyToken(token, h.cfg.JWTSecret)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return nil, false
	}
	return claims, true
}