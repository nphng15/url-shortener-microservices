package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

type Handler struct {
	proxy          *Proxy
	cfg            *Config
	rateLimiter    rateLimiter
	circuitBreaker *CircuitBreaker
	log            *slog.Logger
}

type rateLimiter interface {
	Allow(ctx context.Context, key string, limit int, windowSecs int) (bool, int, error)
}

func NewHandler(proxy *Proxy, cfg *Config, limiter rateLimiter, cb *CircuitBreaker, log *slog.Logger) *Handler {
	return &Handler{
		proxy:          proxy,
		cfg:            cfg,
		rateLimiter:    limiter,
		circuitBreaker: cb,
		log:            log,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := matchRoute(r)
	if route == nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	if route.RateLimitKey != "" {
		allowed, retryAfter, err := h.checkRateLimit(r, route.RateLimitKey)
		if err != nil {
			h.log.Warn("rate limiter failed open", "route", route.RateLimitKey, "error", err)
		} else if !allowed {
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
	if route.Upstream == "url-service" && h.circuitBreaker != nil {
		if err := h.circuitBreaker.Do(r.Context(), func() error {
			status, err := h.proxy.ServeHTTPStatus(w, newReq, route.Upstream)
			if err != nil {
				return err
			}
			if status >= http.StatusInternalServerError {
				return fmt.Errorf("upstream returned %d", status)
			}
			return nil
		}); err != nil {
			if err == ErrCircuitOpen {
				writeError(w, http.StatusServiceUnavailable, "url-service unavailable")
			}
			return
		}
		return
	}

	h.proxy.ServeHTTP(w, newReq, route.Upstream)
}

func (h *Handler) checkRateLimit(r *http.Request, key string) (bool, int, error) {
	if h.rateLimiter == nil {
		return true, 0, nil
	}
	cfg := h.cfg.RedirectRateLimit
	if key == "shorten" {
		cfg = h.cfg.ShortenRateLimit
	}
	return h.rateLimiter.Allow(r.Context(), rateLimitKey(key, clientIP(r)), cfg.Limit, cfg.WindowSecs)
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
