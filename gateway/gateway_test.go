package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ikniz/url-shortener/shared/auth"
	"github.com/ikniz/url-shortener/shared/logger"
)

type fakeRateLimiter struct {
	allowed    bool
	retryAfter int
	err        error
	key        string
}

func (f *fakeRateLimiter) Allow(ctx context.Context, key string, limit int, windowSecs int) (bool, int, error) {
	f.key = key
	return f.allowed, f.retryAfter, f.err
}

func TestCircuitBreakerTransitions(t *testing.T) {
	cb := NewCircuitBreaker(2, time.Millisecond, time.Second)

	for i := 0; i < 2; i++ {
		if err := cb.Do(context.Background(), func() error { return errors.New("upstream failed") }); err == nil {
			t.Fatal("expected upstream error")
		}
	}
	if got := cb.State(); got != StateOpen {
		t.Fatalf("state after failures = %s, want OPEN", got)
	}

	if err := cb.Do(context.Background(), func() error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open circuit error = %v, want ErrCircuitOpen", err)
	}

	time.Sleep(2 * time.Millisecond)
	if err := cb.Do(context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("half-open success: %v", err)
	}
	if got := cb.State(); got != StateClosed {
		t.Fatalf("state after half-open success = %s, want CLOSED", got)
	}

	cb.state = StateHalfOpen
	if err := cb.Do(context.Background(), func() error { return errors.New("probe failed") }); err == nil {
		t.Fatal("expected half-open probe error")
	}
	if got := cb.State(); got != StateOpen {
		t.Fatalf("state after half-open failure = %s, want OPEN", got)
	}
}

func TestRateLimitRejectsAndFailOpen(t *testing.T) {
	cfg := testConfig()
	limiter := &fakeRateLimiter{allowed: false, retryAfter: 42}
	h := NewHandler(NewProxy(nil), cfg, limiter, nil, logger.New("test"))

	req := httptest.NewRequest(http.MethodPost, "/api/shorten", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "42" {
		t.Fatalf("Retry-After = %q, want 42", got)
	}
	if limiter.key != "shorten:192.0.2.10" {
		t.Fatalf("rate key = %q, want shorten:192.0.2.10", limiter.key)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	limiter = &fakeRateLimiter{allowed: true, err: errors.New("redis down")}
	h = NewHandler(NewProxy(map[string]string{"url-service": upstream.URL}), cfg, limiter, NewCircuitBreaker(5, time.Second, time.Second), logger.New("test"))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("fail-open status = %d, want 201", rr.Code)
	}
}

func TestRouterAndPathRewrite(t *testing.T) {
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := testConfig()
	h := NewHandler(
		NewProxy(map[string]string{"url-service": upstream.URL}),
		cfg,
		&fakeRateLimiter{allowed: true},
		NewCircuitBreaker(5, time.Second, time.Second),
		logger.New("test"),
	)

	req := httptest.NewRequest(http.MethodGet, "/r/abc1234", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if gotPath != "/abc1234" {
		t.Fatalf("upstream path = %q, want /abc1234", gotPath)
	}
}

func TestJWTMiddlewareProtectsPrivateRoutes(t *testing.T) {
	secret := "test-secret"
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok || claims.Sub != "user-1" {
			t.Fatalf("claims missing from context")
		}
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	rr := httptest.NewRecorder()
	jwtMiddleware(secret, next).ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+testToken(t, secret))
	rr = httptest.NewRecorder()
	jwtMiddleware(secret, next).ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("valid token status = %d, want 204", rr.Code)
	}
}

func TestJWTMiddlewareSkipsPublicRoutes(t *testing.T) {
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
	rr := httptest.NewRecorder()
	jwtMiddleware("secret", next).ServeHTTP(rr, req)

	if !nextCalled {
		t.Fatal("expected public route to call next")
	}
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
}

func testConfig() *Config {
	return &Config{
		ShortenRateLimit:  RateLimitConfig{Limit: 10, WindowSecs: 60},
		RedirectRateLimit: RateLimitConfig{Limit: 300, WindowSecs: 60},
		CircuitBreaker:    CircuitBreakerConfig{MaxFailures: 5, OpenTimeoutSecs: 30, FailureWindowSecs: 10},
	}
}

func testToken(t *testing.T, secret string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   "user-1",
		"email": "user@example.com",
		"iss":   "url-shortener",
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}
