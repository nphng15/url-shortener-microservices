package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ikniz/url-shortener/shared/auth"
	"golang.org/x/crypto/bcrypt"
)

type mockStore struct {
	insertFn      func(ctx context.Context, email, hash string) (*User, error)
	findByEmailFn func(ctx context.Context, email string) (*User, error)
}

func (m *mockStore) Insert(ctx context.Context, email, hash string) (*User, error) {
	return m.insertFn(ctx, email, hash)
}

func (m *mockStore) FindByEmail(ctx context.Context, email string) (*User, error) {
	return m.findByEmailFn(ctx, email)
}

type mockIssuer struct {
	issueFn func(userID, email string) (string, time.Time, error)
	verifyFn func(tokenString string) (*auth.Claims, error)
}

func (m *mockIssuer) Issue(userID, email string) (string, time.Time, error) {
	return m.issueFn(userID, email)
}

func (m *mockIssuer) Verify(tokenString string) (*auth.Claims, error) {
	return m.verifyFn(tokenString)
}

func TestValidateEmail(t *testing.T) {
	cases := []struct {
		input   string
		wantErr bool
	}{
		{"user@example.com", false},
		{"user+tag@sub.domain.org", false},
		{"", true},
		{"notanemail", true},
		{"@nodomain.com", true},
		{"user@", true},
		{"user @example.com", true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			err := validateEmail(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateEmail(%q) err=%v, wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestValidatePassword(t *testing.T) {
	cases := []struct {
		input   string
		wantErr bool
	}{
		{"12345678", false},
		{"longerpassword", false},
		{"1234567", true},
		{"", true},
		{"        7", false},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			err := validatePassword(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("validatePassword(%q) err=%v, wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestBcryptHasher_HashAndVerify(t *testing.T) {
	h := NewPasswordHasher(bcrypt.MinCost)
	hash, err := h.Hash("mypassword")
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if hash == "" {
		t.Fatal("hash is empty")
	}
	if err := h.Verify("mypassword", hash); err != nil {
		t.Errorf("Verify correct password: %v", err)
	}
	if err := h.Verify("wrongpassword", hash); !errors.Is(err, ErrPasswordMismatch) {
		t.Errorf("Verify wrong password: got %v, want ErrPasswordMismatch", err)
	}
}

func TestBcryptHasher_DifferentHashesSamePassword(t *testing.T) {
	h := NewPasswordHasher(bcrypt.MinCost)
	hash1, _ := h.Hash("samepassword")
	hash2, _ := h.Hash("samepassword")
	if hash1 == hash2 {
		t.Error("bcrypt should produce different hashes (different salts)")
	}
	if err := h.Verify("samepassword", hash1); err != nil {
		t.Errorf("hash1 verify: %v", err)
	}
	if err := h.Verify("samepassword", hash2); err != nil {
		t.Errorf("hash2 verify: %v", err)
	}
}

func TestJWTTokenIssuer_IssueAndVerify(t *testing.T) {
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", 24*time.Hour)
	tokenStr, expiresAt, err := issuer.Issue("user-uuid-123", "user@example.com")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if tokenStr == "" {
		t.Fatal("token string is empty")
	}
	if time.Until(expiresAt) < 23*time.Hour {
		t.Errorf("expiresAt too soon: %v", expiresAt)
	}
	claims, err := issuer.Verify(tokenStr)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Sub != "user-uuid-123" {
		t.Errorf("sub: got %q", claims.Sub)
	}
	if claims.Email != "user@example.com" {
		t.Errorf("email: got %q", claims.Email)
	}
	if claims.Iss != "url-shortener" {
		t.Errorf("iss: got %q", claims.Iss)
	}
}

func TestJWTTokenIssuer_Verify_InvalidSignature(t *testing.T) {
	issuer1 := NewTokenIssuer("secret-one-32-chars-long-exactly", 24*time.Hour)
	issuer2 := NewTokenIssuer("secret-two-32-chars-long-exactly", 24*time.Hour)
	tok, _, _ := issuer1.Issue("uid", "u@e.com")
	_, err := issuer2.Verify(tok)
	if !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("wrong secret verify: got %v, want ErrTokenInvalid", err)
	}
}

func TestJWTTokenIssuer_Verify_Malformed(t *testing.T) {
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", 24*time.Hour)
	_, err := issuer.Verify("not.a.jwt")
	if !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("malformed token: got %v, want ErrTokenInvalid", err)
	}
}

func TestJWTTokenIssuer_Verify_Expired(t *testing.T) {
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", -1*time.Hour)
	tok, _, _ := issuer.Issue("uid", "u@e.com")
	_, err := issuer.Verify(tok)
	if !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("expired token verify: got %v, want ErrTokenInvalid", err)
	}
}

func TestRegisterHandler_ShortPassword(t *testing.T) {
	store := &mockStore{}
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), nil, slog.Default())
	body := `{"email":"test@example.com","password":"1234567"}`
	req := httptest.NewRequest("POST", "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Register(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", rec.Code)
	}
	var resp struct {
		Error string `json:"error"`
		Field string `json:"field"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Field != "password" {
		t.Errorf("field: got %q want password", resp.Field)
	}
}

func TestLoginHandler_UnknownEmail(t *testing.T) {
	store := &mockStore{
		findByEmailFn: func(_ context.Context, _ string) (*User, error) {
			return nil, ErrUserNotFound
		},
	}
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", time.Hour)
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), issuer, slog.Default())
	body := `{"email":"ghost@example.com","password":"anypassword"}`
	req := httptest.NewRequest("POST", "/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Login(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", rec.Code)
	}
	var resp struct {
		Error string `json:"error"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error != "invalid credentials" {
		t.Errorf("error: got %q want 'invalid credentials'", resp.Error)
	}
}

func TestLoginHandler_WrongPassword(t *testing.T) {
	hash, _ := NewPasswordHasher(bcrypt.MinCost).Hash("correctpassword")
	store := &mockStore{
		findByEmailFn: func(_ context.Context, _ string) (*User, error) {
			return &User{ID: "user-123", Email: "u@e.com", PasswordHash: hash}, nil
		},
	}
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", time.Hour)
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), issuer, slog.Default())
	body := `{"email":"user@example.com","password":"wrongpassword"}`
	req := httptest.NewRequest("POST", "/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Login(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", rec.Code)
	}
	var resp struct {
		Error string `json:"error"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error != "invalid credentials" {
		t.Errorf("error: got %q want 'invalid credentials'", resp.Error)
	}
}

func TestRegisterHandler_Success(t *testing.T) {
	store := &mockStore{
		insertFn: func(ctx context.Context, email, hash string) (*User, error) {
			return &User{ID: "test-uuid", Email: email, CreatedAt: time.Now()}, nil
		},
	}
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", time.Hour)
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), issuer, slog.Default())
	body := `{"email":"test@example.com","password":"securepass"}`
	req := httptest.NewRequest("POST", "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Register(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("status: got %d want 201, body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.UserID != "test-uuid" {
		t.Errorf("user_id: got %q", resp.UserID)
	}
	if strings.Contains(rec.Body.String(), "password") {
		t.Error("response body must not contain password or password_hash")
	}
}

func TestRegisterHandler_DuplicateEmail(t *testing.T) {
	store := &mockStore{
		insertFn: func(_ context.Context, _, _ string) (*User, error) {
			return nil, ErrDuplicateEmail
		},
	}
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), nil, slog.Default())
	body := `{"email":"dup@example.com","password":"password123"}`
	req := httptest.NewRequest("POST", "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Register(rec, req)
	if rec.Code != http.StatusConflict {
		t.Errorf("status: got %d want 409", rec.Code)
	}
}

func TestLoginHandler_Success(t *testing.T) {
	hash, _ := NewPasswordHasher(bcrypt.MinCost).Hash("correctpassword")
	store := &mockStore{
		findByEmailFn: func(_ context.Context, email string) (*User, error) {
			return &User{ID: "user-123", Email: email, PasswordHash: hash}, nil
		},
	}
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", time.Hour)
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), issuer, slog.Default())
	body := `{"email":"user@example.com","password":"correctpassword"}`
	req := httptest.NewRequest("POST", "/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Login(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d want 200, body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Token == "" {
		t.Error("token is empty")
	}
	if resp.ExpiresAt == "" {
		t.Error("expires_at is empty")
	}
}

func TestLoginHandler_InvalidCredentials(t *testing.T) {
	hash, _ := NewPasswordHasher(bcrypt.MinCost).Hash("correctpassword")
	store := &mockStore{
		findByEmailFn: func(_ context.Context, email string) (*User, error) {
			return &User{ID: "user-123", Email: email, PasswordHash: hash}, nil
		},
	}
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", time.Hour)
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), issuer, slog.Default())
	body := `{"email":"user@example.com","password":"wrongpassword"}`
	req := httptest.NewRequest("POST", "/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Login(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", rec.Code)
	}
}

func TestMeHandler_ValidToken(t *testing.T) {
	store := &mockStore{}
	issuer := NewTokenIssuer("test-secret-32-chars-long-exactly", time.Hour)
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), issuer, slog.Default())
	claims := &auth.Claims{Sub: "user-uuid", Email: "user@example.com", Iss: "url-shortener"}
	ctx := context.WithValue(context.Background(), auth.TestClaimsKey{}, claims)
	req := httptest.NewRequest("GET", "/me", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.Me(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d want 200", rec.Code)
	}
	var resp struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.UserID != "user-uuid" {
		t.Errorf("user_id: got %q", resp.UserID)
	}
}

func TestMeHandler_NoClaims(t *testing.T) {
	store := &mockStore{}
	h := NewHandler(store, NewPasswordHasher(bcrypt.MinCost), nil, slog.Default())
	req := httptest.NewRequest("GET", "/me", nil)
	rec := httptest.NewRecorder()
	h.Me(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", rec.Code)
	}
}