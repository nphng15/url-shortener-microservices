package main

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ikniz/url-shortener/shared/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
)

// --- Base62 Tests ---

func TestBase62RoundTrip(t *testing.T) {
	testCases := []int64{
		0,
		1,
		61,
		62,
		12345,
		3521614606207, // 62^7 - 1 (max representation for 7 base62 chars)
		3521614606208, // 62^7 (should wrap/mod to 0)
		9999999999999,
	}

	for _, val := range testCases {
		n := big.NewInt(val)
		encoded := Encode(n)
		if len(encoded) != shortCodeLength {
			t.Errorf("Encode(%d) length = %d, want %d", val, len(encoded), shortCodeLength)
		}

		decoded, err := Decode(encoded)
		if err != nil {
			t.Errorf("Decode(%q) error: %v", encoded, err)
			continue
		}

		// Since Encode does Mod(n, 62^7), the decoded value should be equal to val % 62^7.
		limit := new(big.Int).Exp(big.NewInt(62), big.NewInt(shortCodeLength), nil)
		expected := new(big.Int).Mod(n, limit)

		if decoded.Cmp(expected) != 0 {
			t.Errorf("Roundtrip failed for %d: encoded as %q, decoded as %s, want %s", val, encoded, decoded, expected)
		}
	}
}

func TestBase62DecodeErrors(t *testing.T) {
	invalidCodes := []string{
		"",
		"123456",    // too short
		"12345678",   // too long
		"123456?",    // invalid character
		"abc-xyz",    // invalid character
	}

	for _, code := range invalidCodes {
		_, err := Decode(code)
		if err == nil {
			t.Errorf("Decode(%q) expected error, got nil", code)
		}
	}
}

// --- Codegen Tests ---

func TestCodegen(t *testing.T) {
	generator := NewShortCodeGenerator()

	code1 := generator.Generate()
	if len(code1) != 7 {
		t.Errorf("Generate() length = %d, want 7", len(code1))
	}

	for _, char := range code1 {
		if strings.IndexByte(base62Alphabet, byte(char)) == -1 {
			t.Errorf("Generate() output %q contains invalid base62 char %q", code1, char)
		}
	}

	code2 := generator.Generate()
	if len(code2) != 7 {
		t.Errorf("Generate() length = %d, want 7", len(code2))
	}

	for _, char := range code2 {
		if strings.IndexByte(base62Alphabet, byte(char)) == -1 {
			t.Errorf("Generate() output %q contains invalid base62 char %q", code2, char)
		}
	}

	if code1 == code2 {
		t.Errorf("Generate() returned identical codes: %q and %q", code1, code2)
	}
}

// --- Cache Tests ---

func TestRedisCache(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	cache := NewRedisCache(client)
	ctx := context.Background()

	// Test Cache Miss
	missResult, err := cache.Get(ctx, "miss123")
	if err != nil {
		t.Errorf("expected no error on miss, got: %v", err)
	}
	if missResult != nil {
		t.Errorf("expected nil result on miss, got: %+v", missResult)
	}

	// Test Cache Hit
	expiry := time.Now().Add(1 * time.Hour).Truncate(time.Second) // truncate to avoid subsecond serialization differences
	cachedURL := &CachedURL{
		OriginalURL: "https://example.com/test",
		ExpiresAt:   &expiry,
		IsActive:    true,
	}

	err = cache.Set(ctx, "hitcode", cachedURL, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to set cache: %v", err)
	}

	hitResult, err := cache.Get(ctx, "hitcode")
	if err != nil {
		t.Errorf("expected no error on hit, got: %v", err)
	}
	if hitResult == nil {
		t.Fatal("expected cached url to be found, got nil")
	}

	if hitResult.OriginalURL != cachedURL.OriginalURL {
		t.Errorf("OriginalURL: got %q, want %q", hitResult.OriginalURL, cachedURL.OriginalURL)
	}
	if hitResult.IsActive != cachedURL.IsActive {
		t.Errorf("IsActive: got %t, want %t", hitResult.IsActive, cachedURL.IsActive)
	}
	if hitResult.ExpiresAt == nil || !hitResult.ExpiresAt.Equal(*cachedURL.ExpiresAt) {
		t.Errorf("ExpiresAt: got %v, want %v", hitResult.ExpiresAt, cachedURL.ExpiresAt)
	}

	// Test Delete
	err = cache.Delete(ctx, "hitcode")
	if err != nil {
		t.Errorf("expected no error on delete, got: %v", err)
	}
	deletedResult, err := cache.Get(ctx, "hitcode")
	if err != nil {
		t.Errorf("expected no error on check after delete, got: %v", err)
	}
	if deletedResult != nil {
		t.Errorf("expected nil after delete, got: %+v", deletedResult)
	}

	// Test Error Fallback
	badClient := redis.NewClient(&redis.Options{
		Addr: "localhost:9999", // non-existent redis port
	})
	badCache := NewRedisCache(badClient)

	fallbackResult, err := badCache.Get(ctx, "fallback")
	if err != nil {
		t.Errorf("expected Get on bad client to not return error (non-fatal), got: %v", err)
	}
	if fallbackResult != nil {
		t.Errorf("expected Get on bad client to return nil, got: %+v", fallbackResult)
	}
}

// --- HTTP Handler & Mock Definitions ---

type mockTx struct {
	pgx.Tx
}

func (m *mockTx) Commit(ctx context.Context) error {
	return nil
}

func (m *mockTx) Rollback(ctx context.Context) error {
	return nil
}

type mockPool struct{}

func (m *mockPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return &mockTx{}, nil
}

type mockURLStore struct {
	insertFn       func(ctx context.Context, tx pgx.Tx, record *URLRecord) error
	findByCodeFn   func(ctx context.Context, shortCode string) (*URLRecord, error)
	findByUserIDFn func(ctx context.Context, userID string, afterID string, limit int) ([]URLRecord, error)
	deactivateFn   func(ctx context.Context, tx pgx.Tx, shortCode, userID string) error
}

func (m *mockURLStore) Insert(ctx context.Context, tx pgx.Tx, record *URLRecord) error {
	return m.insertFn(ctx, tx, record)
}

func (m *mockURLStore) FindByCode(ctx context.Context, shortCode string) (*URLRecord, error) {
	return m.findByCodeFn(ctx, shortCode)
}

func (m *mockURLStore) FindByUserID(ctx context.Context, userID string, afterID string, limit int) ([]URLRecord, error) {
	return m.findByUserIDFn(ctx, userID, afterID, limit)
}

func (m *mockURLStore) Deactivate(ctx context.Context, tx pgx.Tx, shortCode, userID string) error {
	return m.deactivateFn(ctx, tx, shortCode, userID)
}

type mockOutboxStore struct {
	insertEventFn      func(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error
	fetchUnpublishedFn func(ctx context.Context, limit int) ([]*OutboxRecord, error)
	markPublishedFn    func(ctx context.Context, id string) error
}

func (m *mockOutboxStore) InsertEvent(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
	return m.insertEventFn(ctx, tx, outbox)
}

func (m *mockOutboxStore) FetchUnpublished(ctx context.Context, limit int) ([]*OutboxRecord, error) {
	return m.fetchUnpublishedFn(ctx, limit)
}

func (m *mockOutboxStore) MarkPublished(ctx context.Context, id string) error {
	return m.markPublishedFn(ctx, id)
}

type mockCache struct {
	getFn    func(ctx context.Context, code string) (*CachedURL, error)
	setFn    func(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error
	deleteFn func(ctx context.Context, code string) error
}

func (m *mockCache) Get(ctx context.Context, code string) (*CachedURL, error) {
	return m.getFn(ctx, code)
}

func (m *mockCache) Set(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error {
	return m.setFn(ctx, code, cached, ttl)
}

func (m *mockCache) Delete(ctx context.Context, code string) error {
	return m.deleteFn(ctx, code)
}

type mockGenerator struct {
	generateFn func() string
}

func (m *mockGenerator) Generate() string {
	return m.generateFn()
}

func TestHandlerShorten(t *testing.T) {
	claims := &auth.Claims{Sub: "user-123", Email: "user@example.com", Iss: "url-shortener"}
	ctx := context.WithValue(context.Background(), auth.TestClaimsKey{}, claims)

	t.Run("Success", func(t *testing.T) {
		var storeInserted, outboxInserted, cacheSet bool
		store := &mockURLStore{
			insertFn: func(ctx context.Context, tx pgx.Tx, record *URLRecord) error {
				storeInserted = true
				if record.ShortCode != "abc1234" {
					t.Errorf("unexpected short code: %s", record.ShortCode)
				}
				return nil
			},
		}
		outbox := &mockOutboxStore{
			insertEventFn: func(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
				outboxInserted = true
				return nil
			},
		}
		cache := &mockCache{
			setFn: func(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error {
				cacheSet = true
				return nil
			},
		}
		codegen := &mockGenerator{
			generateFn: func() string {
				return "abc1234"
			},
		}

		handler := NewHTTPHandler(&mockPool{}, store, outbox, cache, codegen, "http://localhost")

		body := `{"url":"https://example.com/test","expires_in_hours":12}`
		req := httptest.NewRequest("POST", "/shorten", strings.NewReader(body)).WithContext(ctx)
		rec := httptest.NewRecorder()

		handler.HandleShorten(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("status: got %d, want 201", rec.Code)
		}

		var resp ShortenResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.ShortCode != "abc1234" {
			t.Errorf("ShortCode: got %q, want %q", resp.ShortCode, "abc1234")
		}
		if resp.ShortURL != "http://localhost/abc1234" {
			t.Errorf("ShortURL: got %q", resp.ShortURL)
		}

		// Wait slightly to let async cache.Set run
		time.Sleep(10 * time.Millisecond)

		if !storeInserted {
			t.Error("store.Insert was not called")
		}
		if !outboxInserted {
			t.Error("outbox.InsertEvent was not called")
		}
		if !cacheSet {
			t.Error("cache.Set was not called")
		}
	})

	t.Run("Collision Retry Success", func(t *testing.T) {
		attempts := 0
		store := &mockURLStore{
			insertFn: func(ctx context.Context, tx pgx.Tx, record *URLRecord) error {
				attempts++
				if attempts == 1 {
					return &pgconn.PgError{Code: "23505"} // unique violation
				}
				return nil // success on second attempt
			},
		}
		outbox := &mockOutboxStore{
			insertEventFn: func(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
				return nil
			},
		}
		cache := &mockCache{
			setFn: func(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error {
				return nil
			},
		}
		codes := []string{"coll123", "succ123"}
		codegenIdx := 0
		codegen := &mockGenerator{
			generateFn: func() string {
				c := codes[codegenIdx]
				codegenIdx++
				return c
			},
		}

		handler := NewHTTPHandler(&mockPool{}, store, outbox, cache, codegen, "http://localhost")

		body := `{"url":"https://example.com/test","expires_in_hours":12}`
		req := httptest.NewRequest("POST", "/shorten", strings.NewReader(body)).WithContext(ctx)
		rec := httptest.NewRecorder()

		handler.HandleShorten(rec, req)

		if rec.Code != http.StatusCreated {
			t.Errorf("status: got %d, want 201", rec.Code)
		}

		var resp ShortenResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.ShortCode != "succ123" {
			t.Errorf("expected success code %q, got %q", "succ123", resp.ShortCode)
		}
		if attempts != 2 {
			t.Errorf("expected 2 store attempts, got %d", attempts)
		}
	})

	t.Run("Unauthorized", func(t *testing.T) {
		handler := NewHTTPHandler(&mockPool{}, &mockURLStore{}, &mockOutboxStore{}, &mockCache{}, &mockGenerator{}, "http://localhost")
		req := httptest.NewRequest("POST", "/shorten", strings.NewReader(`{"url":"https://example.com"}`)) // no auth context
		rec := httptest.NewRecorder()

		handler.HandleShorten(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status: got %d, want 401", rec.Code)
		}
	})

	t.Run("Invalid URL", func(t *testing.T) {
		handler := NewHTTPHandler(&mockPool{}, &mockURLStore{}, &mockOutboxStore{}, &mockCache{}, &mockGenerator{}, "http://localhost")
		req := httptest.NewRequest("POST", "/shorten", strings.NewReader(`{"url":"invalid-url-no-scheme"}`)).WithContext(ctx)
		rec := httptest.NewRecorder()

		handler.HandleShorten(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status: got %d, want 400", rec.Code)
		}
	})
}

func TestHandlerRedirect(t *testing.T) {
	t.Run("Cache Hit", func(t *testing.T) {
		var cacheGetCalled, storeFindCalled, outboxEventCalled bool
		expiry := time.Now().Add(1 * time.Hour)
		cache := &mockCache{
			getFn: func(ctx context.Context, code string) (*CachedURL, error) {
				cacheGetCalled = true
				return &CachedURL{
					OriginalURL: "https://cachehit.com",
					ExpiresAt:   &expiry,
					IsActive:    true,
				}, nil
			},
		}
		store := &mockURLStore{
			findByCodeFn: func(ctx context.Context, shortCode string) (*URLRecord, error) {
				storeFindCalled = true
				return nil, nil
			},
		}
		outbox := &mockOutboxStore{
			insertEventFn: func(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
				outboxEventCalled = true
				return nil
			},
		}

		handler := NewHTTPHandler(&mockPool{}, store, outbox, cache, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/abc1234", nil)
		req.SetPathValue("code", "abc1234")
		rec := httptest.NewRecorder()

		handler.HandleRedirect(rec, req)

		if rec.Code != http.StatusPermanentRedirect {
			t.Errorf("status: got %d, want 308", rec.Code)
		}
		if rec.Header().Get("Location") != "https://cachehit.com" {
			t.Errorf("Location header: got %q", rec.Header().Get("Location"))
		}
		if !cacheGetCalled {
			t.Error("cache.Get was not called")
		}
		if storeFindCalled {
			t.Error("store.FindByCode should NOT have been called on cache hit")
		}

		// Wait slightly to let async analytics outbox write run
		time.Sleep(10 * time.Millisecond)

		if !outboxEventCalled {
			t.Error("analytics outbox event was not inserted")
		}
	})

	t.Run("Cache Miss - DB Hit", func(t *testing.T) {
		var cacheGetCalled, storeFindCalled, cacheSetCalled, outboxEventCalled bool
		expiry := time.Now().Add(1 * time.Hour)
		cache := &mockCache{
			getFn: func(ctx context.Context, code string) (*CachedURL, error) {
				cacheGetCalled = true
				return nil, nil // miss
			},
			setFn: func(ctx context.Context, code string, cached *CachedURL, ttl time.Duration) error {
				cacheSetCalled = true
				return nil
			},
		}
		store := &mockURLStore{
			findByCodeFn: func(ctx context.Context, shortCode string) (*URLRecord, error) {
				storeFindCalled = true
				return &URLRecord{
					ID:          "uuid-url-123",
					ShortCode:   "abc1234",
					OriginalURL: "https://dbhit.com",
					ExpiresAt:   &expiry,
					IsActive:    true,
				}, nil
			},
		}
		outbox := &mockOutboxStore{
			insertEventFn: func(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
				outboxEventCalled = true
				return nil
			},
		}

		handler := NewHTTPHandler(&mockPool{}, store, outbox, cache, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/abc1234", nil)
		req.SetPathValue("code", "abc1234")
		rec := httptest.NewRecorder()

		handler.HandleRedirect(rec, req)

		if rec.Code != http.StatusPermanentRedirect {
			t.Errorf("status: got %d, want 308", rec.Code)
		}
		if rec.Header().Get("Location") != "https://dbhit.com" {
			t.Errorf("Location header: got %q", rec.Header().Get("Location"))
		}
		if !cacheGetCalled {
			t.Error("cache.Get was not called")
		}
		if !storeFindCalled {
			t.Error("store.FindByCode was not called on cache miss")
		}

		// Wait slightly to let async tasks run (cache set & analytics)
		time.Sleep(10 * time.Millisecond)

		if !cacheSetCalled {
			t.Error("cache.Set was not called asynchronously")
		}
		if !outboxEventCalled {
			t.Error("analytics outbox event was not inserted")
		}
	})

	t.Run("Deactivated URL", func(t *testing.T) {
		cache := &mockCache{
			getFn: func(ctx context.Context, code string) (*CachedURL, error) {
				return nil, nil // miss
			},
		}
		store := &mockURLStore{
			findByCodeFn: func(ctx context.Context, shortCode string) (*URLRecord, error) {
				return &URLRecord{
					ShortCode:   "abc1234",
					OriginalURL: "https://dbhit.com",
					IsActive:    false, // deactivated
				}, nil
			},
		}
		handler := NewHTTPHandler(&mockPool{}, store, &mockOutboxStore{}, cache, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/abc1234", nil)
		req.SetPathValue("code", "abc1234")
		rec := httptest.NewRecorder()

		handler.HandleRedirect(rec, req)

		if rec.Code != http.StatusGone {
			t.Errorf("status: got %d, want 410 (Gone)", rec.Code)
		}
	})

	t.Run("Expired URL", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Hour)
		cache := &mockCache{
			getFn: func(ctx context.Context, code string) (*CachedURL, error) {
				return nil, nil // miss
			},
		}
		store := &mockURLStore{
			findByCodeFn: func(ctx context.Context, shortCode string) (*URLRecord, error) {
				return &URLRecord{
					ShortCode:   "abc1234",
					OriginalURL: "https://dbhit.com",
					ExpiresAt:   &past, // expired
					IsActive:    true,
				}, nil
			},
		}
		handler := NewHTTPHandler(&mockPool{}, store, &mockOutboxStore{}, cache, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/abc1234", nil)
		req.SetPathValue("code", "abc1234")
		rec := httptest.NewRecorder()

		handler.HandleRedirect(rec, req)

		if rec.Code != http.StatusGone {
			t.Errorf("status: got %d, want 410 (Gone)", rec.Code)
		}
	})

	t.Run("Not Found", func(t *testing.T) {
		cache := &mockCache{
			getFn: func(ctx context.Context, code string) (*CachedURL, error) {
				return nil, nil // miss
			},
		}
		store := &mockURLStore{
			findByCodeFn: func(ctx context.Context, shortCode string) (*URLRecord, error) {
				return nil, pgx.ErrNoRows // not found in pgx
			},
		}
		handler := NewHTTPHandler(&mockPool{}, store, &mockOutboxStore{}, cache, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/abc1234", nil)
		req.SetPathValue("code", "abc1234")
		rec := httptest.NewRecorder()

		handler.HandleRedirect(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status: got %d, want 404 (Not Found)", rec.Code)
		}
	})
}

func TestHandlerList(t *testing.T) {
	claims := &auth.Claims{Sub: "user-123", Email: "user@example.com", Iss: "url-shortener"}
	ctx := context.WithValue(context.Background(), auth.TestClaimsKey{}, claims)

	t.Run("Success", func(t *testing.T) {
		store := &mockURLStore{
			findByUserIDFn: func(ctx context.Context, userID string, afterID string, limit int) ([]URLRecord, error) {
				if userID != "user-123" {
					t.Errorf("unexpected userID: %s", userID)
				}
				if limit != 20 {
					t.Errorf("unexpected limit requested: %d", limit)
				}
				return []URLRecord{
					{ID: "id1", ShortCode: "code1", OriginalURL: "https://google.com", UserID: "user-123"},
					{ID: "id2", ShortCode: "code2", OriginalURL: "https://yahoo.com", UserID: "user-123"},
				}, nil
			},
		}
		handler := NewHTTPHandler(&mockPool{}, store, &mockOutboxStore{}, &mockCache{}, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/urls?limit=20", nil).WithContext(ctx)
		rec := httptest.NewRecorder()

		handler.HandleGetUrls(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status: got %d, want 200", rec.Code)
		}

		var resp ListURLsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if len(resp.URLs) != 2 {
			t.Errorf("expected 2 urls, got %d", len(resp.URLs))
		}
		if resp.HasMore {
			t.Error("expected has_more to be false")
		}
		if resp.NextCursor != "" {
			t.Errorf("expected empty next_cursor, got %q", resp.NextCursor)
		}
	})

	t.Run("Pagination HasMore", func(t *testing.T) {
		store := &mockURLStore{
			findByUserIDFn: func(ctx context.Context, userID string, afterID string, limit int) ([]URLRecord, error) {
				return []URLRecord{
					{ID: "id1", ShortCode: "code1", OriginalURL: "https://google.com"},
					{ID: "id2", ShortCode: "code2", OriginalURL: "https://yahoo.com"},
					{ID: "id3", ShortCode: "code3", OriginalURL: "https://bing.com"}, // extra record
				}, nil
			},
		}
		handler := NewHTTPHandler(&mockPool{}, store, &mockOutboxStore{}, &mockCache{}, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("GET", "/urls?limit=2&after=id0", nil).WithContext(ctx)
		rec := httptest.NewRecorder()

		handler.HandleGetUrls(rec, req)

		var resp ListURLsResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)

		if len(resp.URLs) != 2 {
			t.Errorf("expected 2 urls, got %d (extra should be sliced off)", len(resp.URLs))
		}
		if !resp.HasMore {
			t.Error("expected has_more to be true")
		}
		if resp.NextCursor != "id2" {
			t.Errorf("expected next_cursor to be 'id2', got %q", resp.NextCursor)
		}
	})
}

func TestHandlerDelete(t *testing.T) {
	claims := &auth.Claims{Sub: "user-123", Email: "user@example.com", Iss: "url-shortener"}
	ctx := context.WithValue(context.Background(), auth.TestClaimsKey{}, claims)

	t.Run("Success", func(t *testing.T) {
		var storeDeactivated, outboxInserted, cacheDeleted bool
		store := &mockURLStore{
			deactivateFn: func(ctx context.Context, tx pgx.Tx, shortCode, userID string) error {
				storeDeactivated = true
				if shortCode != "del123" || userID != "user-123" {
					t.Errorf("unexpected shortCode %s or userID %s", shortCode, userID)
				}
				return nil
			},
		}
		outbox := &mockOutboxStore{
			insertEventFn: func(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
				outboxInserted = true
				return nil
			},
		}
		cache := &mockCache{
			deleteFn: func(ctx context.Context, code string) error {
				cacheDeleted = true
				if code != "del123" {
					t.Errorf("unexpected deleted code: %s", code)
				}
				return nil
			},
		}

		handler := NewHTTPHandler(&mockPool{}, store, outbox, cache, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("DELETE", "/urls/del123", nil).WithContext(ctx)
		req.SetPathValue("code", "del123")
		rec := httptest.NewRecorder()

		handler.HandleDeactivateUrl(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("status: got %d, want 204", rec.Code)
		}
		if !storeDeactivated {
			t.Error("store.Deactivate was not called")
		}
		if !outboxInserted {
			t.Error("outbox.InsertEvent was not called")
		}
		if !cacheDeleted {
			t.Error("cache.Delete was not called")
		}
	})

	t.Run("Forbidden (Not Owner or Already Inactive)", func(t *testing.T) {
		store := &mockURLStore{
			deactivateFn: func(ctx context.Context, tx pgx.Tx, shortCode, userID string) error {
				return pgx.ErrNoRows // simulate not found / not owner in pgx
			},
		}
		handler := NewHTTPHandler(&mockPool{}, store, &mockOutboxStore{}, &mockCache{}, &mockGenerator{}, "http://localhost")

		req := httptest.NewRequest("DELETE", "/urls/del123", nil).WithContext(ctx)
		req.SetPathValue("code", "del123")
		rec := httptest.NewRecorder()

		handler.HandleDeactivateUrl(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("status: got %d, want 403", rec.Code)
		}
	})
}
