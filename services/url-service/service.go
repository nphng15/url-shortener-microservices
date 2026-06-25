package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/ikniz/url-shortener/shared/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type HTTPError struct {
	Status int
	Err    error
}

func (e *HTTPError) Error() string {
	return e.Err.Error()
}

type RedirectInfo struct {
	OriginalURL string
	UserID      string
	UserEmail   string
	IpHash      string
}

type ShortenRequest struct {
	URL            string `json:"url"`
	ExpiresInHours int    `json:"expires_in_hours"`
}

type ShortenResponse struct {
	ShortCode   string    `json:"short_code"`
	ShortURL    string    `json:"short_url"`
	OriginalURL string    `json:"original_url"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type ListURLsResponse struct {
	URLs       []URLRecord `json:"urls"`
	NextCursor string      `json:"next_cursor"`
	HasMore    bool        `json:"has_more"`
}

type pgxPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type URLService struct {
	pool         pgxPool // Required to start database transactions
	store        URLStore
	outboxStore  OutboxStore
	cache        Cache
	cgen         ShortCodeGenerator
	shortURLBase string
}

func NewURLService(pool pgxPool, store URLStore, outboxStore OutboxStore, cache Cache, cgen ShortCodeGenerator, shortURLBase string) *URLService {
	return &URLService{
		pool:         pool,
		store:        store,
		outboxStore:  outboxStore,
		cache:        cache,
		cgen:         cgen,
		shortURLBase: shortURLBase,
	}
}

func (s *URLService) ShortenURL(ctx context.Context, url, userID, userEmail string, expiresInHours int) (ShortenResponse, *HTTPError) {

	if err := ValidateURL(url); err != nil {
		return ShortenResponse{}, &HTTPError{
			Status: http.StatusBadRequest,
			Err:    ErrInvalidURL,
		}
	}

	// Normalize expires_in
	if expiresInHours <= 0 || expiresInHours > 24*365 {
		expiresInHours = 24 // 1 day default
	}
	expiresAt := time.Now().Add(time.Duration(expiresInHours) * time.Hour)

	var shortCode string
	var success bool

	for attempt := 0; attempt < 3; attempt++ {
		shortCode = s.cgen.Generate()

		// 5. BEGIN tx -> Insert URL + Insert outbox -> COMMIT
		// pgx.BeginFunc automatically handles Rollback on error and Commit on success
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {

			// INSERT URL
			ur := &URLRecord{
				ID:          uuid.NewString(),
				ShortCode:   shortCode,
				OriginalURL: url,
				UserID:      userID,
				UserEmail:   userEmail,
				CreatedAt:   time.Now(),
				ExpiresAt:   &expiresAt,
				IsActive:    true,
			}
			if err := s.store.Insert(ctx, tx, ur); err != nil {
				return err
			}

			// INSERT OUTBOX
			event := events.URLCreatedEvent{
				BaseEvent:   events.NewBaseEvent(events.EventTypeURLCreated, ""),
				ShortCode:   shortCode,
				OriginalURL: url,
				UserID:      userID,
				UserEmail:   userEmail,
				ExpiresAt:   &expiresAt,
			}
			payload, _ := json.Marshal(event)

			outbox := &OutboxRecord{
				ID:        uuid.NewString(),
				EventType: string(events.EventTypeURLCreated),
				Payload:   payload,
				CreatedAt: time.Now(),
			}
			if err := s.outboxStore.InsertEvent(ctx, tx, outbox); err != nil {
				return err
			}

			return nil
		})

		if err == nil {
			success = true
			break
		}

		// If error is a unique constraint violation (collision)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // 23505 = UNIQUE_VIOLATION
			time.Sleep(time.Duration(attempt*50) * time.Millisecond) // exponential backoff
			continue
		}

		return ShortenResponse{}, &HTTPError{
			Status: http.StatusInternalServerError,
			Err:    ErrDatabaseError,
		}
	}

	if !success {
		return ShortenResponse{}, &HTTPError{
			Status: http.StatusConflict,
			Err:    ErrAlreadyExists,
		}
	}

	go func() {
		cached := &CachedURL{
			OriginalURL: url,
			UserID:      userID,
			UserEmail:   userEmail,
			ExpiresAt:   &expiresAt,
			IsActive:    true,
		}
		ttl := time.Duration(expiresInHours) * time.Hour
		_ = s.cache.Set(context.Background(), shortCode, cached, ttl)
	}()

	return ShortenResponse{
		ShortCode:   shortCode,
		ShortURL:    s.shortURLBase + "/" + shortCode,
		OriginalURL: url,
		ExpiresAt:   expiresAt,
	}, nil
}

func (s *URLService) RedirectToURL(ctx context.Context, shortCode string, remoteAddr string) (*RedirectInfo, *HTTPError) {
	// Check cache first (with a small timeout to avoid blocking)
	ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	cached, err := s.cache.Get(ctx, shortCode)
	if err == nil && cached != nil {
		if !cached.IsActive {
			return nil, &HTTPError{
				Status: http.StatusGone,
				Err:    ErrDeactivated,
			}
		}
		if cached.ExpiresAt != nil && time.Now().After(*cached.ExpiresAt) {
			return nil, &HTTPError{
				Status: http.StatusGone,
				Err:    ErrExpired,
			}
		}

		return &RedirectInfo{
			OriginalURL: cached.OriginalURL,
			UserID:      cached.UserID,
			UserEmail:   cached.UserEmail,
			IpHash:      hashIP(remoteAddr),
		}, nil
	}

	// Cache MISS or error → fetch from DB
	urlRecord, err := s.store.FindByCode(ctx, shortCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &HTTPError{
				Status: http.StatusNotFound,
				Err:    ErrNotFound,
			}
		} else {
			return nil, &HTTPError{
				Status: http.StatusInternalServerError,
				Err:    ErrDatabaseError,
			}
		}
	}

	if !urlRecord.IsActive {
		return nil, &HTTPError{
			Status: http.StatusGone,
			Err:    ErrDeactivated,
		}
	}
	if urlRecord.ExpiresAt != nil && time.Now().After(*urlRecord.ExpiresAt) {
		return nil, &HTTPError{
			Status: http.StatusGone,
			Err:    ErrExpired,
		}
	}

	// Cache the result for future requests (fire and forget)
	go func() {
		ttl := time.Hour // Default TTL if no expiry
		if urlRecord.ExpiresAt != nil {
			ttl = time.Until(*urlRecord.ExpiresAt)
			if ttl < 0 {
				ttl = 0
			}
		}

		cached := &CachedURL{
			OriginalURL: urlRecord.OriginalURL,
			UserID:      urlRecord.UserID,
			UserEmail:   urlRecord.UserEmail,
			ExpiresAt:   urlRecord.ExpiresAt,
			IsActive:    urlRecord.IsActive,
		}
		_ = s.cache.Set(context.Background(), shortCode, cached, ttl)
	}()

	return &RedirectInfo{
		OriginalURL: urlRecord.OriginalURL,
		UserID:      urlRecord.UserID,
		UserEmail:   urlRecord.UserEmail,
		IpHash:      hashIP(remoteAddr),
	}, nil
}

func (s *URLService) GetUserUrls(ctx context.Context, userID, afterID string, limit int) (*ListURLsResponse, *HTTPError) {
	urls, err := s.store.FindByUserID(ctx, userID, afterID, limit)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &HTTPError{
				Status: http.StatusOK,
				Err:    ErrNotFound,
			}
		}
		return nil, &HTTPError{
			Status: http.StatusInternalServerError,
			Err:    ErrDatabaseError,
		}
	}

	// Determine next cursor and hasMore
	var nextCursor string
	hasMore := len(urls) > limit

	if hasMore {
		// Slice off the extra record we fetched
		urls = urls[:limit]
		nextCursor = urls[len(urls)-1].ID
	}

	return &ListURLsResponse{
		URLs:       urls,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *URLService) DeactivateURL(ctx context.Context, shortCode, userID, userEmail string) *HTTPError {

	// 2. BEGIN tx -> Deactivate + Insert outbox -> COMMIT
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Deactivate URL (checks ownership implicitly via the WHERE clause)
		if err := s.store.Deactivate(ctx, tx, shortCode, userID); err != nil {
			return err
		}

		// Create the URLDeletedEvent
		event := events.URLDeletedEvent{
			BaseEvent: events.NewBaseEvent(events.EventTypeURLDeleted, ""),
			ShortCode: shortCode,
			UserID:    userID,
			UserEmail: userEmail,
		}
		payload, _ := json.Marshal(event)

		outbox := &OutboxRecord{
			ID:        uuid.NewString(),
			EventType: string(events.EventTypeURLDeleted),
			Payload:   payload,
			CreatedAt: time.Now(),
		}

		// Insert the event into the outbox in the same transaction
		return s.outboxStore.InsertEvent(ctx, tx, outbox)
	})

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// user_id didn't match, or shortcode doesn't exist/is already inactive -> 403
			return &HTTPError{
				Status: http.StatusForbidden,
				Err:    ErrForbidden,
			}
		} else {
			return &HTTPError{
				Status: http.StatusInternalServerError,
				Err:    ErrDatabaseError,
			}
		}
	}

	// Redis Delete
	_ = s.cache.Delete(context.Background(), shortCode)

	// Return 204 No Content
	return nil
}
