package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// URLRecord is the domain object mapped from the urls table.
// Never returned directly in HTTP responses — projection structs handle serialization.
type URLRecord struct {
	ID          string // UUID string
	ShortCode   string
	OriginalURL string
	UserID      string // UUID string
	UserEmail   string
	CreatedAt   time.Time
	ExpiresAt   *time.Time // nil if no expiry
	IsActive    bool
}

type URLStore interface {
	Insert(ctx context.Context, tx pgx.Tx, record *URLRecord) error
	FindByCode(ctx context.Context, shortCode string) (*URLRecord, error)
	FindByUserID(ctx context.Context, userID string, afterID string, limit int) ([]URLRecord, error)
	Deactivate(ctx context.Context, tx pgx.Tx, shortCode, userID string) error
}

type pgxURLStore struct {
	pool *pgxpool.Pool
}

func NewURLStore(pool *pgxpool.Pool) URLStore {
	return &pgxURLStore{pool: pool}
}

func (s *pgxURLStore) Insert(ctx context.Context, tx pgx.Tx, record *URLRecord) error {
	const query = `INSERT INTO urls (id, short_code, original_url, user_id, user_email, created_at, expires_at, is_active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err := tx.Exec(ctx, query, record.ID, record.ShortCode, record.OriginalURL, record.UserID, record.UserEmail, record.CreatedAt, record.ExpiresAt, record.IsActive)
	return err
}

func (s *pgxURLStore) FindByCode(ctx context.Context, shortCode string) (*URLRecord, error) {
	const query = `SELECT id, short_code, original_url, user_id, user_email, created_at, expires_at, is_active FROM urls
	WHERE short_code = $1 AND is_active = true AND (expires_at IS NULL OR expires_at > NOW())`
	var r URLRecord
	err := s.pool.QueryRow(ctx, query, shortCode).Scan(&r.ID, &r.ShortCode, &r.OriginalURL, &r.UserID, &r.UserEmail, &r.CreatedAt, &r.ExpiresAt, &r.IsActive)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

func (s *pgxURLStore) FindByUserID(ctx context.Context, userID string, afterID string, limit int) ([]URLRecord, error) {
	var query string
	var args []any

	// Fetch limit + 1 to determine if there is a next page
	fetchLimit := limit + 1

	if afterID != "" {
		query = `SELECT id, short_code, original_url, user_id, user_email, created_at, expires_at, is_active FROM urls 
		WHERE user_id = $1 AND id < $2 AND is_active = true AND (expires_at IS NULL OR expires_at > NOW()) 
		ORDER BY id DESC LIMIT $3`
		args = []any{userID, afterID, fetchLimit}
	} else {
		query = `SELECT id, short_code, original_url, user_id, user_email, created_at, expires_at, is_active FROM urls 
		WHERE user_id = $1 AND is_active = true AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY id DESC LIMIT $2`
		args = []any{userID, fetchLimit}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []URLRecord
	for rows.Next() {
		var r URLRecord
		if err := rows.Scan(&r.ID, &r.ShortCode, &r.OriginalURL, &r.UserID, &r.UserEmail, &r.CreatedAt, &r.ExpiresAt, &r.IsActive); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *pgxURLStore) Deactivate(ctx context.Context, tx pgx.Tx, shortCode, userID string) error {
	const query = `UPDATE urls SET is_active = false WHERE short_code = $1 AND user_id = $2 AND is_active = true`
	cmdTag, err := tx.Exec(ctx, query, shortCode, userID)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
