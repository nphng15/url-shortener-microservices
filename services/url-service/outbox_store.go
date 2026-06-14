package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRecord is the domain object mapped from the outbox table.
type OutboxRecord struct {
	ID          string // UUID string
	EventType   string
	Payload     []byte // raw JSONB bytes; sent as AMQP message body
	CreatedAt   time.Time
	PublishedAt *time.Time // nil if unpublished
}

type OutboxStore interface {
	InsertEvent(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error
	FetchUnpublished(ctx context.Context, limit int) ([]*OutboxRecord, error)
	MarkPublished(ctx context.Context, id string) error
}

type pgxOutboxStore struct {
	pool *pgxpool.Pool
}

func NewOutboxStore(pool *pgxpool.Pool) OutboxStore {
	return &pgxOutboxStore{pool: pool}
}

func (s *pgxOutboxStore) InsertEvent(ctx context.Context, tx pgx.Tx, outbox *OutboxRecord) error {
	const query = `INSERT INTO outbox (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)`
	if tx != nil {
		_, err := tx.Exec(ctx, query, outbox.ID, outbox.EventType, outbox.Payload, outbox.CreatedAt)
		return err
	}
	_, err := s.pool.Exec(ctx, query, outbox.ID, outbox.EventType, outbox.Payload, outbox.CreatedAt)
	return err
}

func (s *pgxOutboxStore) FetchUnpublished(ctx context.Context, limit int) ([]*OutboxRecord, error) {
	const query = `
		SELECT id, event_type, payload, created_at, published_at FROM outbox 
		WHERE published_at IS NULL 
		ORDER BY created_at ASC 
		LIMIT $1 
		FOR UPDATE SKIP LOCKED
	`
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []*OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.ID, &r.EventType, &r.Payload, &r.CreatedAt, &r.PublishedAt); err != nil {
			return nil, err
		}
		results = append(results, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *pgxOutboxStore) MarkPublished(ctx context.Context, id string) error {
	const query = `UPDATE outbox SET published_at = now() WHERE id = $1 AND published_at IS NULL`
	cmdTag, err := s.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
