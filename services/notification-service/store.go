package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	insertNotificationSQL = `
		INSERT INTO notifications (user_id, event_type, payload, status)
		VALUES ($1, $2, $3, 'pending')
		RETURNING id, created_at
	`
	markNotificationSentSQL = `
		UPDATE notifications
		SET status = 'sent', sent_at = now()
		WHERE id = $1
	`
	listNotificationsSQL = `
		SELECT id, user_id, event_type, payload, status, created_at, sent_at
		FROM notifications
		WHERE user_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT $2
	`
	listNotificationsAfterSQL = `
		SELECT id, user_id, event_type, payload, status, created_at, sent_at
		FROM notifications
		WHERE user_id = $1
		  AND (created_at, id) < (
			SELECT created_at, id FROM notifications WHERE id = $2 AND user_id = $1
		  )
		ORDER BY created_at DESC, id DESC
		LIMIT $3
	`
)

type Notification struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	SentAt    *time.Time      `json:"sent_at,omitempty"`
}

type NotificationRecord struct {
	UserID    string
	UserEmail string
	EventType string
	Payload   json.RawMessage
}

type NotificationRepository interface {
	InsertNotification(ctx context.Context, rec *NotificationRecord) (*Notification, error)
	ListByUser(ctx context.Context, userID, afterID string, limit int) ([]Notification, string, error)
}

type pgxNotificationStore struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewNotificationStore(pool *pgxpool.Pool, log *slog.Logger) NotificationRepository {
	return &pgxNotificationStore{pool: pool, log: log}
}

func (s *pgxNotificationStore) InsertNotification(ctx context.Context, rec *NotificationRecord) (*Notification, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin notification transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	notification := &Notification{
		UserID:    rec.UserID,
		EventType: rec.EventType,
		Payload:   rec.Payload,
		Status:    "pending",
	}
	if err := tx.QueryRow(ctx, insertNotificationSQL, rec.UserID, rec.EventType, rec.Payload).Scan(&notification.ID, &notification.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert notification: %w", err)
	}

	s.log.Info("mock email sent", "to", rec.UserEmail, "type", rec.EventType)

	if _, err := tx.Exec(ctx, markNotificationSentSQL, notification.ID); err != nil {
		return nil, fmt.Errorf("mark notification sent: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit notification transaction: %w", err)
	}
	committed = true

	now := time.Now().UTC()
	notification.Status = "sent"
	notification.SentAt = &now
	return notification, nil
}

func (s *pgxNotificationStore) ListByUser(ctx context.Context, userID, afterID string, limit int) ([]Notification, string, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := queryNotifications(ctx, s.pool, userID, afterID, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	notifications, err := scanNotifications(rows)
	if err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(notifications) > limit {
		notifications = notifications[:limit]
		nextCursor = notifications[len(notifications)-1].ID
	}
	return notifications, nextCursor, nil
}

func queryNotifications(ctx context.Context, pool *pgxpool.Pool, userID, afterID string, limit int) (pgx.Rows, error) {
	if afterID == "" {
		rows, err := pool.Query(ctx, listNotificationsSQL, userID, limit)
		if err != nil {
			return nil, fmt.Errorf("query notifications: %w", err)
		}
		return rows, nil
	}

	rows, err := pool.Query(ctx, listNotificationsAfterSQL, userID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("query notifications after cursor: %w", err)
	}
	return rows, nil
}

func scanNotifications(rows pgx.Rows) ([]Notification, error) {
	notifications := []Notification{}
	for rows.Next() {
		var notification Notification
		if err := rows.Scan(
			&notification.ID,
			&notification.UserID,
			&notification.EventType,
			&notification.Payload,
			&notification.Status,
			&notification.CreatedAt,
			&notification.SentAt,
		); err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		notifications = append(notifications, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notifications: %w", err)
	}
	return notifications, nil
}
