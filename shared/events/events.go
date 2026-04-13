package events

import (
	"time"

	"github.com/google/uuid"
)

// EventType defines the type of event.
type EventType string

const (
	EventTypeURLCreated       EventType = "url.created"
	EventTypeURLClicked       EventType = "url.clicked"
	EventTypeURLDeleted       EventType = "url.deleted"
	EventTypeMilestoneReached EventType = "milestone.reached"
)

// BaseEvent contains common fields for all events.
type BaseEvent struct {
	EventType     string    `json:"event_type"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id"`
	EventID       string    `json:"event_id"`
}

// NewBaseEvent creates a new BaseEvent with default values (UUID v4, UTC time).
func NewBaseEvent(eventType EventType, correlationID string) BaseEvent {
	return BaseEvent{
		EventType:     string(eventType),
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		EventID:       uuid.New().String(),
	}
}

// URLCreatedEvent is emitted when a new short URL is created.
type URLCreatedEvent struct {
	BaseEvent
	ShortCode   string     `json:"short_code"`
	OriginalURL string     `json:"original_url"`
	UserID      string     `json:"user_id"`
	UserEmail   string     `json:"user_email"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

// URLClickedEvent is emitted when a short URL is clicked.
type URLClickedEvent struct {
	BaseEvent
	ShortCode string    `json:"short_code"`
	IPHash    string    `json:"ip_hash"`
	UserAgent string    `json:"user_agent"`
	Referer   string    `json:"referer,omitempty"`
	ClickedAt time.Time `json:"clicked_at"`
}

// URLDeletedEvent is emitted when a short URL is deleted.
type URLDeletedEvent struct {
	BaseEvent
	ShortCode string `json:"short_code"`
	UserID    string `json:"user_id"`
	UserEmail string `json:"user_email"`
}

// MilestoneReachedEvent is emitted when a short URL reaches a click milestone.
type MilestoneReachedEvent struct {
	BaseEvent
	ShortCode   string `json:"short_code"`
	UserID      string `json:"user_id"`
	UserEmail   string `json:"user_email"`
	MilestoneN  int    `json:"milestone"`
	TotalClicks int64  `json:"total_clicks"`
}
