package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ikniz/url-shortener/shared/auth"
	"github.com/ikniz/url-shortener/shared/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNotificationConsumer_UsesRoutingKeyAsEventType(t *testing.T) {
	consumer := testNotificationConsumer()
	ack := &fakeAcknowledger{}
	body := mustJSON(t, events.URLCreatedEvent{
		BaseEvent: events.NewBaseEvent(events.EventType("payload.event"), "corr-1"),
		ShortCode: "abc123",
		UserID:    "user-1",
		UserEmail: "user@example.com",
	})
	delivery := amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, RoutingKey: string(events.EventTypeURLCreated), Body: body}

	rec, eventID, ok := consumer.notificationFromDelivery(delivery)
	if !ok {
		t.Fatal("notificationFromDelivery ok = false, want true")
	}
	if eventID == "" {
		t.Fatal("eventID is empty")
	}
	if rec.EventType != string(events.EventTypeURLCreated) {
		t.Fatalf("EventType = %q, want routing key", rec.EventType)
	}
	if ack.acks != 0 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want no ack/nack before insert", ack.acks, ack.nacks)
	}
}

func TestNotificationConsumer_UnknownRoutingKeyAcksNoInsert(t *testing.T) {
	store := &fakeNotificationStore{}
	consumer := testNotificationConsumer()
	consumer.store = store
	ack := &fakeAcknowledger{}
	body := mustJSON(t, events.URLCreatedEvent{
		BaseEvent: events.NewBaseEvent(events.EventTypeURLCreated, "corr-1"),
		ShortCode: "abc123",
		UserID:    "user-1",
		UserEmail: "user@example.com",
	})
	delivery := amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, RoutingKey: "unknown.event", Body: body}

	consumer.processDelivery(context.Background(), delivery)

	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want acks=1 nacks=0", ack.acks, ack.nacks)
	}
	if store.insertCount != 0 {
		t.Fatalf("insert count = %d, want 0", store.insertCount)
	}
}

func TestNotificationConsumer_MilestoneEmptyUserIDAcksNoInsert(t *testing.T) {
	store := &fakeNotificationStore{}
	consumer := testNotificationConsumer()
	consumer.store = store
	ack := &fakeAcknowledger{}
	body := mustJSON(t, events.MilestoneReachedEvent{
		BaseEvent:   events.NewBaseEvent(events.EventTypeMilestoneReached, "corr-1"),
		ShortCode:   "abc123",
		MilestoneN:  10,
		TotalClicks: 10,
		UserID:      "",
		UserEmail:   "user@example.com",
	})
	delivery := amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, RoutingKey: string(events.EventTypeMilestoneReached), Body: body}

	consumer.processDelivery(context.Background(), delivery)

	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want acks=1 nacks=0", ack.acks, ack.nacks)
	}
	if store.insertCount != 0 {
		t.Fatalf("insert count = %d, want 0", store.insertCount)
	}
}

func TestNotificationHandler_InvalidAfterCursorReturns400(t *testing.T) {
	store := &fakeNotificationStore{}
	handler := NewNotificationHandler(store)
	req := authenticatedNotificationRequest("/notifications?after=not-a-uuid")
	rec := httptest.NewRecorder()

	handler.List(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if store.listCount != 0 {
		t.Fatalf("list count = %d, want 0", store.listCount)
	}
}

func TestNotificationHandler_EmptyNotificationsResponse(t *testing.T) {
	store := &fakeNotificationStore{notifications: nil, nextCursor: ""}
	handler := NewNotificationHandler(store)
	req := authenticatedNotificationRequest("/notifications")
	rec := httptest.NewRecorder()

	handler.List(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got notificationListResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Notifications == nil || len(got.Notifications) != 0 {
		t.Fatalf("notifications = %#v, want empty slice", got.Notifications)
	}
	if got.NextCursor != nil {
		t.Fatalf("next_cursor = %q, want nil", *got.NextCursor)
	}
}

func TestNotificationHandler_LimitDefaultAndMax(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantLimit int
	}{
		{name: "default", url: "/notifications", wantLimit: defaultNotificationLimit},
		{name: "max", url: "/notifications?limit=999", wantLimit: maxNotificationLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeNotificationStore{}
			handler := NewNotificationHandler(store)
			req := authenticatedNotificationRequest(tt.url)
			rec := httptest.NewRecorder()

			handler.List(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			if store.limit != tt.wantLimit {
				t.Fatalf("limit = %d, want %d", store.limit, tt.wantLimit)
			}
		})
	}
}

func testNotificationConsumer() *NotificationConsumer {
	return &NotificationConsumer{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return b
}

func authenticatedNotificationRequest(target string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	ctx := context.WithValue(req.Context(), auth.TestClaimsKey{}, &auth.Claims{Sub: "user-1"})
	return req.WithContext(ctx)
}

type fakeNotificationStore struct {
	insertCount   int
	inserted      *NotificationRecord
	insertErr     error
	listCount     int
	userID        string
	afterID       string
	limit         int
	notifications []Notification
	nextCursor    string
	listErr       error
}

func (s *fakeNotificationStore) InsertNotification(ctx context.Context, rec *NotificationRecord) (*Notification, error) {
	s.insertCount++
	s.inserted = rec
	if s.insertErr != nil {
		return nil, s.insertErr
	}
	return &Notification{ID: "notif-1", UserID: rec.UserID, EventType: rec.EventType, Payload: rec.Payload, Status: "sent", CreatedAt: time.Now().UTC()}, nil
}

func (s *fakeNotificationStore) ListByUser(ctx context.Context, userID, afterID string, limit int) ([]Notification, string, error) {
	s.listCount++
	s.userID = userID
	s.afterID = afterID
	s.limit = limit
	return s.notifications, s.nextCursor, s.listErr
}

type fakeAcknowledger struct {
	acks    int
	nacks   int
	rejects int
}

func (a *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	a.acks++
	return nil
}

func (a *fakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nacks++
	return nil
}

func (a *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	a.rejects++
	return nil
}
