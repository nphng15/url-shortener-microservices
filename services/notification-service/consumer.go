package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/ikniz/url-shortener/shared/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

type NotificationConsumer struct {
	conn    *RabbitMQConn
	store   NotificationRepository
	log     *slog.Logger
	healthy atomic.Bool
}

func NewNotificationConsumer(conn *RabbitMQConn, store NotificationRepository, log *slog.Logger) *NotificationConsumer {
	return &NotificationConsumer{conn: conn, store: store, log: log}
}

func (c *NotificationConsumer) Run(ctx context.Context) {
	if err := c.conn.Channel.Qos(1, 0, false); err != nil {
		c.log.Error("set notification consumer qos", "error", err)
		os.Exit(1)
	}

	deliveries, err := c.conn.Channel.Consume(notificationQueue, "", false, false, false, false, nil)
	if err != nil {
		c.log.Error("start notification consumer", "error", err)
		os.Exit(1)
	}

	c.healthy.Store(true)
	c.log.Info("notification consumer started", "queue", notificationQueue)

	for {
		select {
		case <-ctx.Done():
			c.healthy.Store(false)
			c.log.Info("notification consumer stopped")
			return
		case delivery, ok := <-deliveries:
			if !ok {
				c.healthy.Store(false)
				c.log.Warn("notification delivery channel closed")
				<-ctx.Done()
				return
			}
			c.processDelivery(ctx, delivery)
		}
	}
}

func (c *NotificationConsumer) processDelivery(ctx context.Context, delivery amqp.Delivery) {
	started := time.Now()
	defer c.recoverDeliveryPanic(delivery)

	rec, eventID, ok := c.notificationFromDelivery(delivery)
	if !ok {
		return
	}

	if _, err := c.store.InsertNotification(ctx, rec); err != nil {
		c.log.Error("insert notification", "event_id", eventID, "event_type", rec.EventType, "error", err)
		nackRequeue(delivery, c.log)
		return
	}

	ack(delivery, c.log)
	c.log.Info("notification processed", "event_id", eventID, "event_type", rec.EventType, "duration_ms", time.Since(started).Milliseconds())
}

func (c *NotificationConsumer) notificationFromDelivery(delivery amqp.Delivery) (*NotificationRecord, string, bool) {
	eventType, eventID, err := parseBaseEvent(delivery.Body)
	if err != nil {
		c.log.Warn("invalid notification event", "body", truncate(string(delivery.Body), 200), "error", err)
		ack(delivery, c.log)
		return nil, "", false
	}

	var rec *NotificationRecord
	switch eventType {
	case string(events.EventTypeURLCreated):
		rec, err = notificationFromURLCreated(delivery.Body)
	case string(events.EventTypeURLDeleted):
		rec, err = notificationFromURLDeleted(delivery.Body)
	case string(events.EventTypeMilestoneReached):
		rec, err = notificationFromMilestoneReached(delivery.Body)
	default:
		err = fmt.Errorf("unsupported event type %q", eventType)
	}
	if err != nil {
		c.log.Warn("invalid notification payload", "event_id", eventID, "event_type", eventType, "error", err)
		ack(delivery, c.log)
		return nil, eventID, false
	}
	return rec, eventID, true
}

func parseBaseEvent(body []byte) (string, string, error) {
	var base events.BaseEvent
	if err := json.Unmarshal(body, &base); err != nil {
		return "", "", fmt.Errorf("parse base event: %w", err)
	}
	if base.EventID == "" || base.EventType == "" {
		return "", "", fmt.Errorf("event_id and event_type are required")
	}
	return base.EventType, base.EventID, nil
}

func notificationFromURLCreated(body []byte) (*NotificationRecord, error) {
	var evt events.URLCreatedEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil, fmt.Errorf("parse url created event: %w", err)
	}
	if evt.UserID == "" || evt.UserEmail == "" {
		return nil, fmt.Errorf("user_id and user_email are required")
	}
	return newNotificationRecord(evt.UserID, evt.UserEmail, evt.EventType, body), nil
}

func notificationFromURLDeleted(body []byte) (*NotificationRecord, error) {
	var evt events.URLDeletedEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil, fmt.Errorf("parse url deleted event: %w", err)
	}
	if evt.UserID == "" || evt.UserEmail == "" {
		return nil, fmt.Errorf("user_id and user_email are required")
	}
	return newNotificationRecord(evt.UserID, evt.UserEmail, evt.EventType, body), nil
}

func notificationFromMilestoneReached(body []byte) (*NotificationRecord, error) {
	var evt events.MilestoneReachedEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil, fmt.Errorf("parse milestone reached event: %w", err)
	}
	if evt.UserID == "" {
		return nil, fmt.Errorf("user_id is required")
	}
	return newNotificationRecord(evt.UserID, evt.UserEmail, evt.EventType, body), nil
}

func newNotificationRecord(userID, userEmail, eventType string, body []byte) *NotificationRecord {
	payload := make([]byte, len(body))
	copy(payload, body)
	return &NotificationRecord{UserID: userID, UserEmail: userEmail, EventType: eventType, Payload: payload}
}

func (c *NotificationConsumer) recoverDeliveryPanic(delivery amqp.Delivery) {
	if recovered := recover(); recovered != nil {
		c.log.Error("panic processing notification event", "panic", recovered, "body", truncate(string(delivery.Body), 200))
		ack(delivery, c.log)
	}
}

func ack(delivery amqp.Delivery, log *slog.Logger) {
	if err := delivery.Ack(false); err != nil {
		log.Warn("ack notification delivery", "error", err)
	}
}

func nackRequeue(delivery amqp.Delivery, log *slog.Logger) {
	if err := delivery.Nack(false, true); err != nil {
		log.Warn("nack notification delivery", "error", err)
	}
}
