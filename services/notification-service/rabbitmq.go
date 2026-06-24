package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ikniz/url-shortener/shared/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	notificationExchange = "url-shortener"
	notificationQueue    = "notifications.events"

	rabbitMQInitialBackoff = time.Second
	rabbitMQMaxBackoff     = 30 * time.Second
)

type RabbitMQConn struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

func NewRabbitMQConn(ctx context.Context, url string, log *slog.Logger, maxAttempts int) (*RabbitMQConn, error) {
	if url == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is required")
	}
	backoff := rabbitMQInitialBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		mq, err := dialRabbitMQ(url)
		if err == nil {
			if err := declareNotificationExchange(mq.Channel); err != nil {
				mq.Close()
				return nil, err
			}
			log.Info("rabbitmq connected", "attempt", attempt)
			return mq, nil
		}
		log.Warn("rabbitmq connect failed", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("rabbitmq connect cancelled: %w", ctx.Err())
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > rabbitMQMaxBackoff {
			backoff = rabbitMQMaxBackoff
		}
	}
	return nil, fmt.Errorf("rabbitmq unreachable after %d attempts", maxAttempts)
}

func dialRabbitMQ(url string) (*RabbitMQConn, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	return &RabbitMQConn{Conn: conn, Channel: ch}, nil
}

func declareNotificationExchange(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(notificationExchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	return nil
}

func (r *RabbitMQConn) Close() {
	if r == nil {
		return
	}
	if r.Channel != nil {
		_ = r.Channel.Close()
	}
	if r.Conn != nil {
		_ = r.Conn.Close()
	}
}

func DeclareNotificationQueue(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(notificationQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare notification queue: %w", err)
	}
	for _, routingKey := range notificationRoutingKeys() {
		if err := ch.QueueBind(notificationQueue, routingKey, notificationExchange, false, nil); err != nil {
			return fmt.Errorf("bind notification queue %s: %w", routingKey, err)
		}
	}
	return nil
}

func notificationRoutingKeys() []string {
	return []string{
		string(events.EventTypeURLCreated),
		string(events.EventTypeURLDeleted),
		string(events.EventTypeMilestoneReached),
	}
}
