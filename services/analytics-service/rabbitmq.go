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
	analyticsExchange = "url-shortener"
	analyticsQueue    = "analytics.clicks"

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
			if err := declareAnalyticsExchange(mq.Channel); err != nil {
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

func declareAnalyticsExchange(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(analyticsExchange, "topic", true, false, false, false, nil); err != nil {
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

func DeclareAnalyticsQueue(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(analyticsQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare analytics queue: %w", err)
	}
	if err := ch.QueueBind(analyticsQueue, string(events.EventTypeURLClicked), analyticsExchange, false, nil); err != nil {
		return fmt.Errorf("bind analytics queue: %w", err)
	}
	return nil
}
