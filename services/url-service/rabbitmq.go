package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName = "url-shortener"
	exchangeType = "topic"
)

// RabbitMQConn wraps an AMQP connection and channel.
type RabbitMQConn struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

// NewRabbitMQConn connects to RabbitMQ with exponential backoff.
// Retries up to maxAttempts times before returning error.
// Each attempt is logged. On success, declares the exchange.
//
// Parameters:
//
//	ctx         - for cancellation during retry loop
//	amqpURL     - amqp://user:pass@host:5672/
//	log         - structured logger
//	maxAttempts - typically 10 for startup; backoff doubles each retry (1s→2s→4s...max 30s)
//
// Returns:
//
//	*RabbitMQConn - connected and channel-open; caller owns Close()
//	error         - non-nil after maxAttempts exhausted
func NewRabbitMQConn(ctx context.Context, amqpURL string, log *slog.Logger, maxAttempts int) (*RabbitMQConn, error) {
	var conn *amqp.Connection
	var err error
	backoff := time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		log.Warn("rabbitmq connection attempt failed",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"backoff_seconds", backoff.Seconds(),
			"error", err,
		)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled during rabbitmq connect: %w", ctx.Err())
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("rabbitmq connect after %d attempts: %w", maxAttempts, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}
	if err := declareExchange(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}
	log.Info("connected to RabbitMQ", "exchange", exchangeName)
	return &RabbitMQConn{Conn: conn, Channel: ch}, nil
}
func declareExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // kind: "topic"
		true,         // durable
		false,        // autoDelete
		false,        // internal
		false,        // noWait
		nil,          // args
	)
}

// Close shuts down channel then connection in correct order.
func (r *RabbitMQConn) Close() {
	if r.Channel != nil {
		r.Channel.Close()
	}
	if r.Conn != nil {
		r.Conn.Close()
	}
}
