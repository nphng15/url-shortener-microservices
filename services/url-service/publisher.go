package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQPublisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

type amqpPublisher struct {
	mu           sync.Mutex
	ch           *amqp.Channel
	exchangeName string
	log          *slog.Logger
}

func NewRabbitMQPublisher(ch *amqp.Channel, log *slog.Logger) RabbitMQPublisher {
	return &amqpPublisher{ch: ch, exchangeName: "url-shortener", log: log}
}

func (p *amqpPublisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.ch.PublishWithContext(ctx,
		p.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("amqp publish %s: %w", routingKey, err)
	}
	return nil
}
