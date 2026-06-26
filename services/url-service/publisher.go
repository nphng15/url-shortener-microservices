package main

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQPublisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

type amqpPublisher struct {
	ch *amqp.Channel
	mu sync.Mutex
}

func NewAMQPPublisher(ch *amqp.Channel) RabbitMQPublisher {
	return &amqpPublisher{ch: ch}
}

func (p *amqpPublisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ch.PublishWithContext(ctx,
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}
