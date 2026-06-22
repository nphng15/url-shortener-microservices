package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/ikniz/url-shortener/shared/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AnalyticsPublisher interface {
	PublishMilestone(ctx context.Context, evt *events.MilestoneReachedEvent) error
}

type amqpAnalyticsPublisher struct {
	ch           *amqp.Channel
	exchangeName string
	log          *slog.Logger
}

func NewAnalyticsPublisher(ch *amqp.Channel, log *slog.Logger) AnalyticsPublisher {
	return &amqpAnalyticsPublisher{ch: ch, exchangeName: analyticsExchange, log: log}
}

func (p *amqpAnalyticsPublisher) PublishMilestone(ctx context.Context, evt *events.MilestoneReachedEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal milestone event: %w", err)
	}

	err = p.ch.PublishWithContext(ctx, p.exchangeName, string(events.EventTypeMilestoneReached), false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish milestone event: %w", err)
	}
	p.log.Info("milestone event published", "short_code", evt.ShortCode, "milestone", evt.MilestoneN)
	return nil
}
