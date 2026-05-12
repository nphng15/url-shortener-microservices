package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type OutboxCoordinator struct {
	store       OutboxRepository
	publisher   RabbitMQPublisher
	workerCh    chan *OutboxRecord
	log         *slog.Logger
	interval    time.Duration
	workerCount int
}

func NewOutboxCoordinator(
	store OutboxRepository,
	publisher RabbitMQPublisher,
	log *slog.Logger,
	interval time.Duration,
	workerCount int,
) *OutboxCoordinator {
	return &OutboxCoordinator{
		store:       store,
		publisher:   publisher,
		workerCh:    make(chan *OutboxRecord, 50),
		log:         log,
		interval:    interval,
		workerCount: workerCount,
	}
}

func (c *OutboxCoordinator) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.runWorker(ctx)
		}()
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(c.workerCh)
			wg.Wait()
			c.log.Info("outbox coordinator stopped")
			return
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

func (c *OutboxCoordinator) poll(ctx context.Context) {
	rows, err := c.store.FetchUnpublished(ctx, 50)
	if err != nil {
		c.log.Warn("outbox fetch failed", "error", err)
		return
	}
	for _, row := range rows {
		select {
		case c.workerCh <- row:
		case <-ctx.Done():
			return
		}
	}
}

func (c *OutboxCoordinator) runWorker(ctx context.Context) {
	for row := range c.workerCh {
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := c.publisher.Publish(publishCtx, row.EventType, row.Payload)
		cancel()
		if err != nil {
			c.log.Warn("outbox publish failed",
				"event_type", row.EventType,
				"outbox_id", row.ID,
				"error", err,
			)
			continue
		}
		if err := c.store.MarkPublished(context.Background(), row.ID); err != nil {
			c.log.Warn("outbox mark published failed",
				"outbox_id", row.ID,
				"error", err,
			)
		}
	}
}
