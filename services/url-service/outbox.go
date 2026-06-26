package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	outboxBatchSize   = 50
	outboxWorkerCount = 3
	outboxPollEvery   = 2 * time.Second
)

type OutboxCoordinator struct {
	store     OutboxStore
	publisher RabbitMQPublisher
	log       *slog.Logger
}

// Coordinates reading unpublished outbox records from storage and
// publishing them to RabbitMQ, then marking them published.
func NewOutboxCoordinator(store OutboxStore, publisher RabbitMQPublisher, log *slog.Logger) *OutboxCoordinator {
	return &OutboxCoordinator{store: store, publisher: publisher, log: log}
}

func (c *OutboxCoordinator) Run(ctx context.Context) {
	jobs := make(chan *OutboxRecord, outboxBatchSize)
	var workers sync.WaitGroup

	for i := 0; i < outboxWorkerCount; i++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			c.worker(ctx, workerID, jobs)
		}(i + 1)
	}

	ticker := time.NewTicker(outboxPollEvery)
	defer ticker.Stop()
	defer func() {
		close(jobs)
		workers.Wait()
	}()

	for {
		c.poll(ctx, jobs)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *OutboxCoordinator) poll(ctx context.Context, jobs chan<- *OutboxRecord) {
	records, err := c.store.FetchUnpublished(ctx, outboxBatchSize)
	if err != nil {
		c.log.Warn("outbox poll failed", "error", err)
		return
	}

	for _, record := range records {
		select {
		case <-ctx.Done():
			return
		case jobs <- record:
		}
	}
}

func (c *OutboxCoordinator) worker(ctx context.Context, workerID int, jobs <-chan *OutboxRecord) {
	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-jobs:
			if !ok {
				return
			}
			c.publish(ctx, workerID, record)
		}
	}
}

func (c *OutboxCoordinator) publish(ctx context.Context, workerID int, record *OutboxRecord) {
	if err := c.publisher.Publish(ctx, record.EventType, record.Payload); err != nil {
		c.log.Warn("outbox publish failed", "worker", workerID, "outbox_id", record.ID, "event_type", record.EventType, "error", err)
		return
	}

	if err := c.store.MarkPublished(ctx, record.ID); err != nil {
		c.log.Warn("outbox mark published failed", "worker", workerID, "outbox_id", record.ID, "error", err)
	}
}
