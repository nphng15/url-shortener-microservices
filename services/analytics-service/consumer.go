package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/ikniz/url-shortener/shared/events"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ClickConsumer struct {
	conn           *RabbitMQConn
	pool           *pgxpool.Pool
	clickStore     ClickRepository
	milestoneStore MilestoneRepository
	dedupStore     DeduplicationRepository
	checker        *MilestoneChecker
	log            *slog.Logger
	salt           string
	healthy        atomic.Bool
}

func NewClickConsumer(conn *RabbitMQConn, pool *pgxpool.Pool, clickStore ClickRepository, milestoneStore MilestoneRepository, dedupStore DeduplicationRepository, checker *MilestoneChecker, log *slog.Logger, salt string) *ClickConsumer {
	return &ClickConsumer{
		conn:           conn,
		pool:           pool,
		clickStore:     clickStore,
		milestoneStore: milestoneStore,
		dedupStore:     dedupStore,
		checker:        checker,
		log:            log,
		salt:           salt,
	}
}

func (c *ClickConsumer) Run(ctx context.Context) {
	if err := c.conn.Channel.Qos(1, 0, false); err != nil {
		c.log.Error("set analytics consumer qos", "error", err)
		os.Exit(1)
	}

	deliveries, err := c.conn.Channel.Consume(analyticsQueue, "", false, false, false, false, nil)
	if err != nil {
		c.log.Error("start analytics consumer", "error", err)
		os.Exit(1)
	}

	c.healthy.Store(true)
	c.log.Info("analytics click consumer started", "queue", analyticsQueue)

	for {
		select {
		case <-ctx.Done():
			c.healthy.Store(false)
			c.log.Info("analytics click consumer stopped")
			return
		case delivery, ok := <-deliveries:
			if !ok {
				c.healthy.Store(false)
				c.log.Warn("analytics delivery channel closed")
				<-ctx.Done()
				return
			}
			c.processDelivery(ctx, delivery)
		}
	}
}

func (c *ClickConsumer) processDelivery(ctx context.Context, delivery amqp.Delivery) {
	started := time.Now()
	defer c.recoverDeliveryPanic(delivery)

	evt, ok := c.parseDelivery(delivery)
	if !ok {
		return
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		c.log.Error("begin analytics transaction", "event_id", evt.EventID, "error", err)
		nackRequeue(delivery, c.log)
		return
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	exists, err := c.dedupStore.Exists(ctx, tx, evt.EventID)
	if err != nil {
		c.log.Error("check duplicate click event", "event_id", evt.EventID, "error", err)
		nackRequeue(delivery, c.log)
		return
	}
	if exists {
		c.log.Info("duplicate click event discarded", "event_id", evt.EventID)
		ack(delivery, c.log)
		return
	}

	if err := c.dedupStore.Insert(ctx, tx, evt.EventID); err != nil {
		c.log.Error("insert processed click event", "event_id", evt.EventID, "error", err)
		nackRequeue(delivery, c.log)
		return
	}
	if err := c.clickStore.Insert(ctx, tx, clickRecordFromEvent(evt)); err != nil {
		c.log.Error("insert click", "event_id", evt.EventID, "short_code", evt.ShortCode, "error", err)
		nackRequeue(delivery, c.log)
		return
	}
	if err := c.checker.CheckAndPublish(ctx, tx, evt.ShortCode, evt.UserID, evt.UserEmail, evt.CorrelationID); err != nil {
		c.log.Error("check click milestone", "event_id", evt.EventID, "short_code", evt.ShortCode, "error", err)
		nackRequeue(delivery, c.log)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		c.log.Error("commit analytics transaction", "event_id", evt.EventID, "error", err)
		nackRequeue(delivery, c.log)
		return
	}
	committed = true

	ack(delivery, c.log)
	c.log.Info("click processed", "event_id", evt.EventID, "short_code", evt.ShortCode, "correlation_id", evt.CorrelationID, "duration_ms", time.Since(started).Milliseconds())
}

func (c *ClickConsumer) parseDelivery(delivery amqp.Delivery) (*events.URLClickedEvent, bool) {
	var evt events.URLClickedEvent
	if err := json.Unmarshal(delivery.Body, &evt); err != nil {
		c.log.Warn("invalid click event json", "body", truncate(string(delivery.Body), 200), "error", err)
		ack(delivery, c.log)
		return nil, false
	}
	if evt.EventID == "" || evt.ShortCode == "" {
		c.log.Warn("invalid click event", "event_id", evt.EventID, "short_code", evt.ShortCode)
		ack(delivery, c.log)
		return nil, false
	}
	return &evt, true
}

func (c *ClickConsumer) recoverDeliveryPanic(delivery amqp.Delivery) {
	if recovered := recover(); recovered != nil {
		c.log.Error("panic processing click event", "panic", recovered, "body", truncate(string(delivery.Body), 200))
		ack(delivery, c.log)
	}
}

func clickRecordFromEvent(evt *events.URLClickedEvent) *ClickRecord {
	return &ClickRecord{
		ShortCode: evt.ShortCode,
		ClickedAt: evt.ClickedAt,
		IPHash:    evt.IPHash,
		UserAgent: evt.UserAgent,
		Referer:   evt.Referer,
	}
}

func ack(delivery amqp.Delivery, log *slog.Logger) {
	if err := delivery.Ack(false); err != nil {
		log.Warn("ack analytics delivery", "error", err)
	}
}

func nackRequeue(delivery amqp.Delivery, log *slog.Logger) {
	if err := delivery.Nack(false, true); err != nil {
		log.Warn("nack analytics delivery", "error", err)
	}
}
