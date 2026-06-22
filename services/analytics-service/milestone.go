package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ikniz/url-shortener/shared/events"
	"github.com/jackc/pgx/v5"
)

const (
	MilestoneThreshold10   = 10
	MilestoneThreshold100  = 100
	MilestoneThreshold1000 = 1000

	milestonePublishTimeout = 3 * time.Second
)

var MilestoneThresholds = []int{MilestoneThreshold10, MilestoneThreshold100, MilestoneThreshold1000}

type MilestoneChecker struct {
	clickStore     ClickRepository
	milestoneStore MilestoneRepository
	publisher      AnalyticsPublisher
	log            *slog.Logger
}

func NewMilestoneChecker(clickStore ClickRepository, milestoneStore MilestoneRepository, publisher AnalyticsPublisher, log *slog.Logger) *MilestoneChecker {
	return &MilestoneChecker{
		clickStore:     clickStore,
		milestoneStore: milestoneStore,
		publisher:      publisher,
		log:            log,
	}
}

func (c *MilestoneChecker) CheckAndPublish(ctx context.Context, tx pgx.Tx, shortCode, userID, userEmail, corrID string) error {
	totalClicks, err := countClicksInTransaction(ctx, tx, shortCode)
	if err != nil {
		return err
	}

	for _, threshold := range MilestoneThresholds {
		if totalClicks < int64(threshold) {
			continue
		}
		if err := c.publishIfNewMilestone(ctx, tx, shortCode, userID, userEmail, corrID, threshold, totalClicks); err != nil {
			return err
		}
	}
	return nil
}

func countClicksInTransaction(ctx context.Context, tx pgx.Tx, shortCode string) (int64, error) {
	var totalClicks int64
	if err := tx.QueryRow(ctx, countClicksByCodeSQL, shortCode).Scan(&totalClicks); err != nil {
		return 0, fmt.Errorf("count clicks for milestone: %w", err)
	}
	return totalClicks, nil
}

func (c *MilestoneChecker) publishIfNewMilestone(ctx context.Context, tx pgx.Tx, shortCode, userID, userEmail, corrID string, threshold int, totalClicks int64) error {
	exists, err := c.milestoneStore.HasMilestone(ctx, tx, shortCode, threshold)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	if err := c.milestoneStore.Insert(ctx, tx, shortCode, threshold); err != nil {
		return err
	}

	evt := newMilestoneReachedEvent(shortCode, userID, userEmail, corrID, threshold, totalClicks)
	publishCtx, cancel := context.WithTimeout(ctx, milestonePublishTimeout)
	defer cancel()

	if err := c.publisher.PublishMilestone(publishCtx, evt); err != nil {
		c.log.Warn("milestone publish failed", "short_code", shortCode, "milestone", threshold, "error", err)
	}
	return nil
}

func newMilestoneReachedEvent(shortCode, userID, userEmail, corrID string, threshold int, totalClicks int64) *events.MilestoneReachedEvent {
	return &events.MilestoneReachedEvent{
		BaseEvent:   events.NewBaseEvent(events.EventTypeMilestoneReached, corrID),
		ShortCode:   shortCode,
		UserID:      userID,
		UserEmail:   userEmail,
		MilestoneN:  threshold,
		TotalClicks: totalClicks,
	}
}
