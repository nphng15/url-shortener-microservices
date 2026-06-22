package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ikniz/url-shortener/shared/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMilestoneChecker_NoMilestoneBelow10(t *testing.T) {
	checker, tx, milestones, publisher := newTestMilestoneChecker(9)

	if err := checker.CheckAndPublish(context.Background(), tx, "abc123", "", "", "corr-1"); err != nil {
		t.Fatalf("CheckAndPublish returned error: %v", err)
	}
	if milestones.insertCount != 0 {
		t.Fatalf("insert count = %d, want 0", milestones.insertCount)
	}
	if len(publisher.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(publisher.events))
	}
}

func TestMilestoneChecker_Threshold10Triggered(t *testing.T) {
	checker, tx, milestones, publisher := newTestMilestoneChecker(10)

	if err := checker.CheckAndPublish(context.Background(), tx, "abc123", "user-1", "user@example.com", "corr-1"); err != nil {
		t.Fatalf("CheckAndPublish returned error: %v", err)
	}
	if milestones.inserted[10] != 1 {
		t.Fatalf("milestone 10 inserts = %d, want 1", milestones.inserted[10])
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.events))
	}

	evt := publisher.events[0]
	if evt.ShortCode != "abc123" || evt.UserID != "user-1" || evt.UserEmail != "user@example.com" {
		t.Fatalf("unexpected event payload: %+v", evt)
	}
	if evt.MilestoneN != 10 || evt.TotalClicks != 10 || evt.CorrelationID != "corr-1" {
		t.Fatalf("unexpected milestone event: %+v", evt)
	}
}

func TestMilestoneChecker_AlreadyRecorded_NoPublish(t *testing.T) {
	checker, tx, milestones, publisher := newTestMilestoneChecker(10)
	milestones.recorded[10] = true

	if err := checker.CheckAndPublish(context.Background(), tx, "abc123", "", "", "corr-1"); err != nil {
		t.Fatalf("CheckAndPublish returned error: %v", err)
	}
	if milestones.insertCount != 0 {
		t.Fatalf("insert count = %d, want 0", milestones.insertCount)
	}
	if len(publisher.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(publisher.events))
	}
}

func TestMilestoneChecker_PublishFailureContinues(t *testing.T) {
	checker, tx, milestones, publisher := newTestMilestoneChecker(10)
	publisher.err = errors.New("rabbitmq down")

	if err := checker.CheckAndPublish(context.Background(), tx, "abc123", "", "", "corr-1"); err != nil {
		t.Fatalf("CheckAndPublish returned error: %v", err)
	}
	if milestones.inserted[10] != 1 {
		t.Fatalf("milestone 10 inserts = %d, want 1", milestones.inserted[10])
	}
	if len(publisher.events) != 1 {
		t.Fatalf("publish attempts = %d, want 1", len(publisher.events))
	}
}

func TestMilestoneChecker_MultipleThresholdsAtOnce(t *testing.T) {
	checker, tx, milestones, publisher := newTestMilestoneChecker(1000)

	if err := checker.CheckAndPublish(context.Background(), tx, "abc123", "", "", "corr-1"); err != nil {
		t.Fatalf("CheckAndPublish returned error: %v", err)
	}
	if milestones.insertCount != 3 {
		t.Fatalf("insert count = %d, want 3", milestones.insertCount)
	}
	if len(publisher.events) != 3 {
		t.Fatalf("published events = %d, want 3", len(publisher.events))
	}
	for _, threshold := range []int{10, 100, 1000} {
		if milestones.inserted[threshold] != 1 {
			t.Fatalf("milestone %d inserts = %d, want 1", threshold, milestones.inserted[threshold])
		}
	}
}

func newTestMilestoneChecker(totalClicks int64) (*MilestoneChecker, *fakeTx, *fakeMilestoneStore, *fakePublisher) {
	milestones := &fakeMilestoneStore{recorded: map[int]bool{}, inserted: map[int]int{}}
	publisher := &fakePublisher{}
	checker := NewMilestoneChecker(nil, milestones, publisher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return checker, &fakeTx{count: totalClicks}, milestones, publisher
}

type fakeMilestoneStore struct {
	recorded    map[int]bool
	inserted    map[int]int
	insertCount int
}

func (s *fakeMilestoneStore) HasMilestone(ctx context.Context, tx pgx.Tx, shortCode string, milestone int) (bool, error) {
	return s.recorded[milestone], nil
}

func (s *fakeMilestoneStore) Insert(ctx context.Context, tx pgx.Tx, shortCode string, milestone int) error {
	s.insertCount++
	s.inserted[milestone]++
	s.recorded[milestone] = true
	return nil
}

type fakePublisher struct {
	events []*events.MilestoneReachedEvent
	err    error
}

func (p *fakePublisher) PublishMilestone(ctx context.Context, evt *events.MilestoneReachedEvent) error {
	p.events = append(p.events, evt)
	return p.err
}

type fakeTx struct {
	pgx.Tx
	count int64
}

func (tx *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return fakeRow{value: tx.count}
}

func (tx *fakeTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, errors.New("not implemented")
}
func (tx *fakeTx) Commit(ctx context.Context) error   { return nil }
func (tx *fakeTx) Rollback(ctx context.Context) error { return nil }
func (tx *fakeTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("not implemented")
}
func (tx *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (tx *fakeTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (tx *fakeTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("not implemented")
}
func (tx *fakeTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("not implemented")
}
func (tx *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, errors.New("not implemented")
}
func (tx *fakeTx) Conn() *pgx.Conn { return nil }

type fakeRow struct {
	value int64
}

func (r fakeRow) Scan(dest ...any) error {
	v, ok := dest[0].(*int64)
	if !ok {
		return errors.New("fakeRow expects *int64")
	}
	*v = r.value
	return nil
}
