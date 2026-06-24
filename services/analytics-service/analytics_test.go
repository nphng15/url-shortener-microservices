package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ikniz/url-shortener/shared/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestClickConsumer_ParseMalformedJSONAcks(t *testing.T) {
	consumer := testClickConsumer()
	ack := &fakeAcknowledger{}
	delivery := amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: []byte(`{bad-json`)}

	if _, ok := consumer.parseDelivery(delivery); ok {
		t.Fatal("parseDelivery ok = true, want false")
	}
	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want acks=1 nacks=0", ack.acks, ack.nacks)
	}
}

func TestClickConsumer_ParseMissingEventIDAcks(t *testing.T) {
	consumer := testClickConsumer()
	ack := &fakeAcknowledger{}
	delivery := amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: []byte(`{"short_code":"abc123"}`)}

	if _, ok := consumer.parseDelivery(delivery); ok {
		t.Fatal("parseDelivery ok = true, want false")
	}
	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want acks=1 nacks=0", ack.acks, ack.nacks)
	}
}

func TestClickConsumer_ClickRecordUsesEventIPHash(t *testing.T) {
	evt := &events.URLClickedEvent{ShortCode: "abc123", IPHash: "already-hashed", UserAgent: "agent", Referer: "https://example.com"}

	rec := clickRecordFromEvent(evt)

	if rec.IPHash != "already-hashed" {
		t.Fatalf("IPHash = %q, want event IPHash", rec.IPHash)
	}
	if rec.ShortCode != evt.ShortCode || rec.UserAgent != evt.UserAgent || rec.Referer != evt.Referer {
		t.Fatalf("unexpected click record: %+v", rec)
	}
}

func TestStatsHandler_UnknownCodeReturnsZeros(t *testing.T) {
	store := &fakeClickStore{}
	handler := NewStatsHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodGet, "/stats/missing", nil)
	req.SetPathValue("code", "missing")
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ShortCode != "missing" || got.TotalClicks != 0 || got.ClicksLast24h != 0 || got.ClicksLast7d != 0 {
		t.Fatalf("unexpected stats response: %+v", got)
	}
	if got.TopReferers == nil || len(got.TopReferers) != 0 {
		t.Fatalf("top_referers = %#v, want empty slice", got.TopReferers)
	}
}

func TestStatsHandler_TopReferersLimitIsFive(t *testing.T) {
	store := &fakeClickStore{}
	handler := NewStatsHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodGet, "/stats/abc123", nil)
	req.SetPathValue("code", "abc123")
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if store.topReferersLimit != statsTopReferersLimit {
		t.Fatalf("top referers limit = %d, want %d", store.topReferersLimit, statsTopReferersLimit)
	}
}

func TestStatsHandler_StatsDBErrorReturns500(t *testing.T) {
	store := &fakeClickStore{countErr: errors.New("db down")}
	handler := NewStatsHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodGet, "/stats/abc123", nil)
	req.SetPathValue("code", "abc123")
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestStatsHandler_TimeLineInvalidIntervalsReturn400(t *testing.T) {
	for _, interval := range []string{"week", "month", "", "DAY", "Hour"} {
		t.Run(interval, func(t *testing.T) {
			store := &fakeClickStore{}
			handler := NewStatsHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
			req := httptest.NewRequest(http.MethodGet, "/stats/abc123/timeline?interval="+interval, nil)
			req.SetPathValue("code", "abc123")
			rec := httptest.NewRecorder()

			handler.TimeLine(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestStatsHandler_TimeLineValidIntervalsReturnEmptyPoints(t *testing.T) {
	for _, interval := range []string{"day", "hour"} {
		t.Run(interval, func(t *testing.T) {
			store := &fakeClickStore{}
			handler := NewStatsHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
			req := httptest.NewRequest(http.MethodGet, "/stats/abc123/timeline?interval="+interval, nil)
			req.SetPathValue("code", "abc123")
			rec := httptest.NewRecorder()

			handler.TimeLine(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			var got timeLineResponse
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got.ShortCode != "abc123" || got.Interval != interval {
				t.Fatalf("unexpected timeline response: %+v", got)
			}
			if got.Points == nil || len(got.Points) != 0 {
				t.Fatalf("points = %#v, want empty slice", got.Points)
			}
			if store.timelineInterval != interval {
				t.Fatalf("timeline interval = %q, want %q", store.timelineInterval, interval)
			}
		})
	}
}

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

type fakeClickStore struct {
	countErr         error
	topReferersLimit int
	timelineInterval string
}

func (s *fakeClickStore) Insert(ctx context.Context, tx pgx.Tx, rec *ClickRecord) error {
	return nil
}

func (s *fakeClickStore) CountByCode(ctx context.Context, shortCode string) (int64, error) {
	return 0, s.countErr
}

func (s *fakeClickStore) CountByCodeSince(ctx context.Context, shortCode string, since time.Time) (int64, error) {
	return 0, nil
}

func (s *fakeClickStore) TopReferers(ctx context.Context, shortCode string, n int) ([]RefererCount, error) {
	s.topReferersLimit = n
	return nil, nil
}

func (s *fakeClickStore) TimeLineBuckets(ctx context.Context, shortCode string, truncUnit string) ([]TimeLinePoint, error) {
	s.timelineInterval = truncUnit
	return nil, nil
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

func testClickConsumer() *ClickConsumer {
	return &ClickConsumer{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

type fakeAcknowledger struct {
	acks    int
	nacks   int
	rejects int
}

func (a *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	a.acks++
	return nil
}

func (a *fakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nacks++
	return nil
}

func (a *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	a.rejects++
	return nil
}
