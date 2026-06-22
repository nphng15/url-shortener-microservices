package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	insertClickSQL = `
		INSERT INTO clicks (short_code, clicked_at, ip_hash, user_agent, referer)
		VALUES ($1, $2, $3, $4, $5)
	`
	countClicksByCodeSQL      = `SELECT COUNT(*) FROM clicks WHERE short_code = $1`
	countClicksByCodeSinceSQL = `SELECT COUNT(*) FROM clicks WHERE short_code = $1 AND clicked_at >= $2`
	topReferersSQL            = `
		SELECT referer, COUNT(*) AS cnt
		FROM clicks
		WHERE short_code = $1 AND referer IS NOT NULL
		GROUP BY referer
		ORDER BY cnt DESC
		LIMIT $2
	`
	timelineBucketsSQL = `
		SELECT date_trunc($1, clicked_at AT TIME ZONE 'UTC') AS period, COUNT(*) AS clicks
		FROM clicks
		WHERE short_code = $2
		GROUP BY period
		ORDER BY period ASC
	`
	milestoneExistsSQL = `SELECT EXISTS(SELECT 1 FROM milestones WHERE short_code = $1 AND milestone = $2)`
	insertMilestoneSQL = `
		INSERT INTO milestones (short_code, milestone)
		VALUES ($1, $2)
		ON CONFLICT (short_code, milestone) DO NOTHING
	`
	processedEventExistsSQL = `SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`
	insertProcessedEventSQL = `
		INSERT INTO processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
	`
)

type ClickRecord struct {
	ID        string
	ShortCode string
	ClickedAt time.Time
	IPHash    string
	UserAgent string
	Referer   string
}

type MilestoneRecord struct {
	ID          string
	ShortCode   string
	Milestone   int
	TriggeredAt time.Time
}

type StatsResult struct {
	ShortCode     string
	TotalClicks   int64
	ClicksLast24h int64
	ClicksLast7d  int64
	TopReferers   []RefererCount
}

type RefererCount struct {
	Referer string `json:"referer"`
	Count   int64  `json:"count"`
}

type TimeLinePoint struct {
	Period string `json:"period"`
	Clicks int64  `json:"clicks"`
}

type ClickRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, rec *ClickRecord) error
	CountByCode(ctx context.Context, shortCode string) (int64, error)
	CountByCodeSince(ctx context.Context, shortCode string, since time.Time) (int64, error)
	TopReferers(ctx context.Context, shortCode string, n int) ([]RefererCount, error)
	TimeLineBuckets(ctx context.Context, shortCode string, truncUnit string) ([]TimeLinePoint, error)
}

type MilestoneRepository interface {
	HasMilestone(ctx context.Context, tx pgx.Tx, shortCode string, milestone int) (bool, error)
	Insert(ctx context.Context, tx pgx.Tx, shortCode string, milestone int) error
}

type DeduplicationRepository interface {
	Exists(ctx context.Context, tx pgx.Tx, eventID string) (bool, error)
	Insert(ctx context.Context, tx pgx.Tx, eventID string) error
}

type pgxClickStore struct {
	pool *pgxpool.Pool
}

func NewClickStore(pool *pgxpool.Pool) ClickRepository {
	return &pgxClickStore{pool: pool}
}

func (s *pgxClickStore) Insert(ctx context.Context, tx pgx.Tx, rec *ClickRecord) error {
	_, err := tx.Exec(ctx, insertClickSQL,
		rec.ShortCode,
		rec.ClickedAt,
		rec.IPHash,
		rec.UserAgent,
		nullString(rec.Referer),
	)
	if err != nil {
		return fmt.Errorf("insert click: %w", err)
	}
	return nil
}

func (s *pgxClickStore) CountByCode(ctx context.Context, shortCode string) (int64, error) {
	return scanCount(ctx, s.pool, countClicksByCodeSQL, shortCode)
}

func (s *pgxClickStore) CountByCodeSince(ctx context.Context, shortCode string, since time.Time) (int64, error) {
	return scanCount(ctx, s.pool, countClicksByCodeSinceSQL, shortCode, since)
}

func (s *pgxClickStore) TopReferers(ctx context.Context, shortCode string, n int) ([]RefererCount, error) {
	rows, err := s.pool.Query(ctx, topReferersSQL, shortCode, n)
	if err != nil {
		return nil, fmt.Errorf("query top referers: %w", err)
	}
	defer rows.Close()

	return scanRefererCounts(rows)
}

func (s *pgxClickStore) TimeLineBuckets(ctx context.Context, shortCode string, truncUnit string) ([]TimeLinePoint, error) {
	rows, err := s.pool.Query(ctx, timelineBucketsSQL, truncUnit, shortCode)
	if err != nil {
		return nil, fmt.Errorf("query timeline buckets: %w", err)
	}
	defer rows.Close()

	return scanTimeLinePoints(rows)
}

type pgxMilestoneStore struct{}

func NewMilestoneStore() MilestoneRepository {
	return &pgxMilestoneStore{}
}

func (s *pgxMilestoneStore) HasMilestone(ctx context.Context, tx pgx.Tx, shortCode string, milestone int) (bool, error) {
	return scanExists(ctx, tx, milestoneExistsSQL, shortCode, milestone)
}

func (s *pgxMilestoneStore) Insert(ctx context.Context, tx pgx.Tx, shortCode string, milestone int) error {
	if _, err := tx.Exec(ctx, insertMilestoneSQL, shortCode, milestone); err != nil {
		return fmt.Errorf("insert milestone: %w", err)
	}
	return nil
}

type pgxDeduplicationStore struct{}

func NewDeduplicationStore() DeduplicationRepository {
	return &pgxDeduplicationStore{}
}

func (s *pgxDeduplicationStore) Exists(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	return scanExists(ctx, tx, processedEventExistsSQL, eventID)
}

func (s *pgxDeduplicationStore) Insert(ctx context.Context, tx pgx.Tx, eventID string) error {
	if _, err := tx.Exec(ctx, insertProcessedEventSQL, eventID); err != nil {
		return fmt.Errorf("insert processed event: %w", err)
	}
	return nil
}

func nullString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func scanCount(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (int64, error) {
	var count int64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("scan count: %w", err)
	}
	return count, nil
}

func scanExists(ctx context.Context, tx pgx.Tx, sql string, args ...any) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, sql, args...).Scan(&exists); err != nil {
		return false, fmt.Errorf("scan exists: %w", err)
	}
	return exists, nil
}

func scanRefererCounts(rows pgx.Rows) ([]RefererCount, error) {
	counts := []RefererCount{}
	for rows.Next() {
		var count RefererCount
		if err := rows.Scan(&count.Referer, &count.Count); err != nil {
			return nil, fmt.Errorf("scan top referer: %w", err)
		}
		counts = append(counts, count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate top referers: %w", err)
	}
	return counts, nil
}

func scanTimeLinePoints(rows pgx.Rows) ([]TimeLinePoint, error) {
	points := []TimeLinePoint{}
	for rows.Next() {
		var period time.Time
		var point TimeLinePoint
		if err := rows.Scan(&period, &point.Clicks); err != nil {
			return nil, fmt.Errorf("scan timeline bucket: %w", err)
		}
		point.Period = period.UTC().Format(time.RFC3339)
		points = append(points, point)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate timeline buckets: %w", err)
	}
	return points, nil
}
