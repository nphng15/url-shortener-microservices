package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	statsTopReferersLimit = 5
	oneDay                = 24 * time.Hour
)

type statsResponse struct {
	ShortCode     string         `json:"short_code"`
	TotalClicks   int64          `json:"total_clicks"`
	ClicksLast24h int64          `json:"clicks_last_24h"`
	ClicksLast7d  int64          `json:"clicks_last_7d"`
	TopReferers   []RefererCount `json:"top_referers"`
}

type timeLineResponse struct {
	ShortCode string          `json:"short_code"`
	Interval  string          `json:"interval"`
	Points    []TimeLinePoint `json:"points"`
}

type StatsHandler struct {
	clickStore ClickRepository
	log        *slog.Logger
}

func NewStatsHandler(clickStore ClickRepository, log *slog.Logger) *StatsHandler {
	return &StatsHandler{clickStore: clickStore, log: log}
}

func (h *StatsHandler) Stats(w http.ResponseWriter, r *http.Request) {
	shortCode := r.PathValue("code")
	if shortCode == "" {
		writeError(w, http.StatusBadRequest, "short_code is required")
		return
	}

	stats, err := h.loadStats(r.Context(), shortCode)
	if err != nil {
		h.log.Error("load stats", "short_code", shortCode, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load stats")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (h *StatsHandler) TimeLine(w http.ResponseWriter, r *http.Request) {
	shortCode := r.PathValue("code")
	if shortCode == "" {
		writeError(w, http.StatusBadRequest, "short_code is required")
		return
	}

	interval := r.URL.Query().Get("interval")
	if !isValidTimeLineInterval(interval) {
		writeError(w, http.StatusBadRequest, "interval must be day or hour")
		return
	}

	points, err := h.clickStore.TimeLineBuckets(r.Context(), shortCode, interval)
	if err != nil {
		h.log.Error("load timeline", "short_code", shortCode, "interval", interval, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load timeline")
		return
	}
	if points == nil {
		points = []TimeLinePoint{}
	}

	writeJSON(w, http.StatusOK, timeLineResponse{ShortCode: shortCode, Interval: interval, Points: points})
}

func (h *StatsHandler) loadStats(ctx context.Context, shortCode string) (statsResponse, error) {
	response := statsResponse{ShortCode: shortCode}
	group, ctx := errgroup.WithContext(ctx)
	now := time.Now().UTC()

	group.Go(func() error {
		count, err := h.clickStore.CountByCode(ctx, shortCode)
		response.TotalClicks = count
		return err
	})
	group.Go(func() error {
		count, err := h.clickStore.CountByCodeSince(ctx, shortCode, now.Add(-oneDay))
		response.ClicksLast24h = count
		return err
	})
	group.Go(func() error {
		count, err := h.clickStore.CountByCodeSince(ctx, shortCode, now.Add(-7*oneDay))
		response.ClicksLast7d = count
		return err
	})
	group.Go(func() error {
		referers, err := h.clickStore.TopReferers(ctx, shortCode, statsTopReferersLimit)
		response.TopReferers = referers
		return err
	})

	if err := group.Wait(); err != nil {
		return statsResponse{}, err
	}
	if response.TopReferers == nil {
		response.TopReferers = []RefererCount{}
	}
	return response, nil
}

func isValidTimeLineInterval(interval string) bool {
	return interval == "day" || interval == "hour"
}
