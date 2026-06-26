package main

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/ikniz/url-shortener/shared/auth"
)

const (
	defaultNotificationLimit = 20
	maxNotificationLimit     = 100
)

var (
	errInvalidLimit  = errors.New("limit must be a positive integer")
	errInvalidCursor = errors.New("after must be a valid uuid")
)

type notificationListResponse struct {
	Notifications []Notification `json:"notifications"`
	NextCursor    *string        `json:"next_cursor"`
}

type NotificationHandler struct {
	store NotificationRepository
}

func NewNotificationHandler(store NotificationRepository) *NotificationHandler {
	return &NotificationHandler{store: store}
}

func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok || claims.Sub == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	limit, err := parseNotificationLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	afterID, err := parseNotificationCursor(r.URL.Query().Get("after"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	notifications, nextCursor, err := h.store.ListByUser(r.Context(), claims.Sub, afterID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list notifications")
		return
	}
	if notifications == nil {
		notifications = []Notification{}
	}

	writeJSON(w, http.StatusOK, notificationListResponse{
		Notifications: notifications,
		NextCursor:    nullableCursor(nextCursor),
	})
}

func parseNotificationLimit(raw string) (int, error) {
	if raw == "" {
		return defaultNotificationLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, errInvalidLimit
	}
	if limit > maxNotificationLimit {
		return maxNotificationLimit, nil
	}
	return limit, nil
}

func parseNotificationCursor(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if _, err := uuid.Parse(raw); err != nil {
		return "", errInvalidCursor
	}
	return raw, nil
}

func nullableCursor(cursor string) *string {
	if cursor == "" {
		return nil
	}
	return &cursor
}
