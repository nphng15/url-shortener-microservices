package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ikniz/url-shortener/shared/auth"
	"github.com/ikniz/url-shortener/shared/events"
)

type HTTPHandler struct {
	pool         pgxPool
	store        URLStore
	outboxStore  OutboxStore
	cache        Cache
	codegen      ShortCodeGenerator
	shortURLBase string

	urlService *URLService
}

func NewHTTPHandler(pool pgxPool, store URLStore, outboxStore OutboxStore, cache Cache, codegen ShortCodeGenerator, shortURLBase string) *HTTPHandler {
	return &HTTPHandler{
		pool:         pool,
		store:        store,
		outboxStore:  outboxStore,
		cache:        cache,
		codegen:      codegen,
		shortURLBase: shortURLBase,

		urlService: NewURLService(pool, store, outboxStore, cache, codegen, shortURLBase),
	}
}

func (h *HTTPHandler) HandleShorten(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not authenticated")
		return
	}
	userID := claims.Sub
	userEmail := claims.Email

	if userID == "" {
		writeError(w, http.StatusUnauthorized, "invalid user token")
		return
	}

	var req ShortenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	urlRecord, httpError := h.urlService.ShortenURL(r.Context(), req.URL, userID, userEmail, req.ExpiresInHours)
	if httpError != nil {
		writeError(w, httpError.Status, httpError.Err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, urlRecord)
}

func (h *HTTPHandler) HandleRedirect(w http.ResponseWriter, r *http.Request) {
	shortcode := r.PathValue("code")
	if shortcode == "" {
		writeError(w, http.StatusBadRequest, "missing short code")
		return
	}

	redirectInfo, httpError := h.urlService.RedirectToURL(r.Context(), shortcode, r.RemoteAddr)
	if httpError != nil {
		writeError(w, httpError.Status, httpError.Err.Error())
		return
	}

	go h.writeAnalyticsEvent(r, shortcode, redirectInfo.UserID, redirectInfo.UserEmail, redirectInfo.IpHash)

	http.Redirect(w, r, redirectInfo.OriginalURL, http.StatusPermanentRedirect)
}

func (h *HTTPHandler) HandleShortenAnon(w http.ResponseWriter, r *http.Request) {

	var req ShortenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	urlRecord, httpError := h.urlService.ShortenURL(r.Context(), req.URL, uuid.NewString(), uuid.NewString(), req.ExpiresInHours)
	if httpError != nil {
		writeError(w, httpError.Status, httpError.Err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, urlRecord)
}

func (h *HTTPHandler) HandleRedirectAnon(w http.ResponseWriter, r *http.Request) {
	shortcode := r.PathValue("code")
	if shortcode == "" {
		writeError(w, http.StatusBadRequest, "missing short code")
		return
	}

	redirectInfo, httpError := h.urlService.RedirectToURL(r.Context(), shortcode, r.RemoteAddr)
	if httpError != nil {
		writeError(w, httpError.Status, httpError.Err.Error())
		return
	}

	go h.writeAnalyticsEvent(r, shortcode, redirectInfo.UserID, redirectInfo.UserEmail, redirectInfo.IpHash)

	http.Redirect(w, r, redirectInfo.OriginalURL, http.StatusPermanentRedirect)
}

func (h *HTTPHandler) HandleGetUrls(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not authenticated")
		return
	}
	userID := claims.Sub

	var afterID string
	if val := r.URL.Query().Get("after"); val != "" {
		afterID = val
	}
	limit := 20
	if val := r.URL.Query().Get("limit"); val != "" {
		parsed, err := strconv.Atoi(val)
		if err == nil {
			limit = int(math.Max(math.Min(float64(parsed), 100), 1))
		}
	}

	urls, err := h.urlService.GetUserUrls(r.Context(), userID, afterID, limit)
	if err != nil {
		writeError(w, err.Status, err.Err.Error())
		return
	}

	writeJSON(w, http.StatusOK, urls)
}

func (h *HTTPHandler) HandleDeactivateUrl(w http.ResponseWriter, r *http.Request) {
	shortcode := r.PathValue("code")
	if shortcode == "" {
		writeError(w, http.StatusBadRequest, "missing short code")
		return
	}

	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not authenticated")
		return
	}
	userID := claims.Sub
	userEmail := claims.Email

	if userID == "" {
		writeError(w, http.StatusUnauthorized, "invalid user token")
		return
	}

	err := h.urlService.DeactivateURL(r.Context(), shortcode, userID, userEmail)
	if err != nil {
		writeError(w, err.Status, err.Err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- New Helper Method ---

func (h *HTTPHandler) writeAnalyticsEvent(r *http.Request, shortCode, userID, userEmail, ipHash string) {
	// Get User-Agent and Referer from request
	userAgent := r.Header.Get("User-Agent")
	referrer := r.Header.Get("Referer")

	// Create event (using the shared events package)
	event := events.URLClickedEvent{
		BaseEvent: events.NewBaseEvent(events.EventTypeURLClicked, ""),
		ShortCode: shortCode,
		UserID:    userID,
		UserEmail: userEmail,
		IPHash:    ipHash,
		UserAgent: userAgent,
		Referer:   referrer,
		ClickedAt: time.Now(),
	}

	payload, _ := json.Marshal(event)

	outbox := &OutboxRecord{
		ID:        uuid.NewString(),
		EventType: string(events.EventTypeURLClicked),
		Payload:   payload,
		CreatedAt: time.Now(),
	}

	_ = h.outboxStore.InsertEvent(context.Background(), nil, outbox)
}
