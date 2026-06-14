package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ikniz/url-shortener/shared/events"
	"github.com/jackc/pgx/v5/pgxpool"
)

type HTTPHandler struct {
	pool         *pgxpool.Pool // Required to start database transactions
	store        URLStore
	outboxStore  OutboxStore // Required for the outbox
	cache        Cache
	codegen      ShortCodeGenerator
	shortURLBase string

	urlService *URLService
}

func NewHTTPHandler(pool *pgxpool.Pool, store URLStore, outboxStore OutboxStore, cache Cache, codegen ShortCodeGenerator, shortURLBase string) *HTTPHandler {
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

// --- The Handler ---

func (h *HTTPHandler) HandleShorten(w http.ResponseWriter, r *http.Request) {
	// 3. Extract user claims
	// (Assuming your JWT middleware stores a map of claims in context)
	claims, ok := r.Context().Value("claims").(map[string]any)
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not authenticated")
		return
	}
	userID, _ := claims["sub"].(string)
	userEmail, _ := claims["email"].(string)

	if userID == "" {
		writeError(w, http.StatusUnauthorized, "invalid user token")
		return
	}

	// 1. Parse JSON body
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

	// 7. Return 201 Created
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

	go h.writeAnalyticsEvent(r, shortcode, redirectInfo.IpHash)

	// Redirect
	http.Redirect(w, r, redirectInfo.OriginalURL, http.StatusPermanentRedirect)
}

func (h *HTTPHandler) HandleShortenAnon(w http.ResponseWriter, r *http.Request) {

	// 1. Parse JSON body
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

	// 7. Return 201 Created
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

	go h.writeAnalyticsEvent(r, shortcode, redirectInfo.IpHash)

	// Redirect
	http.Redirect(w, r, redirectInfo.OriginalURL, http.StatusPermanentRedirect)
}

func (h *HTTPHandler) HandleGetUrls(w http.ResponseWriter, r *http.Request) {
	// JWT required, extract user_id
	claims, ok := r.Context().Value("claims").(map[string]any)
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not authenticated")
		return
	}
	userID, _ := claims["sub"].(string)

	// Parse query params
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

	// 1. JWT required, extract user_id & email
	claims, ok := r.Context().Value("claims").(map[string]any)
	if !ok {
		writeError(w, http.StatusUnauthorized, "user not authenticated")
		return
	}
	userID, _ := claims["sub"].(string)
	userEmail, _ := claims["email"].(string)

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

func (h *HTTPHandler) writeAnalyticsEvent(r *http.Request, shortCode, ipHash string) {
	// Get User-Agent and Referer from request
	userAgent := r.Header.Get("User-Agent")
	referrer := r.Header.Get("Referer")

	// Create event (using the shared events package)
	event := events.URLClickedEvent{
		BaseEvent: events.NewBaseEvent(events.EventTypeURLClicked, ""),
		ShortCode: shortCode,
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

	// We can ignore the error here - this is an analytics event,
	// the system should still function if analytics DB is down.
	// But for robustness, we could log it.
	_ = h.outboxStore.InsertEvent(context.Background(), nil, outbox)
}
