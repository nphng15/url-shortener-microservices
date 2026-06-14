package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type upstreamPathKey struct{}

// Proxy holds a reverse proxy and one CircuitBreaker per upstream service.
type Proxy struct {
	upstreams map[string]string
	cbs       map[string]*CircuitBreaker
}

// NewProxy creates a Proxy with a dedicated CircuitBreaker for every upstream.
// CB settings: trip after 5 failures in a 10-second window; stay OPEN for 30 s.
func NewProxy(upstreams map[string]string) *Proxy {
	cbs := make(map[string]*CircuitBreaker, len(upstreams))
	for name := range upstreams {
		svcName := name // capture for closure
		cb := NewCircuitBreaker(5, 30*time.Second, 10*time.Second)
		cb.WithStateChange(func(from, to State) {
			// Update Prometheus gauge whenever state changes.
			recordCBState(svcName, to)
			if to == StateOpen {
				recordCBTrip(svcName)
			}
		})
		// Initialise gauge to CLOSED.
		recordCBState(svcName, StateClosed)
		cbs[name] = cb
	}
	return &Proxy{upstreams: upstreams, cbs: cbs}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request, upstreamName string) {
	baseURL, ok := p.upstreams[upstreamName]
	if !ok {
		http.Error(w, "upstream not found", http.StatusBadGateway)
		return
	}

	cb, hasCB := p.cbs[upstreamName]
	if !hasCB {
		p.doProxy(w, r, baseURL)
		return
	}

	start := time.Now()
	ctx := r.Context()

	err := cb.Do(ctx, func() error {
		// Use a custom ResponseWriter to capture the status code.
		rec := newStatusRecorder(w)
		p.doProxy(rec, r, baseURL)

		elapsed := time.Since(start).Seconds()
		requestDuration.WithLabelValues(upstreamName).Observe(elapsed)

		// Treat 5xx as upstream errors so the CB counts them.
		if rec.status >= 500 {
			class := fmt.Sprintf("%dxx", rec.status/100)
			requestsTotal.WithLabelValues(upstreamName, class).Inc()
			return fmt.Errorf("upstream %s returned %d", upstreamName, rec.status)
		}

		class := fmt.Sprintf("%dxx", rec.status/100)
		requestsTotal.WithLabelValues(upstreamName, class).Inc()
		return nil
	})

	if err == ErrCircuitOpen {
		recordCBRejected(upstreamName)
		requestsTotal.WithLabelValues(upstreamName, "circuit_open").Inc()
		http.Error(w, "service temporarily unavailable (circuit open)", http.StatusServiceUnavailable)
	}
	// Other errors: response was already written by doProxy.
}

// doProxy performs the actual reverse-proxy hop.
func (p *Proxy) doProxy(w http.ResponseWriter, r *http.Request, baseURL string) {
	upstreamPath := r.URL.Path
	if v := r.Context().Value(upstreamPathKey{}); v != nil {
		if s, ok := v.(string); ok {
			upstreamPath = s
		}
	}

	director := func(out *http.Request) {
		parsed, err := url.Parse(baseURL)
		if err != nil || parsed.Host == "" {
			out.URL.Scheme = "http"
			out.URL.Host = baseURL
		} else {
			out.URL.Scheme = parsed.Scheme
			out.URL.Host = parsed.Host
		}
		out.URL.Path = upstreamPath

		xff := r.Header.Get("X-Forwarded-For")
		if xff != "" {
			xff += ", "
		}
		out.Header.Set("X-Forwarded-For", xff+r.RemoteAddr)
		out.Header.Set("X-Forwarded-Proto", r.URL.Scheme)

		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if host == "" {
			host = r.RemoteAddr
		}
		out.Header.Set("X-Real-IP", host)
	}

	proxy := &httputil.ReverseProxy{Director: director}
	proxy.ServeHTTP(w, r)
}

// SetUpstreamPath stores a rewritten path into the context.
func SetUpstreamPath(ctx context.Context, path string) context.Context {
	return context.WithValue(ctx, upstreamPathKey{}, path)
}

// statusRecorder wraps http.ResponseWriter to capture the written status code.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w, status: http.StatusOK}
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}