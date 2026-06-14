package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type upstreamPathKey struct{}

type Proxy struct {
	upstreams map[string]string
}

func NewProxy(upstreams map[string]string) *Proxy {
	return &Proxy{upstreams: upstreams}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request, upstreamName string) {
	baseURL, ok := p.upstreams[upstreamName]
	if !ok {
		http.Error(w, "upstream not found", http.StatusBadGateway)
		return
	}

	upstreamPath := r.URL.Path
	if v := r.Context().Value(upstreamPathKey{}); v != nil {
		if s, ok := v.(string); ok {
			upstreamPath = s
		}
	}

	director := func(out *http.Request) {
		parsed, err := url.Parse(baseURL)
		if err != nil || parsed.Host == "" {
			// Treat baseURL as a plain host:port if it has no scheme
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

func SetUpstreamPath(ctx context.Context, path string) context.Context {
	return context.WithValue(ctx, upstreamPathKey{}, path)
}