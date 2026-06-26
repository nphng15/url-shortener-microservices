package main

import (
	"net/http"
	"strings"
)

type Route struct {
	Method       string
	PathPrefix   string
	Upstream     string
	StripPrefix  string
	RequiresAuth bool
	RateLimitKey string
}

var routingTable = []Route{
	{Method: "POST", PathPrefix: "/api/auth/register", Upstream: "user-service", RequiresAuth: false, StripPrefix: "/api/auth"},
	{Method: "POST", PathPrefix: "/api/auth/login", Upstream: "user-service", RequiresAuth: false, StripPrefix: "/api/auth"},
	{Method: "GET", PathPrefix: "/api/me", Upstream: "user-service", RequiresAuth: true, StripPrefix: "/api"},
	{Method: "POST", PathPrefix: "/api/shorten", Upstream: "url-service", RequiresAuth: true, RateLimitKey: "shorten", StripPrefix: "/api"},
	{Method: "GET", PathPrefix: "/api/urls", Upstream: "url-service", RequiresAuth: true, StripPrefix: "/api"},
	{Method: "DELETE", PathPrefix: "/api/urls/", Upstream: "url-service", RequiresAuth: true, StripPrefix: "/api"},
	{Method: "GET", PathPrefix: "/r/", Upstream: "url-service", RequiresAuth: false, RateLimitKey: "redirect", StripPrefix: "/r"},
	{Method: "GET", PathPrefix: "/api/stats/", Upstream: "analytics-service", RequiresAuth: false, StripPrefix: "/api"},
	{Method: "GET", PathPrefix: "/api/notifications", Upstream: "notification-service", RequiresAuth: true, StripPrefix: "/api"},
}

func matchRoute(r *http.Request) *Route {
	for i := range routingTable {
		rt := &routingTable[i]
		if rt.Method != "" && rt.Method != r.Method {
			continue
		}
		if strings.HasPrefix(r.URL.Path, rt.PathPrefix) {
			return rt
		}
	}
	return nil
}
