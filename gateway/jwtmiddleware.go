package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/ikniz/url-shortener/shared/auth"
)

func jwtMiddleware(secret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := matchRoute(r)
		if route == nil || !route.RequiresAuth {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		claims, err := auth.VerifyToken(strings.TrimPrefix(authHeader, "Bearer "), secret)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		ctx := context.WithValue(r.Context(), auth.TestClaimsKey{}, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
