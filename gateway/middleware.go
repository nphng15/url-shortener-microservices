package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/ikniz/url-shortener/shared/auth"
)

func authMiddleware(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHdr := r.Header.Get("Authorization")
			if authHdr == "" || !strings.HasPrefix(authHdr, "Bearer ") {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			token := strings.TrimPrefix(authHdr, "Bearer ")
			claims, err := auth.VerifyToken(token, secret)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			ctx := context.WithValue(r.Context(), auth.TestClaimsKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractBearerToken(r *http.Request) string {
	authHdr := r.Header.Get("Authorization")
	if authHdr == "" || !strings.HasPrefix(authHdr, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHdr, "Bearer ")
}