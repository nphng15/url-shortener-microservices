package auth

import (
	"context"
	"net/http"
	"strings"
)

// JWTMiddleware validates the Authorization header and adds Claims to the context
func JWTMiddleware(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSONError := func(statusCode int, message string) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(statusCode)
				// Ensure we write exactly the JSON the tests expect
				w.Write([]byte(`{"error":"` + message + `"}`))
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeJSONError(http.StatusUnauthorized, "authorization header required")
				return
			}

			if !strings.HasPrefix(authHeader, "Bearer ") {
				writeJSONError(http.StatusUnauthorized, "invalid authorization header format")
				return
			}

			tokenString := strings.TrimPrefix(authHeader, "Bearer ")
			if tokenString == "" {
				writeJSONError(http.StatusUnauthorized, "token is required")
				return
			}

			claims, err := VerifyToken(tokenString, secret)
			if err != nil {
				writeJSONError(http.StatusUnauthorized, "unauthorized")
				return
			}

			ctx := context.WithValue(r.Context(), claimsKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClaimsFromContext extracts Claims injected by JWTMiddleware.
// Returns (nil, false) if the context has no claims (middleware not applied or failed).
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsKey{}).(*Claims)
	if !ok {
		return nil, false
	}
	return claims, true
}
