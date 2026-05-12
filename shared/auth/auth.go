package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrTokenInvalid = errors.New("invalid token")
)

// TokenIssuer abstracts JWT issuance. Allows test doubles.
// Implemented by jwtTokenIssuer in user-service/token.go.
type TokenIssuer interface {
	// Issue creates and signs a JWT for the given user.
	// Returns the signed token string and the expiry time, or an error.
	Issue(userID, email string) (tokenString string, expiresAt time.Time, err error)
	// Verify parses and validates a token string.
	// Returns the Claims on success, or an error if the token is expired,
	// has an invalid signature, or is malformed.
	Verify(tokenString string) (*Claims, error)
}

// claimsKey is the unexported context key type — prevents collisions with other packages.
type claimsKey struct{}

// TestClaimsKey is exported ONLY for use in handler unit tests.
// Do not use in production code; use JWTMiddleware instead.
type TestClaimsKey = claimsKey

type Claims struct {
	// Sub is the user_id UUID string. Named "sub" per JWT RFC 7519 §4.1.2.
	Sub string `json:"sub"`
	// Email is denormalized from the users table for convenience.
	// Avoids downstream services needing a DB lookup to display user context.
	Email string `json:"email"`
	// Iss is the issuer claim. Always "url-shortener".
	Iss string `json:"iss"`
	// Iat is issued-at Unix timestamp (seconds).
	Iat int64 `json:"iat"`
	// Exp is expiry Unix timestamp (seconds). Default: iat + 86400 (24h).
	Exp int64 `json:"exp"`
}

// IsExpired returns true if the current wall time is past Exp.
// Does NOT verify the signature — that is done by VerifyToken.
func (c *Claims) IsExpired() bool {
	return time.Now().Unix() > c.Exp
}

// ExpiresAt converts the Exp Unix timestamp to a time.Time for response serialization.
func (c *Claims) ExpiresAt() time.Time {
	return time.Unix(c.Exp, 0).UTC()
}

// VerifyToken validates the JWT and returns the claims
func VerifyToken(tokenString, secret string) (*Claims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Must check token.Method.(*jwt.SigningMethodHMAC) before accepting the key
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	})

	if err != nil || !token.Valid {
		return nil, ErrTokenInvalid
	}

	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrTokenInvalid
	}

	extractString := func(m jwt.MapClaims, key string) (string, bool) {
		v, exists := m[key]
		if !exists {
			return "", false
		}
		s, ok := v.(string)
		return s, ok
	}

	claims := &Claims{}
	if sub, ok := extractString(mapClaims, "sub"); ok {
		claims.Sub = sub
	} else {
		return nil, ErrTokenInvalid
	}

	if email, ok := extractString(mapClaims, "email"); ok {
		claims.Email = email
	} else {
		return nil, ErrTokenInvalid
	}

	if iss, ok := extractString(mapClaims, "iss"); ok {
		claims.Iss = iss
	} else {
		return nil, ErrTokenInvalid
	}

	if iat, ok := mapClaims["iat"].(float64); ok {
		claims.Iat = int64(iat)
	} else {
		return nil, ErrTokenInvalid
	}

	if exp, ok := mapClaims["exp"].(float64); ok {
		claims.Exp = int64(exp)
	} else {
		return nil, ErrTokenInvalid
	}

	if claims.Iss != "url-shortener" {
		return nil, ErrTokenInvalid
	}

	return claims, nil
}
