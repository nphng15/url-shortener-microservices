package main

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ikniz/url-shortener/shared/auth"
)

type jwtTokenIssuer struct {
	secret []byte
	ttl    time.Duration
}

func NewTokenIssuer(secret string, ttl time.Duration) auth.TokenIssuer {
	return &jwtTokenIssuer{
		secret: []byte(secret),
		ttl:    ttl,
	}
}

func (i *jwtTokenIssuer) Issue(userID, email string) (string, time.Time, error) {
	now := time.Now()
	expiresAt := now.Add(i.ttl)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   userID,
		"email": email,
		"iss":   "url-shortener",
		"iat":   now.Unix(),
		"exp":   expiresAt.Unix(),
	})

	signed, err := token.SignedString(i.secret)
	if err != nil {
		return "", time.Time{}, err
	}

	return signed, expiresAt, nil
}

func (i *jwtTokenIssuer) Verify(tokenString string) (*auth.Claims, error) {
	claims, err := auth.VerifyToken(tokenString, string(i.secret))
	if err != nil {
		return nil, ErrTokenInvalid
	}
	return claims, nil
}