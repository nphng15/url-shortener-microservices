package main

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

type PasswordHasher interface {
	Hash(plaintext string) (string, error)
	Verify(plaintext, hash string) error
}

type bcryptHasher struct {
	cost int
}

func NewPasswordHasher(cost int) PasswordHasher {
	if cost < bcrypt.MinCost {
		cost = bcrypt.MinCost
	}
	if cost > bcrypt.MaxCost {
		cost = bcrypt.MaxCost
	}
	return &bcryptHasher{cost: cost}
}

func (h *bcryptHasher) Hash(plaintext string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(plaintext), h.cost)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (h *bcryptHasher) Verify(plaintext, hash string) error {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(plaintext))
	if err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return ErrPasswordMismatch
		}
		return err
	}
	return nil
}