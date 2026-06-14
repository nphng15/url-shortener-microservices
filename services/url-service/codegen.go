package main

import (
	"crypto/rand"
	"math/big"
)

type ShortCodeGenerator interface {
	Generate() string
}

type cryptoRandGenerator struct{}

func NewShortCodeGenerator() ShortCodeGenerator {
	return &cryptoRandGenerator{}
}

// Generate creates a cryptographically random 7-character base62 short code.
// Uses crypto/rand to fill 8 bytes (64 bits)
// then takes modulo 62^7 = 3,521,614,606,208 to map into base62 space.
//
// Probability analysis:
//
//	62^7 = 3.5 trillion codes.
//	After 1 million URLs: collision probability ≈ 1.4 × 10^-7 per attempt.
//	5-retry budget makes the probability of all 5 colliding negligible.
//
// Returns: 7-character string from base62Alphabet
// Never returns an error (crypto/rand failure panics — system entropy failure is unrecoverable)
func (g *cryptoRandGenerator) Generate() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		// System entropy failure is unrecoverable
		panic(err)
	}
	
	n := new(big.Int).SetBytes(b)
	return Encode(n)
}
