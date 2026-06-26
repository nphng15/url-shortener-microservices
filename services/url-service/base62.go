package main

import (
	"errors"
	"math/big"
	"strings"
)

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
const shortCodeLength = 7

// Encode converts a big.Int to a base62 string of exactly shortCodeLength characters.
// Pads with '0' (alphabet[0]) on the left if the number requires fewer digits.
func Encode(n *big.Int) string {
	if n == nil {
		return ""
	}

	// Convert to unsigned big integer to handle negative input safely (though we expect positive IDs)
	// For negative numbers, we can treat them as their unsigned representation or error,
	// but since this is used for ID -> code, we expect positive.
	// If n < 0, big.Int behavior in Mod might be unexpected if we don't handle it.
	// However, for UUID which are positive, this is fine.
	// But let's play safe with Abs.
	n = new(big.Int).Abs(n)

	// We need to generate exactly shortCodeLength characters.
	// 62^7 is the total number of unique strings of length 7.
	limit := new(big.Int).Exp(big.NewInt(62), big.NewInt(shortCodeLength), nil)

	// n mod 62^7
	n.Mod(n, limit)

	// Build the string in reverse order and then reverse it, or use a fixed-size buffer.
	// Since we know the length, a byte slice is efficient.
	buf := make([]byte, shortCodeLength)
	for i := shortCodeLength - 1; i >= 0; i-- {
		// n % 62
		rem := new(big.Int).Mod(n, big.NewInt(62))
		// n = n / 62
		n.Div(n, big.NewInt(62))
		// Convert remainder to character
		buf[i] = base62Alphabet[rem.Int64()]
	}

	return string(buf)
}

// Decode converts a base62 string of exactly shortCodeLength characters back to a big.Int.
// Returns an error if the string length is not shortCodeLength or if it contains invalid characters.
func Decode(s string) (*big.Int, error) {
	if len(s) != shortCodeLength {
		return nil, errors.New("invalid short code length")
	}

	result := big.NewInt(0)
	base := big.NewInt(62)

	for i := 0; i < len(s); i++ {
		idx := strings.IndexByte(base62Alphabet, s[i])
		if idx == -1 {
			return nil, errors.New("invalid character in short code")
		}
		result.Mul(result, base)
		result.Add(result, big.NewInt(int64(idx)))
	}

	return result, nil
}
