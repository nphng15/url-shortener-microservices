package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Hash IP helper - SHA256 of the IP address string
func hashIP(ipAddr string) string {
	// Remove port if present (e.g., "[IP_ADDRESS]:54321" -> "[IP_ADDRESS]")
	// This ensures [IP_ADDRESS] from different ports hashes to the same value
	host := ipAddr
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}

	data := []byte(host)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]) // Return hex string for easy storage/comparison
}
