package main

import (
	"errors"
	"net/url"
)

func ValidateURL(rawURL string) error {
	if rawURL == "" {
		return errors.New("URL cannot be empty")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return errors.New("invalid URL format")
	}
	// Requirement: Must have scheme http or https
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("URL must start with http or https")
	}
	if u.Host == "" {
		return errors.New("URL must have a host")
	}
	return nil
}
