package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit open")

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

type CircuitBreaker struct {
	mu              sync.Mutex
	state           State
	failures        int
	lastFailureTime time.Time
	maxFailures     int
	openTimeout     time.Duration
	failureWindow   time.Duration
	windowStart     time.Time
	halfOpenProbe   bool
}

func NewCircuitBreaker(maxFailures int, openTimeout, failureWindow time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:   maxFailures,
		openTimeout:   openTimeout,
		failureWindow: failureWindow,
		windowStart:   time.Now(),
	}
}

func (cb *CircuitBreaker) Do(ctx context.Context, upstream func() error) error {
	cb.mu.Lock()

	if cb.state == StateOpen {
		if time.Since(cb.lastFailureTime) > cb.openTimeout {
			cb.state = StateHalfOpen
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
	}
	if cb.state == StateHalfOpen {
		if cb.halfOpenProbe {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.halfOpenProbe = true
	}

	cb.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := upstream()

	if err != nil {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		cb.halfOpenProbe = false

		if cb.state == StateHalfOpen {
			cb.state = StateOpen
			cb.lastFailureTime = time.Now()
			return err
		}

		if time.Since(cb.windowStart) > cb.failureWindow {
			cb.failures = 0
			cb.windowStart = time.Now()
		}

		cb.failures++
		cb.lastFailureTime = time.Now()

		if cb.failures >= cb.maxFailures {
			cb.state = StateOpen
		}

		return err
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.halfOpenProbe = false

	if cb.state == StateHalfOpen {
		cb.state = StateClosed
	}
	cb.failures = 0
	cb.windowStart = time.Now()

	return nil
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
