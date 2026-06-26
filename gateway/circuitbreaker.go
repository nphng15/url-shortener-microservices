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

	// onStateChange is called (without the lock held) whenever state transitions.
	onStateChange func(from, to State)
	halfOpenProbe bool
}

func NewCircuitBreaker(maxFailures int, openTimeout, failureWindow time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:   maxFailures,
		openTimeout:   openTimeout,
		failureWindow: failureWindow,
		windowStart:   time.Now(),
	}
}

// WithStateChange attaches a callback that is invoked on every state transition.
func (cb *CircuitBreaker) WithStateChange(fn func(from, to State)) *CircuitBreaker {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onStateChange = fn
	return cb
}

// notifyStateChange calls the registered callback outside the lock.
// Must be called after releasing the lock.
func (cb *CircuitBreaker) notifyStateChange(from, to State) {
	if cb.onStateChange != nil {
		cb.onStateChange(from, to)
	}
}

func (cb *CircuitBreaker) Do(ctx context.Context, upstream func() error) error { //nolint:unparam
	cb.mu.Lock()
	stateChanged := false
	previousState := State(-1)
	newState := State(-1)

	switch cb.state {
	case StateOpen:
		if time.Since(cb.lastFailureTime) <= cb.openTimeout {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		previousState = cb.state
		cb.state = StateHalfOpen
		cb.halfOpenProbe = true
		stateChanged = true
		newState = StateHalfOpen
	case StateHalfOpen:
		if cb.halfOpenProbe {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.halfOpenProbe = true
	default:
	}
	cb.mu.Unlock()

	if stateChanged {
		cb.notifyStateChange(previousState, newState)
	}

	select {
	case <-ctx.Done():
		cb.mu.Lock()
		if cb.state == StateHalfOpen {
			cb.halfOpenProbe = false
		}
		cb.mu.Unlock()
		return ctx.Err()
	default:
	}

	err := upstream()

	if err != nil {
		cb.mu.Lock()
		if cb.state == StateHalfOpen {
			cb.halfOpenProbe = false
			previousState = cb.state
			cb.state = StateOpen
			cb.lastFailureTime = time.Now()
			cb.mu.Unlock()
			cb.notifyStateChange(previousState, StateOpen)
			return err
		}

		cb.halfOpenProbe = false
		if time.Since(cb.windowStart) > cb.failureWindow {
			cb.failures = 0
			cb.windowStart = time.Now()
		}

		cb.failures++
		cb.lastFailureTime = time.Now()

		if cb.failures >= cb.maxFailures && cb.state != StateOpen {
			previousState = cb.state
			cb.state = StateOpen
			cb.mu.Unlock()
			cb.notifyStateChange(previousState, StateOpen)
			return err
		}

		cb.mu.Unlock()
		return err
	}

	cb.mu.Lock()
	if cb.state == StateHalfOpen {
		previousState = cb.state
		cb.state = StateClosed
		cb.halfOpenProbe = false
		cb.failures = 0
		cb.windowStart = time.Now()
		cb.mu.Unlock()
		cb.notifyStateChange(previousState, StateClosed)
		return nil
	}

	cb.halfOpenProbe = false
	cb.failures = 0
	cb.windowStart = time.Now()
	cb.mu.Unlock()
	return nil
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
