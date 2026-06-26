package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// cbState tracks the current circuit breaker state per upstream service.
	// Values: 0=CLOSED, 1=HALF_OPEN, 2=OPEN
	cbState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gateway",
		Name:      "circuit_breaker_state",
		Help:      "Current state of the circuit breaker per upstream service (0=CLOSED, 1=HALF_OPEN, 2=OPEN)",
	}, []string{"service"})

	// cbTripsTotal counts how many times the CB transitioned to OPEN.
	cbTripsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "circuit_breaker_trips_total",
		Help:      "Total number of times the circuit breaker tripped to OPEN per upstream service",
	}, []string{"service"})

	// cbRejectedTotal counts requests rejected because CB was OPEN.
	cbRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "circuit_breaker_rejected_total",
		Help:      "Total number of requests rejected by an open circuit breaker per upstream service",
	}, []string{"service"})

	// requestsTotal counts all upstream requests by service and HTTP status class.
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "requests_total",
		Help:      "Total number of requests proxied to each upstream service",
	}, []string{"service", "status_class"})

	// requestDuration tracks upstream response latency.
	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gateway",
		Name:      "request_duration_seconds",
		Help:      "Upstream request duration in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"service"})
)

// recordCBState updates the Prometheus gauge for the given service's CB state.
func recordCBState(service string, s State) {
	cbState.WithLabelValues(service).Set(float64(s))
}

// recordCBTrip increments the trip counter when CB transitions to OPEN.
func recordCBTrip(service string) {
	cbTripsTotal.WithLabelValues(service).Inc()
}

// recordCBRejected increments the rejected counter.
func recordCBRejected(service string) {
	cbRejectedTotal.WithLabelValues(service).Inc()
}
