package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// State represents the circuit breaker state
type State int

const (
	StateClosed State = iota // Circuit is closed, requests pass through
	StateHalfOpen            // Circuit is half-open, testing if service recovered
	StateOpen                // Circuit is open, requests fail fast
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF_OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

var (
	ErrOpen = errors.New("circuit breaker is open")
)

// Config holds the circuit breaker configuration
type Config struct {
	MaxFailures      int           // Number of consecutive failures before opening
	ResetTimeout     time.Duration // Time to wait before attempting recovery
	SuccessThreshold int           // Number of successes needed in half-open to close
}

// DefaultConfig returns a sensible default configuration
func DefaultConfig() Config {
	return Config{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		SuccessThreshold: 2,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config  Config
	state   State
	mu      sync.RWMutex

	// Failure tracking
	consecutiveFailures  int
	consecutiveSuccesses int
	lastFailureTime      time.Time
}

// New creates a new circuit breaker with the given config
func New(cfg Config) *CircuitBreaker {
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = DefaultConfig().MaxFailures
	}
	if cfg.ResetTimeout <= 0 {
		cfg.ResetTimeout = DefaultConfig().ResetTimeout
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = DefaultConfig().SuccessThreshold
	}

	return &CircuitBreaker{
		config: cfg,
		state:  StateClosed,
	}
}

// Execute runs the given function through the circuit breaker
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.allowRequest() {
		return ErrOpen
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// allowRequest determines if a request should be allowed
func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
		if now.Sub(cb.lastFailureTime) >= cb.config.ResetTimeout {
			cb.setState(StateHalfOpen)
			cb.consecutiveSuccesses = 0
			return true
		}
		return false
	case StateHalfOpen:
		return true
	}

	return false
}

// recordResult records the result of an operation
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.onFailure()
	} else {
		cb.onSuccess()
	}
}

func (cb *CircuitBreaker) onFailure() {
	cb.consecutiveFailures++
	cb.consecutiveSuccesses = 0
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.consecutiveFailures >= cb.config.MaxFailures {
			cb.setState(StateOpen)
		}
	case StateHalfOpen:
		// Failed in half-open, go back to open
		cb.setState(StateOpen)
	}
}

func (cb *CircuitBreaker) onSuccess() {
	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses++

	switch cb.state {
	case StateHalfOpen:
		if cb.consecutiveSuccesses >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
		}
	}
}

func (cb *CircuitBreaker) setState(newState State) {
	cb.state = newState
}

// State returns the current circuit breaker state
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onSuccess()
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onFailure()
}
