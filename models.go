package lessormq

import (
	"encoding/json"
	"sync"
	"time"
)

// Message is a delivered message: what a handler runs.
type Message struct {
	ID    string `json:"id"`
	Queue string `json:"queue"`
	// Body is the JSON value as published; Decode unmarshals it.
	Body        json.RawMessage   `json:"body"`
	Headers     map[string]string `json:"headers"`
	Attempt     int               `json:"attempt"`
	MaxAttempts int               `json:"max_attempts"`
	CreatedAt   time.Time         `json:"created_at"`
}

// Decode unmarshals the body into v.
func (m *Message) Decode(v any) error {
	return json.Unmarshal(m.Body, v)
}

// IsLastAttempt reports whether this attempt is the last one the queue allows.
func (m *Message) IsLastAttempt() bool {
	return m.Attempt >= m.MaxAttempts
}

// Lease is a claimed message and the proof of holding it. Extend, Ack, Fail and Release
// take the lease; only its receipt is accepted, and only until Until, which Extend moves.
// Safe to use from several goroutines.
type Lease struct {
	Receipt string
	Message *Message

	mu    sync.Mutex
	until time.Time
}

// NewLease builds a lease from its parts, for adapters and tests; Claim returns real ones.
func NewLease(receipt string, until time.Time, message *Message) *Lease {
	return &Lease{Receipt: receipt, Message: message, until: until}
}

// Until is when the lease ends unless it is extended.
func (l *Lease) Until() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.until
}

// Remaining is the time until the lease ends; negative once it has.
func (l *Lease) Remaining() time.Duration {
	return time.Until(l.Until())
}

func (l *Lease) extendedTo(until time.Time) {
	l.mu.Lock()
	l.until = until
	l.mu.Unlock()
}

// Published is the answer to a publish: the message, or the open one with the same dedup key.
type Published struct {
	ID          string    `json:"id"`
	Queue       string    `json:"queue"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	AvailableAt time.Time `json:"available_at"`
	Duplicate   bool      `json:"duplicate"`
}

// Outcomes of a failed attempt.
const (
	Retrying     = "retrying"
	DeadLettered = "dead_lettered"
	Discarded    = "discarded"
)

// FailResult is what the server did with a failed attempt. For logging: a client never acts on it.
type FailResult struct {
	Outcome string `json:"outcome"`
	// AvailableAt is when a retrying message comes back; nil otherwise.
	AvailableAt *time.Time `json:"available_at"`
	Attempt     int        `json:"attempt"`
	MaxAttempts int        `json:"max_attempts"`
}

// Stats are a queue's counts, for monitoring. Not part of the protocol.
type Stats struct {
	Ready                   int `json:"ready"`
	Delayed                 int `json:"delayed"`
	Processing              int `json:"processing"`
	Completed               int `json:"completed"`
	Failed                  int `json:"failed"`
	OldestReadySeconds      int `json:"oldest_ready_seconds"`
	OldestProcessingSeconds int `json:"oldest_processing_seconds"`
}

type wireLease struct {
	Receipt    string    `json:"receipt"`
	LeaseUntil time.Time `json:"lease_until"`
	Message    *Message  `json:"message"`
}
