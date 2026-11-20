package lessormq

import (
	"errors"
	"fmt"
)

// Errors to match with errors.Is. An *APIError matches the one for its problem code:
//
//	if errors.Is(err, lessormq.ErrLeaseLost) { /* the message is someone else's now */ }
var (
	// ErrLeaseLost: the receipt is not the current lease of the message. It expired, was
	// released, or the message was claimed again. Stop reporting on it; the server has
	// already made it available again.
	ErrLeaseLost = errors.New("lessormq: lease lost")
	// ErrValidation: the request was rejected as invalid; APIError.Errors names each field.
	ErrValidation = errors.New("lessormq: validation failed")
	// ErrUnauthorized: the API key is missing, unknown or revoked.
	ErrUnauthorized = errors.New("lessormq: unauthorized")
	// ErrForbidden: the key lacks the scope or the queue, or its tenant is suspended.
	ErrForbidden = errors.New("lessormq: forbidden")
	// ErrNotFound: no such queue or message for this tenant.
	ErrNotFound = errors.New("lessormq: not found")
	// ErrConflict: the message is in a state that does not allow this (invalid_state).
	ErrConflict = errors.New("lessormq: invalid state")
	// ErrPayloadTooLarge: body and headers exceed the queue's max_message_bytes.
	ErrPayloadTooLarge = errors.New("lessormq: payload too large")
	// ErrRateLimited: the tenant exceeded a quota or rate limit, after the retries.
	ErrRateLimited = errors.New("lessormq: rate limited")
	// ErrServer: the server failed (5xx), after the retries.
	ErrServer = errors.New("lessormq: server error")
)

// FieldError names one invalid field of a rejected request.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// APIError is an error answer of the server (RFC 9457 problem details). Code is the
// stable problem code to branch on; errors.Is matches it against the Err variables.
type APIError struct {
	Status int
	Code   string
	Detail string
	Errors []FieldError
	// RetryAfter is the Retry-After of a 429, in seconds; -1 when absent.
	RetryAfter int
}

func (e *APIError) Error() string {
	if e.Detail != "" {
		return "lessormq: " + e.Detail
	}
	return fmt.Sprintf("lessormq: error %d (%s)", e.Status, e.Code)
}

// Is matches the Err variable for the problem code.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrLeaseLost:
		return e.Code == "lease_lost"
	case ErrValidation:
		return e.Code == "validation_failed"
	case ErrUnauthorized:
		return e.Code == "unauthorized"
	case ErrForbidden:
		return e.Code == "forbidden" || e.Code == "tenant_suspended"
	case ErrNotFound:
		return e.Code == "not_found"
	case ErrConflict:
		return e.Code == "invalid_state"
	case ErrPayloadTooLarge:
		return e.Code == "payload_too_large"
	case ErrRateLimited:
		return e.Code == "rate_limited"
	case ErrServer:
		return e.Status >= 500
	}
	return false
}

func codeFor(status int) string {
	switch {
	case status == 401:
		return "unauthorized"
	case status == 403:
		return "forbidden"
	case status == 404:
		return "not_found"
	case status == 413:
		return "payload_too_large"
	case status == 429:
		return "rate_limited"
	case status >= 500:
		return "internal"
	}
	return "validation_failed"
}

// TransportError: the server could not be reached, after the retries.
type TransportError struct {
	Method string
	Path   string
	Err    error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("lessormq: %s %s failed: %v", e.Method, e.Path, e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// Permanent marks an error as permanent: the message itself cannot be processed (it
// cannot be decoded, it fails validation), so retrying it would only fail again. A fact
// for the server; the queue's on_permanent decides what it means.
//
//	return lessormq.Permanent(fmt.Errorf("no recipient"))
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// IsPermanent reports whether an error, or one it wraps, was marked with Permanent or
// has a Permanent() bool method that says so.
func IsPermanent(err error) bool {
	var p interface{ Permanent() bool }
	return errors.As(err, &p) && p.Permanent()
}

type permanentError struct{ err error }

func (e *permanentError) Error() string   { return e.err.Error() }
func (e *permanentError) Unwrap() error   { return e.err }
func (e *permanentError) Permanent() bool { return true }
