// Package lessormq is the Go client and worker for LessorMQ, the message queue over HTTP
// where the server decides and clients only claim, run, ack or fail.
//
//	client := lessormq.New("https://mq.example.com", os.Getenv("LESSORMQ_KEY"))
//	client.Publish(ctx, "mail", map[string]string{"to": "ada@example.com"}, nil)
//
//	worker := lessormq.NewWorker(client, "mail", func(ctx context.Context, job *lessormq.Job) error {
//		return send(ctx, job.Message)
//	}, lessormq.WorkerOptions{Concurrency: 4})
//	worker.Run(ctx)
//
// The client never decides what happens to a message: no retry of a failed job, no
// delay, no dead-lettering. It reports, and the queue decides. It does retry its own
// HTTP calls when that is safe: network errors, 5xx and 429, for every call that can be
// repeated (all of them, given the automatic dedup key on publish).
package lessormq

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// UserAgent is sent with every request unless WithUserAgent says otherwise.
const UserAgent = "lessormq-go/1"

const (
	maxErrorMessage = 65535
	maxErrorType    = 255
)

// Client speaks the LessorMQ protocol. Safe for concurrent use.
type Client struct {
	baseURL      string
	apiKey       string
	http         *http.Client
	httpRetries  int
	retryDelay   time.Duration
	autoDedupKey bool
	timeout      time.Duration
	userAgent    string
	logger       *slog.Logger
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient uses your own http.Client (proxies, TLS, transports). Its Timeout should
// be zero or above 30 seconds: the client sets a deadline per request itself.
func WithHTTPClient(c *http.Client) Option { return func(cl *Client) { cl.http = c } }

// WithRetries sets the retries after a network error, a 5xx or a 429, for calls that are
// safe to repeat (all but a publish without a dedup key). Default 3.
func WithRetries(n int) Option { return func(cl *Client) { cl.httpRetries = n } }

// WithRetryDelay sets the base delay between retries, times the attempt number. Default 100ms.
func WithRetryDelay(d time.Duration) Option { return func(cl *Client) { cl.retryDelay = d } }

// WithoutAutoDedupKey stops giving every publish without a dedup key one of its own.
// Such a publish is then never retried, since a repeat could create a second message.
func WithoutAutoDedupKey() Option { return func(cl *Client) { cl.autoDedupKey = false } }

// WithTimeout sets the deadline per request; a claim gets its wait on top. Default 10s.
func WithTimeout(d time.Duration) Option { return func(cl *Client) { cl.timeout = d } }

// WithUserAgent sets the User-Agent header.
func WithUserAgent(ua string) Option { return func(cl *Client) { cl.userAgent = ua } }

// WithLogger logs retries, and a worker's troubles, to l. Default: nothing is logged.
func WithLogger(l *slog.Logger) Option { return func(cl *Client) { cl.logger = l } }

// New returns a client for the server at baseURL, acting with apiKey.
func New(baseURL, apiKey string, options ...Option) *Client {
	c := &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		apiKey:       apiKey,
		http:         http.DefaultClient,
		httpRetries:  3,
		retryDelay:   100 * time.Millisecond,
		autoDedupKey: true,
		timeout:      10 * time.Second,
		userAgent:    UserAgent,
		logger:       slog.New(discard{}),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// String never shows the key.
func (c *Client) String() string { return "lessormq.Client(" + c.baseURL + ")" }

// PublishOptions are the optional parts of a publish; the zero value is none of them.
type PublishOptions struct {
	Headers map[string]string
	// DedupKey keeps one open message per key: a publish with the key of an open message
	// returns that one (Published.Duplicate).
	DedupKey     string
	DelaySeconds int
	AvailableAt  time.Time
	// MaxAttempts for this message, within the queue's max_attempts_limit; 0 is the queue's.
	MaxAttempts int
}

// Publish puts a message on a queue. Body is anything encoding/json takes, or a
// json.RawMessage. opts may be nil.
func (c *Client) Publish(ctx context.Context, queue string, body any, opts *PublishOptions) (*Published, error) {
	if opts == nil {
		opts = &PublishOptions{}
	}
	dedupKey := opts.DedupKey
	if dedupKey == "" && c.autoDedupKey {
		dedupKey = "lmq-auto-" + randomHex(12)
	}
	payload := map[string]any{"body": body}
	if len(opts.Headers) > 0 {
		payload["headers"] = opts.Headers
	}
	if dedupKey != "" {
		payload["dedup_key"] = dedupKey
	}
	if opts.DelaySeconds > 0 {
		payload["delay_seconds"] = opts.DelaySeconds
	}
	if !opts.AvailableAt.IsZero() {
		payload["available_at"] = opts.AvailableAt.UTC().Format(time.RFC3339)
	}
	if opts.MaxAttempts > 0 {
		payload["max_attempts"] = opts.MaxAttempts
	}
	var published Published
	// Without a dedup key a repeated publish could create a second message.
	_, err := c.send(ctx, call{method: http.MethodPost, path: "/v1/queues/" + segment(queue) + "/messages",
		payload: payload, repeatable: dedupKey != ""}, &published)
	if err != nil {
		return nil, err
	}
	return &published, nil
}

// ClaimOptions are the settings a consumer may pass when it claims; the queue's limits apply.
type ClaimOptions struct {
	// Consumer is shown to administrators.
	Consumer string
	// Route is the path this worker runs jobs through (a VPN exit, a proxy, an API account).
	Route string
	// WaitSeconds waits up to this long for a message (at most 20).
	WaitSeconds int
	// LeaseSeconds is the lease for this claim; 0 is the queue's.
	LeaseSeconds int
}

// Claim leases the next message of a queue, waiting up to opts.WaitSeconds for one. It
// returns nil, nil when nothing became available, or the queue is paused.
//
// Cancelling ctx while a claim waits can leave a message leased on the server, which
// then runs out and counts as an attempt; the Worker therefore lets claims finish.
func (c *Client) Claim(ctx context.Context, queue string, opts ClaimOptions) (*Lease, error) {
	payload := opts.payload()
	var wire wireLease
	// A claim whose answer got lost leaves a lease that simply expires and counts as an
	// attempt, so claims are retried like the rest.
	status, err := c.send(ctx, call{method: http.MethodPost, path: "/v1/queues/" + segment(queue) + "/claims",
		payload: payload, repeatable: true, extra: time.Duration(max(opts.WaitSeconds, 0)) * time.Second}, &wire)
	if err != nil || status == http.StatusNoContent {
		return nil, err
	}
	return NewLease(wire.Receipt, wire.LeaseUntil, wire.Message), nil
}

func (opts ClaimOptions) payload() map[string]any {
	payload := map[string]any{}
	if opts.Consumer != "" {
		payload["consumer"] = opts.Consumer
	}
	if opts.Route != "" {
		payload["route"] = opts.Route
	}
	if opts.WaitSeconds > 0 {
		payload["wait_seconds"] = opts.WaitSeconds
	}
	if opts.LeaseSeconds > 0 {
		payload["lease_seconds"] = opts.LeaseSeconds
	}
	return payload
}

// ClaimBatch leases up to maxMessages (1 to 100) ready messages of a queue at once, oldest
// first, each with a receipt of its own. It waits up to opts.WaitSeconds only while there
// is none, and returns no leases when nothing became available. Report each lease as
// usual, or ack them together with AckBatch.
func (c *Client) ClaimBatch(ctx context.Context, queue string, maxMessages int, opts ClaimOptions) ([]*Lease, error) {
	payload := opts.payload()
	payload["max_messages"] = maxMessages
	var wire struct {
		Items []wireLease `json:"items"`
	}
	status, err := c.send(ctx, call{method: http.MethodPost, path: "/v1/queues/" + segment(queue) + "/claims",
		payload: payload, repeatable: true, extra: time.Duration(max(opts.WaitSeconds, 0)) * time.Second}, &wire)
	if err != nil || status == http.StatusNoContent {
		return nil, err
	}
	leases := make([]*Lease, len(wire.Items))
	for i, w := range wire.Items {
		leases[i] = NewLease(w.Receipt, w.LeaseUntil, w.Message)
	}
	return leases, nil
}

// AckOutcome is what became of one lease of an AckBatch.
type AckOutcome string

const (
	// Acked: the message is completed (also when this lease was acked before).
	Acked AckOutcome = "acked"
	// AckLeaseLost: the receipt is not the current lease; stop reporting on it.
	AckLeaseLost AckOutcome = "lease_lost"
	// AckNotFound: no such message.
	AckNotFound AckOutcome = "not_found"
	// AckForbidden: the key may not use the message's queue.
	AckForbidden AckOutcome = "forbidden"
)

// AckBatch reports success for up to 100 leases in one request. The outcomes are in the
// order of leases; one that cannot be acked does not hold up the others.
func (c *Client) AckBatch(ctx context.Context, leases []*Lease) ([]AckOutcome, error) {
	items := make([]map[string]string, len(leases))
	for i, l := range leases {
		items[i] = map[string]string{"message": l.Message.ID, "receipt": l.Receipt}
	}
	var answer struct {
		Items []struct {
			Outcome AckOutcome `json:"outcome"`
		} `json:"items"`
	}
	if _, err := c.send(ctx, call{method: http.MethodPost, path: "/v1/acks",
		payload: map[string]any{"leases": items}, repeatable: true}, &answer); err != nil {
		return nil, err
	}
	out := make([]AckOutcome, len(leases))
	for i := range out {
		out[i] = AckNotFound
		if i < len(answer.Items) {
			out[i] = answer.Items[i].Outcome
		}
	}
	return out, nil
}

// Extend moves the end of the lease to now plus leaseSeconds (0: the queue's lease).
// It returns an error matching ErrLeaseLost when the lease already ended.
func (c *Client) Extend(ctx context.Context, lease *Lease, leaseSeconds int) error {
	payload := map[string]any{"receipt": lease.Receipt}
	if leaseSeconds > 0 {
		payload["lease_seconds"] = leaseSeconds
	}
	var answer struct {
		LeaseUntil time.Time `json:"lease_until"`
	}
	if _, err := c.send(ctx, call{method: http.MethodPost, path: messagePath(lease, "extend"),
		payload: payload, repeatable: true}, &answer); err != nil {
		return err
	}
	lease.extendedTo(answer.LeaseUntil)
	return nil
}

// Ack reports success. It returns an error matching ErrLeaseLost when the lease already ended.
func (c *Client) Ack(ctx context.Context, lease *Lease) error {
	_, err := c.send(ctx, call{method: http.MethodPost, path: messagePath(lease, "ack"),
		payload: map[string]any{"receipt": lease.Receipt}, repeatable: true}, nil)
	return err
}

// FailOptions are the optional parts of a failure report.
type FailOptions struct {
	// Permanent overrides IsPermanent(err): the input itself cannot be processed.
	Permanent *bool
	// Details is any JSON value with more context.
	Details any
	// Type is the error type; by default the error's ErrorType() string, or its Go type.
	Type string
}

// Fail reports failure with err (its text, its type, and whether it is permanent). The
// server decides what follows and says so in the result. opts may be nil. It returns an
// error matching ErrLeaseLost when the lease already ended.
func (c *Client) Fail(ctx context.Context, lease *Lease, err error, opts *FailOptions) (*FailResult, error) {
	if opts == nil {
		opts = &FailOptions{}
	}
	text := "(no message)"
	if err != nil && err.Error() != "" {
		text = truncate(err.Error(), maxErrorMessage)
	}
	report := map[string]any{"message": text}
	errType := opts.Type
	if errType == "" && err != nil {
		errType = typeOf(err)
	}
	if errType != "" {
		report["type"] = truncate(errType, maxErrorType)
	}
	if opts.Details != nil {
		report["details"] = opts.Details
	}
	permanent := IsPermanent(err)
	if opts.Permanent != nil {
		permanent = *opts.Permanent
	}
	var result FailResult
	if _, sendErr := c.send(ctx, call{method: http.MethodPost, path: messagePath(lease, "fail"),
		payload:    map[string]any{"receipt": lease.Receipt, "error": report, "permanent": permanent},
		repeatable: true}, &result); sendErr != nil {
		return nil, sendErr
	}
	return &result, nil
}

// Release gives the message back without an outcome; the attempt does not count. It
// returns an error matching ErrLeaseLost when the lease already ended.
func (c *Client) Release(ctx context.Context, lease *Lease, delaySeconds int) error {
	payload := map[string]any{"receipt": lease.Receipt}
	if delaySeconds > 0 {
		payload["delay_seconds"] = delaySeconds
	}
	_, err := c.send(ctx, call{method: http.MethodPost, path: messagePath(lease, "release"),
		payload: payload, repeatable: true}, nil)
	return err
}

// Stats returns a queue's counts. Needs the consume or admin scope. Not part of the
// protocol; for monitoring.
func (c *Client) Stats(ctx context.Context, queue string) (*Stats, error) {
	var answer struct {
		Stats Stats `json:"stats"`
	}
	if _, err := c.send(ctx, call{method: http.MethodGet, path: "/v1/queues/" + segment(queue),
		repeatable: true}, &answer); err != nil {
		return nil, err
	}
	return &answer.Stats, nil
}

type call struct {
	method  string
	path    string
	payload any
	// repeatable: it may be sent again after a failure that leaves its effect unknown.
	repeatable bool
	extra      time.Duration
}

func (c *Client) send(ctx context.Context, call call, into any) (int, error) {
	var body []byte
	if call.payload != nil {
		encoded, err := json.Marshal(call.payload)
		if err != nil {
			return 0, fmt.Errorf("lessormq: encode %s: %w", call.path, err)
		}
		body = encoded
	}

	for attempt := 1; ; attempt++ {
		status, raw, header, err := c.do(ctx, call, body)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			if call.repeatable && attempt <= c.httpRetries {
				if err := c.pause(ctx, attempt, -1, call.path, err.Error()); err != nil {
					return 0, err
				}
				continue
			}
			return 0, &TransportError{Method: call.method, Path: call.path, Err: err}
		}

		if status < 400 {
			if into != nil && status != http.StatusNoContent && len(raw) > 0 {
				if err := json.Unmarshal(raw, into); err != nil {
					return status, fmt.Errorf("lessormq: decode %s: %w", call.path, err)
				}
			}
			return status, nil
		}

		apiErr := problem(status, raw, header.Get("Retry-After"))
		// A 429 was not processed at all, so it is always safe to send again.
		retry := apiErr.Code == "rate_limited" || (call.repeatable && status >= 500)
		if retry && attempt <= c.httpRetries {
			if err := c.pause(ctx, attempt, apiErr.RetryAfter, call.path, apiErr.Error()); err != nil {
				return 0, err
			}
			continue
		}
		return status, apiErr
	}
}

func (c *Client) do(ctx context.Context, call call, body []byte) (int, []byte, http.Header, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout+call.extra)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, call.method, c.baseURL+call.path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.http.Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return response.StatusCode, raw, response.Header, nil
}

func (c *Client) pause(ctx context.Context, attempt, retryAfter int, path, reason string) error {
	delay := c.retryDelay * time.Duration(attempt)
	if retryAfter >= 0 {
		delay = time.Duration(retryAfter) * time.Second
	}
	c.logger.Warn("LessorMQ request failed, retrying", "path", path, "attempt", attempt, "delay", delay, "reason", reason)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func problem(status int, raw []byte, retryAfter string) *APIError {
	var body struct {
		Code   string       `json:"code"`
		Detail string       `json:"detail"`
		Errors []FieldError `json:"errors"`
	}
	// A proxy's error page, say, does not decode: the status says enough.
	_ = json.Unmarshal(raw, &body)
	apiErr := &APIError{Status: status, Code: body.Code, Detail: body.Detail, Errors: body.Errors, RetryAfter: -1}
	if apiErr.Code == "" {
		apiErr.Code = codeFor(status)
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds >= 0 {
		apiErr.RetryAfter = seconds
	}
	return apiErr
}

func segment(value string) string { return url.PathEscape(value) }

func messagePath(lease *Lease, action string) string {
	return "/v1/messages/" + segment(lease.Message.ID) + "/" + action
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n])
}

// typeOf names an error for the report: its ErrorType() string when it has one, else its
// Go type, looking through the Permanent marker.
func typeOf(err error) string {
	var named interface{ ErrorType() string }
	if errors.As(err, &named) {
		return named.ErrorType()
	}
	if p, ok := err.(*permanentError); ok {
		err = p.err
	}
	return strings.TrimPrefix(fmt.Sprintf("%T", err), "*")
}

type discard struct{}

func (discard) Enabled(context.Context, slog.Level) bool  { return false }
func (discard) Handle(context.Context, slog.Record) error { return nil }
func (d discard) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discard) WithGroup(string) slog.Handler           { return d }
