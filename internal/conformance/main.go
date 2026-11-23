// Command conformance runs the LessorMQ conformance scenarios through this library
// (conformance/php/src/ProcessAdapter.php in the server repository).
//
// One JSON request per line on stdin, one JSON result per line on stdout. Every call goes
// through lessormq.Client, and its results and errors are turned back into the
// {status, body} the scenarios check:
//
//	LESSORMQ_ADAPTER_CMD="cd clients/go && go run ./internal/conformance" conformance/run.sh
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	lessormq "github.com/gsid-nl/lessormq-go"
)

type request struct {
	Call   string          `json:"call"`
	APIKey string          `json:"api_key"`
	Args   json.RawMessage `json:"args"`
}

type args struct {
	Queue        string            `json:"queue"`
	Body         json.RawMessage   `json:"body"`
	Headers      map[string]string `json:"headers"`
	DedupKey     string            `json:"dedup_key"`
	DelaySeconds int               `json:"delay_seconds"`
	AvailableAt  string            `json:"available_at"`
	MaxAttempts  int               `json:"max_attempts"`
	Consumer     string            `json:"consumer"`
	Route        string            `json:"route"`
	WaitSeconds  int               `json:"wait_seconds"`
	LeaseSeconds int               `json:"lease_seconds"`
	Message      string            `json:"message"`
	Receipt      string            `json:"receipt"`
	Permanent    bool              `json:"permanent"`
	MaxMessages  int               `json:"max_messages"`
	Leases       []args            `json:"leases"`
	Error        struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Details any    `json:"details"`
	} `json:"error"`
}

type result struct {
	Status int `json:"status"`
	Body   any `json:"body"`
}

var (
	url     = env("LESSORMQ_URL", "http://127.0.0.1:18080")
	clients = map[string]*lessormq.Client{}
)

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 16<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		if len(in.Bytes()) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			fmt.Fprintln(os.Stderr, "bad request:", err)
			os.Exit(1)
		}
		var a args
		// The runner sends [] for a step without arguments.
		_ = json.Unmarshal(req.Args, &a)
		res, err := call(req.Call, req.APIKey, a)
		if err != nil {
			fmt.Fprintln(os.Stderr, req.Call+":", err)
			os.Exit(1)
		}
		if err := out.Encode(res); err != nil {
			os.Exit(1)
		}
	}
}

func call(name, apiKey string, a args) (result, error) {
	client, ok := clients[apiKey]
	if !ok {
		client = lessormq.New(url, apiKey, lessormq.WithRetryDelay(50*time.Millisecond))
		clients[apiKey] = client
	}
	ctx := context.Background()
	res, err := do(ctx, client, name, a)
	var apiErr *lessormq.APIError
	if errors.As(err, &apiErr) {
		return result{apiErr.Status, map[string]any{"code": apiErr.Code, "detail": apiErr.Detail, "errors": apiErr.Errors}}, nil
	}
	return res, err
}

func do(ctx context.Context, client *lessormq.Client, name string, a args) (result, error) {
	switch name {
	case "publish":
		opts := &lessormq.PublishOptions{Headers: a.Headers, DedupKey: a.DedupKey, DelaySeconds: a.DelaySeconds, MaxAttempts: a.MaxAttempts}
		if a.AvailableAt != "" {
			at, err := time.Parse(time.RFC3339, a.AvailableAt)
			if err != nil {
				return result{}, err
			}
			opts.AvailableAt = at
		}
		body := a.Body
		if body == nil {
			body = json.RawMessage("null")
		}
		p, err := client.Publish(ctx, a.Queue, body, opts)
		if err != nil {
			return result{}, err
		}
		status := 201
		if p.Duplicate {
			status = 200
		}
		return result{status, map[string]any{
			"id": p.ID, "queue": p.Queue, "status": p.Status, "created_at": format(p.CreatedAt),
			"available_at": format(p.AvailableAt), "duplicate": p.Duplicate,
		}}, nil

	case "claim":
		lease, err := client.Claim(ctx, a.Queue, lessormq.ClaimOptions{
			Consumer: a.Consumer, Route: a.Route, WaitSeconds: a.WaitSeconds, LeaseSeconds: a.LeaseSeconds,
		})
		if err != nil || lease == nil {
			return result{204, nil}, err
		}
		return result{200, leaseBody(lease)}, nil

	case "claim_batch":
		leases, err := client.ClaimBatch(ctx, a.Queue, a.MaxMessages, lessormq.ClaimOptions{
			Consumer: a.Consumer, Route: a.Route, WaitSeconds: a.WaitSeconds, LeaseSeconds: a.LeaseSeconds,
		})
		if err != nil || len(leases) == 0 {
			return result{204, nil}, err
		}
		items := make([]any, len(leases))
		for i, l := range leases {
			items[i] = leaseBody(l)
		}
		return result{200, map[string]any{"items": items}}, nil

	case "ack_batch":
		leases := make([]*lessormq.Lease, len(a.Leases))
		for i, l := range a.Leases {
			leases[i] = leaseOf(l)
		}
		outcomes, err := client.AckBatch(ctx, leases)
		if err != nil {
			return result{}, err
		}
		items := make([]any, len(a.Leases))
		for i, l := range a.Leases {
			items[i] = map[string]any{"message": l.Message, "outcome": outcomes[i]}
		}
		return result{200, map[string]any{"items": items}}, nil

	case "extend":
		lease := leaseOf(a)
		if err := client.Extend(ctx, lease, a.LeaseSeconds); err != nil {
			return result{}, err
		}
		return result{200, map[string]any{"lease_until": format(lease.Until())}}, nil

	case "ack":
		return result{204, nil}, client.Ack(ctx, leaseOf(a))

	case "release":
		return result{204, nil}, client.Release(ctx, leaseOf(a), a.DelaySeconds)

	case "fail":
		permanent := a.Permanent
		r, err := client.Fail(ctx, leaseOf(a), errors.New(a.Error.Message),
			&lessormq.FailOptions{Permanent: &permanent, Type: a.Error.Type, Details: a.Error.Details})
		if err != nil {
			return result{}, err
		}
		var availableAt any
		if r.AvailableAt != nil {
			availableAt = format(*r.AvailableAt)
		}
		return result{200, map[string]any{
			"outcome": r.Outcome, "available_at": availableAt, "attempt": r.Attempt, "max_attempts": r.MaxAttempts,
		}}, nil
	}
	return result{}, fmt.Errorf("unknown call %q", name)
}

// leaseOf is a lease as the scenario names it: the message id and the receipt, which may
// be stale or someone else's on purpose.
func leaseBody(lease *lessormq.Lease) map[string]any {
	m := lease.Message
	return map[string]any{
		"receipt":     lease.Receipt,
		"lease_until": format(lease.Until()),
		"message": map[string]any{
			"id": m.ID, "queue": m.Queue, "body": m.Body, "headers": m.Headers, "attempt": m.Attempt,
			"max_attempts": m.MaxAttempts, "created_at": format(m.CreatedAt),
		},
	}
}

func leaseOf(a args) *lessormq.Lease {
	return lessormq.NewLease(a.Receipt, time.Now(), &lessormq.Message{ID: a.Message})
}

func format(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
