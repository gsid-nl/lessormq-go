package lessormq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

var ctx = context.Background()

func TestPublishSendsTheBodyAndAnAutomaticDedupKey(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/messages", status(201, publishedJSON))

	published, err := f.client().Publish(ctx, "mail", map[string]string{"to": "ada@example.com"},
		&PublishOptions{Headers: map[string]string{"type": "Welcome"}, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}

	sent := f.calls("/v1/queues/mail/messages")[0]
	if fmt.Sprint(sent["body"]) != "map[to:ada@example.com]" || sent["max_attempts"] != 3.0 {
		t.Errorf("sent %v", sent)
	}
	if key, _ := sent["dedup_key"].(string); !strings.HasPrefix(key, "lmq-auto-") || len(key) != 33 {
		t.Errorf("dedup_key %v", sent["dedup_key"])
	}
	if _, has := sent["delay_seconds"]; has {
		t.Error("delay_seconds sent")
	}
	if published.ID != "m1" || published.Duplicate || !published.AvailableAt.Equal(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("published %+v", published)
	}
	header := f.requests[0].header
	if header.Get("Authorization") != "Bearer lmq_key" || !strings.HasPrefix(header.Get("User-Agent"), "lessormq-go/") {
		t.Errorf("headers %v", header)
	}
}

func TestPublishKeepsNullBodiesEncodesQueuesAndWritesUTC(t *testing.T) {
	f := newFake(t).on("/v1/queues/q%2F1/messages", status(201, publishedJSON))
	at := time.Date(2030, 1, 1, 3, 0, 0, 500, time.FixedZone("CET", 3600))

	if _, err := f.client().Publish(ctx, "q/1", nil, &PublishOptions{DedupKey: "k1", AvailableAt: at}); err != nil {
		t.Fatal(err)
	}

	got, _ := json.Marshal(f.calls("/v1/queues/q%2F1/messages")[0])
	if string(got) != `{"available_at":"2030-01-01T02:00:00Z","body":null,"dedup_key":"k1"}` {
		t.Errorf("sent %s", got)
	}
}

func TestPublishWithoutADedupKeyIsNotRetried(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/messages", status(503, `{"code":"internal"}`), status(201, publishedJSON))

	_, err := f.client(WithoutAutoDedupKey()).Publish(ctx, "mail", map[string]any{}, nil)

	if !errors.Is(err, ErrServer) || f.count() != 1 {
		t.Errorf("err %v after %d requests", err, f.count())
	}
}

func TestClaimReturnsNilWhenNothingArrived(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", noContent())

	lease, err := f.client().Claim(ctx, "mail", ClaimOptions{WaitSeconds: 5, Route: "vpn-1", Consumer: "w1"})

	if lease != nil || err != nil {
		t.Fatalf("lease %v, err %v", lease, err)
	}
	got, _ := json.Marshal(f.calls("/v1/queues/mail/claims")[0])
	if string(got) != `{"consumer":"w1","route":"vpn-1","wait_seconds":5}` {
		t.Errorf("sent %s", got)
	}
}

func TestClaimReturnsTheLease(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON))

	lease, err := f.client().Claim(ctx, "mail", ClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}

	var body struct{ To string }
	if err := lease.Message.Decode(&body); err != nil || body.To != "ada@example.com" {
		t.Errorf("body %v, %v", body, err)
	}
	if lease.Receipt != "r1" || lease.Message.Headers["type"] != "Welcome" || lease.Message.MaxAttempts != 7 || lease.Message.IsLastAttempt() {
		t.Errorf("lease %+v %+v", lease, lease.Message)
	}
	if !lease.Until().Equal(time.Date(2030, 1, 1, 0, 0, 30, 0, time.UTC)) {
		t.Errorf("until %v", lease.Until())
	}
}

func TestExtendMovesTheLease(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).
		on("/v1/messages/m1/extend", ok(`{"lease_until":"2030-01-01T00:05:00Z"}`))
	c := f.client()
	lease, _ := c.Claim(ctx, "mail", ClaimOptions{})

	if err := c.Extend(ctx, lease, 300); err != nil {
		t.Fatal(err)
	}

	got, _ := json.Marshal(f.calls("/v1/messages/m1/extend")[0])
	if string(got) != `{"lease_seconds":300,"receipt":"r1"}` || !lease.Until().Equal(time.Date(2030, 1, 1, 0, 5, 0, 0, time.UTC)) {
		t.Errorf("sent %s, until %v", got, lease.Until())
	}
}

type badInput struct{ field string }

func (e badInput) Error() string     { return "bad " + e.field }
func (e badInput) Permanent() bool   { return true }
func (e badInput) ErrorType() string { return "BadInput" }

func TestFailReportsTheErrorItsTypeAndPermanence(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).on("/v1/messages/m1/fail", ok(failJSON))
	c := f.client()
	lease, _ := c.Claim(ctx, "mail", ClaimOptions{})
	no := false

	result, err := c.Fail(ctx, lease, fmt.Errorf("send: %w", context.DeadlineExceeded), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Fail(ctx, lease, Permanent(errors.New("no recipient")), nil)
	_, _ = c.Fail(ctx, lease, fmt.Errorf("decode: %w", badInput{"to"}), nil)
	_, _ = c.Fail(ctx, lease, badInput{"to"}, &FailOptions{Permanent: &no, Type: "Custom", Details: map[string]int{"step": 2}})

	sent := f.calls("/v1/messages/m1/fail")
	expect := []string{
		`{"error":{"message":"send: context deadline exceeded","type":"fmt.wrapError"},"permanent":false,"receipt":"r1"}`,
		`{"error":{"message":"no recipient","type":"errors.errorString"},"permanent":true,"receipt":"r1"}`,
		`{"error":{"message":"decode: bad to","type":"BadInput"},"permanent":true,"receipt":"r1"}`,
		`{"error":{"details":{"step":2},"message":"bad to","type":"Custom"},"permanent":false,"receipt":"r1"}`,
	}
	for i, want := range expect {
		got, _ := json.Marshal(sent[i])
		if string(got) != want {
			t.Errorf("fail %d sent\n %s\nwant\n %s", i, got, want)
		}
	}
	if result.Outcome != Retrying || result.AvailableAt == nil || result.Attempt != 1 {
		t.Errorf("result %+v", result)
	}
}

func TestALostLeaseMatchesErrLeaseLost(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).
		on("/v1/messages/m1/ack", status(409, `{"code":"lease_lost","detail":"The lease ended."}`))
	c := f.client()
	lease, _ := c.Claim(ctx, "mail", ClaimOptions{})

	err := c.Ack(ctx, lease)

	var apiErr *APIError
	if !errors.Is(err, ErrLeaseLost) || !errors.As(err, &apiErr) || apiErr.Status != 409 || err.Error() != "lessormq: The lease ended." {
		t.Errorf("err %v", err)
	}
}

func TestServerAndNetworkErrorsAreRetried(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims",
		answer{drop: true}, answer{status: 502, body: "<html>Bad gateway</html>"}, ok(leaseJSON))

	lease, err := f.client().Claim(ctx, "mail", ClaimOptions{})

	if lease == nil || err != nil || f.count() != 3 {
		t.Errorf("lease %v, err %v, %d requests", lease, err, f.count())
	}
}

func TestRetriesRunOut(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", answer{drop: true})

	_, err := f.client(WithRetries(2)).Claim(ctx, "mail", ClaimOptions{})

	var transport *TransportError
	if !errors.As(err, &transport) || f.count() != 3 {
		t.Errorf("err %v after %d requests", err, f.count())
	}
}

func TestARateLimitWaitsForRetryAfter(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims",
		answer{status: 429, body: `{"code":"rate_limited"}`, header: map[string]string{"Retry-After": "0"}})

	_, err := f.client(WithRetries(1)).Claim(ctx, "mail", ClaimOptions{})

	var apiErr *APIError
	if !errors.Is(err, ErrRateLimited) || !errors.As(err, &apiErr) || apiErr.RetryAfter != 0 || f.count() != 2 {
		t.Errorf("err %v after %d requests", err, f.count())
	}
}

func TestValidationErrorsCarryTheFields(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims",
		status(400, `{"code":"validation_failed","errors":[{"field":"lease_seconds","message":"too long"}]}`))

	_, err := f.client().Claim(ctx, "mail", ClaimOptions{LeaseSeconds: 99999})

	var apiErr *APIError
	if !errors.Is(err, ErrValidation) || !errors.As(err, &apiErr) || apiErr.Errors[0].Field != "lease_seconds" || f.count() != 1 {
		t.Errorf("err %v", err)
	}
}

func TestACancelledContextIsNotRetried(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", answer{drop: true})
	cancelled, cancel := context.WithCancel(ctx)
	f.onRequest = func(string) { cancel() }

	_, err := f.client().Claim(cancelled, "mail", ClaimOptions{})

	if !errors.Is(err, context.Canceled) || f.count() != 1 {
		t.Errorf("err %v after %d requests", err, f.count())
	}
}

func TestStats(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail", ok(`{"name":"mail","stats":{"ready":3,"delayed":1,"processing":2,`+
		`"completed":9,"failed":1,"oldest_ready_seconds":12,"oldest_processing_seconds":4}}`))

	stats, err := f.client().Stats(ctx, "mail")

	if err != nil || stats.Ready != 3 || stats.OldestReadySeconds != 12 {
		t.Errorf("stats %+v, %v", stats, err)
	}
}

func TestTheKeyStaysOutOfTheString(t *testing.T) {
	if strings.Contains(newFake(t).client().String(), "lmq_key") {
		t.Error("key in String()")
	}
}

func TestClaimBatchAndAckBatch(t *testing.T) {
	f := newFake(t).
		on("/v1/queues/mail/claims", ok(`{"items":[`+leaseJSON+`,`+strings.Replace(leaseJSON, `"r1"`, `"r2"`, 1)+`]}`), noContent()).
		on("/v1/acks", ok(`{"items":[{"message":"m1","outcome":"acked"},{"message":"m1","outcome":"lease_lost"}]}`))
	c := f.client()

	leases, err := c.ClaimBatch(ctx, "mail", 10, ClaimOptions{WaitSeconds: 2})
	if err != nil || len(leases) != 2 || leases[1].Receipt != "r2" || leases[0].Message.ID != "m1" {
		t.Fatalf("leases %v, err %v", leases, err)
	}
	got, _ := json.Marshal(f.calls("/v1/queues/mail/claims")[0])
	if string(got) != `{"max_messages":10,"wait_seconds":2}` {
		t.Errorf("sent %s", got)
	}
	outcomes, err := c.AckBatch(ctx, leases)
	if err != nil || len(outcomes) != 2 || outcomes[0] != Acked || outcomes[1] != AckLeaseLost {
		t.Fatalf("outcomes %v, err %v", outcomes, err)
	}
	sent, _ := json.Marshal(f.calls("/v1/acks")[0])
	if string(sent) != `{"leases":[{"message":"m1","receipt":"r1"},{"message":"m1","receipt":"r2"}]}` {
		t.Errorf("sent %s", sent)
	}
	none, err := c.ClaimBatch(ctx, "mail", 10, ClaimOptions{})
	if err != nil || len(none) != 0 {
		t.Fatalf("empty claim: %v %v", none, err)
	}
}
