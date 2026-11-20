package lessormq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func worker(f *fake, handler Handler, options WorkerOptions) *Worker {
	if options.Consumer == "" {
		options.Consumer = "w1"
	}
	if options.WaitSeconds == 0 {
		options.WaitSeconds = 1
	}
	if options.MaxMessages == 0 {
		options.MaxMessages = 1
	}
	return NewWorker(f.client(), "mail", handler, options)
}

func TestAHandlerThatReturnsNilAcks(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).on("/v1/messages/m1/ack", noContent())
	var seen *Message

	w := worker(f, func(ctx context.Context, job *Job) error { seen = job.Message; return nil }, WorkerOptions{})
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if w.Handled() != 1 || seen == nil || seen.ID != "m1" || len(f.calls("/v1/messages/m1/ack")) != 1 {
		t.Errorf("handled %d, seen %v, acks %v", w.Handled(), seen, f.calls("/v1/messages/m1/ack"))
	}
	if claim := f.calls("/v1/queues/mail/claims")[0]; claim["consumer"] != "w1" || claim["wait_seconds"] != 1.0 {
		t.Errorf("claim %v", claim)
	}
}

func TestAHandlerThatFailsOrPanicsIsReportedAndTheWorkerGoesOn(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).on("/v1/messages/m1/fail", ok(failJSON))
	var calls atomic.Int32

	w := worker(f, func(ctx context.Context, job *Job) error {
		if calls.Add(1) == 1 {
			return Permanent(errTest)
		}
		panic("boom")
	}, WorkerOptions{MaxMessages: 2})
	_ = w.Run(ctx)

	sent := f.calls("/v1/messages/m1/fail")
	if w.Handled() != 2 || len(sent) != 2 || sent[0]["permanent"] != true {
		t.Fatalf("handled %d, sent %v", w.Handled(), sent)
	}
	if report := sent[1]["error"].(map[string]any); report["type"] != "panic" || report["message"] != "panic: boom" {
		t.Errorf("panic reported as %v", report)
	}
}

func TestTheLeaseIsExtendedWhileTheHandlerRuns(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).
		on("/v1/messages/m1/extend", ok(`{"lease_until":"2030-01-01T00:01:00Z"}`)).
		on("/v1/messages/m1/ack", noContent())

	_ = worker(f, func(ctx context.Context, job *Job) error {
		time.Sleep(350 * time.Millisecond)
		return nil
	}, WorkerOptions{ExtendEvery: 100 * time.Millisecond, LeaseSeconds: 60}).Run(ctx)

	extends := f.calls("/v1/messages/m1/extend")
	if len(extends) < 2 || len(extends) > 4 || extends[0]["lease_seconds"] != 60.0 || len(f.calls("/v1/messages/m1/ack")) != 1 {
		t.Errorf("extends %v, acks %v", extends, f.calls("/v1/messages/m1/ack"))
	}
}

func TestALostLeaseCancelsTheHandlerAndIsNotReported(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).
		on("/v1/messages/m1/extend", status(409, `{"code":"lease_lost"}`))
	var cause error

	_ = worker(f, func(ctx context.Context, job *Job) error {
		<-ctx.Done()
		cause = context.Cause(ctx)
		return ctx.Err()
	}, WorkerOptions{ExtendEvery: 50 * time.Millisecond}).Run(ctx)

	if !errors.Is(cause, ErrLeaseLost) || len(f.calls("/v1/messages/m1/ack")) != 0 || len(f.calls("/v1/messages/m1/fail")) != 0 {
		t.Errorf("cause %v, acks %v, fails %v", cause, f.calls("/v1/messages/m1/ack"), f.calls("/v1/messages/m1/fail"))
	}
}

func TestALostAckDoesNotStopTheWorker(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).on("/v1/messages/m1/ack", status(409, `{"code":"lease_lost"}`))

	w := worker(f, func(context.Context, *Job) error { return nil }, WorkerOptions{MaxMessages: 2})
	_ = w.Run(ctx)

	if w.Handled() != 2 {
		t.Errorf("handled %d", w.Handled())
	}
}

func TestAClaimFailurePausesAndGoesOn(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", status(503, `{"code":"internal"}`), ok(leaseJSON)).
		on("/v1/messages/m1/ack", noContent())
	w := NewWorker(f.client(WithRetries(0)), "mail", func(context.Context, *Job) error { return nil },
		WorkerOptions{ErrorPause: 200 * time.Millisecond, MaxMessages: 1})

	started := time.Now()
	_ = w.Run(ctx)

	if w.Handled() != 1 || time.Since(started) < 190*time.Millisecond {
		t.Errorf("handled %d in %v", w.Handled(), time.Since(started))
	}
}

func TestCancellingDuringAClaimGivesTheMessageBack(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).on("/v1/messages/m1/release", noContent())
	stop, cancel := context.WithCancel(ctx)
	f.onRequest = func(path string) {
		if path == "/v1/queues/mail/claims" {
			cancel()
		}
	}
	var ran atomic.Bool

	w := worker(f, func(context.Context, *Job) error { ran.Store(true); return nil }, WorkerOptions{MaxMessages: 5})
	if err := w.Run(stop); err != nil {
		t.Fatal(err)
	}

	if ran.Load() || w.Handled() != 0 || len(f.calls("/v1/messages/m1/release")) != 1 {
		t.Errorf("ran %v, handled %d, releases %v", ran.Load(), w.Handled(), f.calls("/v1/messages/m1/release"))
	}
}

func TestConcurrencyRunsHandlersSideBySideAndKeepsToMaxMessages(t *testing.T) {
	f := newFake(t).on("/v1/queues/mail/claims", ok(leaseJSON)).
		on("/v1/messages/m1/ack", noContent()).on("/v1/messages/m1/release", noContent())
	var running, peak atomic.Int32
	var mu sync.Mutex

	w := worker(f, func(context.Context, *Job) error {
		n := running.Add(1)
		mu.Lock()
		if n > peak.Load() {
			peak.Store(n)
		}
		mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		running.Add(-1)
		return nil
	}, WorkerOptions{Concurrency: 4, MaxMessages: 6})
	started := time.Now()
	_ = w.Run(ctx)

	if w.Handled() != 6 || len(f.calls("/v1/messages/m1/ack")) != 6 || peak.Load() != 4 || time.Since(started) > time.Second {
		t.Errorf("handled %d, acks %d, peak %d, took %v", w.Handled(), len(f.calls("/v1/messages/m1/ack")), peak.Load(), time.Since(started))
	}
}
