package lessormq

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Job is what a handler gets: the message, its lease, and a way to keep the lease.
type Job struct {
	Message *Message
	Lease   *Lease
	client  *Client
}

// Extend extends the lease now. The worker already does this on a timer; call it only
// with DisableAutoExtend, or to ask for a longer lease at once (0: the queue's lease).
func (j *Job) Extend(ctx context.Context, leaseSeconds int) error {
	return j.client.Extend(ctx, j.Lease, leaseSeconds)
}

// Handler runs one message. Returning nil acks it; returning an error fails it, as
// permanent when IsPermanent(err). ctx is cancelled when the lease is lost while the
// handler runs: stop then, the message is someone else's now.
type Handler func(ctx context.Context, job *Job) error

// WorkerOptions are how a worker claims and keeps its leases. What happens to a failed
// message is the queue's business, so there is nothing about retries here.
type WorkerOptions struct {
	// Consumer is shown to administrators; host:pid by default, so the workers on
	// one host are told apart.
	Consumer string
	// Route is the path this worker runs jobs through (a VPN exit, a proxy, an API account).
	Route string
	// WaitSeconds is the long-poll wait per claim, at most 20. Default 20.
	WaitSeconds int
	// LeaseSeconds is the lease per claim; 0 is the queue's.
	LeaseSeconds int
	// ExtendEvery is the time between automatic extends; a third of the lease by default.
	ExtendEvery time.Duration
	// DisableAutoExtend stops extending the lease while a handler runs.
	DisableAutoExtend bool
	// Concurrency is the number of messages handled at the same time, each in its own
	// goroutine with its own claims. Default 1.
	Concurrency int
	// MaxMessages stops the worker after this many messages; 0 is no limit.
	MaxMessages int
	// ErrorPause is the wait after the server could not be reached. Default 1s.
	ErrorPause time.Duration
}

// Worker claims messages of one queue and runs a handler on each.
//
// What happens after a failure (another attempt, when, or the dead-letter set) is
// decided by the queue. While the handler runs, the lease is extended on a timer.
type Worker struct {
	client  *Client
	queue   string
	handler Handler
	options WorkerOptions
	logger  *slog.Logger
	handled atomic.Int64
	// started counts messages taken on, so concurrent goroutines never pass MaxMessages.
	started atomic.Int64
}

// NewWorker returns a worker for queue on client, running handler on every message.
func NewWorker(client *Client, queue string, handler Handler, options WorkerOptions) *Worker {
	if options.WaitSeconds == 0 {
		options.WaitSeconds = 20
	}
	if options.Concurrency < 1 {
		options.Concurrency = 1
	}
	if options.ErrorPause == 0 {
		options.ErrorPause = time.Second
	}
	if options.Consumer == "" {
		options.Consumer = defaultConsumer()
	}
	return &Worker{client: client, queue: queue, handler: handler, options: options, logger: client.logger}
}

// Run claims and handles messages until ctx is done or MaxMessages are handled, and
// returns once every message in hand is reported.
//
// Cancelling ctx stops the worker after the current messages: handlers are not
// interrupted, and a claim that is already waiting is not cut off (that could leave a
// leased message to run out). It ends within WaitSeconds, and a message it brings is
// given back with Release. Run returns nil then.
func (w *Worker) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	var wg sync.WaitGroup
	for i := 0; i < w.options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil && !w.done() {
				w.RunOnce(ctx)
			}
		}()
	}
	wg.Wait()
	return nil
}

// Handled is the number of messages handled so far.
func (w *Worker) Handled() int { return int(w.handled.Load()) }

func (w *Worker) done() bool {
	return w.options.MaxMessages > 0 && int(w.started.Load()) >= w.options.MaxMessages
}

// take reserves one of MaxMessages for a claimed message.
func (w *Worker) take() bool {
	if w.options.MaxMessages <= 0 {
		return true
	}
	if int(w.started.Add(1)) > w.options.MaxMessages {
		w.started.Add(-1)
		return false
	}
	return true
}

// RunOnce makes one claim and runs the handler on what it got. It reports whether a
// message was handled. Once ctx is done, a message the claim brings is given back.
func (w *Worker) RunOnce(ctx context.Context) bool {
	// Detached from ctx: see Run.
	background := context.WithoutCancel(ctx)
	lease, err := w.client.Claim(background, w.queue, ClaimOptions{
		Consumer:     w.options.Consumer,
		Route:        w.options.Route,
		WaitSeconds:  w.options.WaitSeconds,
		LeaseSeconds: w.options.LeaseSeconds,
	})
	if err != nil {
		w.logger.Error("Claim failed", "queue", w.queue, "error", err)
		timer := time.NewTimer(w.options.ErrorPause)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		return false
	}
	if lease == nil {
		return false
	}
	if ctx.Err() != nil || !w.take() {
		// Asked to stop while the claim waited, or the other goroutines reached
		// MaxMessages: give the message back untouched.
		w.report(lease, "release", func() error { return w.client.Release(background, lease, 0) })
		return false
	}

	w.handle(background, lease)
	w.handled.Add(1)
	return true
}

func (w *Worker) handle(ctx context.Context, lease *Lease) {
	handlerCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	stopKeeping := func() {}
	if !w.options.DisableAutoExtend {
		stopKeeping = w.keep(ctx, lease, cancel)
	}
	err := w.run(handlerCtx, &Job{Message: lease.Message, Lease: lease, client: w.client})
	stopKeeping()

	if errors.Is(context.Cause(handlerCtx), ErrLeaseLost) {
		w.logger.Warn("Lease lost while the handler ran; not reporting", "message_id", lease.Message.ID)
		return
	}
	if err == nil {
		w.report(lease, "ack", func() error { return w.client.Ack(ctx, lease) })
		return
	}
	w.report(lease, "fail", func() error {
		result, failErr := w.client.Fail(ctx, lease, err, nil)
		if failErr == nil {
			w.logger.Info("Message failed", "message_id", lease.Message.ID, "error", err,
				"outcome", result.Outcome, "attempt", result.Attempt, "max_attempts", result.MaxAttempts)
		}
		return failErr
	})
}

// run calls the handler, turning a panic into a failed attempt.
func (w *Worker) run(ctx context.Context, job *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{value: r}
		}
	}()
	return w.handler(ctx, job)
}

// keep extends the lease every interval until stopped; a lost lease cancels the
// handler's context with ErrLeaseLost. It returns the stop, which waits for the extender.
func (w *Worker) keep(ctx context.Context, lease *Lease, lost context.CancelCauseFunc) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			interval := w.options.ExtendEvery
			if interval <= 0 {
				interval = max(time.Second, lease.Remaining()/3)
			}
			timer := time.NewTimer(interval)
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			err := w.client.Extend(ctx, lease, w.options.LeaseSeconds)
			if errors.Is(err, ErrLeaseLost) {
				lost(ErrLeaseLost)
				return
			}
			if err != nil {
				w.logger.Warn("Extend failed; trying again", "error", err)
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// report acks, fails or releases, keeping the worker alive whatever happens: a lost lease
// means the message is someone else's now; any other failure means the lease will expire
// and the server will make the message available again.
func (w *Worker) report(lease *Lease, what string, call func() error) {
	err := call()
	switch {
	case err == nil:
	case errors.Is(err, ErrLeaseLost):
		w.logger.Warn("Lease lost before "+what+"; the message is no longer ours", "message_id", lease.Message.ID)
	default:
		w.logger.Error(what+" failed; the lease will expire and the message come back",
			"message_id", lease.Message.ID, "error", err)
	}
}

type panicError struct{ value any }

func (e *panicError) Error() string     { return "panic: " + toString(e.value) }
func (e *panicError) ErrorType() string { return "panic" }

func toString(v any) string {
	switch v := v.(type) {
	case error:
		return v.Error()
	case string:
		return v
	}
	return slog.AnyValue(v).String()
}

// defaultConsumer is the host and the process, or empty when the host is unknown.
func defaultConsumer() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return ""
	}

	return host + ":" + strconv.Itoa(os.Getpid())
}
