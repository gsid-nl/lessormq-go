# LessorMQ for Go

Client and worker for [LessorMQ](https://lessormq.dev), the message queue over HTTP where
the server decides and clients only claim, run, ack or fail. Standard library only:
`net/http`, `context` and `log/slog`. Go 1.22+. Module `github.com/gsid-nl/lessormq-go`.

## Install

```sh
go get github.com/gsid-nl/lessormq-go
```

This module is published from `clients/go` in [gsid-nl/lessormq](https://github.com/gsid-nl/lessormq);
issues and pull requests go there.

## Connect

```go
client := lessormq.New("https://mq.example.com", os.Getenv("LESSORMQ_KEY"))
```

The key is a tenant's API key with the scopes the program needs (`publish`, `consume`).
Options, after the key:

| Option | Default | |
|---|---|---|
| `WithTimeout(d)` | 10s | the deadline per request; a claim gets its wait on top |
| `WithRetries(n)` | 3 | retries after a network error, a 5xx or a 429 |
| `WithRetryDelay(d)` | 100ms | times the attempt number, or the server's `Retry-After` |
| `WithHTTPClient(c)` | `http.DefaultClient` | proxies, TLS, transports; its own `Timeout` zero or above 30s |
| `WithLogger(l)` | nothing logged | retries and a worker's troubles |
| `WithoutAutoDedupKey()` | | a publish without a dedup key gets none, and is then never retried |
| `WithUserAgent(ua)` | | |

## Publish

```go
_, err := client.Publish(ctx, "mail", map[string]string{"to": "ada@example.com"}, nil)

_, err = client.Publish(ctx, "invoices", Invoice{ID: 1042}, &lessormq.PublishOptions{
	DedupKey:     "invoice-1042", // one open message per key
	DelaySeconds: 600,
	Headers:      map[string]string{"type": "InvoiceDue"},
})
```

Every publish carries a dedup key (your own, or one the client makes up), so the client
can safely retry it after a network error without creating a second message.

## Work

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

worker := lessormq.NewWorker(client, "mail", func(ctx context.Context, job *lessormq.Job) error {
	var mail Mail
	if err := job.Message.Decode(&mail); err != nil {
		return lessormq.Permanent(err)
	}
	return send(ctx, mail)
}, lessormq.WorkerOptions{Route: os.Getenv("VPN_ID"), Concurrency: 4})

worker.Run(ctx)
```

- The handler returns nil: the message is acked.
- It returns an error (or panics): the message is failed with the error's text and type.
  Wrap it with `lessormq.Permanent`, or give your error a `Permanent() bool` method, when
  the input itself is unusable. An `ErrorType() string` method names the type.
- What happens after a failure is not up to the worker. The queue's settings decide
  whether and when the message comes back, and when it goes to the dead-letter set.
- While the handler runs, a goroutine extends the lease every third of it. When the lease
  is lost anyway, the handler's context is cancelled with cause `ErrLeaseLost` and the
  result is not reported: the message is someone else's by then.
- `Concurrency` runs that many handlers side by side, each with its own claims.
- Cancelling the context passed to `Run` stops the worker after the current messages.
  Handlers are not interrupted, and a claim already waiting is not cut off: it ends
  within `WaitSeconds`, and a message it brings is given back.

`WorkerOptions`: `Consumer` (the host name by default), `Route`, `WaitSeconds` (20),
`LeaseSeconds`, `ExtendEvery`, `DisableAutoExtend`, `Concurrency` (1), `MaxMessages`,
`ErrorPause`.

## Without the worker

```go
lease, err := client.Claim(ctx, "mail", lessormq.ClaimOptions{WaitSeconds: 20})
if err == nil && lease != nil {
	if err := send(ctx, lease.Message); err != nil {
		result, _ := client.Fail(ctx, lease, err, nil) // result.Outcome: retrying, dead_lettered or discarded
	} else {
		client.Ack(ctx, lease)
	}
}
```

`client.Extend` keeps a lease while a long job runs, and `client.Release` hands a message
back without counting the attempt. All of them return an error matching `ErrLeaseLost`
once the lease has ended: drop the message then, it is someone else's now.

Many short jobs go faster in batches: `client.ClaimBatch(ctx, "mail", 50, opts)` leases up
to 50 ready messages at once (each with its own lease), and `client.AckBatch(ctx, leases)`
acks them in one request, with an outcome per lease (`Acked`, `AckLeaseLost`, ...). A
message that fails is reported on its own with `Fail`.

## Errors

The server's errors are `*lessormq.APIError` (with `Status`, `Code` and `Errors`) and
match with `errors.Is`: `ErrLeaseLost`, `ErrValidation`, `ErrUnauthorized`,
`ErrForbidden`, `ErrNotFound`, `ErrConflict`, `ErrPayloadTooLarge`, `ErrRateLimited` and
`ErrServer`. A server that cannot be reached gives a `*lessormq.TransportError`.

Network errors, 5xx and 429 are retried (see [Connect](#connect)). That is the only
retrying the library does.

## Development

```sh
cd clients/go
go test -race ./...
```

The library passes the LessorMQ [conformance suite](https://github.com/gsid-nl/lessormq/tree/main/conformance)
through `internal/conformance`, from the repository root:

```sh
LESSORMQ_ADAPTER_CMD="cd clients/go && go run ./internal/conformance" conformance/run.sh
```
