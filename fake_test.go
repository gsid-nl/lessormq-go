package lessormq

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

const leaseJSON = `{"receipt":"r1","lease_until":"2030-01-01T00:00:30Z","message":{"id":"m1","queue":"mail",` +
	`"body":{"to":"ada@example.com"},"headers":{"type":"Welcome"},"attempt":1,"max_attempts":7,"created_at":"2030-01-01T00:00:00Z"}}`

const failJSON = `{"outcome":"retrying","available_at":"2030-01-01T00:00:10Z","attempt":1,"max_attempts":7}`

const publishedJSON = `{"id":"m1","queue":"mail","status":"pending","created_at":"2030-01-01T00:00:00Z",` +
	`"available_at":"2030-01-01T00:00:00Z","duplicate":false}`

// answer is one scripted response; drop closes the connection instead.
type answer struct {
	status int
	body   string
	header map[string]string
	drop   bool
}

// fake is a scripted LessorMQ server: answers queued per path (the last one repeats),
// every request recorded.
type fake struct {
	t        *testing.T
	mu       sync.Mutex
	answers  map[string][]answer
	requests []recorded
	server   *httptest.Server
	// onRequest runs before each answer.
	onRequest func(path string)
}

type recorded struct {
	path   string
	header http.Header
	body   map[string]any
}

func newFake(t *testing.T) *fake {
	f := &fake{t: t, answers: map[string][]answer{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fake) on(path string, answers ...answer) *fake {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[path] = append(f.answers[path], answers...)
	return f
}

func (f *fake) calls(path string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, r := range f.requests {
		if r.path == path {
			out = append(out, r.body)
		}
	}
	return out
}

func (f *fake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fake) client(options ...Option) *Client {
	return New(f.server.URL+"/", "lmq_key", append([]Option{WithRetryDelay(0)}, options...)...)
}

func (f *fake) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	body := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			f.t.Errorf("request body is not a JSON object: %s", raw)
		}
	}
	path := r.URL.EscapedPath()

	f.mu.Lock()
	f.requests = append(f.requests, recorded{path: path, header: r.Header.Clone(), body: body})
	hook := f.onRequest
	queue := f.answers[path]
	var a answer
	switch {
	case len(queue) == 0:
		a = answer{status: 404, body: `{"code":"not_found","detail":"no answer scripted"}`}
	case len(queue) == 1:
		a = queue[0]
	default:
		a, f.answers[path] = queue[0], queue[1:]
	}
	f.mu.Unlock()

	if hook != nil {
		hook(path)
	}
	if a.drop {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	for name, value := range a.header {
		w.Header().Set(name, value)
	}
	if a.body != "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(a.status)
	_, _ = io.WriteString(w, a.body)
}

func ok(body string) answer            { return answer{status: 200, body: body} }
func noContent() answer                { return answer{status: 204} }
func status(s int, body string) answer { return answer{status: s, body: body} }

var errTest = errors.New("test")
