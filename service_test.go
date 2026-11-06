package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"

	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Use real HTTP and TLS over net.Pipe so the tests also run without socket access.
type pipeListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443} }
func (l *pipeListener) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-ctx.Done():
		client.Close()
		server.Close()
		return nil, ctx.Err()
	case <-l.done:
		client.Close()
		server.Close()
		return nil, net.ErrClosed
	}
}
func memoryServer(t *testing.T, h http.Handler, useTLS bool) *httptest.Server {
	t.Helper()
	l := &pipeListener{conns: make(chan net.Conn), done: make(chan struct{})}
	server := &httptest.Server{Listener: l, Config: &http.Server{Handler: h}}
	if useTLS {
		server.StartTLS()
	} else {
		server.Start()
	}
	server.Client().Transport.(*http.Transport).DialContext = l.dial
	t.Cleanup(server.Close)
	return server
}

func testConfig(t *testing.T) config {
	t.Helper()
	return config{DataDir: t.TempDir(), EncryptionKey: bytes.Repeat([]byte{7}, 32), WebhookHosts: map[string]bool{}, Workers: 4, DeliveryWorkers: 4, MaxJobs: 1000, MaxDeliveryAttempts: 3, ModelTimeout: time.Second, WebhookTimeout: time.Second, Retention: time.Hour}
}

type receivedWebhook struct {
	Body    string            `json:"body"`
	Headers map[string]string `json:"headers"`
}

func fixture(t *testing.T, c config, upstream, hook http.HandlerFunc) (*service, *httptest.Server) {
	t.Helper()
	model := memoryServer(t, upstream, false)
	callback := memoryServer(t, hook, true)
	s, err := newService(c)
	if err != nil {
		t.Fatal(err)
	}
	s.upstream = model.URL
	s.modelClient.CloseIdleConnections()
	s.modelClient = model.Client()
	s.modelClient.Timeout = c.ModelTimeout
	s.webhookClient.CloseIdleConnections()
	s.webhookClient = callback.Client()
	s.webhookClient.Timeout = c.WebhookTimeout
	api := memoryServer(t, s.handler(), false)
	t.Cleanup(s.close)
	return s, api
}
func startTest(t *testing.T, s *service) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s.start(ctx)
	t.Cleanup(func() { cancel(); s.wg.Wait() })
}
func requestTest(t *testing.T, api *httptest.Server, method, path, token, body, key string) (int, http.Header, map[string]json.RawMessage) {
	t.Helper()
	req, _ := http.NewRequest(method, api.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	res, err := api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var got map[string]json.RawMessage
	if err = json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, res.Header, got
}

const validSubmission = `{"model":"test/model","messages":[{"role":"user","content":"hi"}],"temperature":0.2,"background":true,"webhook_url":"https://example.com/callback"}`

func submitTest(t *testing.T, api *httptest.Server, token, key, body string) string {
	t.Helper()
	status, _, got := requestTest(t, api, "POST", "/api/v1/chat/completions", token, body, key)
	if status != 200 {
		t.Fatalf("submit: %d %s", status, got["error"])
	}
	var id string
	_ = json.Unmarshal(got["id"], &id)
	if id == "" {
		t.Fatal("missing ID")
	}
	return id
}
func waitStatus(t *testing.T, api *httptest.Server, token, id, state string) map[string]json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, h, got := requestTest(t, api, "GET", "/api/v1/chat/completions/"+id, token, "", "")
		if status != 200 {
			t.Fatalf("retrieve status %d", status)
		}
		if h.Get("X-Webhook-State") == state {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, state)
	return nil
}
func TestFinalEvents(t *testing.T) {
	for _, tc := range []struct {
		name       string
		code       int
		body, want string
		slow       bool
	}{
		{"success", 200, `{"id":"upstream-1","choices":[{"message":{"content":"hello"}}]}`, "completed", false},
		{"provider_error", 429, `{"error":{"code":429,"message":"rate limited"}}`, "failed", false},
		{"embedded_error", 200, `{"error":{"code":502}}`, "failed", false},
		{"completion_error", 200, `{"choices":[{"finish_reason":"error"}]}`, "failed", false},
		{"invalid_json", 200, `<html>bad gateway</html>`, "failed", false},
		{"null", 200, `null`, "failed", false},
		{"timeout", 200, `{}`, "failed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			c.ModelTimeout = 50 * time.Millisecond
			events := make(chan event, 1)
			s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer caller-one" {
					t.Error("caller token was not forwarded")
				}
				var data map[string]json.RawMessage
				_ = json.NewDecoder(r.Body).Decode(&data)
				if data["background"] != nil || data["webhook_url"] != nil || data["webhook_secret"] != nil {
					t.Error("proxy options leaked upstream")
				}
				if string(data["temperature"]) != "0.2" {
					t.Error("model parameter lost")
				}
				if tc.slow {
					select {
					case <-r.Context().Done():
						return
					case <-time.After(100 * time.Millisecond):
					}
				}
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, tc.body)
			}, func(w http.ResponseWriter, r *http.Request) {
				var e event
				_ = json.NewDecoder(r.Body).Decode(&e)
				events <- e
				w.WriteHeader(204)
			})
			startTest(t, s)
			id := submitTest(t, api, "caller-one", "", validSubmission)
			got := waitStatus(t, api, "caller-one", id, "delivered")
			if string(got["status"]) != fmt.Sprintf("%q", tc.want) {
				t.Fatalf("result %v", got)
			}
			e := <-events
			if e.Type != "chat.completion."+tc.want || e.Object != "event" {
				t.Fatalf("event %+v", e)
			}
			if tc.name == "success" && string(got["upstream_id"]) != `"upstream-1"` {
				t.Error("upstream ID lost")
			}
		})
	}
}
func TestTokenIsolationIdempotencyAndDurability(t *testing.T) {
	c := testConfig(t)
	c.MaxJobs = 2
	var upstreamTokens sync.Map
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		upstreamTokens.Store(r.Header.Get("Authorization"), true)
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	status, _, _ := requestTest(t, api, "POST", "/v1/chat/completions", "", validSubmission, "")
	if status != 401 {
		t.Fatal("missing bearer accepted")
	}
	id := submitTest(t, api, "private-token-a", "same", validSubmission)
	id2 := submitTest(t, api, "private-token-a", "same", validSubmission)
	if id != id2 {
		t.Fatal("duplicate job")
	}
	status, _, _ = requestTest(t, api, "POST", "/v1/chat/completions", "private-token-a", strings.Replace(validSubmission, "hi", "other", 1), "same")
	if status != 409 {
		t.Fatal("changed body accepted with same key")
	}
	other := submitTest(t, api, "private-token-b", "same", validSubmission)
	if other == id {
		t.Fatal("tokens share idempotency keys")
	}
	for _, path := range []string{"/v1/chat/completions/" + id, "/v1/chat/completions/" + id + "/redeliver"} {
		status, _, _ = requestTest(t, api, "GET", path, "private-token-b", "", "")
		if status != 404 {
			t.Fatal("another caller can see job")
		}
	}
	status, _, _ = requestTest(t, api, "POST", "/v1/chat/completions", "private-token-a", validSubmission, "")
	if status != 503 {
		t.Fatal("capacity limit ignored")
	}
	for _, jID := range []string{id, other} {
		data, err := os.ReadFile(filepath.Join(c.DataDir, jID+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("private-token-")) {
			t.Fatal("plaintext API key persisted")
		}
	}
	s.close()
	reopened, err := newService(c)
	if err != nil {
		t.Fatal(err)
	}
	reopened.upstream = s.upstream
	reopened.modelClient = s.modelClient
	reopened.webhookClient = s.webhookClient
	s.store = reopened.store
	api2 := memoryServer(t, reopened.handler(), false)
	if resumed := submitTest(t, api2, "private-token-a", "same", validSubmission); resumed != id {
		t.Fatal("idempotency lost on restart")
	}
	startTest(t, reopened)
	waitStatus(t, api2, "private-token-a", id, "delivered")
	waitStatus(t, api2, "private-token-b", other, "delivered")
	if _, ok := upstreamTokens.Load("Bearer private-token-a"); !ok {
		t.Fatal("first token lost on restart")
	}
	if _, ok := upstreamTokens.Load("Bearer private-token-b"); !ok {
		t.Fatal("second token lost on restart")
	}
	data, _ := os.ReadFile(filepath.Join(c.DataDir, id+".json"))
	var record map[string]json.RawMessage
	_ = json.Unmarshal(data, &record)
	if record["credential"] != nil {
		t.Fatal("completed job retains API credential")
	}
}
func TestWebhookRetryExhaustionAndRedelivery(t *testing.T) {
	c := testConfig(t)
	c.MaxDeliveryAttempts = 2
	var calls, models atomic.Int32
	var mu sync.Mutex
	var ids []string
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		models.Add(1)
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ids = append(ids, r.Header.Get("Webhook-Id"))
		mu.Unlock()
		if calls.Add(1) <= 2 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	})
	startTest(t, s)
	id := submitTest(t, api, "caller", "", validSubmission)
	waitStatus(t, api, "caller", id, "dead")
	code, _, _ := requestTest(t, api, "POST", "/v1/chat/completions/"+id+"/redeliver", "caller", "", "")
	if code != 200 {
		t.Fatal(code)
	}
	waitStatus(t, api, "caller", id, "delivered")
	if models.Load() != 1 || calls.Load() != 3 {
		t.Fatal("delivery retry repeated a model call")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 3 || ids[0] != ids[1] || ids[0] != ids[2] {
		t.Fatal("event ID changed on retry")
	}
}
func TestRestartDuringModelCall(t *testing.T) {
	c := testConfig(t)
	var calls atomic.Int32
	entered := make(chan struct{})
	events := make(chan event, 1)
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}, func(w http.ResponseWriter, r *http.Request) {
		var e event
		_ = json.NewDecoder(r.Body).Decode(&e)
		events <- e
		w.WriteHeader(204)
	})
	id := submitTest(t, api, "caller", "", validSubmission)
	ctx, cancel := context.WithCancel(context.Background())
	s.start(ctx)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("model did not start")
	}
	cancel()
	s.wg.Wait()
	// Recreate the durable state left by a hard stop during the request.
	s.store.mu.Lock()
	j := *s.store.jobs[id]
	j.State = "running"
	j.Event = nil
	j.Result = nil
	if err := s.store.save(&j); err != nil {
		t.Fatal(err)
	}
	s.store.mu.Unlock()
	s.close()
	reopened, err := newService(c)
	if err != nil {
		t.Fatal(err)
	}
	reopened.upstream = s.upstream
	reopened.modelClient = s.modelClient
	reopened.webhookClient = s.webhookClient
	s.store = reopened.store
	restarted := memoryServer(t, reopened.handler(), false)
	startTest(t, reopened)
	got := waitStatus(t, restarted, "caller", id, "delivered")
	e := <-events
	if e.Type != "chat.completion.incomplete" || calls.Load() != 1 || string(got["status"]) != `"incomplete"` {
		t.Fatal("restart repeated an uncertain model call")
	}
}
func TestThreeHundredConcurrentModelCalls(t *testing.T) {
	c := testConfig(t)
	c.Workers = 300
	c.ModelTimeout = 30 * time.Second
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var active, peak, callbacks atomic.Int32
	entered := make(chan struct{}, 300)
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}, func(w http.ResponseWriter, r *http.Request) { callbacks.Add(1); w.WriteHeader(204) })
	ids := make([]string, 305)
	for i := range ids {
		ids[i] = submitTest(t, api, "caller", "", validSubmission)
	}
	startTest(t, s)
	deadline := time.After(25 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case <-entered:
		case <-deadline:
			t.Fatalf("only %d concurrent calls started", i)
		}
	}
	once.Do(func() { close(release) })
	for _, id := range ids {
		waitStatus(t, api, "caller", id, "delivered")
	}
	if peak.Load() != 300 || callbacks.Load() != 305 {
		t.Fatalf("peak=%d callbacks=%d", peak.Load(), callbacks.Load())
	}
	t.Logf("305 jobs completed; peak model concurrency %d", peak.Load())
}
func TestWebhookNetworkPolicy(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "::1", "::ffff:127.0.0.1", "10.1.2.3", "169.254.169.254", "100.100.100.200", "192.0.2.1", "64:ff9b::a00:1", "2002:a00:1::1"} {
		if publicIP(netip.MustParseAddr(ip)) {
			t.Errorf("accepted blocked address %s", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !publicIP(netip.MustParseAddr(ip)) {
			t.Errorf("blocked public address %s", ip)
		}
	}
	client := newClient(time.Second, true)
	defer client.CloseIdleConnections()
	if res, err := client.Get("https://127.0.0.1/"); err == nil {
		res.Body.Close()
		t.Fatal("private network request accepted")
	}
	for _, url := range []string{"http://example.com/", "https://example.com:8080/", "https://user:pass@example.com/", "https://example.com/#fragment"} {
		if validateWebhook(url, nil) == nil {
			t.Errorf("accepted %s", url)
		}
	}
	if validateWebhook("https://other.example.com/", map[string]bool{"example.com": true}) == nil {
		t.Fatal("ignored host list")
	}
	if validateWebhook("https://other.example.com/", nil) != nil {
		t.Fatal("public service requires host registration")
	}
}
func TestWebhookIsRequiredForEveryTextRequest(t *testing.T) {
	c := testConfig(t)
	var upstreamCalls atomic.Int32
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	for _, path := range []string{"/api/v1/chat/completions", "/v1/responses"} {
		for _, body := range []string{
			`{"model":"test/model","messages":[]}`,
			`{"model":"test/model","input":"hello","background":true}`,
			`{"background":true,"webhook_url":""}`,
			`{"background":true,"webhook_url":null}`,
			`{"background":"true"}`,
			`{"background":true,"stream":true,"webhook_url":"https://example.com"}`,
			`{"webhook_url":"http://example.com"}`,
			`{"webhook_url":"https://example.com","webhook_secret":"bad"}`,
			`{"background":false,"webhook_url":"https://example.com"}`,
		} {
			code, _, _ := requestTest(t, api, "POST", path, "caller", body, "")
			if code != 400 {
				t.Errorf("status %d for %s %s; want 400", code, path, body)
			}
		}
	}
	for _, path := range []string{"/v1/completions", "/v1/messages", "/v1/other-text-route"} {
		code, _, _ := requestTest(t, api, "POST", path, "caller", `{"model":"test/model"}`, "")
		if code != 404 {
			t.Errorf("unsupported submission route %s returned %d", path, code)
		}
	}
	if upstreamCalls.Load() != 0 {
		t.Fatal("request without a valid webhook reached OpenRouter")
	}
	// A valid webhook starts an asynchronous job without a background opt-in.
	id := submitTest(t, api, "caller", "", `{"model":"test/model","messages":[],"webhook_url":"https://example.com/callback"}`)
	if upstreamCalls.Load() != 0 {
		t.Fatal("model call ran inside the submission handler")
	}
	startTest(t, s)
	got := waitStatus(t, api, "caller", id, "delivered")
	if string(got["status"]) != `"completed"` || upstreamCalls.Load() != 1 {
		t.Fatal("webhook job did not finish exactly once")
	}
}
