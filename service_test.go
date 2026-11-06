package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
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
	return config{DataDir: t.TempDir(), Token: strings.Repeat("t", 32), OpenRouterKey: "fake-upstream-key", WebhookSecret: strings.Repeat("s", 32), WebhookHosts: map[string]bool{"example.com": true}, Workers: 4, DeliveryWorkers: 4, MaxJobs: 1000, MaxDeliveryAttempts: 3, ModelTimeout: time.Second, WebhookTimeout: time.Second, Retention: time.Hour}
}
func fixture(t *testing.T, c config, upstream, hook http.HandlerFunc) (*service, *httptest.Server) {
	t.Helper()
	model := memoryServer(t, upstream, false)
	t.Cleanup(model.Close)
	callback := memoryServer(t, hook, true)
	t.Cleanup(callback.Close)
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
	t.Cleanup(api.Close)
	t.Cleanup(s.close)
	return s, api
}
func startTest(t *testing.T, s *service) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s.start(ctx)
	t.Cleanup(func() { cancel(); s.wg.Wait() })
}
func submitTest(t *testing.T, api *httptest.Server, c config, key string, body string) (string, int) {
	t.Helper()
	r, _ := http.NewRequest("POST", api.URL+"/v1/jobs", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+c.Token)
	r.Header.Set("Idempotency-Key", key)
	res, err := api.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var result struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(res.Body).Decode(&result)
	return result.ID, res.StatusCode
}

const validSubmission = `{"webhook_url":"https://example.com/callback","request":{"model":"test/model","messages":[{"role":"user","content":"hi"}],"temperature":0.2},"metadata":{"order_id":42}}`

func waitStatus(t *testing.T, api *httptest.Server, c config, id, state string) map[string]json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", api.URL+"/v1/jobs/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+c.Token)
		res, err := api.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]json.RawMessage
		err = json.NewDecoder(res.Body).Decode(&got)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got["state"]) == fmt.Sprintf("%q", state) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, state)
	return nil
}
func TestFinalEventsAndSignatures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		response string
		want     string
		slow     bool
	}{
		{"success", 200, `{"id":"upstream-1","choices":[{"message":{"content":"hello"}}]}`, "job.succeeded", false},
		{"provider_error", 429, `{"error":{"code":429,"message":"rate limited"}}`, "job.failed", false},
		{"embedded_error", 200, `{"error":{"code":502}}`, "job.failed", false},
		{"completion_error", 200, `{"choices":[{"finish_reason":"error"}]}`, "job.failed", false},
		{"invalid_json", 200, `<html>bad gateway</html>`, "job.failed", false},
		{"null_response", 200, `null`, "job.failed", false},
		{"timeout", 200, `{}`, "job.timed_out", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			c.ModelTimeout = 50 * time.Millisecond
			events := make(chan event, 1)
			s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fake-upstream-key" {
					t.Error("missing upstream auth")
				}
				var data map[string]json.RawMessage
				_ = json.NewDecoder(r.Body).Decode(&data)
				if string(data["temperature"]) != "0.2" {
					t.Error("proxy dropped request parameter")
				}
				if tc.slow {
					select {
					case <-r.Context().Done():
						return
					case <-time.After(200 * time.Millisecond):
					}
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.response)
			}, func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				mac := hmac.New(sha256.New, []byte(c.WebhookSecret))
				mac.Write([]byte(r.Header.Get("Webhook-Timestamp") + "."))
				mac.Write(data)
				if r.Header.Get("Webhook-Signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
					t.Error("invalid webhook signature")
				}
				var e event
				if err := json.Unmarshal(data, &e); err != nil {
					t.Error(err)
				}
				if r.Header.Get("Webhook-Id") != e.ID {
					t.Error("webhook ID mismatch")
				}
				events <- e
				w.WriteHeader(204)
			})
			startTest(t, s)
			id, status := submitTest(t, api, c, "", validSubmission)
			if status != 202 {
				t.Fatalf("submit status %d", status)
			}
			got := waitStatus(t, api, c, id, "delivered")
			if got["request"] != nil || got["webhook_url"] != nil {
				t.Error("status exposes private input")
			}
			e := <-events
			if e.Type != tc.want || e.JobID != id {
				t.Fatalf("event %+v", e)
			}
			if !tc.slow && json.Valid([]byte(tc.response)) && !bytes.Equal(e.Response, []byte(tc.response)) {
				t.Errorf("response changed: %s", e.Response)
			}
			if string(e.Metadata) != `{"order_id":42}` {
				t.Error("metadata lost")
			}
		})
	}
}
func TestAdmissionAndIdempotency(t *testing.T) {
	c := testConfig(t)
	c.MaxJobs = 1
	s, api := fixture(t, c, func(http.ResponseWriter, *http.Request) { t.Error("upstream called before workers start") }, func(http.ResponseWriter, *http.Request) {})
	res, err := api.Client().Post(api.URL+"/v1/jobs", "application/json", strings.NewReader(validSubmission))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatal("missing auth accepted")
	}
	for _, body := range []string{strings.Replace(validSubmission, "https:", "http:", 1), strings.Replace(validSubmission, "example.com", "localhost", 1), strings.Replace(validSubmission, `"temperature":0.2`, `"stream":true`, 1), validSubmission + `{}`} {
		_, status := submitTest(t, api, c, "", body)
		if status != 400 {
			t.Fatalf("invalid request accepted: %d", status)
		}
	}
	id, status := submitTest(t, api, c, "same", validSubmission)
	if status != 202 {
		t.Fatal(status)
	}
	id2, status := submitTest(t, api, c, "same", validSubmission)
	if status != 202 || id != id2 {
		t.Fatal("duplicate job created")
	}
	_, status = submitTest(t, api, c, "same", strings.Replace(validSubmission, "hi", "different", 1))
	if status != 409 {
		t.Fatal("key reuse did not conflict")
	}
	_, status = submitTest(t, api, c, "", validSubmission)
	if status != 503 {
		t.Fatal("capacity limit ignored")
	}
	// Open the real durable queue again and repeat the HTTP request.
	s.close()
	reopened, err := newService(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	s.store = reopened.store
	reopenedAPI := memoryServer(t, reopened.handler(), false)
	defer reopenedAPI.Close()
	id2, status = submitTest(t, reopenedAPI, c, "same", validSubmission)
	if status != 202 || id != id2 {
		t.Fatal("idempotency lost on restart")
	}
}
func TestWebhookRetryExhaustionAndRedelivery(t *testing.T) {
	c := testConfig(t)
	c.MaxDeliveryAttempts = 2
	var calls atomic.Int32
	var models atomic.Int32
	var eventMu sync.Mutex
	var ids []string
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		models.Add(1)
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}, func(w http.ResponseWriter, r *http.Request) {
		eventMu.Lock()
		ids = append(ids, r.Header.Get("Webhook-Id"))
		eventMu.Unlock()
		if calls.Add(1) <= 2 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	})
	startTest(t, s)
	id, _ := submitTest(t, api, c, "", validSubmission)
	waitStatus(t, api, c, id, "dead")
	req, _ := http.NewRequest("POST", api.URL+"/v1/jobs/"+id+"/redeliver", nil)
	req.Header.Set("Authorization", "Bearer "+c.Token)
	res, err := api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 202 {
		t.Fatal(res.StatusCode)
	}
	waitStatus(t, api, c, id, "delivered")
	if models.Load() != 1 || calls.Load() != 3 {
		t.Fatal("redelivery repeated model call or failed to retry")
	}
	eventMu.Lock()
	defer eventMu.Unlock()
	if len(ids) != 3 || ids[0] != ids[1] || ids[0] != ids[2] {
		t.Fatal("event ID changed across retries")
	}
}
func TestRestartDuringModelCall(t *testing.T) {
	c := testConfig(t)
	var calls atomic.Int32
	entered := make(chan struct{})
	got := make(chan event, 1)
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}, func(w http.ResponseWriter, r *http.Request) {
		var e event
		_ = json.NewDecoder(r.Body).Decode(&e)
		got <- e
		w.WriteHeader(204)
	})
	id, _ := submitTest(t, api, c, "", validSubmission)
	ctx, cancel := context.WithCancel(context.Background())
	s.start(ctx)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("model did not start")
	}
	cancel()
	s.wg.Wait()
	// A hard stop can leave the last durable state as running.
	s.store.mu.Lock()
	j := *s.store.jobs[id]
	j.State = "running"
	j.Event = nil
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
	reopened.webhookClient = s.webhookClient
	s.store = reopened.store
	restartedAPI := memoryServer(t, reopened.handler(), false)
	defer restartedAPI.Close()
	startTest(t, reopened)
	waitStatus(t, restartedAPI, c, id, "delivered")
	e := <-got
	if e.Type != "job.interrupted" || calls.Load() != 1 {
		t.Fatal("restart repeated an uncertain model call")
	}
}
func TestThreeHundredConcurrentModelCalls(t *testing.T) {
	c := testConfig(t)
	c.Workers = 300
	c.ModelTimeout = 20 * time.Second
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var active, peak atomic.Int32
	var callbacks atomic.Int32
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
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}, func(w http.ResponseWriter, r *http.Request) { callbacks.Add(1); w.WriteHeader(204) })
	var ids []string
	for i := 0; i < 305; i++ {
		id, status := submitTest(t, api, c, "", validSubmission)
		if status != 202 {
			t.Fatal(status)
		}
		ids = append(ids, id)
	}
	startTest(t, s)
	deadline := time.After(15 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case <-entered:
		case <-deadline:
			t.Fatalf("only %d concurrent calls started", i)
		}
	}
	if peak.Load() != 300 {
		t.Fatalf("peak %d", peak.Load())
	}
	once.Do(func() { close(release) })
	for _, id := range ids {
		waitStatus(t, api, c, id, "delivered")
	}
	if peak.Load() > 300 || callbacks.Load() != 305 {
		t.Fatalf("peak=%d callbacks=%d", peak.Load(), callbacks.Load())
	}
	t.Logf("Completed 305 jobs; peak concurrent model calls: %d", peak.Load())
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
	for _, url := range []string{"http://example.com/", "https://example.com:8080/", "https://user:pass@example.com/", "https://example.com/#fragment", "https://other.example.com/"} {
		if validateWebhook(url, map[string]bool{"example.com": true}) == nil {
			t.Errorf("accepted %s", url)
		}
	}
}
