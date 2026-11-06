package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type logCapture struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (c *logCapture) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.Write(data)
}
func (c *logCapture) String() string { c.mu.Lock(); defer c.mu.Unlock(); return c.data.String() }

func TestRequestAndJobLogsExcludeSecrets(t *testing.T) {
	capture := new(logCapture)
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(capture, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	c := testConfig(t)
	secret := newSigningSecret()
	var attempts atomic.Int32
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"id":"provider-1","choices":[{"message":{"content":"private-answer-marker"}}]}`)
	}, func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	s.start(ctx)
	t.Cleanup(func() { cancel(); s.wg.Wait() })
	body := `{"model":"test/model","messages":[{"role":"user","content":"private-prompt-marker"}],"webhook_url":"https://example.com/callback?key=private-hook-token","webhook_secret":"` + secret + `"}`
	status, headers, got := requestTest(t, api, "POST", "/api/v1/chat/completions?key=private-query-marker", "private-caller-token", body, "")
	if status != 200 {
		t.Fatal(status)
	}
	requestID := headers.Get("X-Request-Id")
	var id string
	_ = json.Unmarshal(got["id"], &id)
	waitStatus(t, api, "private-caller-token", id, "delivered")
	rejected, _ := http.NewRequest("POST", api.URL+"/v1/chat/completions?key=private-rejected-query", strings.NewReader(`{"messages":[{"role":"user","content":"private-rejected-prompt"}]}`))
	rejected.Header.Set("Authorization", "Bearer private-rejected-token")
	res, err := api.Client().Do(rejected)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatal(res.StatusCode)
	}
	cancel()
	s.wg.Wait()
	api.Close()
	raw := capture.String()
	for _, sensitive := range []string{"private-caller-token", "private-prompt-marker", "private-answer-marker", "private-hook-token", "private-query-marker", "private-rejected-query", "private-rejected-prompt", "private-rejected-token", secret, "https://example.com/callback"} {
		if strings.Contains(raw, sensitive) {
			t.Fatalf("logs leaked sensitive value %q", sensitive)
		}
	}
	events := map[string]int{}
	sawAccepted, sawRejected, sawRetry, sawDelivered := false, false, false, false
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		message, _ := record["msg"].(string)
		events[message]++
		switch message {
		case "http request":
			if record["status"] == float64(200) && requestID != "" && record["request_id"] == requestID {
				sawAccepted = true
			}
			if record["status"] == float64(400) {
				sawRejected = true
			}
		case "job queued":
			if record["job_id"] != id || record["request_id"] != requestID {
				t.Error("queue event cannot be linked to the accepted request")
			}
		case "model request started", "model request finished", "webhook delivery started", "webhook delivery finished":
			if record["job_id"] != id {
				t.Errorf("%s lacks the job ID", message)
			}
			if message == "webhook delivery finished" {
				if record["webhook_status"] == float64(503) && record["delivery_state"] == "delivering" && record["retry_at"] != nil {
					sawRetry = true
				}
				if record["webhook_status"] == float64(204) && record["delivery_state"] == "delivered" {
					sawDelivered = true
				}
			}
		}
	}
	for _, message := range []string{"job queued", "model request started", "model request finished", "webhook delivery started", "webhook delivery finished"} {
		if events[message] == 0 {
			t.Errorf("missing operational event: %s", message)
		}
	}
	if !sawAccepted || !sawRejected || !sawRetry || !sawDelivered {
		t.Fatalf("missing request/delivery evidence: accepted=%v rejected=%v retry=%v delivered=%v", sawAccepted, sawRejected, sawRetry, sawDelivered)
	}
}

func TestReadOnlyProxyLogsKeepHTTPFlush(t *testing.T) {
	c := testConfig(t)
	release := make(chan struct{})
	defer close(release)
	_, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}, func(http.ResponseWriter, *http.Request) { t.Error("read-only proxy should not send a webhook") })
	req, _ := http.NewRequest("GET", api.URL+"/api/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	client := *api.Client()
	client.Timeout = time.Second
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data := make([]byte, len("data: first\n\n"))
	if _, err = io.ReadFull(res.Body, data); err != nil || string(data) != "data: first\n\n" {
		t.Fatalf("proxy did not flush first event: %q %v", data, err)
	}
	if res.StatusCode != 200 {
		t.Fatal(res.StatusCode)
	}
}
