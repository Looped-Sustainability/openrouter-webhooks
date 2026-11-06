package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const nativeChat = `{"id":"upstream-chat","object":"chat.completion","created":1700000000,"model":"test/model","system_fingerprint":null,"choices":[{"index":0,"message":{"role":"assistant","content":"Hello"},"finish_reason":"stop"}]}`
const nativeResponse = `{"id":"upstream-response","object":"response","created_at":1700000000,"completed_at":1700000001,"status":"completed","model":"test/model","error":null,"incomplete_details":null,"frequency_penalty":null,"presence_penalty":null,"instructions":null,"metadata":{},"parallel_tool_calls":true,"temperature":1,"top_p":1,"tool_choice":"auto","tools":[],"output":[{"id":"msg_test","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello","annotations":[]}]}]}`

func TestRealClients(t *testing.T) {
	if _, err := os.Stat("node_modules/@openrouter/sdk/package.json"); err != nil {
		t.Fatal("install client test dependencies first: npm ci --ignore-scripts --legacy-peer-deps")
	}
	c := testConfig(t)
	callbacks := make(chan receivedWebhook, 10)
	var requestsA, requestsB atomic.Int32
	s, _ := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer caller-a":
			requestsA.Add(1)
		case "Bearer caller-b":
			requestsB.Add(1)
		case "Bearer invalid":
			writeJSON(w, 401, map[string]any{"error": map[string]any{"code": 401, "message": "Invalid upstream key"}})
			return
		default:
			t.Error("wrong caller token forwarded")
		}
		var body map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		for _, key := range []string{"background", "webhook_url", "webhook_secret"} {
			if body[key] != nil {
				t.Errorf("proxy field %s reached upstream", key)
			}
		}
		if r.Header.Get("X-Background") != "" || r.Header.Get("X-Webhook-Secret") != "" {
			t.Error("proxy header reached upstream")
		}
		if r.URL.Path == "/responses" {
			writeJSON(w, 200, json.RawMessage(nativeResponse))
			return
		}
		if r.URL.Path != "/chat/completions" {
			t.Errorf("wrong upstream path %s", r.URL.Path)
		}
		writeJSON(w, 200, json.RawMessage(nativeChat))
	}, func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		headers := map[string]string{}
		for key, values := range r.Header {
			headers[strings.ToLower(key)] = strings.Join(values, ",")
		}
		callbacks <- receivedWebhook{string(data), headers}
		w.WriteHeader(204)
	})
	startTest(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "scripts/sdk-test.mjs")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), maxResponseBytes)
	encoder := json.NewEncoder(stdin)
	for scanner.Scan() {
		var message struct {
			ID                           int `json:"id"`
			Operation, Method, URL, Body string
			Headers                      map[string]string
		}
		if err = json.Unmarshal(scanner.Bytes(), &message); err != nil {
			cancel()
			t.Fatal(err)
		}
		reply := map[string]any{"id": message.ID}
		if message.Operation == "webhook" {
			select {
			case callback := <-callbacks:
				reply["body"] = callback.Body
				reply["headers"] = callback.Headers
			case <-ctx.Done():
				cancel()
				t.Fatalf("webhook timeout: %s", stderr.String())
			}
		} else {
			req := httptest.NewRequest(message.Method, message.URL, strings.NewReader(message.Body))
			for k, v := range message.Headers {
				req.Header.Set(k, v)
			}
			recorder := httptest.NewRecorder()
			s.handler().ServeHTTP(recorder, req)
			reply["status"] = recorder.Code
			reply["body"] = recorder.Body.String()
			headers := map[string]string{}
			for k, v := range recorder.Header() {
				headers[k] = strings.Join(v, ",")
			}
			reply["headers"] = headers
		}
		if err = encoder.Encode(reply); err != nil {
			cancel()
			t.Fatal(err)
		}
	}
	stdin.Close()
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err = command.Wait(); err != nil {
		t.Fatalf("client tests: %v\n%s", err, stderr.String())
	}
	if requestsA.Load() != 2 || requestsB.Load() != 2 {
		t.Fatal("SDK requests did not reach upstream with both caller tokens")
	}
	t.Log(strings.TrimSpace(stderr.String()))
}
