package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func basetenFixture(t *testing.T, c config, upstream, hook http.HandlerFunc) (*service, *httptest.Server) {
	t.Helper()
	s, api := fixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(strings.ToLower(r.Host), "openrouter.invalid") {
			t.Error("baseten job arrived with the openrouter host")
		}
		upstream(w, r)
	}, hook)
	modelURL := s.upstream
	s.upstream = "http://openrouter.invalid"
	s.baseten = modelURL
	return s, api
}

func basetenBody(path string, webhook bool) string {
	base := `"model":"test/model"`
	switch path {
	case "/responses":
		base += `,"input":"hi"`
	case "/systemone":
		base += `,"state":"hi"`
	default:
		base += `,"messages":[{"role":"user","content":"hi"}]`
	}
	if webhook {
		base += `,"webhook_url":"https://example.com/callback"`
	}
	return `{` + base + `}`
}

func waitBaseten(t *testing.T, api *httptest.Server, token, path, id, state string) map[string]json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, h, got := requestTest(t, api, "GET", "/baseten/v1"+path+"/"+id, token, "", "")
		if status != 200 {
			t.Fatalf("retrieve status %d %s", status, got["error"])
		}
		if h.Get("X-Webhook-State") == state {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, state)
	return nil
}

func TestVersion2JobsWithoutProviderStillLoad(t *testing.T) {
	c := testConfig(t)
	now := nowUTC()
	id := "chatcmpl_proxy_" + newID()
	record := job{Version: 2, ID: id, Owner: ownerID("caller"), Path: "/chat/completions", Model: "test/model", State: "delivered", CreatedAt: now, UpdatedAt: now, Result: json.RawMessage(`{"id":"` + id + `","status":"completed"}`)}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"provider"`) {
		t.Fatal("empty provider should be omitted from version 2 files")
	}
	if err = os.WriteFile(filepath.Join(c.DataDir, id+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := newService(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := s.store.jobs[id]
	if j == nil || j.Provider != "" || j.Version != 2 {
		t.Fatalf("loaded job %+v", j)
	}
}

func TestBasetenTextPathsRequireWebhook(t *testing.T) {
	c := testConfig(t)
	var upstreamCalls atomic.Int32
	_, api := basetenFixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}, func(http.ResponseWriter, *http.Request) { t.Error("webhook sent") })
	for _, path := range []string{"/chat/completions", "/responses", "/messages", "/systemone"} {
		code, _, _ := requestTest(t, api, "POST", "/baseten/v1"+path, "caller", basetenBody(path, false), "")
		if code != 400 {
			t.Errorf("%s without webhook returned %d", path, code)
		}
	}
	for _, path := range []string{"/v1/messages", "/v1/systemone", "/api/v1/messages", "/api/v1/systemone"} {
		code, _, _ := requestTest(t, api, "POST", path, "caller", `{"model":"test/model","messages":[],"webhook_url":"https://example.com/callback"}`, "")
		if code != 404 {
			t.Errorf("%s returned %d; want 404", path, code)
		}
	}
	code, _, got := requestTest(t, api, "POST", "/baseten/v1/completions", "caller", `{"model":"test/model","webhook_url":"https://example.com/callback"}`, "")
	if code != 404 || !strings.Contains(string(got["error"]), "Messages") {
		t.Errorf("unsupported baseten POST returned %d %s", code, got["error"])
	}
	req, _ := http.NewRequest("POST", api.URL+"/baseten/v1/messages", strings.NewReader(basetenBody("/messages", true)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", "baseten-key")
	res, err := api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Errorf("x-api-key without bearer returned %d", res.StatusCode)
	}
	if upstreamCalls.Load() != 0 {
		t.Fatal("rejected request reached upstream")
	}
}

func TestBasetenTextJobs(t *testing.T) {
	c := testConfig(t)
	type seen struct {
		Host, Path, Auth, Body string
	}
	var calls atomic.Int32
	upstreams := make(chan seen, 8)
	events := make(chan event, 8)
	s, api := basetenFixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		upstreams <- seen{r.Host, r.URL.Path, r.Header.Get("Authorization"), string(body)}
		switch r.URL.Path {
		case "/responses":
			_, _ = io.WriteString(w, `{"id":"up-resp","object":"response","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}],"status":"completed"}`)
		case "/messages":
			_, _ = io.WriteString(w, `{"id":"up-msg","type":"message","role":"assistant","model":"test/model","content":[{"type":"text","text":"hello"}],"stop_reason":"max_tokens","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`)
		case "/systemone":
			_, _ = io.WriteString(w, `{"id":"up-sys","answers":{"route":"billing"}}`)
		default:
			_, _ = io.WriteString(w, `{"id":"up-chat","object":"chat.completion","choices":[{"message":{"content":"hello"}}]}`)
		}
	}, func(w http.ResponseWriter, r *http.Request) {
		var e event
		_ = json.NewDecoder(r.Body).Decode(&e)
		events <- e
		w.WriteHeader(204)
	})
	startTest(t, s)
	for _, tc := range []struct {
		path, prefix, event string
	}{
		{"/chat/completions", "chatcmpl_proxy_", "chat.completion.completed"},
		{"/responses", "resp_proxy_", "response.completed"},
		{"/messages", "msg_proxy_", "message.completed"},
		{"/systemone", "sys_proxy_", "systemone.completed"},
	} {
		status, headers, got := requestTest(t, api, "POST", "/baseten/v1"+tc.path, "caller-one", basetenBody(tc.path, true), "")
		if status != 200 {
			t.Fatalf("%s submit %d %s", tc.path, status, got["error"])
		}
		var id string
		_ = json.Unmarshal(got["id"], &id)
		if !strings.HasPrefix(id, tc.prefix) || string(got["status"]) != `"queued"` {
			t.Fatalf("%s queued %s %s", tc.path, id, got["status"])
		}
		if loc := headers.Get("Location"); loc != "/baseten/v1"+tc.path+"/"+id {
			t.Fatalf("%s location %q", tc.path, loc)
		}
		if tc.path == "/messages" {
			if string(got["type"]) != `"message"` || string(got["object"]) == `"chat.completion"` || string(got["content"]) != "[]" {
				t.Fatalf("messages queued body %v", got)
			}
		}
		if tc.path == "/systemone" {
			if string(got["object"]) != `"systemone"` || string(got["answers"]) != "{}" {
				t.Fatalf("systemone queued body %v", got)
			}
		}
		finished := waitBaseten(t, api, "caller-one", tc.path, id, "delivered")
		if string(finished["id"]) != `"`+id+`"` || string(finished["status"]) != `"completed"` {
			t.Fatalf("%s result %v", tc.path, finished)
		}
		up := <-upstreams
		if up.Path != tc.path || up.Auth != "Bearer caller-one" {
			t.Fatalf("%s upstream %q %q", tc.path, up.Path, up.Auth)
		}
		if strings.Contains(up.Body, "webhook_url") || strings.Contains(up.Body, "webhook_secret") || strings.Contains(up.Body, `"background"`) {
			t.Fatalf("%s leaked proxy fields: %s", tc.path, up.Body)
		}
		e := <-events
		if e.Type != tc.event {
			t.Fatalf("%s event %q", tc.path, e.Type)
		}
		var data map[string]any
		if json.Unmarshal(e.Data, &data) != nil || data["id"] != id {
			t.Fatalf("%s webhook data %s", tc.path, e.Data)
		}
		if tc.path == "/messages" {
			content, _ := data["content"].([]any)
			if data["type"] != "message" || len(content) == 0 {
				t.Fatalf("messages webhook %s", e.Data)
			}
		}
		if tc.path == "/systemone" {
			answers, _ := data["answers"].(map[string]any)
			if data["object"] != "systemone" || answers["route"] != "billing" {
				t.Fatalf("systemone webhook %s", e.Data)
			}
		}
		code, _, _ := requestTest(t, api, "GET", "/baseten/v1"+tc.path+"/"+id, "caller-one", "", "")
		if code != 200 {
			t.Fatalf("retrieve %s %d", tc.path, code)
		}
		if code, _, _ = requestTest(t, api, "GET", "/baseten/v1"+tc.path+"/"+id, "other-token", "", ""); code != 404 {
			t.Fatalf("other token %s %d", tc.path, code)
		}
		if code, _, _ = requestTest(t, api, "GET", "/api/v1"+tc.path+"/"+id, "caller-one", "", ""); code != 404 {
			t.Fatalf("openrouter prefix returned baseten job %s %d", tc.path, code)
		}
		if code, _, _ = requestTest(t, api, "GET", "/v1"+tc.path+"/"+id, "caller-one", "", ""); code != 404 {
			t.Fatalf("/v1 prefix returned baseten job %s %d", tc.path, code)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("upstream calls %d", calls.Load())
	}
}

func TestBasetenUpstream401IsOneFailedWebhook(t *testing.T) {
	c := testConfig(t)
	var models atomic.Int32
	events := make(chan event, 1)
	s, api := basetenFixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		models.Add(1)
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":{"message":"invalid key"}}`)
	}, func(w http.ResponseWriter, r *http.Request) {
		var e event
		_ = json.NewDecoder(r.Body).Decode(&e)
		events <- e
		w.WriteHeader(204)
	})
	startTest(t, s)
	status, _, got := requestTest(t, api, "POST", "/baseten/v1/messages", "caller", basetenBody("/messages", true), "")
	if status != 200 {
		t.Fatal(status)
	}
	var id string
	_ = json.Unmarshal(got["id"], &id)
	result := waitBaseten(t, api, "caller", "/messages", id, "delivered")
	if string(result["status"]) != `"failed"` {
		t.Fatalf("status %s", result["status"])
	}
	e := <-events
	if e.Type != "message.failed" {
		t.Fatalf("event %s", e.Type)
	}
	time.Sleep(50 * time.Millisecond)
	if models.Load() != 1 {
		t.Fatal("model was called again after 401")
	}
}

func TestBasetenModelsPassThrough(t *testing.T) {
	c := testConfig(t)
	var methods []string
	_, api := basetenFixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.Method != "GET" || r.URL.Path != "/models" {
			t.Errorf("unexpected upstream %s %s", r.Method, r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"test/model"}]}`)
	}, func(http.ResponseWriter, *http.Request) { t.Error("models requested a webhook") })
	req, _ := http.NewRequest("GET", api.URL+"/baseten/v1/models", nil)
	req.Header.Set("Authorization", "Bearer caller")
	res, err := api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !strings.Contains(string(body), "test/model") {
		t.Fatalf("models %d %s", res.StatusCode, body)
	}
	if len(methods) != 1 || methods[0] != "GET /models" {
		t.Fatalf("upstream %v", methods)
	}
}

func TestBasetenIdempotencyIncludesProvider(t *testing.T) {
	c := testConfig(t)
	_, api := basetenFixture(t, c, func(http.ResponseWriter, *http.Request) {}, func(http.ResponseWriter, *http.Request) {})
	body := basetenBody("/chat/completions", true)
	status, _, got := requestTest(t, api, "POST", "/baseten/v1/chat/completions", "caller", body, "same-key")
	if status != 200 {
		t.Fatal(status)
	}
	var id string
	_ = json.Unmarshal(got["id"], &id)
	status, _, again := requestTest(t, api, "POST", "/baseten/v1/chat/completions", "caller", body, "same-key")
	var againID string
	_ = json.Unmarshal(again["id"], &againID)
	if status != 200 || againID != id {
		t.Fatalf("same request %d %s", status, againID)
	}
	if status, _, _ = requestTest(t, api, "POST", "/baseten/v1/responses", "caller", basetenBody("/responses", true), "same-key"); status != 409 {
		t.Fatalf("different path %d", status)
	}
	if status, _, _ = requestTest(t, api, "POST", "/baseten/v1/messages", "caller", basetenBody("/messages", true), "same-key"); status != 409 {
		t.Fatalf("different baseten path %d", status)
	}
	if status, _, _ = requestTest(t, api, "POST", "/api/v1/chat/completions", "caller", body, "same-key"); status != 409 {
		t.Fatalf("different provider %d", status)
	}
}

func TestBasetenMessageErrorTypeFails(t *testing.T) {
	c := testConfig(t)
	s, api := basetenFixture(t, c, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"type":"error","error":{"message":"bad request"}}`)
	}, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	startTest(t, s)
	status, _, got := requestTest(t, api, "POST", "/baseten/v1/messages", "caller", basetenBody("/messages", true), "")
	if status != 200 {
		t.Fatal(status)
	}
	var id string
	_ = json.Unmarshal(got["id"], &id)
	result := waitBaseten(t, api, "caller", "/messages", id, "delivered")
	if string(result["status"]) != `"failed"` {
		t.Fatalf("type error status %s", result["status"])
	}
}
