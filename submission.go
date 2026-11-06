package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func nowUTC() time.Time { return time.Now().UTC() }
func (s *service) submit(w http.ResponseWriter, r *http.Request, token, path string, body map[string]json.RawMessage) {
	if s.failed.Load() {
		apiError(w, 503, "storage failure")
		return
	}
	if value, ok := body["background"]; ok {
		var background bool
		if json.Unmarshal(value, &background) != nil || !background {
			apiError(w, 400, "text requests always run in the background; omit background or set it to true")
			return
		}
	}
	if value := r.Header.Get("X-Background"); value != "" && value != "true" {
		apiError(w, 400, "text requests always run in the background; omit X-Background or set it to true")
		return
	}
	var stream bool
	if v, ok := body["stream"]; ok && json.Unmarshal(v, &stream) != nil {
		apiError(w, 400, "stream must be a boolean")
		return
	}
	if stream {
		apiError(w, 400, "background streaming is not supported; omit stream or set it to false")
		return
	}
	webhook, secret := "", ""
	for key, dst := range map[string]*string{"webhook_url": &webhook, "webhook_secret": &secret} {
		if v, ok := body[key]; ok && json.Unmarshal(v, dst) != nil {
			apiError(w, 400, key+" must be a string")
			return
		}
	}
	if v := r.Header.Get("X-Webhook-URL"); v != "" {
		webhook = v
	}
	if v := r.Header.Get("X-Webhook-Secret"); v != "" {
		secret = v
	}
	if webhook == "" {
		apiError(w, 400, "webhook_url is required for every text request")
		return
	}
	if err := validateWebhook(webhook, s.c.WebhookHosts); err != nil {
		apiError(w, 400, err.Error())
		return
	}
	if secret != "" {
		if _, err := signingKey(secret); err != nil {
			apiError(w, 400, err.Error())
			return
		}
	}

	key := r.Header.Get("Idempotency-Key")
	if len(key) > 256 {
		apiError(w, 400, "Idempotency-Key exceeds 256 bytes")
		return
	}
	var model string
	_ = json.Unmarshal(body["model"], &model)
	delete(body, "background")
	delete(body, "webhook_url")
	delete(body, "webhook_secret")
	raw, _ := json.Marshal(body)
	headers := r.Header.Clone()
	stripProxyHeaders(headers)
	headers.Del("Authorization")
	headers.Del("Proxy-Authorization")
	headers.Del("Content-Length")
	headers.Del("Host")
	headers.Del("Cookie")
	headers.Del("Connection")
	headers.Del("Accept-Encoding")
	encoded, _ := json.Marshal(struct {
		Path, Webhook, Secret string
		Body                  json.RawMessage
	}{path, webhook, secret, raw})
	sum := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(sum[:])
	owner := ownerID(token)
	st := s.store
	st.mu.Lock()
	defer st.mu.Unlock()
	if key != "" {
		for _, j := range st.jobs {
			if j.Owner == owner && j.IdempotencyKey == key {
				if j.Fingerprint != fingerprint {
					apiError(w, 409, "Idempotency-Key was already used with a different request")
					return
				}
				slog.Info("job submission reused", "request_id", w.Header().Get("X-Request-Id"), "job_id", j.ID)
				accepted(w, j)
				return
			}
		}
	}
	if len(st.jobs) >= s.c.MaxJobs {
		w.Header().Set("Retry-After", "60")
		apiError(w, 503, "job store is full")
		return
	}
	prefix := "resp_proxy_"
	if path == "/chat/completions" {
		prefix = "chatcmpl_proxy_"
	}
	id := prefix + newID()
	credential, err := sealCredential(s.c.EncryptionKey, id, token)
	if err != nil {
		apiError(w, 503, "could not encrypt credential")
		return
	}
	if secret == "" {
		secret = newSigningSecret()
	}
	now := nowUTC()
	j := &job{Version: 2, ID: id, Owner: owner, Path: path, Model: model, State: "queued", CreatedAt: now, UpdatedAt: now, WebhookURL: webhook, WebhookSecret: secret, Credential: credential, Headers: headers, Request: raw, IdempotencyKey: key, Fingerprint: fingerprint}
	if !s.persist(j) {
		apiError(w, 503, "could not persist job")
		return
	}
	slog.Info("job queued", "request_id", w.Header().Get("X-Request-Id"), "job_id", j.ID, "route", j.Path)
	accepted(w, j)
}
func accepted(w http.ResponseWriter, j *job) {
	w.Header().Set("Location", "/api/v1"+j.Path+"/"+j.ID)
	if j.WebhookSecret != "" {
		w.Header().Set("X-Webhook-Secret", j.WebhookSecret)
	}
	writeJSON(w, 200, json.RawMessage(j.response()))
}
func (s *service) localJob(w http.ResponseWriter, r *http.Request, token, path, id string) {
	redeliver := strings.HasSuffix(id, "/redeliver")
	id = strings.TrimSuffix(id, "/redeliver")
	st := s.store
	st.mu.Lock()
	defer st.mu.Unlock()
	j := st.jobs[id]
	if j == nil || j.Owner != ownerID(token) || j.Path != path {
		apiError(w, 404, "response not found")
		return
	}
	if r.Method == "GET" && !redeliver {
		if r.URL.Query().Get("stream") == "true" {
			apiError(w, 400, "background streaming is not supported")
			return
		}
		w.Header().Set("X-Webhook-State", j.State)
		writeJSON(w, 200, json.RawMessage(j.response()))
		return
	}
	if r.Method == "POST" && redeliver {
		if s.failed.Load() {
			apiError(w, 503, "storage failure")
			return
		}
		if j.State != "dead" {
			apiError(w, 409, "only exhausted webhook deliveries can be retried")
			return
		}
		next := *j
		next.State = "delivering"
		next.Attempts = 0
		next.NextAttempt = time.Time{}
		next.DeliveryError = ""
		if !s.persist(&next) {
			apiError(w, 503, "could not persist job")
			return
		}
		slog.Info("webhook redelivery queued", "request_id", w.Header().Get("X-Request-Id"), "job_id", next.ID, "event_id", next.Event.ID)
		accepted(w, &next)
		return
	}
	apiError(w, 400, "this operation is not supported for proxy background jobs")
}

func responseStatus(data []byte, httpStatus int) (string, string) {
	var body struct {
		Error   json.RawMessage `json:"error"`
		Status  string          `json:"status"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &body) != nil || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return "failed", "upstream returned invalid JSON"
	}
	if httpStatus < 200 || httpStatus >= 300 || (len(body.Error) > 0 && !bytes.Equal(bytes.TrimSpace(body.Error), []byte("null"))) {
		return "failed", "OpenRouter returned an error"
	}
	if body.Status == "failed" || body.Status == "incomplete" || body.Status == "cancelled" {
		return body.Status, "OpenRouter did not complete the response"
	}
	if body.Status == "queued" || body.Status == "in_progress" {
		return "incomplete", "upstream returned a pending result when a final result was required"
	}
	for _, choice := range body.Choices {
		if choice.FinishReason == "error" {
			return "failed", "OpenRouter returned a completion error"
		}
	}
	return "completed", ""
}
