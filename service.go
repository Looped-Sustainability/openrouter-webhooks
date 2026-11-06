package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxRequestBytes = 1 << 20
const maxResponseBytes = 16 << 20

type service struct {
	c                          config
	store                      *store
	modelClient, webhookClient *http.Client
	upstream                   string
	wg                         sync.WaitGroup
	failed                     atomic.Bool
}

func newService(c config) (*service, error) {
	st, err := openStore(c.DataDir)
	if err != nil {
		return nil, err
	}
	return &service{c: c, store: st, modelClient: newClient(c.ModelTimeout, false), webhookClient: newClient(c.WebhookTimeout, true), upstream: "https://openrouter.ai/api/v1/chat/completions"}, nil
}
func (s *service) close() {
	s.modelClient.CloseIdleConnections()
	s.webhookClient.CloseIdleConnections()
	s.store.close()
}
func (s *service) persist(j *job) bool {
	j.UpdatedAt = time.Now().UTC()
	if err := s.store.save(j); err != nil {
		s.failed.Store(true)
		slog.Error("job storage failed; admission and scheduling stopped", "job_id", j.ID, "error", err)
		return false
	}
	snapshot := *j
	s.store.jobs[j.ID] = &snapshot
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func apiError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func (s *service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if s.failed.Load() {
			apiError(w, 503, "storage failure")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/jobs", s.submit)
	mux.HandleFunc("GET /v1/jobs/{id}", s.status)
	mux.HandleFunc("POST /v1/jobs/{id}/redeliver", s.redeliver)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && !authorized(r, s.c.Token) {
			apiError(w, 401, "unauthorized")
			return
		}
		if s.failed.Load() && r.Method != "GET" {
			apiError(w, 503, "storage failure")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type submission struct {
	WebhookURL string          `json:"webhook_url"`
	Request    json.RawMessage `json:"request"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

func (s *service) submit(w http.ResponseWriter, r *http.Request) {
	var in submission
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		apiError(w, 400, "invalid JSON or request exceeds 1 MiB")
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		apiError(w, 400, "expected one JSON object")
		return
	}
	if err := validateWebhook(in.WebhookURL, s.c.WebhookHosts); err != nil {
		apiError(w, 400, err.Error())
		return
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(in.Request, &body) != nil || body == nil {
		apiError(w, 400, "request must be an OpenRouter chat completion object")
		return
	}
	if stream, ok := body["stream"]; ok && !bytes.Equal(bytes.TrimSpace(stream), []byte("false")) {
		apiError(w, 400, "stream must be false or omitted")
		return
	}
	if _, ok := body["messages"]; !ok {
		apiError(w, 400, "request.messages is required")
		return
	}
	var model string
	var models []string
	_ = json.Unmarshal(body["model"], &model)
	_ = json.Unmarshal(body["models"], &models)
	if model == "" && len(models) == 0 {
		apiError(w, 400, "request.model or request.models is required")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 256 {
		apiError(w, 400, "Idempotency-Key exceeds 256 bytes")
		return
	}
	encoded, _ := json.Marshal(in)
	sum := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(sum[:])
	st := s.store
	st.mu.Lock()
	defer st.mu.Unlock()
	if key != "" {
		for _, j := range st.jobs {
			if j.IdempotencyKey == key {
				if j.Fingerprint != fingerprint {
					apiError(w, 409, "Idempotency-Key was already used with a different request")
					return
				}
				accepted(w, j)
				return
			}
		}
	}
	if len(st.jobs) >= s.c.MaxJobs {
		w.Header().Set("Retry-After", "60")
		apiError(w, 503, "job store is full; retry after completed jobs expire")
		return
	}
	now := time.Now().UTC()
	j := &job{ID: newID(), State: "queued", CreatedAt: now, UpdatedAt: now, WebhookURL: in.WebhookURL, Request: in.Request, Metadata: in.Metadata, IdempotencyKey: key, Fingerprint: fingerprint}
	if !s.persist(j) {
		apiError(w, 503, "could not persist job")
		return
	}
	accepted(w, j)
}
func accepted(w http.ResponseWriter, j *job) {
	w.Header().Set("Location", "/v1/jobs/"+j.ID)
	writeJSON(w, 202, map[string]string{"id": j.ID, "state": j.State, "status_url": "/v1/jobs/" + j.ID})
}
func (s *service) status(w http.ResponseWriter, r *http.Request) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	j := s.store.jobs[r.PathValue("id")]
	if j == nil {
		apiError(w, 404, "job not found")
		return
	}
	writeJSON(w, 200, struct {
		ID            string    `json:"id"`
		State         string    `json:"state"`
		CreatedAt     time.Time `json:"created_at"`
		UpdatedAt     time.Time `json:"updated_at"`
		Attempts      int       `json:"webhook_attempts"`
		DeliveryError string    `json:"delivery_error,omitempty"`
		Event         *event    `json:"event,omitempty"`
	}{j.ID, j.State, j.CreatedAt, j.UpdatedAt, j.Attempts, j.DeliveryError, j.Event})
}
func (s *service) redeliver(w http.ResponseWriter, r *http.Request) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	j := s.store.jobs[r.PathValue("id")]
	if j == nil {
		apiError(w, 404, "job not found")
		return
	}
	if j.State != "dead" {
		apiError(w, 409, "only jobs with exhausted webhook attempts can be redelivered")
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
	accepted(w, &next)
}
func (s *service) start(ctx context.Context) {
	models := make(chan *job)
	deliveries := make(chan *job)
	for i := 0; i < s.c.Workers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for j := range models {
				s.complete(ctx, j)
				s.release(j.ID)
			}
		}()
	}
	for i := 0; i < s.c.DeliveryWorkers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for j := range deliveries {
				s.deliver(ctx, j)
				s.release(j.ID)
			}
		}()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(models)
		defer close(deliveries)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.schedule(ctx, now, models, deliveries)
			}
		}
	}()
}
func (s *service) release(id string) {
	s.store.mu.Lock()
	delete(s.store.busy, id)
	s.store.mu.Unlock()
}
func (s *service) schedule(ctx context.Context, now time.Time, models, deliveries chan *job) {
	st := s.store
	st.mu.Lock()
	defer st.mu.Unlock()
	if s.failed.Load() || ctx.Err() != nil {
		return
	}
	for id, j := range st.jobs {
		if st.busy[id] {
			continue
		}
		if (j.State == "delivered" || j.State == "dead") && now.Sub(j.UpdatedAt) > s.c.Retention {
			if err := s.remove(id); err != nil {
				s.failed.Store(true)
				slog.Error("job cleanup failed", "job_id", id)
				return
			}
			delete(st.jobs, id)
			continue
		}
		next := *j
		switch j.State {
		case "queued":
			// The unbuffered handoff reserves a worker. busy protects the job until completion.
			st.busy[id] = true
			select {
			case models <- &next:
			default:
				delete(st.busy, id)
			}
		case "delivering":
			if now.Before(j.NextAttempt) {
				continue
			}
			st.busy[id] = true
			select {
			case deliveries <- &next:
			default:
				delete(st.busy, id)
			}
		}
	}
}
func (s *service) complete(ctx context.Context, j *job) {
	s.store.mu.Lock()
	j.State = "running"
	ok := s.persist(j)
	s.store.mu.Unlock()
	if !ok {
		return
	}
	e := &event{ID: newID(), JobID: j.ID, CreatedAt: time.Now().UTC(), Metadata: j.Metadata, Type: "job.failed"}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.upstream, bytes.NewReader(j.Request))
	if err == nil {
		req.Header.Set("Authorization", "Bearer "+s.c.OpenRouterKey)
		req.Header.Set("Content-Type", "application/json")
		var res *http.Response
		res, err = s.modelClient.Do(req)
		if err == nil {
			e.StatusCode = res.StatusCode
			var data []byte
			data, err = io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
			res.Body.Close()
			if err == nil && len(data) > maxResponseBytes {
				err = errors.New("upstream response exceeds 16 MiB")
			}
			if err == nil {
				if !json.Valid(data) {
					err = errors.New("upstream returned invalid JSON")
				} else {
					e.Response = data
					var result struct {
						Error   json.RawMessage `json:"error"`
						Choices []struct {
							FinishReason string `json:"finish_reason"`
						} `json:"choices"`
					}
					if bytes.Equal(bytes.TrimSpace(data), []byte("null")) || json.Unmarshal(data, &result) != nil {
						err = errors.New("upstream response must be a JSON object")
					} else {
						embedded := len(result.Error) > 0 && string(result.Error) != "null"
						for _, choice := range result.Choices {
							if choice.FinishReason == "error" {
								embedded = true
							}
						}
						if res.StatusCode >= 200 && res.StatusCode < 300 && !embedded {
							e.Type = "job.succeeded"
						} else {
							e.Error = "OpenRouter returned an error"
						}
					}
				}
			}
		}
	}
	if err != nil {
		e.Error = "OpenRouter request failed"
		if ctx.Err() != nil {
			e.Type = "job.interrupted"
			e.Error = "Service stopped during the model call; upstream outcome is unknown"
		} else if errors.Is(err, context.DeadlineExceeded) {
			e.Type = "job.timed_out"
			e.Error = "OpenRouter request timed out; upstream outcome is unknown"
		}
	}
	e.CreatedAt = time.Now().UTC()
	j.State = "delivering"
	j.Event = e
	j.Request = nil
	s.store.mu.Lock()
	s.persist(j)
	s.store.mu.Unlock()
}
func (s *service) deliver(ctx context.Context, j *job) {
	if ctx.Err() != nil {
		return
	}
	if j.Attempts >= s.c.MaxDeliveryAttempts {
		j.State = "dead"
		s.store.mu.Lock()
		s.persist(j)
		s.store.mu.Unlock()
		return
	}
	j.Attempts++
	s.store.mu.Lock()
	ok := s.persist(j)
	s.store.mu.Unlock()
	if !ok {
		return
	}
	data, _ := json.Marshal(j.Event)
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(s.c.WebhookSecret))
	mac.Write([]byte(stamp + "."))
	mac.Write(data)
	req, err := http.NewRequestWithContext(ctx, "POST", j.WebhookURL, bytes.NewReader(data))
	if err == nil {
		err = validateWebhook(j.WebhookURL, s.c.WebhookHosts)
	}
	status := 0
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Webhook-Id", j.Event.ID)
		req.Header.Set("Webhook-Timestamp", stamp)
		req.Header.Set("Webhook-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		req.Header.Set("User-Agent", "openrouter-webhooks/1")
		var res *http.Response
		res, err = s.webhookClient.Do(req)
		if err == nil {
			status = res.StatusCode
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
		}
	}
	j.DeliveryError = ""
	if err == nil && status >= 200 && status < 300 {
		j.State = "delivered"
	} else {
		j.DeliveryError = "Webhook request failed"
		if status != 0 {
			j.DeliveryError = fmt.Sprintf("Webhook returned HTTP %d", status)
		}
		if j.Attempts >= s.c.MaxDeliveryAttempts {
			j.State = "dead"
		} else {
			delay := time.Second * time.Duration(1<<min(j.Attempts, 10))
			j.NextAttempt = time.Now().Add(delay + time.Duration(time.Now().UnixNano()%int64(delay/4)))
		}
	}
	s.store.mu.Lock()
	s.persist(j)
	s.store.mu.Unlock()
}
