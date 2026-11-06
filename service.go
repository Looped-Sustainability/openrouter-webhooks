package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

const maxRequestBytes = 1 << 20
const maxResponseBytes = 16 << 20

type service struct {
	c                          config
	store                      *store
	modelClient, webhookClient *http.Client
	upstream                   string
	slots                      chan struct{}
	wg                         sync.WaitGroup
	failed                     atomic.Bool
}

func newService(c config) (*service, error) {
	if len(c.EncryptionKey) != 32 {
		return nil, errors.New("invalid data encryption key")
	}
	st, err := openStore(c.DataDir)
	if err != nil {
		return nil, err
	}
	for _, j := range st.jobs {
		if j.State == "queued" {
			if _, err := openCredential(c.EncryptionKey, j.ID, j.Credential); err != nil {
				st.close()
				return nil, errors.New("could not decrypt queued credentials; restore the original DATA_ENCRYPTION_KEY")
			}
		}
	}
	return &service{c: c, store: st, modelClient: newClient(c.ModelTimeout, false), webhookClient: newClient(c.WebhookTimeout, true), upstream: "https://openrouter.ai/api/v1", slots: make(chan struct{}, c.Workers)}, nil
}
func (s *service) close() {
	s.modelClient.CloseIdleConnections()
	s.webhookClient.CloseIdleConnections()
	s.store.close()
}
func (s *service) persist(j *job) bool {
	j.UpdatedAt = nowUTC()
	if err := s.store.save(j); err != nil {
		s.failed.Store(true)
		slog.Error("job storage failed; admission and scheduling stopped", "job_id", j.ID, "error", err)
		return false
	}
	copy := *j
	s.store.jobs[j.ID] = &copy
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func apiError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": message, "type": "proxy_error", "code": status}})
}
func (s *service) handler() http.Handler {
	return accessLog(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if s.failed.Load() {
				apiError(w, 503, "storage failure")
			} else {
				writeJSON(w, 200, map[string]string{"status": "ok", "revision": buildRevision})
			}
			return
		}
		token := bearer(r)
		if token == "" {
			apiError(w, 401, "a bearer token is required")
			return
		}
		path := r.URL.Path
		if strings.HasPrefix(path, "/api/v1/") {
			path = strings.TrimPrefix(path, "/api/v1")
		} else if strings.HasPrefix(path, "/v1/") {
			path = strings.TrimPrefix(path, "/v1")
		} else {
			apiError(w, 404, "use the /api/v1 or /v1 base URL")
			return
		}
		for _, base := range []string{"/responses/", "/chat/completions/"} {
			if strings.HasPrefix(path, base) {
				id := strings.TrimPrefix(path, base)
				if strings.HasPrefix(id, "resp_proxy_") || strings.HasPrefix(id, "chatcmpl_proxy_") {
					s.localJob(w, r, token, strings.TrimSuffix(base, "/"), id)
					return
				}
			}
		}
		if r.Method == "POST" && (path == "/chat/completions" || path == "/responses") {
			raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
			if err != nil {
				apiError(w, 413, "request exceeds 1 MiB")
				return
			}
			var body map[string]json.RawMessage
			if err = json.Unmarshal(raw, &body); err != nil || body == nil {
				apiError(w, 400, "expected a JSON object")
				return
			}
			s.submit(w, r, token, path, body)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			apiError(w, 404, "only chat completions and Responses support text submission through this proxy")
			return
		}
		s.forward(w, r, path)
	}))
}
func stripProxyHeaders(h http.Header) {
	for key := range h {
		lower := strings.ToLower(key)
		if lower == "x-background" || strings.HasPrefix(lower, "x-webhook-") {
			h.Del(key)
		}
	}
}
func (s *service) forward(w http.ResponseWriter, r *http.Request, path string) {
	ctx, cancel := context.WithTimeout(r.Context(), s.c.ModelTimeout)
	defer cancel()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		apiError(w, 504, "upstream request timed out waiting for capacity")
		return
	}
	target, err := url.Parse(s.upstream)
	if err != nil {
		apiError(w, 502, "upstream configuration error")
		return
	}
	proxy := &httputil.ReverseProxy{Transport: s.modelClient.Transport, FlushInterval: -1, Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(target)
		pr.Out.URL.Path = strings.TrimSuffix(target.Path, "/") + path
		pr.Out.URL.RawPath = ""
		pr.Out.Host = target.Host
		stripProxyHeaders(pr.Out.Header)
	}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		if ctx.Err() != nil {
			apiError(w, 504, "upstream request timed out")
		} else {
			apiError(w, 502, "upstream request failed")
		}
	}}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}
