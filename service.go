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

type textAPI struct {
	path, idPrefix, eventPrefix string
	providers                   []string
}

var textAPIs = []textAPI{
	{"/chat/completions", "chatcmpl_proxy_", "chat.completion.", []string{"openrouter", "baseten"}},
	{"/responses", "resp_proxy_", "response.", []string{"openrouter", "baseten"}},
	{"/messages", "msg_proxy_", "message.", []string{"baseten"}},
	{"/systemone", "sys_proxy_", "systemone.", []string{"baseten"}},
}

type service struct {
	c                          config
	store                      *store
	modelClient, webhookClient *http.Client
	upstream, baseten          string
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
func publicRoute(urlPath string) (provider, path string, ok bool) {
	switch {
	case strings.HasPrefix(urlPath, "/baseten/v1/"):
		return "baseten", strings.TrimPrefix(urlPath, "/baseten/v1"), true
	case strings.HasPrefix(urlPath, "/api/v1/"):
		return "openrouter", strings.TrimPrefix(urlPath, "/api/v1"), true
	case strings.HasPrefix(urlPath, "/v1/"):
		return "openrouter", strings.TrimPrefix(urlPath, "/v1"), true
	default:
		return "", "", false
	}
}
func lookupTextAPI(path, provider string) (textAPI, bool) {
	for _, api := range textAPIs {
		if api.path != path {
			continue
		}
		for _, allowed := range api.providers {
			if allowed == provider {
				return api, true
			}
		}
		return textAPI{}, false
	}
	return textAPI{}, false
}
func localJobID(id string) bool {
	for _, api := range textAPIs {
		if strings.HasPrefix(id, api.idPrefix) {
			return true
		}
	}
	return false
}
func eventPrefix(path string) string {
	for _, api := range textAPIs {
		if api.path == path {
			return api.eventPrefix
		}
	}
	return "chat.completion."
}
func jobProvider(j *job) string {
	if j.Provider == "baseten" {
		return "baseten"
	}
	return "openrouter"
}
func apiPrefix(provider string) string {
	if provider == "baseten" {
		return "/baseten/v1"
	}
	return "/api/v1"
}
func textSubmissionError(provider string) string {
	if provider == "baseten" {
		return "only chat completions, Responses, Messages, and System One support text submission through this proxy"
	}
	return "only chat completions and Responses support text submission through this proxy"
}
func (s *service) providerURL(provider string) string {
	if provider == "baseten" {
		if s.baseten != "" {
			return s.baseten
		}
		return "https://inference.baseten.co/v1"
	}
	if s.upstream != "" {
		return s.upstream
	}
	return "https://openrouter.ai/api/v1"
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
		if r.URL.Path == "/admin" || strings.HasPrefix(r.URL.Path, "/admin/") {
			s.admin(w, r)
			return
		}
		token := bearer(r)
		if token == "" {
			apiError(w, 401, "a bearer token is required")
			return
		}
		provider, path, ok := publicRoute(r.URL.Path)
		if !ok {
			apiError(w, 404, "use the /api/v1, /v1, or /baseten/v1 base URL")
			return
		}
		for _, api := range textAPIs {
			base := api.path + "/"
			if strings.HasPrefix(path, base) {
				id := strings.TrimPrefix(path, base)
				if localJobID(id) {
					s.localJob(w, r, token, provider, api.path, id)
					return
				}
			}
		}
		if r.Method == "POST" {
			if _, ok := lookupTextAPI(path, provider); ok {
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
				s.submit(w, r, token, provider, path, body)
				return
			}
			apiError(w, 404, textSubmissionError(provider))
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			apiError(w, 404, textSubmissionError(provider))
			return
		}
		s.forward(w, r, path, provider)
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
func (s *service) forward(w http.ResponseWriter, r *http.Request, path, provider string) {
	ctx, cancel := context.WithTimeout(r.Context(), s.c.ModelTimeout)
	defer cancel()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		apiError(w, 504, "upstream request timed out waiting for capacity")
		return
	}
	target, err := url.Parse(s.providerURL(provider))
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
