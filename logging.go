package main

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type responseStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseStatusWriter) WriteHeader(status int) {
	if status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseStatusWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}
func (w *responseStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := "req_" + newID()
		w.Header().Set("X-Request-Id", requestID)
		response := &responseStatusWriter{ResponseWriter: w}
		defer func() {
			status := response.status
			if status == 0 {
				status = http.StatusOK
			}
			level := slog.LevelInfo
			if status >= 500 {
				level = slog.LevelError
			} else if status >= 400 {
				level = slog.LevelWarn
			}
			slog.Log(r.Context(), level, "http request", "request_id", requestID, "method", r.Method, "route", logRoute(r.URL.Path), "status", status, "duration_ms", time.Since(started).Milliseconds())
		}()
		next.ServeHTTP(response, r)
	})
}

// Log route labels, never arbitrary paths or query strings supplied by callers.
func logRoute(path string) string {
	if path == "/healthz" || path == "/admin/jobs" {
		return path
	}
	if strings.HasPrefix(path, "/admin/jobs/") {
		return "/admin/jobs/{id}"
	}
	if strings.HasPrefix(path, "/api/v1/") {
		path = strings.TrimPrefix(path, "/api/v1")
	} else if strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	} else {
		return "unknown"
	}
	for _, base := range []string{"/chat/completions", "/responses"} {
		if path == base {
			return base
		}
		if strings.HasPrefix(path, base+"/") {
			if strings.HasSuffix(path, "/redeliver") {
				return base + "/{id}/redeliver"
			}
			return base + "/{id}"
		}
	}
	return "upstream"
}
