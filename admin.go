package main

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

type adminListItem struct {
	ID           string    `json:"id"`
	State        string    `json:"state"`
	Model        string    `json:"model"`
	Path         string    `json:"path"`
	UpdatedAt    time.Time `json:"updated_at"`
	ResultStatus string    `json:"result_status,omitempty"`
}

func adminTokenOK(got, want string) bool {
	if got == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *service) admin(w http.ResponseWriter, r *http.Request) {
	if s.c.AdminToken == "" {
		apiError(w, 404, "not found")
		return
	}
	if !adminTokenOK(bearer(r), s.c.AdminToken) {
		apiError(w, 401, "invalid admin token")
		return
	}
	if r.Method != http.MethodGet {
		apiError(w, 404, "not found")
		return
	}
	if r.URL.Path == "/admin/jobs" {
		writeJSON(w, 200, map[string]any{"jobs": s.adminList()})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/admin/jobs/")
	if id == r.URL.Path || id == "" || strings.Contains(id, "/") {
		apiError(w, 404, "not found")
		return
	}
	j, ok := s.adminJob(id)
	if !ok {
		apiError(w, 404, "job not found")
		return
	}
	writeJSON(w, 200, j)
}

func (s *service) adminList() []adminListItem {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	items := make([]adminListItem, 0, len(s.store.jobs))
	for _, j := range s.store.jobs {
		items = append(items, adminListItem{ID: j.ID, State: j.State, Model: j.Model, Path: j.Path, UpdatedAt: j.UpdatedAt, ResultStatus: resultStatus(j.Result)})
	}
	sort.Slice(items, func(i, k int) bool {
		if !items[i].UpdatedAt.Equal(items[k].UpdatedAt) {
			return items[i].UpdatedAt.After(items[k].UpdatedAt)
		}
		return items[i].ID < items[k].ID
	})
	return items
}

func (s *service) adminJob(id string) (job, bool) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	j := s.store.jobs[id]
	if j == nil {
		return job{}, false
	}
	return cloneJob(j), true
}

func cloneJob(j *job) job {
	c := *j
	c.Request = append(json.RawMessage(nil), j.Request...)
	c.Result = append(json.RawMessage(nil), j.Result...)
	if j.Event != nil {
		ev := *j.Event
		ev.Data = append(json.RawMessage(nil), j.Event.Data...)
		c.Event = &ev
	}
	if j.Headers != nil {
		h := make(map[string][]string, len(j.Headers))
		for key, values := range j.Headers {
			h[key] = append([]string(nil), values...)
		}
		c.Headers = h
	}
	return c
}

func resultStatus(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var body struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return ""
	}
	return body.Status
}
