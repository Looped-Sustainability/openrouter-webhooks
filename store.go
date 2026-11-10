package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type event struct {
	ID         string          `json:"id"`
	Object     string          `json:"object"`
	Type       string          `json:"type"`
	CreatedAt  int64           `json:"created_at"`
	Data       json.RawMessage `json:"data"`
	StatusCode int             `json:"status_code,omitempty"`
}
type job struct {
	Version        int                 `json:"version"`
	ID             string              `json:"id"`
	Owner          string              `json:"owner"`
	Path           string              `json:"path"`
	Model          string              `json:"model"`
	State          string              `json:"state"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
	WebhookURL     string              `json:"webhook_url,omitempty"`
	WebhookSecret  string              `json:"webhook_secret,omitempty"`
	Credential     string              `json:"credential,omitempty"`
	Headers        map[string][]string `json:"headers,omitempty"`
	Request        json.RawMessage     `json:"request,omitempty"`
	Result         json.RawMessage     `json:"result,omitempty"`
	IdempotencyKey string              `json:"idempotency_key,omitempty"`
	Fingerprint    string              `json:"fingerprint,omitempty"`
	Event          *event              `json:"event,omitempty"`
	Attempts       int                 `json:"webhook_attempts"`
	NextAttempt    time.Time           `json:"next_attempt,omitzero"`
	DeliveryError  string              `json:"delivery_error,omitempty"`
}
type store struct {
	mu   sync.Mutex
	dir  string
	lock *os.File
	jobs map[string]*job
	busy map[string]bool
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func openStore(dir string) (*store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("data directory is already in use")
	}
	s := &store{dir: dir, lock: lock, jobs: map[string]*job{}, busy: map[string]bool{}}
	entries, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		s.close()
		return nil, err
	}
	for _, path := range entries {
		data, err := os.ReadFile(path)
		if err != nil {
			s.close()
			return nil, err
		}
		var j job
		if err = json.Unmarshal(data, &j); err != nil || j.Version != 2 || j.ID == "" || filepath.Base(path) != j.ID+".json" {
			s.close()
			return nil, fmt.Errorf("invalid or unsupported job file %s", path)
		}
		switch j.State {
		case "running":
			j.finish(nil, 0, "incomplete", "Process stopped during the model call. The upstream outcome is unknown.")
			if err = s.save(&j); err != nil {
				s.close()
				return nil, err
			}
		case "queued", "delivering", "delivered", "dead":
		default:
			s.close()
			return nil, fmt.Errorf("unknown state in %s", path)
		}
		if j.State == "delivering" && j.Event == nil {
			s.close()
			return nil, fmt.Errorf("missing event in %s", path)
		}
		s.jobs[j.ID] = &j
	}
	return s, nil
}
func (s *store) close() { _ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN); _ = s.lock.Close() }

// save must run under mu after startup. Publish memory only after the durable write.
func (s *store) save(j *job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".job-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.dir, j.ID+".json")); err != nil {
		return err
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *service) remove(id string) error {
	if err := os.Remove(filepath.Join(s.store.dir, id+".json")); err != nil {
		return err
	}
	d, err := os.Open(s.store.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
