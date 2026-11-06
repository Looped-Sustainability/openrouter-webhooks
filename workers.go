package main

import (
	"context"
	"log/slog"
	"time"
)

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
