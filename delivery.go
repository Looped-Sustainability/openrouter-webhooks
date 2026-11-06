package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

func (s *service) complete(ctx context.Context, j *job) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.store.mu.Lock()
	j.State = "running"
	ok := s.persist(j)
	s.store.mu.Unlock()
	if !ok {
		return
	}
	var data []byte
	statusCode := 0
	status, message := "failed", "OpenRouter request failed"
	token, err := openCredential(s.c.EncryptionKey, j.ID, j.Credential)
	if err == nil {
		var req *http.Request
		req, err = http.NewRequestWithContext(ctx, "POST", s.upstream+j.Path, bytes.NewReader(j.Request))
		if err == nil {
			req.Header = http.Header(j.Headers).Clone()
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			var res *http.Response
			res, err = s.modelClient.Do(req)
			if err == nil {
				statusCode = res.StatusCode
				data, err = io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
				res.Body.Close()
				if err == nil && len(data) > maxResponseBytes {
					data = nil
					err = errors.New("upstream response exceeds limit")
				}
				if err == nil {
					status, message = responseStatus(data, statusCode)
				}
			}
		}
	}
	if err != nil {
		data = nil
		if ctx.Err() != nil {
			status = "incomplete"
			message = "Process stopped during model call; upstream outcome is unknown"
		} else if errors.Is(err, context.DeadlineExceeded) {
			message = "OpenRouter request timed out; upstream outcome is unknown"
		}
	}
	j.finish(data, statusCode, status, message)
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
	key, err := signingKey(j.WebhookSecret)
	status := 0
	if err == nil {
		err = validateWebhook(j.WebhookURL, s.c.WebhookHosts)
	}
	if err == nil {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(j.Event.ID + "." + stamp + "."))
		mac.Write(data)
		var req *http.Request
		req, err = http.NewRequestWithContext(ctx, "POST", j.WebhookURL, bytes.NewReader(data))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Webhook-Id", j.Event.ID)
			req.Header.Set("Webhook-Timestamp", stamp)
			req.Header.Set("Webhook-Signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
			req.Header.Set("User-Agent", "openrouter-webhooks/2")
			var res *http.Response
			res, err = s.webhookClient.Do(req)
			if err == nil {
				status = res.StatusCode
				_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
				res.Body.Close()
			}
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
